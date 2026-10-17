package state

import (
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/lzwhehe/loggate/internal/event"
)

// Reason says why an event/destination pair was parked in L1.
type Reason uint8

const (
	ReasonNone        Reason = iota
	ReasonQueueFull          // L2 full
	ReasonBreakerOpen        // destination circuit breaker open
	ReasonNotActive          // destination registered but not Active (draining, unregistered)
	ReasonNoDest             // destination referenced by a route but not configured
	ReasonSendFailed         // a real send attempt failed (Retryable/Throttled)
	ReasonShutdown           // parked from L2 during graceful shutdown
)

func (r Reason) String() string {
	switch r {
	case ReasonNone:
		return "none"
	case ReasonQueueFull:
		return "queue_full"
	case ReasonBreakerOpen:
		return "breaker_open"
	case ReasonNotActive:
		return "not_active"
	case ReasonNoDest:
		return "no_dest"
	case ReasonSendFailed:
		return "send_failed"
	case ReasonShutdown:
		return "shutdown"
	default:
		return fmt.Sprintf("reason(%d)", uint8(r))
	}
}

// Ref locates an event's record in the WAL. L1 stores this pointer, never a
// copy of the event body.
type Ref struct {
	SegID  uint64
	Offset int64
	Length uint32
}

// Slot is the per-destination retry state inside an L1 record.
type Slot struct {
	Dest     event.DestID
	Reason   Reason
	Attempts uint16 // real send attempts so far
	LastErr  uint16 // delivery.Kind of the last failure (0 if none)
	NextAt   int64  // unix nanos: earliest redelivery time (key of due/<dest>)
	FirstAt  int64  // unix nanos: when this destination was first parked (orphan TTL)
}

// Record is one L1 entry (DESIGN §3 L1Record).
//
// Invariants: Pending, Acked and Dead are disjoint subsets of Owned;
// len(Slots) == popcount(Owned) with one slot per owned destination. A
// record is deleted as soon as Pending becomes empty.
type Record struct {
	Seq        uint64
	Ref        Ref
	ReceivedAt int64 // unix nanos, copied from the event (max_age)
	Owned      event.DestMask
	Pending    event.DestMask
	Acked      event.DestMask
	Dead       event.DestMask
	ParkedAt   int64 // unix nanos of the first Park
	Slots      []Slot
}

// Slot returns a pointer to d's slot, or nil.
func (r *Record) Slot(d event.DestID) *Slot {
	for i := range r.Slots {
		if r.Slots[i].Dest == d {
			return &r.Slots[i]
		}
	}
	return nil
}

const (
	recVersion    = 1
	recHeaderSize = 1 + 8 + 8 + 8 + 4 + 8 + 8*4 + 8 + 1
	slotSize      = 1 + 1 + 2 + 2 + 8 + 8
)

var errBadRecord = errors.New("state: malformed l1 record")

func (r *Record) encode() []byte {
	b := make([]byte, 0, recHeaderSize+len(r.Slots)*slotSize)
	b = append(b, recVersion)
	b = binary.BigEndian.AppendUint64(b, r.Seq)
	b = binary.BigEndian.AppendUint64(b, r.Ref.SegID)
	b = binary.BigEndian.AppendUint64(b, uint64(r.Ref.Offset))
	b = binary.BigEndian.AppendUint32(b, r.Ref.Length)
	b = binary.BigEndian.AppendUint64(b, uint64(r.ReceivedAt))
	for _, m := range []event.DestMask{r.Owned, r.Pending, r.Acked, r.Dead} {
		b = binary.BigEndian.AppendUint64(b, uint64(m))
	}
	b = binary.BigEndian.AppendUint64(b, uint64(r.ParkedAt))
	b = append(b, uint8(len(r.Slots)))
	for _, s := range r.Slots {
		b = append(b, uint8(s.Dest), uint8(s.Reason))
		b = binary.BigEndian.AppendUint16(b, s.Attempts)
		b = binary.BigEndian.AppendUint16(b, s.LastErr)
		b = binary.BigEndian.AppendUint64(b, uint64(s.NextAt))
		b = binary.BigEndian.AppendUint64(b, uint64(s.FirstAt))
	}
	return b
}

// decodeRecord copies everything out of b (bbolt memory is only valid
// inside the transaction).
func decodeRecord(b []byte) (*Record, error) {
	if len(b) < recHeaderSize || b[0] != recVersion {
		return nil, errBadRecord
	}
	r := &Record{}
	p := 1
	u64 := func() uint64 { v := binary.BigEndian.Uint64(b[p:]); p += 8; return v }
	r.Seq = u64()
	r.Ref.SegID = u64()
	r.Ref.Offset = int64(u64())
	r.Ref.Length = binary.BigEndian.Uint32(b[p:])
	p += 4
	r.ReceivedAt = int64(u64())
	r.Owned = event.DestMask(u64())
	r.Pending = event.DestMask(u64())
	r.Acked = event.DestMask(u64())
	r.Dead = event.DestMask(u64())
	r.ParkedAt = int64(u64())
	n := int(b[p])
	p++
	if len(b) != p+n*slotSize {
		return nil, errBadRecord
	}
	r.Slots = make([]Slot, n)
	for i := range r.Slots {
		s := &r.Slots[i]
		s.Dest = event.DestID(b[p])
		s.Reason = Reason(b[p+1])
		s.Attempts = binary.BigEndian.Uint16(b[p+2:])
		s.LastErr = binary.BigEndian.Uint16(b[p+4:])
		p += 6
		s.NextAt = int64(u64())
		s.FirstAt = int64(u64())
	}
	return r, nil
}

func seqKey(seq uint64) []byte { return binary.BigEndian.AppendUint64(nil, seq) }

// dueKey is NextAt|Seq, so a cursor walks entries in due order. NextAt is
// clamped at 0 (keys are unsigned).
func dueKey(nextAt int64, seq uint64) []byte {
	b := make([]byte, 0, 16)
	b = binary.BigEndian.AppendUint64(b, uint64(max(nextAt, 0)))
	return binary.BigEndian.AppendUint64(b, seq)
}

func parseDueKey(k []byte) (nextAt int64, seq uint64) {
	return int64(binary.BigEndian.Uint64(k[:8])), binary.BigEndian.Uint64(k[8:16])
}
