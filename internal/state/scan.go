package state

import (
	"fmt"
	"sync"
	"time"

	bolt "go.etcd.io/bbolt"

	"github.com/lzwhehe/loggate/internal/event"
)

// OwnedRange returns Owned masks of the L1 records with from <= seq <= to.
// Recovery uses it to skip (seq, d) pairs that L1 is responsible for.
func (s *Store) OwnedRange(from, to uint64) (map[uint64]event.DestMask, error) {
	out := map[uint64]event.DestMask{}
	err := s.db.View(func(tx *bolt.Tx) error {
		c := tx.Bucket(bucketL1).Cursor()
		for k, v := c.Seek(seqKey(from)); k != nil; k, v = c.Next() {
			r, err := decodeRecord(v)
			if err != nil {
				return fmt.Errorf("state: l1 key %x: %w", k, err)
			}
			if r.Seq > to {
				break
			}
			out[r.Seq] = r.Owned
		}
		return nil
	})
	return out, err
}

// DueItem is one redeliverable (seq, dest) pair.
type DueItem struct {
	Seq        uint64
	Ref        Ref
	ReceivedAt int64
	Attempts   uint16
	NextAt     int64
}

// Due returns up to limit entries of due/<d> with NextAt <= now whose
// record is still pending for d and for which claim(seq) returns true
// (claim lets the caller reserve the pair atomically with selection; it is
// not called for stale entries). Stale index keys (record gone or no longer
// pending for d) are returned for cleanup via DeleteDue. At most scanMax
// keys are examined.
func (s *Store) Due(d event.DestID, now time.Time, limit, scanMax int, claim func(seq uint64) bool) (items []DueItem, stale [][]byte, err error) {
	nowNs := now.UnixNano()
	err = s.db.View(func(tx *bolt.Tx) error {
		db := tx.Bucket(bucketDue).Bucket([]byte{byte(d)})
		if db == nil {
			return nil
		}
		l1 := tx.Bucket(bucketL1)
		c := db.Cursor()
		scanned := 0
		for k, _ := c.First(); k != nil && len(items) < limit && scanned < scanMax; k, _ = c.Next() {
			scanned++
			nextAt, seq := parseDueKey(k)
			if nextAt > nowNs {
				break
			}
			r, err := getRecord(l1, seq)
			if err != nil {
				return err
			}
			var sl *Slot
			if r != nil && r.Pending.Has(d) {
				sl = r.Slot(d)
			}
			if sl == nil || max(sl.NextAt, 0) != nextAt {
				stale = append(stale, append([]byte(nil), k...))
				continue
			}
			if !claim(seq) {
				continue
			}
			items = append(items, DueItem{Seq: seq, Ref: r.Ref, ReceivedAt: r.ReceivedAt, Attempts: sl.Attempts, NextAt: sl.NextAt})
		}
		return nil
	})
	return items, stale, err
}

// SweepItem is a (seq, dest) pair the sweeper must move to the DLQ.
type SweepItem struct {
	Seq        uint64
	Dest       event.DestID
	Ref        Ref
	ReceivedAt int64
	Attempts   uint16
	Reason     Reason
	LastErr    uint16
	Why        string // "orphan_ttl_expired" or "max_age"
}

// SweepPolicy decides which pending slots have expired.
type SweepPolicy struct {
	Now       time.Time
	OrphanTTL time.Duration // for destinations that are not Active
	MaxAge    time.Duration // for every destination, measured from ReceivedAt
	// Active reports whether a destination is currently Active.
	Active func(event.DestID) bool
	// Claim reserves (seq, dest); pairs that cannot be claimed (in flight
	// elsewhere) are skipped this round.
	Claim func(seq uint64, d event.DestID) bool
	Limit int
}

// SweepCandidates scans L1 for expired pending slots (DESIGN §0.5 item 3):
// slots of non-Active destinations parked longer than OrphanTTL, and any
// slot whose event is older than MaxAge. It is independent of destination
// state and of the redeliver loops.
func (s *Store) SweepCandidates(p SweepPolicy) ([]SweepItem, error) {
	now := p.Now.UnixNano()
	var out []SweepItem
	err := s.db.View(func(tx *bolt.Tx) error {
		c := tx.Bucket(bucketL1).Cursor()
		for k, v := c.First(); k != nil && len(out) < p.Limit; k, v = c.Next() {
			r, err := decodeRecord(v)
			if err != nil {
				return fmt.Errorf("state: l1 key %x: %w", k, err)
			}
			for _, d := range r.Pending.Bits() {
				sl := r.Slot(d)
				if sl == nil {
					continue
				}
				why := ""
				switch {
				case p.MaxAge > 0 && r.ReceivedAt > 0 && now-r.ReceivedAt >= int64(p.MaxAge):
					why = "max_age"
				case p.OrphanTTL > 0 && !p.Active(d) && now-sl.FirstAt >= int64(p.OrphanTTL):
					why = "orphan_ttl_expired"
				}
				if why == "" || !p.Claim(r.Seq, d) {
					continue
				}
				out = append(out, SweepItem{Seq: r.Seq, Dest: d, Ref: r.Ref, ReceivedAt: r.ReceivedAt,
					Attempts: sl.Attempts, Reason: sl.Reason, LastErr: sl.LastErr, Why: why})
			}
		}
		return nil
	})
	return out, err
}

// Claims serialises work on individual (seq, dest) L1 pairs between the
// redeliver loops, the workers and the sweeper: whoever holds the claim is
// the only one allowed to Ack/Fail/Dead the pair.
type Claims struct {
	mu sync.Mutex
	m  map[claimKey]struct{}
}

type claimKey struct {
	seq  uint64
	dest event.DestID
}

// NewClaims returns an empty claim set.
func NewClaims() *Claims { return &Claims{m: map[claimKey]struct{}{}} }

// TryClaim claims (seq, d); false if already claimed.
func (c *Claims) TryClaim(seq uint64, d event.DestID) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	k := claimKey{seq, d}
	if _, ok := c.m[k]; ok {
		return false
	}
	c.m[k] = struct{}{}
	return true
}

// Release drops a claim.
func (c *Claims) Release(seq uint64, d event.DestID) {
	c.mu.Lock()
	delete(c.m, claimKey{seq, d})
	c.mu.Unlock()
}

// Len returns the number of outstanding claims.
func (c *Claims) Len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.m)
}
