package state

import (
	"encoding/binary"
	"fmt"

	bolt "go.etcd.io/bbolt"

	"github.com/lzwhehe/loggate/internal/event"
)

type opKind uint8

const (
	opBarrier opKind = iota
	opPark
	opAck
	opFail
	opDead
	opCkpt
	opDeleteDue
	opExec
)

// Done is an operation completion callback. applied reports whether the
// operation changed state (e.g. Ack of a seq that is not pending for the
// destination is a no-op). It runs on the writer goroutine after commit and
// must not block or submit further operations.
type Done func(applied bool, err error)

type op struct {
	kind opKind
	park ParkReq

	seq      uint64
	dest     event.DestID
	attempts uint16
	lastErr  uint16
	nextAt   int64
	reason   Reason

	ckpts   map[event.DestID]uint64
	dueKeys [][]byte
	exec    func(tx *bolt.Tx, d *delta) error

	done    Done
	applied bool
}

// delta collects in-memory index changes of one transaction; they are
// applied only if the transaction commits.
type delta struct {
	segRef   map[uint64]int
	pending  [event.MaxDests]int64
	ckpt     map[event.DestID]uint64
	registry map[string]*RegEntry // nil value = deleted
	maxSeq   uint64
}

// ParkReq hands responsibility for (Seq, Dest) to L1.
type ParkReq struct {
	Seq        uint64
	Dest       event.DestID
	Ref        Ref
	ReceivedAt int64
	Reason     Reason
	Attempts   uint16 // real send attempts already made
	LastErr    uint16
	NextAt     int64 // unix nanos
}

func (s *Store) submit(o *op) {
	s.closeMu.RLock()
	defer s.closeMu.RUnlock()
	if s.closed {
		if o.done != nil {
			o.done(false, ErrClosed)
		}
		return
	}
	s.ops <- o
}

// Park records (Seq, Dest) as pending in L1 and indexes it in due/<Dest>.
// done runs after commit; only then may the caller Resolve its tracker
// (DESIGN §4.2: the reverse order opens a loss window).
func (s *Store) Park(req ParkReq, done Done) { s.submit(&op{kind: opPark, park: req, done: done}) }

// Ack marks (seq, d) delivered.
func (s *Store) Ack(seq uint64, d event.DestID, done Done) {
	s.submit(&op{kind: opAck, seq: seq, dest: d, done: done})
}

// Fail records a failed (or postponed) redelivery: attempts is the new
// absolute attempt count (unchanged for non-counting postponements such as
// BreakerOpen), nextAt the next due time.
func (s *Store) Fail(seq uint64, d event.DestID, attempts, lastErr uint16, nextAt int64, done Done) {
	s.submit(&op{kind: opFail, seq: seq, dest: d, attempts: attempts, lastErr: lastErr, nextAt: nextAt, done: done})
}

// Dead marks (seq, d) dead. The caller must have fsynced the DLQ entry
// before calling (DESIGN §0.5 item 4).
func (s *Store) Dead(seq uint64, d event.DestID, done Done) {
	s.submit(&op{kind: opDead, seq: seq, dest: d, done: done})
}

// SaveCheckpoints persists watermarks; a stored watermark never decreases.
func (s *Store) SaveCheckpoints(w map[event.DestID]uint64, done Done) {
	s.submit(&op{kind: opCkpt, ckpts: w, done: done})
}

// DeleteDue removes stale due/<d> keys (re-validated inside the write).
func (s *Store) DeleteDue(d event.DestID, keys [][]byte, done Done) {
	s.submit(&op{kind: opDeleteDue, dest: d, dueKeys: keys, done: done})
}

func (s *Store) execSync(fn func(tx *bolt.Tx, d *delta) error) error {
	ch := make(chan error, 1)
	s.submit(&op{kind: opExec, exec: fn, done: func(_ bool, err error) { ch <- err }})
	return <-ch
}

// run is the single writer goroutine.
func (s *Store) run() {
	defer close(s.done)
	for {
		var first *op
		select {
		case first = <-s.ops:
		case <-s.quit:
			// Drain what was submitted before close.
			for {
				select {
				case o := <-s.ops:
					s.commit(s.collect(o))
				default:
					return
				}
			}
		}
		s.commit(s.collect(first))
	}
}

func (s *Store) collect(first *op) []*op {
	batch := []*op{first}
	for len(batch) < s.opts.MaxOpsPerTx {
		select {
		case o := <-s.ops:
			batch = append(batch, o)
		default:
			return batch
		}
	}
	return batch
}

func (s *Store) commit(batch []*op) {
	if s.aborted.Load() {
		return // simulated crash: nothing commits, nobody is told
	}
	d := &delta{segRef: map[uint64]int{}, ckpt: map[event.DestID]uint64{}, registry: map[string]*RegEntry{}}
	err := s.db.Update(func(tx *bolt.Tx) error {
		for _, o := range batch {
			o.applied = false
			if err := s.apply(tx, o, d); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		s.log.Error("l1 transaction failed", "ops", len(batch), "err", err)
		for _, o := range batch {
			if o.done != nil {
				o.done(false, fmt.Errorf("state: commit: %w", err))
			}
		}
		return
	}
	s.mu.Lock()
	for seg, n := range d.segRef {
		if v := s.segRef[seg] + n; v > 0 {
			s.segRef[seg] = v
		} else {
			delete(s.segRef, seg)
		}
	}
	for id, w := range d.ckpt {
		s.ckpt[id] = w
	}
	for name, e := range d.registry {
		if e == nil {
			delete(s.registry, name)
		} else {
			s.registry[name] = *e
		}
	}
	s.maxSeq = max(s.maxSeq, d.maxSeq)
	s.mu.Unlock()
	for i, n := range d.pending {
		if n != 0 {
			s.pending[i].Add(n)
		}
	}
	for _, o := range batch {
		if o.done != nil {
			o.done(o.applied, nil)
		}
	}
}

func getRecord(b *bolt.Bucket, seq uint64) (*Record, error) {
	v := b.Get(seqKey(seq))
	if v == nil {
		return nil, nil
	}
	r, err := decodeRecord(v)
	if err != nil {
		return nil, fmt.Errorf("seq %d: %w", seq, err)
	}
	return r, nil
}

func dueBucket(tx *bolt.Tx, d event.DestID) (*bolt.Bucket, error) {
	b, err := tx.Bucket(bucketDue).CreateBucketIfNotExists([]byte{byte(d)})
	if err != nil {
		return nil, fmt.Errorf("due bucket %d: %w", d, err)
	}
	return b, nil
}

func (s *Store) apply(tx *bolt.Tx, o *op, d *delta) error {
	l1 := tx.Bucket(bucketL1)
	switch o.kind {
	case opBarrier:
		return nil
	case opExec:
		return o.exec(tx, d)
	case opCkpt:
		b := tx.Bucket(bucketCkpt)
		for id, w := range o.ckpts {
			prev := uint64(0)
			if v := b.Get([]byte{byte(id)}); len(v) == 8 {
				prev = binary.BigEndian.Uint64(v)
			}
			if w <= prev {
				continue
			}
			if err := b.Put([]byte{byte(id)}, binary.BigEndian.AppendUint64(nil, w)); err != nil {
				return err
			}
			d.ckpt[id] = w
			o.applied = true
		}
		return nil
	case opDeleteDue:
		db, err := dueBucket(tx, o.dest)
		if err != nil {
			return err
		}
		for _, k := range o.dueKeys {
			nextAt, seq := parseDueKey(k)
			r, err := getRecord(l1, seq)
			if err != nil {
				return err
			}
			if r != nil && r.Pending.Has(o.dest) {
				if sl := r.Slot(o.dest); sl != nil && max(sl.NextAt, 0) == nextAt {
					continue // became valid again
				}
			}
			if err := db.Delete(k); err != nil {
				return err
			}
			o.applied = true
		}
		return nil
	case opPark:
		return s.applyPark(tx, l1, o, d)
	}

	// Ack / Fail / Dead operate on an existing pending slot.
	r, err := getRecord(l1, o.seq)
	if err != nil {
		return err
	}
	if r == nil || !r.Pending.Has(o.dest) {
		return nil // stale: already acked/dead elsewhere; no-op
	}
	sl := r.Slot(o.dest)
	if sl == nil {
		return fmt.Errorf("seq %d: pending dest %d without slot", o.seq, o.dest)
	}
	db, err := dueBucket(tx, o.dest)
	if err != nil {
		return err
	}
	if err := db.Delete(dueKey(sl.NextAt, o.seq)); err != nil {
		return err
	}
	o.applied = true
	switch o.kind {
	case opFail:
		sl.Attempts = o.attempts
		if o.lastErr != 0 {
			sl.LastErr = o.lastErr
		}
		sl.NextAt = o.nextAt
		if err := db.Put(dueKey(sl.NextAt, o.seq), nil); err != nil {
			return err
		}
		return l1.Put(seqKey(o.seq), r.encode())
	case opAck:
		r.Pending, r.Acked = r.Pending.Clear(o.dest), r.Acked.Set(o.dest)
	case opDead:
		r.Pending, r.Dead = r.Pending.Clear(o.dest), r.Dead.Set(o.dest)
	}
	d.pending[o.dest]--
	if r.Pending == 0 {
		d.segRef[r.Ref.SegID]--
		return l1.Delete(seqKey(o.seq))
	}
	return l1.Put(seqKey(o.seq), r.encode())
}

func (s *Store) applyPark(tx *bolt.Tx, l1 *bolt.Bucket, o *op, d *delta) error {
	p := o.park
	if !p.Dest.Valid() {
		return fmt.Errorf("park seq %d: invalid dest %d", p.Seq, p.Dest)
	}
	now := s.opts.Clock.Now().UnixNano()
	r, err := getRecord(l1, p.Seq)
	if err != nil {
		return err
	}
	db, err := dueBucket(tx, p.Dest)
	if err != nil {
		return err
	}
	if r == nil {
		r = &Record{Seq: p.Seq, Ref: p.Ref, ReceivedAt: p.ReceivedAt, ParkedAt: now}
		d.segRef[p.Ref.SegID]++
		d.maxSeq = max(d.maxSeq, p.Seq)
	}
	sl := r.Slot(p.Dest)
	switch {
	case sl == nil:
		r.Owned = r.Owned.Set(p.Dest)
		r.Slots = append(r.Slots, Slot{Dest: p.Dest, FirstAt: now})
		sl = &r.Slots[len(r.Slots)-1]
		r.Pending = r.Pending.Set(p.Dest)
		d.pending[p.Dest]++
	case r.Pending.Has(p.Dest):
		// Re-park of a pending slot: move its due entry.
		if err := db.Delete(dueKey(sl.NextAt, p.Seq)); err != nil {
			return err
		}
	default:
		// Previously acked/dead for this dest (not expected in normal
		// operation): take responsibility again.
		r.Acked, r.Dead = r.Acked.Clear(p.Dest), r.Dead.Clear(p.Dest)
		r.Pending = r.Pending.Set(p.Dest)
		d.pending[p.Dest]++
	}
	sl.Reason = p.Reason
	sl.Attempts = max(sl.Attempts, p.Attempts)
	if p.LastErr != 0 {
		sl.LastErr = p.LastErr
	}
	sl.NextAt = p.NextAt
	if err := db.Put(dueKey(sl.NextAt, p.Seq), nil); err != nil {
		return err
	}
	o.applied = true
	return l1.Put(seqKey(p.Seq), r.encode())
}
