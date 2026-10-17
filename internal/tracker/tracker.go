// Package tracker implements the per-destination AckTracker and the global
// dispatch cursor (DESIGN §4.6, §0.5 item 2).
//
// For destination d, every seq handed to d by the dispatcher is Added (in
// increasing order) and later Resolved once responsibility for it is
// durable elsewhere: the sink confirmed it, L1 committed a Park, or the DLQ
// fsynced it. The watermark is
//
//	W(d) = min(oldest unresolved seq − 1, dispatchCursor)
//
// where dispatchCursor is the highest seq whose Adds for *all* targets have
// completed. Every seq <= W(d) is therefore either resolved for d or not
// targeted at d, which is exactly what recovery needs to skip it. A
// destination without traffic follows the cursor, so it never pins GC.
package tracker

import (
	"sort"
	"sync"
	"sync/atomic"

	"github.com/lzwhehe/loggate/internal/event"
)

// Tracker tracks unresolved seqs of one destination.
type Tracker struct {
	mu       sync.Mutex
	seqs     []uint64 // ascending; seqs[head:] are live
	resolved []bool
	head     int
	open     int // unresolved count
}

// Add registers seq as in flight. Seqs must be added in increasing order.
func (t *Tracker) Add(seq uint64) {
	t.mu.Lock()
	if n := len(t.seqs); n > t.head && t.seqs[n-1] >= seq {
		t.mu.Unlock()
		panic("tracker: Add out of order")
	}
	t.seqs = append(t.seqs, seq)
	t.resolved = append(t.resolved, false)
	t.open++
	t.mu.Unlock()
}

// Resolve marks seq resolved. It reports false if seq is not tracked (or
// was already resolved).
func (t *Tracker) Resolve(seq uint64) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	live := t.seqs[t.head:]
	i := sort.Search(len(live), func(i int) bool { return live[i] >= seq })
	if i == len(live) || live[i] != seq || t.resolved[t.head+i] {
		return false
	}
	t.resolved[t.head+i] = true
	t.open--
	for t.head < len(t.seqs) && t.resolved[t.head] {
		t.head++
	}
	if t.head == len(t.seqs) {
		t.seqs, t.resolved, t.head = t.seqs[:0], t.resolved[:0], 0
	} else if t.head > 1024 && t.head > len(t.seqs)/2 {
		n := copy(t.seqs, t.seqs[t.head:])
		copy(t.resolved, t.resolved[t.head:])
		t.seqs, t.resolved, t.head = t.seqs[:n], t.resolved[:n], 0
	}
	return true
}

// Unresolved returns the number of unresolved seqs.
func (t *Tracker) Unresolved() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.open
}

// watermark computes min(oldest unresolved − 1, cursor). cursor must have
// been read before calling (see Set.Watermark).
func (t *Tracker) watermark(cursor uint64) uint64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.head < len(t.seqs) {
		return min(t.seqs[t.head]-1, cursor)
	}
	return cursor
}

// Set holds one Tracker per possible DestID plus the dispatch cursor.
type Set struct {
	trackers [event.MaxDests]Tracker
	cursor   atomic.Uint64
}

// NewSet returns a Set whose cursor starts at initial (the highest seq that
// needs no tracking, e.g. min(ckpt) before replay).
func NewSet(initial uint64) *Set {
	s := &Set{}
	s.cursor.Store(initial)
	return s
}

// Add registers seq for d.
func (s *Set) Add(d event.DestID, seq uint64) { s.trackers[d&63].Add(seq) }

// Resolve resolves seq for d.
func (s *Set) Resolve(d event.DestID, seq uint64) bool { return s.trackers[d&63].Resolve(seq) }

// Publish advances the dispatch cursor to seq. Call it only after all Adds
// for seq (and every lower seq) have completed. The cursor never moves
// backwards.
func (s *Set) Publish(seq uint64) {
	for {
		cur := s.cursor.Load()
		if seq <= cur || s.cursor.CompareAndSwap(cur, seq) {
			return
		}
	}
}

// Cursor returns the dispatch cursor.
func (s *Set) Cursor() uint64 { return s.cursor.Load() }

// Watermark returns W(d). The cursor is read before the tracker is
// inspected: every seq <= that cursor value has already been Added, so an
// empty tracker really means "nothing outstanding up to cursor".
func (s *Set) Watermark(d event.DestID) uint64 {
	c := s.cursor.Load()
	return s.trackers[d&63].watermark(c)
}

// Unresolved returns d's unresolved count.
func (s *Set) Unresolved(d event.DestID) int { return s.trackers[d&63].Unresolved() }
