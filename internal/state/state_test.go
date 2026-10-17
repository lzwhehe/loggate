package state

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	bolt "go.etcd.io/bbolt"

	"github.com/lzwhehe/loggate/internal/clock"
	"github.com/lzwhehe/loggate/internal/event"
)

func openStore(t *testing.T, path string, clk clock.Clock) *Store {
	t.Helper()
	s, err := Open(Options{Path: path, Clock: clk})
	if err != nil {
		t.Fatal(err)
	}
	return s
}

// wait runs an async op and returns its result.
func wait(t *testing.T, f func(Done)) bool {
	t.Helper()
	type res struct {
		applied bool
		err     error
	}
	ch := make(chan res, 1)
	f(func(a bool, err error) { ch <- res{a, err} })
	r := <-ch
	if r.err != nil {
		t.Fatal(r.err)
	}
	return r.applied
}

func park(t *testing.T, s *Store, seq uint64, d event.DestID, seg uint64, nextAt int64) {
	t.Helper()
	wait(t, func(done Done) {
		s.Park(ParkReq{Seq: seq, Dest: d, Ref: Ref{SegID: seg, Offset: int64(seq) * 10, Length: 10}, ReceivedAt: 1, Reason: ReasonSendFailed, Attempts: 1, NextAt: nextAt}, done)
	})
}

func dueSeqs(t *testing.T, s *Store, d event.DestID, now time.Time) []uint64 {
	t.Helper()
	items, _, err := s.Due(d, now, 100, 1000, func(uint64) bool { return true })
	if err != nil {
		t.Fatal(err)
	}
	var out []uint64
	for _, it := range items {
		out = append(out, it.Seq)
	}
	return out
}

func TestParkMergeAckDelete(t *testing.T) {
	clk := clock.NewFake(time.Unix(100, 0))
	path := filepath.Join(t.TempDir(), "state.db")
	s := openStore(t, path, clk)

	park(t, s, 7, 1, 5, 10)
	park(t, s, 7, 3, 5, 20) // merge: second destination of the same event
	park(t, s, 8, 1, 5, 30)
	r, _ := s.Get(7)
	if r.Owned != event.DestMask(0).Set(1).Set(3) || r.Pending != r.Owned || len(r.Slots) != 2 {
		t.Fatalf("merged record: %+v", r)
	}
	if s.SegRef(5) != 2 || s.Pending(1) != 2 || s.Pending(3) != 1 {
		t.Fatalf("segref %d pending1 %d pending3 %d", s.SegRef(5), s.Pending(1), s.Pending(3))
	}
	if got := dueSeqs(t, s, 1, time.Unix(0, 25)); len(got) != 1 || got[0] != 7 {
		t.Fatalf("due(1) at 25 = %v", got)
	}

	if !wait(t, func(d Done) { s.Ack(7, 1, d) }) {
		t.Fatal("ack not applied")
	}
	if wait(t, func(d Done) { s.Ack(7, 1, d) }) {
		t.Fatal("second ack applied")
	}
	r, _ = s.Get(7)
	if r.Pending != event.DestMask(0).Set(3) || !r.Acked.Has(1) {
		t.Fatalf("after ack: %+v", r)
	}
	if s.SegRef(5) != 2 {
		t.Fatal("segref dropped before record empty")
	}
	wait(t, func(d Done) { s.Dead(7, 3, d) })
	if r, _ := s.Get(7); r != nil {
		t.Fatalf("record not deleted when pending empty: %+v", r)
	}
	if s.SegRef(5) != 1 || s.Pending(3) != 0 || s.Pending(1) != 1 {
		t.Fatalf("segref %d pending3 %d", s.SegRef(5), s.Pending(3))
	}
	if got := dueSeqs(t, s, 1, time.Unix(0, 1000)); len(got) != 1 || got[0] != 8 {
		t.Fatalf("due(1) = %v", got)
	}

	// Reopen rebuilds segRef/pending/maxSeq.
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s = openStore(t, path, clk)
	defer s.Close()
	if s.SegRef(5) != 1 || s.Pending(1) != 1 || s.MaxL1Seq() != 8 {
		t.Fatalf("rebuilt segref %d pending %d max %d", s.SegRef(5), s.Pending(1), s.MaxL1Seq())
	}
}

func TestFailMovesDueAndStaleIndex(t *testing.T) {
	clk := clock.NewFake(time.Unix(100, 0))
	s := openStore(t, filepath.Join(t.TempDir(), "s.db"), clk)
	defer s.Close()
	park(t, s, 1, 0, 1, 10)
	wait(t, func(d Done) { s.Fail(1, 0, 4, 1, 500, d) })
	if got := dueSeqs(t, s, 0, time.Unix(0, 100)); len(got) != 0 {
		t.Fatalf("still due at old time: %v", got)
	}
	items, _, _ := s.Due(0, time.Unix(0, 500), 10, 10, func(uint64) bool { return true })
	if len(items) != 1 || items[0].Attempts != 4 {
		t.Fatalf("due after fail: %+v", items)
	}
	// Claimed items are skipped but not treated as stale.
	items, stale, _ := s.Due(0, time.Unix(0, 500), 10, 10, func(uint64) bool { return false })
	if len(items) != 0 || len(stale) != 0 {
		t.Fatalf("claimed: items %v stale %v", items, stale)
	}
	// Inject a stale index entry and check it is reported and removable.
	err := s.execSync(func(tx *bolt.Tx, _ *delta) error {
		b, _ := dueBucket(tx, 0)
		return b.Put(dueKey(5, 99), nil)
	})
	if err != nil {
		t.Fatal(err)
	}
	_, stale, _ = s.Due(0, time.Unix(0, 1000), 10, 10, func(uint64) bool { return true })
	if len(stale) != 1 {
		t.Fatalf("stale = %d", len(stale))
	}
	wait(t, func(d Done) { s.DeleteDue(0, stale, d) })
	_, stale, _ = s.Due(0, time.Unix(0, 1000), 10, 10, func(uint64) bool { return true })
	if len(stale) != 0 {
		t.Fatal("stale entry survived")
	}
}

func TestCallbacksAfterCommitAndAbort(t *testing.T) {
	path := filepath.Join(t.TempDir(), "s.db")
	s := openStore(t, path, nil)
	park(t, s, 1, 0, 1, 0)
	// Callback runs after commit: a fresh read transaction sees the record.
	seen := make(chan bool, 1)
	s.Park(ParkReq{Seq: 2, Dest: 0, Ref: Ref{SegID: 1}}, func(_ bool, err error) {
		r, _ := s.Get(2)
		seen <- err == nil && r != nil
	})
	if !<-seen {
		t.Fatal("record not visible in callback")
	}
	if err := s.Abort(); err != nil {
		t.Fatal(err)
	}
	s = openStore(t, path, nil)
	defer s.Close()
	if s.Count() != 2 {
		t.Fatalf("count = %d", s.Count())
	}
}

func TestCheckpointMonotonic(t *testing.T) {
	path := filepath.Join(t.TempDir(), "s.db")
	s := openStore(t, path, nil)
	wait(t, func(d Done) { s.SaveCheckpoints(map[event.DestID]uint64{0: 50, 1: 10}, d) })
	wait(t, func(d Done) { s.SaveCheckpoints(map[event.DestID]uint64{0: 40, 1: 12}, d) })
	if s.Checkpoint(0) != 50 || s.Checkpoint(1) != 12 {
		t.Fatalf("ckpt %v", s.Checkpoints())
	}
	if err := s.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	s.Close()
	s = openStore(t, path, nil)
	defer s.Close()
	if s.Checkpoint(0) != 50 || s.Checkpoint(1) != 12 {
		t.Fatalf("ckpt after reopen %v", s.Checkpoints())
	}
}

func TestRegistryStableIDsAndTombstones(t *testing.T) {
	clk := clock.NewFake(time.Unix(1000, 0))
	path := filepath.Join(t.TempDir(), "s.db")
	s := openStore(t, path, clk)
	res, err := s.Reconcile([]string{"b", "a"}, []string{"ghost"}, 77, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	ids := map[string]event.DestID{}
	for n, e := range res.Entries {
		ids[n] = e.ID
	}
	if ids["b"] != 0 || ids["a"] != 1 || ids["ghost"] != 2 || res.Entries["ghost"].State != RegAbsent {
		t.Fatalf("entries %+v", res.Entries)
	}
	if s.Checkpoint(ids["a"]) != 77 {
		t.Fatal("new id ckpt not initialised")
	}
	s.Close()

	// Reordered config, "a" removed: IDs stable, "a" tombstoned, "c" new
	// does not reuse 0.
	s = openStore(t, path, clk)
	res, err = s.Reconcile([]string{"c", "b"}, nil, 90, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if res.Entries["b"].ID != 0 || res.Entries["a"].State != RegTombstone || res.Entries["c"].ID != 3 || res.Entries["ghost"].State != RegTombstone {
		t.Fatalf("entries %+v", res.Entries)
	}
	// After the tombstone TTL with nothing pending, the ID is freed.
	park(t, s, 5, ids["ghost"], 1, 0) // ghost still has pending work
	clk.Advance(2 * time.Hour)
	res, err = s.Reconcile([]string{"c", "b"}, nil, 95, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := res.Entries["a"]; ok || len(res.Expired) != 1 {
		t.Fatalf("a not expired: %+v", res)
	}
	if _, ok := res.Entries["ghost"]; !ok {
		t.Fatal("ghost expired despite pending L1 work")
	}
	s.Close()
}

func TestSweepCandidates(t *testing.T) {
	clk := clock.NewFake(time.Unix(0, 0))
	s := openStore(t, filepath.Join(t.TempDir(), "s.db"), clk)
	defer s.Close()
	park(t, s, 1, 0, 1, 0) // dest 0 active
	park(t, s, 2, 1, 1, 0) // dest 1 inactive
	clk.Advance(time.Minute)
	claims := NewClaims()
	pol := SweepPolicy{
		Now: clk.Now(), OrphanTTL: 30 * time.Second, MaxAge: time.Hour,
		Active: func(d event.DestID) bool { return d == 0 },
		Claim:  claims.TryClaim, Limit: 100,
	}
	items, err := s.SweepCandidates(pol)
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 1 || items[0].Seq != 2 || items[0].Why != "orphan_ttl_expired" {
		t.Fatalf("items %+v", items)
	}
	// Already claimed: skipped.
	if items, _ := s.SweepCandidates(pol); len(items) != 0 {
		t.Fatalf("claimed item swept twice: %+v", items)
	}
	pol.MaxAge = time.Nanosecond // ReceivedAt = 1ns
	items, _ = s.SweepCandidates(pol)
	if len(items) != 1 || items[0].Seq != 1 || items[0].Why != "max_age" {
		t.Fatalf("max_age items %+v", items)
	}
}
