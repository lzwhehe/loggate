package gateway

import (
	"context"
	"errors"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/lzwhehe/loggate/internal/config"
	"github.com/lzwhehe/loggate/internal/delivery"
	"github.com/lzwhehe/loggate/internal/dlq"
	"github.com/lzwhehe/loggate/internal/event"
	"github.com/lzwhehe/loggate/internal/sink"
	"github.com/lzwhehe/loggate/internal/state"
)

var errDown = errors.New("connection refused (simulated)")

func readDLQ(t *testing.T, dir string) []dlq.Entry {
	t.Helper()
	es, err := dlq.ReadAll(filepath.Join(dir, "data", "dlq"))
	if err != nil {
		t.Fatal(err)
	}
	return es
}

// G2: one sink fails (Retryable) for a while. The healthy sink receives
// every event exactly once; the failing sink's events are parked in L1 and
// delivered exactly once in total after recovery; L1 drains to 0.
func TestPartialFailureOnlyFailedDestinationRetried(t *testing.T) {
	dir := t.TempDir()
	fakes := map[string]*fakeSink{"healthy": newFake(), "flaky": newFake()}
	cfg := testCfg(t, dir, `
destinations:
  - { name: healthy, type: file, config: { path: "fake:healthy" } }
  - { name: flaky, type: file, breaker: { failures: 3, open_for: 30ms }, config: { path: "fake:flaky" } }
routes:
  - { match: {}, to: [healthy, flaky] }
`)
	var down atomic.Bool
	down.Store(true)
	fakes["flaky"].setFail(func(*sink.Envelope) error {
		if down.Load() {
			return errDown
		}
		return nil
	})
	g := start(t, cfg, fakes)
	var acked []uint64
	for range 10 {
		acked = append(acked, post(t, g.Addr(), 30, plain)...)
	}
	eventually(t, "healthy delivery", func() bool { return fakes["healthy"].count() == len(acked) })
	flakyID, _ := g.table.ID("flaky")
	eventually(t, "flaky parked", func() bool { return g.st.Pending(flakyID) == int64(len(acked)) })
	if fakes["flaky"].count() != 0 {
		t.Fatal("flaky delivered while down")
	}
	// More traffic during the outage.
	acked = append(acked, post(t, g.Addr(), 50, plain)...)
	down.Store(false)
	acked = append(acked, post(t, g.Addr(), 50, plain)...)

	eventually(t, "flaky recovery", func() bool { return fakes["flaky"].count() == len(acked) })
	eventually(t, "L1 drained", func() bool { return g.st.Count() == 0 })
	eventually(t, "healthy all", func() bool { return fakes["healthy"].count() == len(acked) })
	shutdown(t, g)
	checkExactlyOnce(t, "healthy", fakes["healthy"].snapshot(), acked)
	checkExactlyOnce(t, "flaky", fakes["flaky"].snapshot(), acked)
	if es := readDLQ(t, dir); len(es) != 0 {
		t.Fatalf("unexpected DLQ entries: %d", len(es))
	}
}

// NonRetryable errors go straight to the DLQ; the DLQ holds exactly the
// failed (seq, dest) pairs, each with a copy of the body.
func TestNonRetryableToDLQ(t *testing.T) {
	dir := t.TempDir()
	fakes := map[string]*fakeSink{"a": newFake(), "b": newFake()}
	cfg := testCfg(t, dir, `
destinations:
  - { name: a, type: file, config: { path: "fake:a" } }
  - { name: b, type: file, config: { path: "fake:b" } }
routes:
  - { match: {}, to: [a, b] }
`)
	fakes["b"].setFail(func(e *sink.Envelope) error {
		if e.Seq%5 == 0 {
			return delivery.NewError(delivery.NonRetryable, errors.New("http status 400"))
		}
		return nil
	})
	g := start(t, cfg, fakes)
	acked := post(t, g.Addr(), 100, plain)
	eventually(t, "deliveries", func() bool {
		return fakes["a"].count() == 100 && fakes["b"].count() == 80 && g.dlq.Total() == 20
	})
	eventually(t, "tracker resolved", func() bool { return g.trackers.Watermark(1) == acked[len(acked)-1] })
	shutdown(t, g)
	want := map[uint64]bool{}
	for _, s := range acked {
		if s%5 == 0 {
			want[s] = true
		}
	}
	es := readDLQ(t, dir)
	if len(es) != len(want) {
		t.Fatalf("dlq has %d entries, want %d", len(es), len(want))
	}
	for _, e := range es {
		if !want[e.Seq] || e.Dest != 1 || e.DestName != "b" || e.Reason != "non_retryable" || e.Attempts != 1 || len(e.Body) == 0 || e.IdemKey.IsZero() {
			t.Fatalf("bad dlq entry %+v", e)
		}
		delete(want, e.Seq)
	}
	if g.st.Count() != 0 {
		t.Fatal("L1 not empty")
	}
}

// Exhausting max_attempts sends the pair to the DLQ with the attempt count.
func TestBudgetExhaustionToDLQ(t *testing.T) {
	dir := t.TempDir()
	fakes := map[string]*fakeSink{"a": newFake()}
	cfg := testCfg(t, dir, `
retry: { base: 1ms, max: 5ms, max_attempts: 3, max_age: 1h, redeliver_interval: 5ms }
destinations:
  - { name: a, type: file, breaker: { failures: 1000 }, config: { path: "fake:a" } }
routes:
  - { match: {}, to: [a] }
`)
	fakes["a"].setFail(func(*sink.Envelope) error { return errDown })
	g := start(t, cfg, fakes)
	acked := post(t, g.Addr(), 20, plain)
	eventually(t, "dlq", func() bool { return g.dlq.Total() == 20 })
	eventually(t, "L1 empty", func() bool { return g.st.Count() == 0 })
	shutdown(t, g)
	es := readDLQ(t, dir)
	seen := map[uint64]bool{}
	for _, e := range es {
		if e.Reason != "max_attempts" || e.Attempts != 3 {
			t.Fatalf("entry %+v", e)
		}
		seen[e.Seq] = true
	}
	for _, s := range acked {
		if !seen[s] {
			t.Fatalf("seq %d missing from DLQ", s)
		}
	}
}

// A route names a destination that is not configured: its events park
// (NoDest, no attempts) and are delivered once it is registered within
// orphan_ttl.
func TestOrphanDeliveredAfterRegistration(t *testing.T) {
	dir := t.TempDir()
	fakes := map[string]*fakeSink{"a": newFake(), "ghost": newFake()}
	cfg := testCfg(t, dir, `
l1: { orphan_ttl: 1h, sweep_interval: 10ms }
destinations:
  - { name: a, type: file, config: { path: "fake:a" } }
routes:
  - { match: {}, to: [a, ghost] }
`)
	g := start(t, cfg, fakes)
	acked := post(t, g.Addr(), 40, plain)
	eventually(t, "ghost parked", func() bool { return g.st.Pending(1) == 40 })
	r, _ := g.st.Get(acked[0])
	if sl := r.Slot(1); sl == nil || sl.Reason != state.ReasonNoDest || sl.Attempts != 0 {
		t.Fatalf("slot %+v", sl)
	}
	err := g.Register(context.Background(), config.Destination{Name: "ghost", Type: "fake", Config: map[string]any{"name": "ghost"}})
	if err != nil {
		t.Fatal(err)
	}
	more := post(t, g.Addr(), 10, plain)
	eventually(t, "ghost delivery", func() bool { return fakes["ghost"].count() == 50 })
	eventually(t, "L1 empty", func() bool { return g.st.Count() == 0 })
	shutdown(t, g)
	checkExactlyOnce(t, "ghost", fakes["ghost"].snapshot(), append(acked, more...))
	checkExactlyOnce(t, "a", fakes["a"].snapshot(), append(acked, more...))
}

// Without registration, the global sweeper moves orphaned pairs to the DLQ
// after orphan_ttl (S4b), independent of any destination loop.
func TestOrphanTTLSweepsToDLQ(t *testing.T) {
	dir := t.TempDir()
	fakes := map[string]*fakeSink{"a": newFake()}
	cfg := testCfg(t, dir, `
l1: { orphan_ttl: 150ms, sweep_interval: 10ms }
destinations:
  - { name: a, type: file, config: { path: "fake:a" } }
routes:
  - { match: {}, to: [a, ghost] }
`)
	g := start(t, cfg, fakes)
	acked := post(t, g.Addr(), 30, plain)
	eventually(t, "swept", func() bool { return g.dlq.Count(1) == 30 })
	eventually(t, "L1 empty", func() bool { return g.st.Count() == 0 })
	shutdown(t, g)
	checkExactlyOnce(t, "a", fakes["a"].snapshot(), acked)
	for _, e := range readDLQ(t, dir) {
		if e.Reason != "orphan_ttl_expired" || e.DestName != "ghost" || len(e.Body) < 10 {
			t.Fatalf("entry %+v", e)
		}
	}
}

// Draining: the in-flight batch completes through the normal result path,
// the rest of L2 is parked (NotActive), new events park too, and nothing is
// lost: re-registering delivers the remainder.
func TestUnregisterDrainsWithoutLoss(t *testing.T) {
	dir := t.TempDir()
	fakes := map[string]*fakeSink{"a": newFake()}
	cfg := testCfg(t, dir, `
l1: { orphan_ttl: 1h }
destinations:
  - { name: a, type: file, queue: { max_batch: 16 }, config: { path: "fake:a" } }
routes:
  - { match: {}, to: [a] }
`)
	f := fakes["a"]
	f.gate, f.inside = make(chan struct{}), make(chan int, 10)
	g := start(t, cfg, fakes)
	acked := post(t, g.Addr(), 100, plain)
	inflight := <-f.inside
	eventually(t, "rest queued", func() bool { return g.slot(0).Depth() == 100-inflight })

	done := make(chan error, 1)
	go func() { done <- g.Unregister(context.Background(), "a") }()
	eventually(t, "draining", func() bool { return g.slot(0).State().String() == "draining" })
	f.mu.Lock()
	f.inside = nil
	f.mu.Unlock()
	close(f.gate)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if got := f.count(); got != inflight {
		t.Fatalf("delivered %d, want exactly the in-flight batch of %d", got, inflight)
	}
	if p := g.st.Pending(0); p != int64(100-inflight) {
		t.Fatalf("L1 pending %d, want %d", p, 100-inflight)
	}
	r, _ := g.st.Get(acked[len(acked)-1])
	if sl := r.Slot(0); sl == nil || sl.Reason != state.ReasonNotActive || sl.Attempts != 0 {
		t.Fatalf("parked slot %+v", sl)
	}
	// Everything dispatched so far is resolved: tracker watermark = cursor.
	if w, c := g.trackers.Watermark(0), g.trackers.Cursor(); w != c {
		t.Fatalf("watermark %d cursor %d", w, c)
	}
	more := post(t, g.Addr(), 10, plain) // parks as NotActive
	eventually(t, "parked new", func() bool { return g.st.Pending(0) == int64(110-inflight) })

	f.mu.Lock()
	f.gate = nil
	f.mu.Unlock()
	if err := g.Register(context.Background(), config.Destination{Name: "a", Type: "fake", Config: map[string]any{"name": "a"}}); err != nil {
		t.Fatal(err)
	}
	eventually(t, "redelivered", func() bool { return f.count() == 110 })
	eventually(t, "L1 empty", func() bool { return g.st.Count() == 0 })
	shutdown(t, g)
	checkExactlyOnce(t, "a", f.snapshot(), append(acked, more...))
}

// GC keeps segments referenced by L1 and removes the others once the
// checkpoint has passed them.
func TestSegmentGC(t *testing.T) {
	dir := t.TempDir()
	fakes := map[string]*fakeSink{"a": newFake()}
	cfg := testCfg(t, dir, `
wal: { segment_bytes: 4KiB, fsync_interval: 1ms }
l1: { checkpoint_interval: 10ms, gc_interval: 10ms }
destinations:
  - { name: a, type: file, breaker: { failures: 100000 }, config: { path: "fake:a" } }
routes:
  - { match: {}, to: [a] }
`)
	var stuck atomic.Uint64
	stuck.Store(3)
	fakes["a"].setFail(func(e *sink.Envelope) error {
		if e.Seq == stuck.Load() {
			return errDown
		}
		return nil
	})
	g := start(t, cfg, fakes)
	var acked []uint64
	for range 20 {
		acked = append(acked, post(t, g.Addr(), 20, plain)...)
	}
	last := acked[len(acked)-1]
	eventually(t, "delivery", func() bool { return fakes["a"].count() == len(acked)-1 })
	eventually(t, "checkpoint", func() bool { return g.st.Checkpoint(0) == last })
	segOf3 := segmentOf(t, g, 3)
	eventually(t, "gc", func() bool {
		segs := g.wal.Segments()
		return len(segs) == 2 && segs[0] == segOf3 // pinned segment + active
	})
	if g.st.SegRef(segOf3) != 1 {
		t.Fatal("pinned segment has no L1 reference")
	}
	stuck.Store(0)
	eventually(t, "redelivered", func() bool { return fakes["a"].count() == len(acked) })
	eventually(t, "gc of released segment", func() bool { return len(g.wal.Segments()) == 1 })
	if g.st.Count() != 0 {
		t.Fatal("L1 not empty")
	}
	shutdown(t, g)
	checkExactlyOnce(t, "a", fakes["a"].snapshot(), acked)
}

func segmentOf(t *testing.T, g *Gateway, seq uint64) uint64 {
	t.Helper()
	var seg uint64
	eventually(t, "L1 record", func() bool {
		r, err := g.st.Get(seq)
		if err != nil || r == nil {
			return false
		}
		seg = r.Ref.SegID
		return true
	})
	return seg
}

// Backpressure: with block_all, exceeding a destination's L1 quota makes
// ingest answer 429 until the backlog drains.
func TestQuotaBlockAll429(t *testing.T) {
	dir := t.TempDir()
	fakes := map[string]*fakeSink{"a": newFake()}
	cfg := testCfg(t, dir, `
l1: { max_records_per_dest: 10, on_quota: block_all }
destinations:
  - { name: a, type: file, config: { path: "fake:a" } }
routes:
  - { match: {}, to: [a] }
`)
	var down atomic.Bool
	down.Store(true)
	fakes["a"].setFail(func(*sink.Envelope) error {
		if down.Load() {
			return errDown
		}
		return nil
	})
	g := start(t, cfg, fakes)
	acked := post(t, g.Addr(), 15, plain)
	eventually(t, "over quota", func() bool { return g.deps.OverQuota() })
	if _, err := postErr(g.Addr(), 1, plain); err == nil {
		t.Fatal("ingest accepted while over quota")
	}
	down.Store(false)
	eventually(t, "drained", func() bool { return !g.deps.OverQuota() && g.st.Count() == 0 })
	acked = append(acked, post(t, g.Addr(), 1, plain)...)
	eventually(t, "delivered", func() bool { return fakes["a"].count() == 16 })
	shutdown(t, g)
	checkExactlyOnce(t, "a", fakes["a"].snapshot(), acked)
}

// DestIDs are stable across restarts and config reordering.
func TestStableDestIDsAcrossRestart(t *testing.T) {
	dir := t.TempDir()
	cfg := testCfg(t, dir, `
destinations:
  - { name: x, type: file, config: { path: "$OUT/x.jsonl" } }
  - { name: y, type: file, config: { path: "$OUT/y.jsonl" } }
routes:
  - { match: {}, to: [x, y] }
`)
	g := start(t, cfg, nil)
	shutdown(t, g)
	cfg.Destinations[0], cfg.Destinations[1] = cfg.Destinations[1], cfg.Destinations[0]
	g = start(t, cfg, nil)
	defer shutdown(t, g)
	if id, _ := g.table.ID("x"); id != 0 {
		t.Fatalf("x moved to id %d", id)
	}
	if g.slot(event.DestID(1)).Name() != "y" {
		t.Fatal("y not at id 1")
	}
}
