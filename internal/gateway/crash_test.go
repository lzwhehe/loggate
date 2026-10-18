package gateway

import (
	"context"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/lzwhehe/loggate/internal/sink"
	sqlitesink "github.com/lzwhehe/loggate/internal/sink/sqlite"
	"github.com/lzwhehe/loggate/internal/worker"
)

// Crash tests simulate kill -9 in-process with Gateway.crash: uncommitted
// L1 operations are discarded, no final checkpoint is written, in-flight
// sink results and L2 contents are dropped. Acceptance: after a restart on
// the same data dir every acked event reaches every target at least once,
// and the idempotent sqlite sink stores no duplicate rows.

const crashCfg = `
destinations:
  - { name: siem, type: sqlite, config: { path: "$OUT/siem.db" } }
  - { name: f, type: file, config: { path: "fake:f" } }
routes:
  - { match: {}, to: [siem, f] }
`

func sqliteRows(t *testing.T, dir string) map[uint64]int {
	t.Helper()
	rows, err := sqlitesink.ReadRows(context.Background(), filepath.Join(dir, "out", "siem.db"))
	if err != nil {
		t.Fatal(err)
	}
	out := map[uint64]int{}
	keys := map[string]bool{}
	for _, r := range rows {
		out[r.Seq]++
		if keys[string(r.IdemKey)] {
			t.Fatalf("duplicate idem_key row for seq %d", r.Seq)
		}
		keys[string(r.IdemKey)] = true
	}
	return out
}

func slotsAborted(g *Gateway) bool {
	for _, s := range g.slots.Load() {
		if s != nil && s.State() != worker.Unregistered {
			return false
		}
	}
	return true
}

// Crash point 1: events are fsynced and acked, but the dispatcher has not
// handed them to any destination. Recovery replays them from the WAL.
func TestCrashAfterCommitBeforeDispatch(t *testing.T) {
	dir := t.TempDir()
	fakes := map[string]*fakeSink{"f": newFake()}
	cfg := testCfg(t, dir, crashCfg)
	gate := make(chan struct{})
	g := start(t, cfg, fakes, func(o *Options) { o.BeforeDispatch = func() { <-gate } })
	acked := post(t, g.Addr(), 100, plain)

	crashed := make(chan struct{})
	go func() { g.Crash(); close(crashed) }()
	eventually(t, "slots aborted", func() bool { return slotsAborted(g) })
	close(gate) // the dispatcher now only sees aborted slots and a closed L1
	<-crashed
	if fakes["f"].count() != 0 {
		t.Fatal("delivered before the crash point")
	}

	g2 := start(t, cfg, fakes)
	if g2.Replayed() != 100 {
		t.Fatalf("replayed %d, want 100", g2.Replayed())
	}
	eventually(t, "replayed delivery", func() bool {
		return fakes["f"].count() == 100 && len(sqliteRows(t, dir)) == 100
	})
	shutdown(t, g2)
	checkExactlyOnce(t, "f", fakes["f"].snapshot(), acked)
	checkExactlyOnce(t, "siem rows", sqliteRows(t, dir), acked)
}

// Crash point 2: one destination failed and its events were parked (L1
// committed, tracker resolved); the healthy sqlite sink already has them.
// After restart, L1 redelivers to the failed destination only; replay
// skips (seq, d) pairs L1 owns.
func TestCrashAfterParkCommit(t *testing.T) {
	dir := t.TempDir()
	fakes := map[string]*fakeSink{"f": newFake()}
	cfg := testCfg(t, dir, crashCfg)
	var down atomic.Bool
	down.Store(true)
	fakes["f"].setFail(func(*sink.Envelope) error {
		if down.Load() {
			return errDown
		}
		return nil
	})
	g := start(t, cfg, fakes)
	acked := post(t, g.Addr(), 100, plain)
	fID, _ := g.table.ID("f")
	eventually(t, "parked and stored", func() bool {
		return g.st.Pending(fID) == 100 && len(sqliteRows(t, dir)) == 100
	})
	g.Crash()

	down.Store(false)
	g2 := start(t, cfg, fakes)
	eventually(t, "redelivery", func() bool { return fakes["f"].count() == 100 })
	eventually(t, "L1 empty", func() bool { return g2.st.Count() == 0 })
	shutdown(t, g2)
	checkExactlyOnce(t, "f", fakes["f"].snapshot(), acked)
	checkExactlyOnce(t, "siem rows", sqliteRows(t, dir), acked)
}

// Crash point 3: every sink succeeded but no checkpoint was persisted.
// Recovery replays everything: the non-idempotent file sink sees
// duplicates (expected, bounded by the checkpoint interval), the sqlite
// sink absorbs them (stored dup = 0).
func TestCrashAfterSinkSuccessBeforeCheckpoint(t *testing.T) {
	dir := t.TempDir()
	cfg := testCfg(t, dir, `
l1: { checkpoint_interval: 1h }
destinations:
  - { name: siem, type: sqlite, config: { path: "$OUT/siem.db" } }
  - { name: archive, type: file, config: { path: "$OUT/archive.jsonl" } }
routes:
  - { match: {}, to: [siem, archive] }
`)
	archive := filepath.Join(dir, "out", "archive.jsonl")
	g := start(t, cfg, nil)
	acked := post(t, g.Addr(), 100, plain)
	eventually(t, "delivered", func() bool { return g.Delivered() == 200 })
	g.Crash()
	if n := len(readFileSink(t, archive)); n != 100 {
		t.Fatalf("archive has %d seqs before restart", n)
	}

	g2 := start(t, cfg, nil)
	if g2.Replayed() != 100 {
		t.Fatalf("replayed %d, want 100", g2.Replayed())
	}
	eventually(t, "replay delivered", func() bool { return g2.Delivered() == 200 })
	shutdown(t, g2)
	got := readFileSink(t, archive)
	checkAtLeastOnce(t, "archive", got, acked)
	dups := 0
	for _, n := range got {
		dups += n - 1
	}
	if dups != 100 {
		t.Fatalf("archive raw dups = %d, want 100 (full replay without checkpoint)", dups)
	}
	checkExactlyOnce(t, "siem rows", sqliteRows(t, dir), acked)

	// A third start after the graceful shutdown replays nothing.
	g3 := start(t, cfg, nil)
	if g3.Replayed() != 0 {
		t.Fatalf("replayed %d after graceful shutdown", g3.Replayed())
	}
	shutdown(t, g3)
}

// Mixed: crash while one destination is mid-outage and traffic continues,
// twice in a row; at-least-once everywhere, no stored duplicates.
func TestRepeatedCrashesDuringOutage(t *testing.T) {
	dir := t.TempDir()
	fakes := map[string]*fakeSink{"f": newFake()}
	cfg := testCfg(t, dir, crashCfg)
	var down atomic.Bool
	fakes["f"].setFail(func(e *sink.Envelope) error {
		if down.Load() && e.Seq%2 == 0 {
			return errDown
		}
		return nil
	})
	var acked []uint64
	for round := range 3 {
		down.Store(round < 2)
		g := start(t, cfg, fakes)
		for range 5 {
			acked = append(acked, post(t, g.Addr(), 20, plain)...)
		}
		if round < 2 {
			n := len(acked)
			eventually(t, "sqlite caught up", func() bool { return len(sqliteRows(t, dir)) == n })
			g.Crash()
			continue
		}
		eventually(t, "all delivered", func() bool {
			return fakes["f"].count() == len(acked) && g.st.Count() == 0 && len(sqliteRows(t, dir)) == len(acked)
		})
		shutdown(t, g)
	}
	checkAtLeastOnce(t, "f", fakes["f"].snapshot(), acked)
	checkExactlyOnce(t, "siem rows", sqliteRows(t, dir), acked)
}
