package gateway

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"path/filepath"
	"testing"
	"time"
)

func TestEndToEnd1000Events(t *testing.T) {
	dir := t.TempDir()
	cfg := testCfg(t, dir, `
destinations:
  - { name: archive, type: file, config: { path: "$OUT/archive.jsonl" } }
routes:
  - { match: {}, to: [archive] }
`)
	path := filepath.Join(dir, "out", "archive.jsonl")
	g := start(t, cfg, nil)

	resp, err := http.Get("http://" + g.Addr() + "/healthz")
	if err != nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("healthz: %v %v", resp, err)
	}
	resp.Body.Close()

	acked := postEvents(t, g.Addr(), 20, 10, 5) // 1000 events
	if len(acked) != 1000 {
		t.Fatalf("acked %d events, want 1000", len(acked))
	}
	eventually(t, "1000 deliveries", func() bool { return g.Delivered() == 1000 })
	shutdown(t, g)
	checkExactlyOnce(t, "archive", readFileSink(t, path), acked)

	// Restart on the same data dir: nothing is replayed (the final
	// checkpoint covers everything) and seqs are never reused.
	g2 := start(t, cfg, nil)
	if g2.Replayed() != 0 {
		t.Fatalf("replayed %d after graceful shutdown", g2.Replayed())
	}
	more := postEvents(t, g2.Addr(), 2, 5, 10)
	eventually(t, "100 more deliveries", func() bool { return g2.Delivered() == 100 })
	shutdown(t, g2)
	var maxOld uint64
	for _, s := range acked {
		maxOld = max(maxOld, s)
	}
	for _, s := range more {
		if s <= maxOld {
			t.Fatalf("seq %d reused after restart (old max %d)", s, maxOld)
		}
	}
	checkExactlyOnce(t, "archive", readFileSink(t, path), append(acked, more...))
}

// Graceful shutdown with a full L2 queue: what is not delivered is parked
// (reason Shutdown); the restart redelivers it; dup = 0 even on a
// non-idempotent sink.
func TestGracefulRestartNoDuplicates(t *testing.T) {
	dir := t.TempDir()
	fakes := map[string]*fakeSink{"a": newFake()}
	cfg := testCfg(t, dir, `
destinations:
  - { name: a, type: file, queue: { max_batch: 10 }, config: { path: "fake:a" } }
routes:
  - { match: {}, to: [a] }
`)
	f := fakes["a"]
	f.gate, f.inside = make(chan struct{}), make(chan int, 100)
	g := start(t, cfg, fakes)
	acked := post(t, g.Addr(), 200, plain)
	<-f.inside // first batch in flight, the rest waits in L2
	done := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		done <- g.Shutdown(ctx)
	}()
	eventually(t, "draining", func() bool { return g.slot(0).State().String() != "active" })
	close(f.gate)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	first := f.count()
	if first == 0 || first == 200 {
		t.Fatalf("expected a partial delivery before shutdown, got %d", first)
	}

	f.gate = nil
	g2 := start(t, cfg, fakes)
	if g2.Replayed() != 0 {
		t.Fatalf("replayed %d after graceful shutdown", g2.Replayed())
	}
	eventually(t, "all delivered", func() bool { return f.count() == 200 })
	eventually(t, "L1 empty", func() bool { return g2.st.Count() == 0 })
	shutdown(t, g2)
	checkExactlyOnce(t, "a", f.snapshot(), acked)
}

func TestShutdownRejectsNewIngest(t *testing.T) {
	cfg := testCfg(t, t.TempDir(), `
destinations:
  - { name: archive, type: file, config: { path: "$OUT/a.jsonl" } }
routes:
  - { match: {}, to: [archive] }
`)
	g := start(t, cfg, nil)
	addr := g.Addr()
	shutdown(t, g)
	if _, err := http.Post("http://"+addr+"/v1/events", "application/json", bytes.NewBufferString(`{"a":1}`)); err == nil {
		t.Fatal("post after shutdown succeeded")
	}
	if err := g.Shutdown(context.Background()); err != nil {
		t.Fatalf("second shutdown: %v", err)
	}
}

func TestStatsEndpoint(t *testing.T) {
	cfg := testCfg(t, t.TempDir(), `
destinations:
  - { name: archive, type: file, config: { path: "$OUT/a.jsonl" } }
routes:
  - { match: {}, to: [archive, ghost] }
`)
	g := start(t, cfg, nil)
	defer shutdown(t, g)
	post(t, g.Addr(), 5, plain)
	eventually(t, "delivery", func() bool { return g.Delivered() == 5 && g.st.Pending(1) == 5 })
	resp, err := http.Get("http://" + g.Addr() + "/v1/stats")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var st Stats
	if err := json.NewDecoder(resp.Body).Decode(&st); err != nil {
		t.Fatal(err)
	}
	if len(st.Destinations) != 2 || st.Destinations[0].Name != "archive" || st.Destinations[0].Delivered != 5 {
		t.Fatalf("stats %+v", st)
	}
	if gh := st.Destinations[1]; gh.Name != "ghost" || gh.Registry != "absent" || gh.L1Pending != 5 {
		t.Fatalf("ghost stats %+v", gh)
	}
}
