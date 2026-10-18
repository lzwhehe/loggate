package demo

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/lzwhehe/loggate/internal/mask"
	"github.com/lzwhehe/loggate/internal/piibench"
	"github.com/lzwhehe/loggate/internal/ui"
)

func eventually(t *testing.T, what string, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func row(s ui.Snapshot, dest string) (ui.ReconRow, bool) {
	for _, r := range s.Demo.Recon {
		if r.Dest == dest {
			return r, true
		}
	}
	return ui.ReconRow{}, false
}

func destRow(s ui.Snapshot, dest string) ui.DestRow {
	for _, d := range s.Dests {
		if d.Name == dest {
			return d
		}
	}
	return ui.DestRow{}
}

func allSettledNoLoss(t *testing.T, s ui.Snapshot) bool {
	t.Helper()
	if len(s.Demo.Recon) != 3 {
		return false
	}
	for _, r := range s.Demo.Recon {
		if r.Misrouted != 0 {
			t.Fatalf("%s: misrouted %d", r.Dest, r.Misrouted)
		}
		if r.Lost != nil && *r.Lost != 0 {
			t.Fatalf("%s: lost %d", r.Dest, *r.Lost)
		}
		if !r.Settled || r.Outstanding != 0 {
			return false
		}
	}
	return true
}

// The whole demo story in-process: traffic, soar down (L1 grows while the
// healthy destinations keep delivering), restore (L1 drains), simulated
// crash + restart, and the reconciliation ends with lost = 0 everywhere.
func TestDemoEndToEnd(t *testing.T) {
	if testing.Short() {
		t.Skip("end-to-end demo")
	}
	r, err := New(Options{Dir: t.TempDir(), EPS: 200, Seed: 7})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := r.Start(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = r.Stop(context.Background()) })

	eventually(t, "traffic", 20*time.Second, func() bool { return r.load.acked.Load() > 200 })
	if err := r.Fault("soar", ModeDown, 0); err != nil {
		t.Fatal(err)
	}
	eventually(t, "soar L1 backlog", 20*time.Second, func() bool { return destRow(r.Snapshot(), "soar").L1Pending > 20 })
	archiveBefore := destRow(r.Snapshot(), "archive").Delivered
	eventually(t, "archive keeps delivering", 10*time.Second, func() bool {
		return destRow(r.Snapshot(), "archive").Delivered > archiveBefore+50
	})
	if err := r.Fault("soar", ModeOK, 0); err != nil {
		t.Fatal(err)
	}
	eventually(t, "soar drained", 30*time.Second, func() bool { return destRow(r.Snapshot(), "soar").L1Pending == 0 })

	if err := r.KillRestart(ctx); err != nil {
		t.Fatal(err)
	}
	if s := r.Snapshot(); s.Gateway.Boots != 2 || s.Gateway.Status != "running" {
		t.Fatalf("after restart: %+v", s.Gateway)
	}
	before := r.load.acked.Load()
	eventually(t, "traffic after restart", 20*time.Second, func() bool { return r.load.acked.Load() > before+100 })

	r.load.paused.Store(true)
	eventually(t, "reconciliation settles with lost=0", 30*time.Second, func() bool { return allSettledNoLoss(t, r.Snapshot()) })
	s := r.Snapshot()
	for _, d := range []string{"archive", "siem", "soar"} {
		rr, _ := row(s, d)
		if rr.Expected == 0 || rr.Received != rr.Expected {
			t.Fatalf("%s: %+v", d, rr)
		}
	}
	if sq, _ := row(s, "siem"); sq.StoredDup != 0 {
		t.Fatalf("siem stored duplicates: %+v", sq)
	}
	if sr, _ := row(s, "soar"); sr.StoredDup != 0 {
		t.Fatalf("soar duplicates under new idem keys: %+v", sr)
	}
	if destRow(s, "soar").LevelBlocked == 0 {
		t.Fatalf("expected max_level blocks for soar")
	}

	// Masking panel: post-mask content only; a fresh engine finds no PII
	// left in what the dashboard shows.
	if len(s.Demo.Masked) != maskRingSize {
		t.Fatalf("masked ring has %d events", len(s.Demo.Masked))
	}
	eng, err := mask.New(mask.Config{})
	if err != nil {
		t.Fatal(err)
	}
	for _, m := range s.Demo.Masked {
		if _, res := eng.MaskString(m.Masked); res.Hits != 0 {
			t.Fatalf("dashboard event %d still contains PII (%d hits)", m.Seq, res.Hits)
		}
	}
}

func TestGuard(t *testing.T) {
	h := guard(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) }))
	cases := []struct {
		name, remote, host, hdr string
		want                    int
	}{
		{"ok", "127.0.0.1:5000", "127.0.0.1:8081", "1", http.StatusNoContent},
		{"localhost name", "[::1]:5000", "localhost:8081", "1", http.StatusNoContent},
		{"remote client", "192.0.2.10:5000", "127.0.0.1:8081", "1", http.StatusForbidden},
		{"rebinding host", "127.0.0.1:5000", "evil.example:8081", "1", http.StatusForbidden},
		{"no csrf header", "127.0.0.1:5000", "127.0.0.1:8081", "", http.StatusForbidden},
	}
	for _, c := range cases {
		req := httptest.NewRequest(http.MethodPost, "/demo/fault", strings.NewReader(`{}`))
		req.RemoteAddr, req.Host = c.remote, c.host
		if c.hdr != "" {
			req.Header.Set("X-LogGate-Demo", c.hdr)
		}
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		if rec.Code != c.want {
			t.Errorf("%s: %d, want %d", c.name, rec.Code, c.want)
		}
	}
}

func TestGeneratorOracle(t *testing.T) {
	g := &generator{s: piibench.NewSynth(1)}
	kinds := map[string]int{}
	soar := 0
	for range 2000 {
		ev := g.next()
		kinds[ev.kind]++
		if ev.targets&bitArchive == 0 || ev.targets&bitSIEM == 0 {
			t.Fatalf("every event goes to archive and siem: %+v", ev)
		}
		if ev.targets&bitSOAR != 0 {
			soar++
			if ev.kind == "app_json" {
				t.Fatalf("app events are not auth events")
			}
		}
	}
	if kinds["sshd"] == 0 || kinds["auth_json"] == 0 || kinds["app_json"] == 0 || soar == 0 {
		t.Fatalf("kinds %v soar %d", kinds, soar)
	}
}

func TestNewRejectsUsedDir(t *testing.T) {
	dir := t.TempDir()
	r, err := New(Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := r.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := New(Options{Dir: dir}); err == nil {
		t.Fatal("reusing a demo data dir must fail")
	}
}
