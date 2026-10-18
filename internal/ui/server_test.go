package ui

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"testing"
	"time"
)

func testSnapshot() Snapshot {
	lost := uint64(0)
	return Snapshot{
		At: time.Now().UnixMilli(), Mode: "demo",
		Gateway: GatewayView{Status: "running", Boots: 1, Acked: 3},
		Dests:   []DestRow{{Name: "soar", Type: "webhook", Lifecycle: "active", Breaker: "closed", Fault: &FaultView{Mode: "ok"}}},
		Demo: &DemoView{Recon: []ReconRow{{Dest: "soar", Expected: 3, Received: 3, Lost: &lost, Settled: true}},
			Masked: []MaskedEvent{{Seq: 1, Level: "general", Masked: `{"msg":"call 138****5678"}`, Hits: 1}}},
	}
}

func TestAssetsEmbedded(t *testing.T) {
	for _, name := range []string{"index.html", "app.js", "app.css"} {
		b, err := fs.ReadFile(Assets(), name)
		if err != nil || len(b) == 0 {
			t.Fatalf("asset %s: %v (%d bytes)", name, err, len(b))
		}
		// Self-contained: no CDN or other remote resources.
		if regexp.MustCompile(`(?i)(src|href)\s*=\s*["']?(https?:)?//`).Match(b) || strings.Contains(string(b), "https://") {
			t.Fatalf("asset %s references a remote resource", name)
		}
	}
	srv := httptest.NewServer(Handler(Options{Snapshot: testSnapshot}))
	defer srv.Close()
	for path, want := range map[string]string{"/ui/": "<title>", "/ui/app.js": "EventSource", "/ui/app.css": "prefers-color-scheme"} {
		resp, err := http.Get(srv.URL + path)
		if err != nil {
			t.Fatal(err)
		}
		b, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK || !strings.Contains(string(b), want) {
			t.Fatalf("GET %s: %d, missing %q", path, resp.StatusCode, want)
		}
		if csp := resp.Header.Get("Content-Security-Policy"); !strings.Contains(csp, "default-src 'self'") {
			t.Fatalf("GET %s: CSP %q", path, csp)
		}
	}
}

func TestStreamEmitsValidJSON(t *testing.T) {
	srv := httptest.NewServer(Handler(Options{Snapshot: testSnapshot, Interval: 20 * time.Millisecond}))
	defer srv.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL+"/ui/api/stream", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if ct := resp.Header.Get("Content-Type"); ct != "text/event-stream" {
		t.Fatalf("content type %q", ct)
	}
	sc := bufio.NewScanner(resp.Body)
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	events, sawName := 0, false
	for events < 3 && sc.Scan() {
		line := sc.Text()
		if line == "event: snapshot" {
			sawName = true
		}
		data, ok := strings.CutPrefix(line, "data: ")
		if !ok {
			continue
		}
		if !json.Valid([]byte(data)) {
			t.Fatalf("invalid JSON in SSE data: %q", data)
		}
		var s Snapshot
		if err := json.Unmarshal([]byte(data), &s); err != nil || s.Mode != "demo" || len(s.Demo.Recon) != 1 || *s.Demo.Recon[0].Lost != 0 {
			t.Fatalf("decode snapshot: %v %+v", err, s)
		}
		events++
	}
	if events < 3 || !sawName {
		t.Fatalf("got %d events (named: %v), err %v", events, sawName, sc.Err())
	}
}

func TestSnapshotEndpoint(t *testing.T) {
	srv := httptest.NewServer(Handler(Options{Snapshot: testSnapshot}))
	defer srv.Close()
	resp, err := http.Get(srv.URL + "/ui/api/snapshot")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var s Snapshot
	if err := json.NewDecoder(resp.Body).Decode(&s); err != nil || s.Gateway.Acked != 3 {
		t.Fatalf("snapshot: %v %+v", err, s)
	}
}

func TestDemoEndpointsOnlyInDemoMode(t *testing.T) {
	paths := []string{"/demo/fault", "/demo/kill-restart", "/demo/unregister", "/demo/register"}
	off := httptest.NewServer(Handler(Options{Snapshot: testSnapshot}))
	defer off.Close()
	for _, p := range paths {
		resp, err := http.Post(off.URL+p, "application/json", strings.NewReader(`{"dest":"soar","mode":"down"}`))
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusNotFound && resp.StatusCode != http.StatusMethodNotAllowed {
			t.Fatalf("demo off: POST %s = %d, want 404/405", p, resp.StatusCode)
		}
	}
	hit := 0
	on := httptest.NewServer(Handler(Options{Snapshot: testSnapshot, Demo: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hit++
		w.WriteHeader(http.StatusTeapot)
	})}))
	defer on.Close()
	for _, p := range paths {
		resp, err := http.Post(on.URL+p, "application/json", strings.NewReader(`{}`))
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusTeapot {
			t.Fatalf("demo on: POST %s = %d", p, resp.StatusCode)
		}
	}
	if hit != len(paths) {
		t.Fatalf("demo handler hit %d times", hit)
	}
}
