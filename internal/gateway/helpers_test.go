package gateway

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/lzwhehe/loggate/internal/config"
	"github.com/lzwhehe/loggate/internal/sink"
)

// fakeSink is an in-process sink whose behaviour tests control.
type fakeSink struct {
	mu     sync.Mutex
	got    map[uint64]int // successful deliveries per seq
	calls  int
	fail   func(env *sink.Envelope) error // nil result = success
	gate   chan struct{}                  // if set, Write blocks until closed
	inside chan int                       // if set, receives the batch size when Write blocks
}

func newFake() *fakeSink { return &fakeSink{got: map[uint64]int{}} }

func (f *fakeSink) setFail(fn func(*sink.Envelope) error) {
	f.mu.Lock()
	f.fail = fn
	f.mu.Unlock()
}

func (f *fakeSink) write(ctx context.Context, batch []*sink.Envelope) []error {
	f.mu.Lock()
	gate, inside := f.gate, f.inside
	f.mu.Unlock()
	if gate != nil {
		if inside != nil {
			inside <- len(batch)
		}
		select {
		case <-gate:
		case <-ctx.Done():
			return sink.FillErrors(len(batch), ctx.Err())
		}
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	errs := make([]error, len(batch))
	for i, env := range batch {
		if f.fail != nil {
			if err := f.fail(env); err != nil {
				errs[i] = err
				continue
			}
		}
		f.got[env.Seq]++
	}
	return errs
}

func (f *fakeSink) snapshot() map[uint64]int {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make(map[uint64]int, len(f.got))
	for k, v := range f.got {
		out[k] = v
	}
	return out
}

func (f *fakeSink) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.got)
}

// fakeHandle binds a configured destination to a test-owned fakeSink by
// config.name.
type fakeHandle struct {
	fakes map[string]*fakeSink
	f     *fakeSink
}

func (h *fakeHandle) Init(_ context.Context, cfg map[string]any) error {
	n, _ := cfg["name"].(string)
	if h.f = h.fakes[n]; h.f == nil {
		return fmt.Errorf("no fake named %q", n)
	}
	return nil
}
func (h *fakeHandle) Write(ctx context.Context, b []*sink.Envelope) []error { return h.f.write(ctx, b) }
func (h *fakeHandle) Health(context.Context) error                          { return nil }
func (h *fakeHandle) Close(context.Context) error                           { return nil }
func (h *fakeHandle) Idempotent() bool                                      { return false }

func registryWith(fakes map[string]*fakeSink) *sink.Registry {
	r := BuiltinSinks()
	_ = r.Register("fake", func() sink.Sink { return &fakeHandle{fakes: fakes} })
	return r
}

// testCfg parses yaml ($OUT = output dir) with fast default intervals
// (unless body sets wal:/retry:), then turns destinations whose path starts
// with "fake:" into fakes.
func testCfg(t *testing.T, dir, body string) *config.Config {
	t.Helper()
	y := fmt.Sprintf(`
node_id: gw-test
data_dir: %q
ingest: { http: { listen: "127.0.0.1:0", max_body: 4MiB } }
`, filepath.Join(dir, "data"))
	if !strings.Contains(body, "\nwal:") {
		y += "wal: { segment_bytes: 64KiB, fsync_interval: 1ms, fsync_max_events: 256 }\n"
	}
	if !strings.Contains(body, "\nretry:") {
		y += "retry: { base: 2ms, max: 20ms, max_attempts: 1000, max_age: 1h, redeliver_interval: 5ms }\n"
	}
	y += body
	y = strings.ReplaceAll(y, "$OUT", filepath.ToSlash(filepath.Join(dir, "out")))
	c, err := config.Parse([]byte(y))
	if err != nil {
		t.Fatal(err)
	}
	for i, d := range c.Destinations {
		if p, _ := d.Config["path"].(string); strings.HasPrefix(p, "fake:") {
			c.Destinations[i].Type = "fake"
			c.Destinations[i].Config = map[string]any{"name": strings.TrimPrefix(p, "fake:")}
		}
	}
	return c
}

func start(t *testing.T, cfg *config.Config, fakes map[string]*fakeSink, mod ...func(*Options)) *Gateway {
	t.Helper()
	o := Options{IdemKey: []byte("e2e-test-key-0123456789"), Sinks: registryWith(fakes), BackoffSeed: 1}
	for _, m := range mod {
		m(&o)
	}
	g, err := New(context.Background(), cfg, o)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(g.crash) // no-op after Shutdown; releases files if a test fails early
	if err := g.Start(); err != nil {
		t.Fatal(err)
	}
	return g
}

func shutdown(t *testing.T, g *Gateway) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := g.Shutdown(ctx); err != nil {
		t.Fatalf("shutdown: %v", err)
	}
}

func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

type acceptResp struct {
	Accepted int    `json:"accepted"`
	FirstSeq uint64 `json:"first_seq"`
}

// post sends one request with n events built by mk and returns acked seqs.
func post(t *testing.T, addr string, n int, mk func(i int) string) []uint64 {
	t.Helper()
	s, err := postErr(addr, n, mk)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func postErr(addr string, n int, mk func(i int) string) ([]uint64, error) {
	var body bytes.Buffer
	body.WriteByte('[')
	for i := range n {
		if i > 0 {
			body.WriteByte(',')
		}
		body.WriteString(mk(i))
	}
	body.WriteByte(']')
	resp, err := http.Post("http://"+addr+"/v1/events", "application/json", &body)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	var ar acceptResp
	if err := json.NewDecoder(resp.Body).Decode(&ar); err != nil || resp.StatusCode != http.StatusOK || ar.Accepted != n {
		return nil, fmt.Errorf("post: status %d accepted %d err %v", resp.StatusCode, ar.Accepted, err)
	}
	out := make([]uint64, n)
	for i := range out {
		out[i] = ar.FirstSeq + uint64(i)
	}
	return out, nil
}

func plain(i int) string { return fmt.Sprintf(`{"i":%d,"msg":"login failed"}`, i) }

// postEvents sends workers*perWorker requests of batch events concurrently.
func postEvents(t *testing.T, addr string, workers, perWorker, batch int) []uint64 {
	t.Helper()
	var mu sync.Mutex
	var acked []uint64
	var wg sync.WaitGroup
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range perWorker {
				s, err := postErr(addr, batch, plain)
				if err != nil {
					t.Error(err)
					return
				}
				mu.Lock()
				acked = append(acked, s...)
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	return acked
}

// readFileSink returns seq -> count from a file sink.
func readFileSink(t *testing.T, path string) map[uint64]int {
	t.Helper()
	f, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return map[uint64]int{}
	}
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	seqs := map[uint64]int{}
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		var line struct {
			Seq uint64 `json:"seq"`
		}
		if err := json.Unmarshal(sc.Bytes(), &line); err != nil {
			t.Fatalf("bad line %q: %v", sc.Text(), err)
		}
		seqs[line.Seq]++
	}
	return seqs
}

// checkExactlyOnce asserts got has exactly the acked seqs, once each.
func checkExactlyOnce(t *testing.T, what string, got map[uint64]int, acked []uint64) {
	t.Helper()
	want := map[uint64]bool{}
	for _, s := range acked {
		want[s] = true
	}
	for s := range want {
		if got[s] != 1 {
			t.Fatalf("%s: acked seq %d delivered %d times", what, s, got[s])
		}
	}
	for s, n := range got {
		if !want[s] {
			t.Fatalf("%s: unexpected seq %d delivered %d times", what, s, n)
		}
	}
}

// checkAtLeastOnce asserts every acked seq was delivered.
func checkAtLeastOnce(t *testing.T, what string, got map[uint64]int, acked []uint64) {
	t.Helper()
	for _, s := range acked {
		if got[s] < 1 {
			t.Fatalf("%s: acked seq %d lost", what, s)
		}
	}
}
