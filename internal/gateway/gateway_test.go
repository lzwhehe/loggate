package gateway

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/lzwhehe/loggate/internal/config"
)

func testConfig(t *testing.T, dir string) *config.Config {
	t.Helper()
	c, err := config.Parse(fmt.Appendf(nil, `
node_id: gw-e2e
data_dir: %q
wal: { segment_bytes: 64KiB, fsync_interval: 2ms, fsync_max_events: 256 }
ingest: { http: { listen: "127.0.0.1:0", max_body: 1MiB } }
destinations:
  - { name: archive, type: file, config: { path: %q } }
routes:
  - { match: {}, to: [archive] }
`, filepath.Join(dir, "data"), filepath.Join(dir, "out", "archive.jsonl")))
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func startGateway(t *testing.T, cfg *config.Config) *Gateway {
	t.Helper()
	g, err := New(context.Background(), cfg, Options{IdemKey: []byte("e2e-test-key-0123456789")})
	if err != nil {
		t.Fatal(err)
	}
	if err := g.Start(); err != nil {
		t.Fatal(err)
	}
	return g
}

type acceptResp struct {
	Accepted int    `json:"accepted"`
	FirstSeq uint64 `json:"first_seq"`
}

// postEvents sends workers*perWorker requests of batch events each and
// returns every acknowledged seq.
func postEvents(t *testing.T, addr string, workers, perWorker, batch int) []uint64 {
	t.Helper()
	var mu sync.Mutex
	var acked []uint64
	var wg sync.WaitGroup
	client := &http.Client{Timeout: 10 * time.Second}
	for w := range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range perWorker {
				var body bytes.Buffer
				body.WriteByte('[')
				for j := range batch {
					if j > 0 {
						body.WriteByte(',')
					}
					fmt.Fprintf(&body, `{"worker":%d,"req":%d,"idx":%d,"msg":"login failed for user%d"}`, w, i, j, j)
				}
				body.WriteByte(']')
				resp, err := client.Post("http://"+addr+"/v1/events", "application/json", &body)
				if err != nil {
					t.Error(err)
					return
				}
				var ar acceptResp
				err = json.NewDecoder(resp.Body).Decode(&ar)
				resp.Body.Close()
				if resp.StatusCode != http.StatusOK || err != nil || ar.Accepted != batch {
					t.Errorf("status %d accepted %d err %v", resp.StatusCode, ar.Accepted, err)
					return
				}
				mu.Lock()
				for k := range batch {
					acked = append(acked, ar.FirstSeq+uint64(k))
				}
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	return acked
}

// readSink returns seq -> count and idem_key -> count from the file sink.
func readSink(t *testing.T, path string) (map[uint64]int, map[string]int) {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	seqs, keys := map[uint64]int{}, map[string]int{}
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		var line struct {
			Seq     uint64 `json:"seq"`
			IdemKey string `json:"idem_key"`
		}
		if err := json.Unmarshal(sc.Bytes(), &line); err != nil {
			t.Fatalf("bad line %q: %v", sc.Text(), err)
		}
		seqs[line.Seq]++
		keys[line.IdemKey]++
	}
	if err := sc.Err(); err != nil {
		t.Fatal(err)
	}
	return seqs, keys
}

func checkExactlyOnce(t *testing.T, seqs map[uint64]int, acked []uint64) {
	t.Helper()
	ackSet := map[uint64]bool{}
	for _, s := range acked {
		if ackSet[s] {
			t.Fatalf("seq %d acknowledged twice", s)
		}
		ackSet[s] = true
	}
	for s := range ackSet {
		if seqs[s] != 1 {
			t.Fatalf("acked seq %d delivered %d times", s, seqs[s])
		}
	}
	for s, n := range seqs {
		if !ackSet[s] {
			t.Fatalf("unacked seq %d delivered %d times", s, n)
		}
	}
}

func TestEndToEnd1000Events(t *testing.T) {
	dir := t.TempDir()
	cfg := testConfig(t, dir)
	g := startGateway(t, cfg)

	resp, err := http.Get("http://" + g.Addr() + "/healthz")
	if err != nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("healthz: %v %v", resp, err)
	}
	resp.Body.Close()

	acked := postEvents(t, g.Addr(), 20, 10, 5) // 1000 events
	if len(acked) != 1000 {
		t.Fatalf("acked %d events, want 1000", len(acked))
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := g.Shutdown(ctx); err != nil {
		t.Fatalf("shutdown: %v", err)
	}
	if g.Delivered() != 1000 {
		t.Fatalf("delivered = %d", g.Delivered())
	}

	seqs, keys := readSink(t, cfg.Destinations[0].Config["path"].(string))
	if len(seqs) != 1000 {
		t.Fatalf("sink has %d distinct seqs, want 1000", len(seqs))
	}
	checkExactlyOnce(t, seqs, acked)
	if len(keys) != 1000 {
		t.Fatalf("sink has %d distinct idem keys, want 1000", len(keys))
	}

	// Restart on the same data dir: recovery keeps seqs monotonic.
	g2 := startGateway(t, cfg)
	more := postEvents(t, g2.Addr(), 2, 5, 10)
	if err := g2.Shutdown(ctx); err != nil {
		t.Fatalf("shutdown 2: %v", err)
	}
	var maxOld uint64
	for _, s := range acked {
		maxOld = max(maxOld, s)
	}
	for _, s := range more {
		if s <= maxOld {
			t.Fatalf("seq %d reused after restart (old max %d)", s, maxOld)
		}
	}
	seqs, _ = readSink(t, cfg.Destinations[0].Config["path"].(string))
	checkExactlyOnce(t, seqs, append(acked, more...))
}

func TestShutdownRejectsNewIngest(t *testing.T) {
	cfg := testConfig(t, t.TempDir())
	g := startGateway(t, cfg)
	addr := g.Addr()
	if err := g.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := http.Post("http://"+addr+"/v1/events", "application/json", bytes.NewBufferString(`{"a":1}`)); err == nil {
		t.Fatal("post after shutdown succeeded")
	}
	if err := g.Shutdown(context.Background()); err != nil {
		t.Fatalf("second shutdown: %v", err)
	}
}
