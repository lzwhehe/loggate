package loadgen

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"
)

func TestScheduleMath(t *testing.T) {
	if n := Count(2000, 5*time.Minute); n != 600000 {
		t.Fatalf("count = %d", n)
	}
	if n := Count(3, 1500*time.Millisecond); n != 4 {
		t.Fatalf("count(3 eps, 1.5s) = %d", n)
	}
	start := time.Unix(100, 0)
	if got := ScheduledAt(start, 2000, 0); !got.Equal(start) {
		t.Fatalf("event 0 at %v", got)
	}
	if got := ScheduledAt(start, 2000, 1); got.Sub(start) != 500*time.Microsecond {
		t.Fatalf("event 1 offset %v", got.Sub(start))
	}
	// No drift: event i is exactly i/rate after start (rounded to ns),
	// not an accumulation of rounded intervals.
	if got := ScheduledAt(start, 3, 3_000_000); got.Sub(start) != 1_000_000*time.Second {
		t.Fatalf("event 3e6 at 3 eps offset %v", got.Sub(start))
	}
	if got := ScheduledAt(start, 3, 1); got.Sub(start) != 333333333*time.Nanosecond {
		t.Fatalf("1/3 s = %v", got.Sub(start))
	}
}

func TestClassOfDeterministicAndWeighted(t *testing.T) {
	cls := []Class{{"auth", 0.5}, {"net", 0.3}, {"app", 0.2}}
	counts := map[string]int{}
	for s := uint64(1); s <= 100000; s++ {
		c := ClassOf(cls, 7, s)
		if c != ClassOf(cls, 7, s) {
			t.Fatal("not deterministic")
		}
		counts[c]++
	}
	for _, c := range cls {
		got := float64(counts[c.Name]) / 100000
		if math.Abs(got-c.Weight) > 0.01 {
			t.Fatalf("class %s share %.3f, want %.2f", c.Name, got, c.Weight)
		}
	}
	diff := 0
	for s := uint64(1); s <= 1000; s++ {
		if ClassOf(cls, 7, s) != ClassOf(cls, 8, s) {
			diff++
		}
	}
	if diff == 0 {
		t.Fatal("seed has no effect")
	}
}

func TestBodyIsValidJSON(t *testing.T) {
	b := Body("run-1", 42, 1792247327050031700, "auth", 300)
	var m map[string]any
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.UseNumber()
	if err := dec.Decode(&m); err != nil {
		t.Fatal(err)
	}
	if m["test_seq"].(json.Number).String() != "42" || m["scheduled_send_ns"].(json.Number).String() != "1792247327050031700" || m["class"] != "auth" {
		t.Fatalf("%v", m)
	}
	if len(b) != 300 {
		t.Fatalf("payload size %d", len(b))
	}
}

// Open loop: a slow server does not slow the sender down, and latency is
// measured from the scheduled time. Only 200s are promised.
func TestRunOpenLoopAndPromisedSet(t *testing.T) {
	var n atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer tok" {
			w.WriteHeader(401)
			return
		}
		b, _ := io.ReadAll(r.Body)
		var ev struct {
			TestSeq uint64 `json:"test_seq"`
		}
		_ = json.Unmarshal(b, &ev)
		n.Add(1)
		time.Sleep(100 * time.Millisecond) // much slower than the 5ms interval
		switch {
		case ev.TestSeq%10 == 0:
			w.WriteHeader(429)
		case ev.TestSeq%10 == 5:
			w.WriteHeader(503)
		default:
			w.WriteHeader(200)
		}
	}))
	defer srv.Close()
	out := t.TempDir()
	start := time.Now().Add(50 * time.Millisecond)
	sum, res, err := Run(context.Background(), Config{
		URL: srv.URL, Token: "tok", RunID: "t", Rate: 200, Duration: time.Second, Start: start,
		Seed: 1, Classes: []Class{{"a", 1}}, PayloadSize: 64, OutDir: out,
	})
	if err != nil {
		t.Fatal(err)
	}
	if sum.Planned != 200 || len(res) != 200 {
		t.Fatalf("planned %d", sum.Planned)
	}
	if sum.Acked != 160 || sum.ByStatus["429"] != 20 || sum.ByStatus["503"] != 20 {
		t.Fatalf("by status %v acked %d", sum.ByStatus, sum.Acked)
	}
	// All 200 requests were dispatched within ~1s although each takes 100ms.
	last := res[len(res)-1]
	if lag := time.Duration(last.SentNS - last.ScheduledNS); lag > 50*time.Millisecond {
		t.Fatalf("sender fell behind: last dispatch lag %v", lag)
	}
	if sum.AckLatencyMS.P50 < 100 {
		t.Fatalf("latency must include server time from schedule, p50 %.1f", sum.AckLatencyMS.P50)
	}
	if sum.MaxInFlight < 10 {
		t.Fatalf("expected concurrency, max inflight %d", sum.MaxInFlight)
	}
	back, err := ReadSent(filepath.Join(out, "sent.csv"))
	if err != nil || len(back) != 200 {
		t.Fatalf("read back: %d %v", len(back), err)
	}
	if back[9].Status != 429 || back[0].Status != 200 || back[0].ScheduledNS != start.UnixNano() {
		t.Fatalf("round trip: %+v %+v", back[0], back[9])
	}
	bm, err := os.ReadFile(filepath.Join(out, "acked.bitmap"))
	if err != nil {
		t.Fatal(err)
	}
	acked := 0
	for i := range 200 {
		if bm[i/8]&(1<<(i%8)) != 0 {
			acked++
			if res[i].Status != 200 {
				t.Fatalf("bitmap marks %d acked", i+1)
			}
		}
	}
	if acked != 160 {
		t.Fatalf("bitmap acked %d", acked)
	}
}

func TestRetriesKeepTestSeq(t *testing.T) {
	var calls atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) == 1 {
			w.WriteHeader(503)
			return
		}
		w.WriteHeader(200)
	}))
	defer srv.Close()
	sum, res, err := Run(context.Background(), Config{URL: srv.URL, RunID: "t", Rate: 10, Duration: 100 * time.Millisecond,
		Classes: []Class{{"a", 1}}, Retries: 2, RetryDelay: time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	if sum.Acked != 1 || res[0].Attempts != 2 || res[0].TestSeq != 1 {
		t.Fatalf("%+v %+v", sum, res[0])
	}
}

func TestParseClasses(t *testing.T) {
	c, err := ParseClasses("auth=0.5, net=0.3,app=0.2")
	if err != nil || len(c) != 3 || c[1].Name != "net" || c[1].Weight != 0.3 {
		t.Fatalf("%v %v", c, err)
	}
	for _, bad := range []string{"", "a", "a=0", "=1", "a=x"} {
		if _, err := ParseClasses(bad); err == nil {
			t.Fatalf("%q accepted", bad)
		}
	}
}
