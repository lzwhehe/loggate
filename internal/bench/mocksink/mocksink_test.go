package mocksink

import (
	"bytes"
	"fmt"
	"net/http"
	"path/filepath"
	"testing"
	"time"

	"github.com/lzwhehe/loggate/internal/bench/journal"
)

func start(t *testing.T) (*Server, string) {
	t.Helper()
	dir := t.TempDir()
	s, err := Start(Config{Sinks: map[string]string{"soar": "127.0.0.1:0"}, JournalDir: dir})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s, dir
}

func body(seqs ...uint64) []byte {
	var b bytes.Buffer
	b.WriteByte('[')
	for i, s := range seqs {
		if i > 0 {
			b.WriteByte(',')
		}
		fmt.Fprintf(&b, `{"seq":%d,"idem_key":"k%d","fields":{"unmapped":{"run_id":"r","test_seq":%d,"scheduled_send_ns":1792247327050031700}}}`, s+100, s, s)
	}
	b.WriteByte(']')
	return b.Bytes()
}

func post(addr string, b []byte) (int, error) {
	c := &http.Client{Timeout: 5 * time.Second}
	resp, err := c.Post("http://"+addr+"/ingest", "application/json", bytes.NewReader(b))
	if err != nil {
		return 0, err
	}
	resp.Body.Close()
	return resp.StatusCode, nil
}

func journalOf(t *testing.T, s *Server, dir string) []journal.Record {
	t.Helper()
	if err := s.Flush(); err != nil {
		t.Fatal(err)
	}
	recs, torn, err := journal.ReadFile(filepath.Join(dir, "soar.csv"))
	if err != nil || torn != 0 {
		t.Fatalf("journal: %v torn %d", err, torn)
	}
	return recs
}

func TestOKJournalsEveryEvent(t *testing.T) {
	s, dir := start(t)
	addr := s.Addr("soar")
	if c, err := post(addr, body(1, 2, 3)); err != nil || c != 200 {
		t.Fatalf("post: %d %v", c, err)
	}
	recs := journalOf(t, s, dir)
	if len(recs) != 3 || recs[0].TestSeq != 1 || recs[0].IdemKey != "k1" || recs[0].GWSeq != 101 ||
		recs[0].ScheduledNS != 1792247327050031700 || recs[0].Status != 200 || recs[0].RunID != "r" || recs[0].Dest != "soar" {
		t.Fatalf("%+v", recs)
	}
	if c, _ := post(addr, []byte("not json")); c != 400 {
		t.Fatalf("bad body: %d", c)
	}
	if st := s.Stats()[0]; st.Requests != 1 || st.Accepted != 3 || st.BadRequests != 1 {
		t.Fatalf("stats %+v", st)
	}
}

// down closes the listener: clients get a connection error, not an HTTP
// status; ok re-binds the same address.
func TestDownRefusesConnections(t *testing.T) {
	s, dir := start(t)
	addr := s.Addr("soar")
	if err := s.SetFault(Fault{Dest: "soar", Mode: "down"}); err != nil {
		t.Fatal(err)
	}
	if c, err := post(addr, body(1)); err == nil {
		t.Fatalf("down sink answered HTTP %d", c)
	}
	if st := s.Stats()[0]; st.Up {
		t.Fatal("stats say up")
	}
	if err := s.SetFault(Fault{Dest: "soar", Mode: "ok"}); err != nil {
		t.Fatal(err)
	}
	if s.Addr("soar") != addr {
		t.Fatalf("address changed: %s -> %s", addr, s.Addr("soar"))
	}
	if c, err := post(addr, body(2)); err != nil || c != 200 {
		t.Fatalf("after restore: %d %v", c, err)
	}
	if recs := journalOf(t, s, dir); len(recs) != 1 || recs[0].TestSeq != 2 {
		t.Fatalf("journal %+v", recs)
	}
}

// slow holds the request; a client that gives up still gets its events
// journaled (the receiver processed them).
func TestSlowProcessesAfterClientTimeout(t *testing.T) {
	s, dir := start(t)
	addr := s.Addr("soar")
	if err := s.SetFault(Fault{Dest: "soar", Mode: "slow", DelayMS: 300}); err != nil {
		t.Fatal(err)
	}
	c := &http.Client{Timeout: 50 * time.Millisecond}
	t0 := time.Now()
	if resp, err := c.Post("http://"+addr+"/ingest", "application/json", bytes.NewReader(body(7))); err == nil {
		resp.Body.Close()
		t.Fatal("expected client timeout")
	}
	deadline := time.Now().Add(5 * time.Second)
	for s.Stats()[0].Accepted == 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	recs := journalOf(t, s, dir)
	if len(recs) != 1 || recs[0].Status != 200 {
		t.Fatalf("journal %+v", recs)
	}
	if d := time.Unix(0, recs[0].RecvNS).Sub(t0); d < 290*time.Millisecond {
		t.Fatalf("recorded after %v, want >= delay", d)
	}
}

// Error injection is a pure function of (seed, request index).
func TestErrorInjectionDeterministic(t *testing.T) {
	statuses := func() []int {
		s, _ := start(t)
		if err := s.SetFault(Fault{Dest: "soar", Mode: "ok", ErrorCode: 400, ErrorRate: 0.3, Seed: 42}); err != nil {
			t.Fatal(err)
		}
		var out []int
		for i := range 200 {
			c, err := post(s.Addr("soar"), body(uint64(i+1)))
			if err != nil {
				t.Fatal(err)
			}
			out = append(out, c)
		}
		return out
	}
	a, b := statuses(), statuses()
	bad := 0
	for i := range a {
		if a[i] != b[i] {
			t.Fatalf("request %d: %d vs %d", i, a[i], b[i])
		}
		want := 200
		if Uniform(42, uint64(i)) < 0.3 {
			want = 400
			bad++
		}
		if a[i] != want {
			t.Fatalf("request %d: got %d want %d", i, a[i], want)
		}
	}
	if bad < 40 || bad > 80 {
		t.Fatalf("%d/200 rejected, want about 60", bad)
	}
}

func TestFaultValidation(t *testing.T) {
	for _, f := range []Fault{{Mode: "broken"}, {Mode: "ok", ErrorRate: 2}, {Mode: "ok", ErrorRate: 0.5, ErrorCode: 200}, {Mode: "slow", DelayMS: -1}} {
		if f.Validate() == nil {
			t.Fatalf("%+v accepted", f)
		}
	}
	s, _ := start(t)
	if err := s.SetFault(Fault{Dest: "nope", Mode: "ok"}); err == nil {
		t.Fatal("unknown sink accepted")
	}
}

func TestUniformRange(t *testing.T) {
	var sum float64
	for n := uint64(0); n < 10000; n++ {
		u := Uniform(1, n)
		if u < 0 || u >= 1 {
			t.Fatalf("u=%v", u)
		}
		sum += u
	}
	if m := sum / 10000; m < 0.48 || m > 0.52 {
		t.Fatalf("mean %v", m)
	}
}
