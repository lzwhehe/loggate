package httpingest

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/lzwhehe/loggate/internal/clock"
	"github.com/lzwhehe/loggate/internal/event"
	"github.com/lzwhehe/loggate/internal/mask"
	"github.com/lzwhehe/loggate/internal/wal"
)

type allRouter struct{}

func (allRouter) Route(*event.LogEvent) event.DestMask { return event.DestMask(0).Set(0) }

type env struct {
	h   *Handler
	w   *wal.WAL
	ffs *wal.FaultFS
	fc  *clock.Fake
}

func newEnv(t *testing.T, walOpts wal.Options, opts Options) *env {
	t.Helper()
	ffs := wal.NewFaultFS(nil)
	walOpts.Dir = t.TempDir()
	walOpts.FS = ffs
	if walOpts.FsyncInterval == 0 {
		walOpts.FsyncInterval = -1 // commit as soon as the queue drains
	}
	w, err := wal.Open(walOpts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { w.Close() })
	keyer, _ := event.NewIdemKeyer([]byte("0123456789abcdef0123"), "gw-test", 1)
	opts.Keyer = keyer
	if opts.MaxBody == 0 {
		opts.MaxBody = 1 << 20
	}
	fc := clock.NewFake(time.Unix(1_700_000_000, 0))
	opts.Clock = fc
	h, err := New(w, allRouter{}, opts)
	if err != nil {
		t.Fatal(err)
	}
	return &env{h: h, w: w, ffs: ffs, fc: fc}
}

func (e *env) post(body string, hdr ...string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPost, "/v1/events", strings.NewReader(body))
	for i := 0; i+1 < len(hdr); i += 2 {
		req.Header.Set(hdr[i], hdr[i+1])
	}
	rec := httptest.NewRecorder()
	e.h.ServeHTTP(rec, req)
	return rec
}

func walEvents(t *testing.T, w *wal.WAL) []event.LogEvent {
	t.Helper()
	it := w.NewIterator(0)
	defer it.Close()
	var out []event.LogEvent
	for {
		r, err := it.Next()
		if errors.Is(err, io.EOF) {
			return out
		}
		if err != nil {
			t.Fatal(err)
		}
		var ev event.LogEvent
		if err := json.Unmarshal(r.Payload, &ev); err != nil {
			t.Fatal(err)
		}
		if ev.Seq != r.Seq {
			t.Fatalf("payload seq %d != record seq %d", ev.Seq, r.Seq)
		}
		out = append(out, ev)
	}
}

func TestRespondsOnlyAfterFsync(t *testing.T) {
	// The WAL waits for the fake clock before fsyncing, so the response
	// must not be written until the clock is advanced.
	walClock := clock.NewFake(time.Unix(0, 0))
	e := newEnv(t, wal.Options{Clock: walClock, FsyncInterval: 10 * time.Millisecond, FsyncMaxEvents: 1000}, Options{})

	done := make(chan *httptest.ResponseRecorder)
	go func() { done <- e.post(`{"msg":"hello","n":1}`) }()
	walClock.BlockUntilTimers(1)
	select {
	case <-done:
		t.Fatal("responded before fsync")
	case <-time.After(30 * time.Millisecond):
	}
	if n := e.ffs.SyncCount(); n != 0 {
		t.Fatalf("sync count before advance = %d", n)
	}
	walClock.Advance(10 * time.Millisecond)
	rec := <-done
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s", rec.Code, rec.Body)
	}
	if e.ffs.SyncCount() != 1 {
		t.Fatalf("sync count after response = %d", e.ffs.SyncCount())
	}
	var resp acceptResponse
	json.Unmarshal(rec.Body.Bytes(), &resp)
	if resp.Accepted != 1 || resp.FirstSeq != 1 {
		t.Fatalf("resp = %+v", resp)
	}
	evs := walEvents(t, e.w)
	if len(evs) != 1 {
		t.Fatalf("wal has %d events", len(evs))
	}
	ev := evs[0]
	if ev.SourceID != AnonymousSource || ev.IdemKey.IsZero() || ev.ReceivedAt != e.fc.Now().UnixNano() || !ev.Targets.Has(0) {
		t.Fatalf("event = %+v", ev)
	}
	if um := ev.Fields["unmapped"].(map[string]any); um["msg"] != "hello" {
		t.Fatalf("unmapped = %v", um)
	}
	if ev.Raw != nil || ev.Fields["class_uid"] != float64(0) {
		t.Fatalf("raw = %s class_uid = %v", ev.Raw, ev.Fields["class_uid"])
	}
}

func TestArrayAccepted(t *testing.T) {
	e := newEnv(t, wal.Options{}, Options{})
	rec := e.post(` [{"a":1},{"b":2}, {"c":3}] `)
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d %s", rec.Code, rec.Body)
	}
	var resp acceptResponse
	json.Unmarshal(rec.Body.Bytes(), &resp)
	if resp.Accepted != 3 || resp.FirstSeq != 1 {
		t.Fatalf("resp = %+v", resp)
	}
	rec = e.post(`{"d":4}`)
	json.Unmarshal(rec.Body.Bytes(), &resp)
	if resp.FirstSeq != 4 {
		t.Fatalf("second first_seq = %d", resp.FirstSeq)
	}
	evs := walEvents(t, e.w)
	if len(evs) != 4 {
		t.Fatalf("wal has %d events", len(evs))
	}
	seen := map[event.IdemKey]bool{}
	for _, ev := range evs {
		if seen[ev.IdemKey] {
			t.Fatal("duplicate idem key")
		}
		seen[ev.IdemKey] = true
	}
}

func TestWALFailureReturns503(t *testing.T) {
	e := newEnv(t, wal.Options{}, Options{})
	e.ffs.FailNextSync(errors.New("injected EIO /secret/path"))
	rec := e.post(`[{"a":1},{"a":2}]`)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d", rec.Code)
	}
	if strings.Contains(rec.Body.String(), "secret") {
		t.Fatalf("internal error leaked: %s", rec.Body)
	}
	e.ffs.FailNextWrite(wal.ErrNoSpace)
	if rec := e.post(`{"a":3}`); rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("ENOSPC status = %d", rec.Code)
	}
	if rec := e.post(`{"a":4}`); rec.Code != http.StatusOK {
		t.Fatalf("after recovery status = %d", rec.Code)
	}
	if evs := walEvents(t, e.w); len(evs) != 1 {
		t.Fatalf("wal holds %d events, want only the acked one", len(evs))
	}
}

func TestMaxBody(t *testing.T) {
	e := newEnv(t, wal.Options{}, Options{MaxBody: 64})
	big := `{"pad":"` + strings.Repeat("x", 100) + `"}`
	if rec := e.post(big); rec.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d", rec.Code)
	}
	if rec := e.post(`{"ok":true}`); rec.Code != http.StatusOK {
		t.Fatalf("small body status = %d", rec.Code)
	}
	if n := len(walEvents(t, e.w)); n != 1 {
		t.Fatalf("wal has %d events", n)
	}
}

func TestBadRequests(t *testing.T) {
	e := newEnv(t, wal.Options{}, Options{})
	for _, body := range []string{``, `   `, `"str"`, `42`, `[]`, `[1,2]`, `[{"a":1}, "x"]`, `{"a":`, `{"a":1}{"b":2}`, `[{"a":1}`} {
		if rec := e.post(body); rec.Code != http.StatusBadRequest {
			t.Errorf("body %q: status = %d", body, rec.Code)
		}
	}
	if n := len(walEvents(t, e.w)); n != 0 {
		t.Fatalf("wal has %d events", n)
	}
	req := httptest.NewRequest(http.MethodGet, "/v1/events", nil)
	rec := httptest.NewRecorder()
	e.h.ServeHTTP(rec, req)
	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("GET /v1/events = %d", rec.Code)
	}
}

func TestBearerTokenBindsSource(t *testing.T) {
	e := newEnv(t, wal.Options{}, Options{Tokens: map[string]string{"tok-1": "app1", "tok-2": "app2"}})
	if rec := e.post(`{"a":1}`); rec.Code != http.StatusUnauthorized {
		t.Fatalf("no token: %d", rec.Code)
	}
	if rec := e.post(`{"a":1}`, "Authorization", "Bearer nope"); rec.Code != http.StatusUnauthorized {
		t.Fatalf("bad token: %d", rec.Code)
	}
	if rec := e.post(`{"a":1}`, "Authorization", "Bearer tok-2"); rec.Code != http.StatusOK {
		t.Fatalf("good token: %d", rec.Code)
	}
	evs := walEvents(t, e.w)
	if len(evs) != 1 || evs[0].SourceID != "http:app2" {
		t.Fatalf("events = %+v", evs)
	}
}

func TestHealthAndDraining(t *testing.T) {
	e := newEnv(t, wal.Options{}, Options{})
	get := func() int {
		rec := httptest.NewRecorder()
		e.h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/healthz", nil))
		return rec.Code
	}
	if c := get(); c != http.StatusOK {
		t.Fatalf("healthz = %d", c)
	}
	e.h.SetDraining(true)
	if c := get(); c != http.StatusServiceUnavailable {
		t.Fatalf("draining healthz = %d", c)
	}
	if rec := e.post(`{"a":1}`); rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("draining post = %d", rec.Code)
	}
}

func TestBackpressure429AndStats(t *testing.T) {
	var over bool
	e := newEnv(t, wal.Options{}, Options{
		Backpressure: func() bool { return over },
		RetryAfter:   3 * time.Second,
		Stats:        func() any { return map[string]int{"l1_pending": 7} },
	})
	if rec := e.post(`{"a":1}`); rec.Code != http.StatusOK {
		t.Fatalf("status %d", rec.Code)
	}
	over = true
	rec := e.post(`{"a":2}`)
	if rec.Code != http.StatusTooManyRequests || rec.Header().Get("Retry-After") != "3" {
		t.Fatalf("status %d retry-after %q", rec.Code, rec.Header().Get("Retry-After"))
	}
	if n := len(walEvents(t, e.w)); n != 1 {
		t.Fatalf("rejected event reached the WAL (%d events)", n)
	}
	if e.h.Rejected() != 1 {
		t.Fatal("rejected counter")
	}
	req := httptest.NewRequest(http.MethodGet, "/v1/stats", nil)
	srec := httptest.NewRecorder()
	e.h.ServeHTTP(srec, req)
	if srec.Code != http.StatusOK || !strings.Contains(srec.Body.String(), `"l1_pending":7`) {
		t.Fatalf("stats: %d %s", srec.Code, srec.Body.String())
	}
}

// Synthetic test values: a valid-checksum ID number (GB 11643 example), a
// Visa test card, and a phone number with an allocated prefix.
const (
	testID    = "11010519491231002X"
	testCard  = "4111111111111111"
	testPhone = "13812345678"
)

func newMasker(t *testing.T) *mask.Engine {
	t.Helper()
	m, err := mask.New(mask.Config{Fields: map[string]mask.Policy{"unmapped.test_seq": mask.PolicyKeep}})
	if err != nil {
		t.Fatal(err)
	}
	return m
}

func TestMaskedBeforeWAL(t *testing.T) {
	e := newEnv(t, wal.Options{}, Options{Masker: newMasker(t)})
	body := `{"test_seq":` + testCard + `,"id":"` + testID + `","card":"` + testCard + `","note":"call ` + testPhone + `"}`
	if rec := e.post(body); rec.Code != http.StatusOK {
		t.Fatalf("status %d", rec.Code)
	}
	lines := "sshd[1]: Failed password for " + testPhone + " from 192.0.2.1 port 22 ssh2\r\n\nplain " + testID + "\n"
	if rec := e.post(lines, "Content-Type", "text/plain; charset=utf-8"); rec.Code != http.StatusOK {
		t.Fatalf("text status %d %s", rec.Code, rec.Body)
	}
	// Raw WAL bytes: no PII except the field explicitly kept.
	it := e.w.NewIterator(0)
	defer it.Close()
	var payloads []string
	for {
		r, err := it.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		payloads = append(payloads, string(r.Payload))
	}
	if len(payloads) != 3 {
		t.Fatalf("wal has %d records", len(payloads))
	}
	for i, p := range payloads {
		for _, pii := range []string{testID, testPhone, `"` + testCard + `"`, "call " + testCard} {
			if strings.Contains(p, pii) {
				t.Errorf("record %d contains %q: %s", i, pii, p)
			}
		}
	}
	if !strings.Contains(payloads[0], `"test_seq":`+testCard) {
		t.Errorf("kept field was masked: %s", payloads[0])
	}
	evs := walEvents(t, e.w)
	if evs[0].DataLevel != event.LevelImportant || len(evs[0].DataClass) != 3 {
		t.Errorf("json event level %v classes %v", evs[0].DataLevel, evs[0].DataClass)
	}
	if evs[1].ClassUID != 3002 || evs[1].StatusID != 2 || evs[2].ClassUID != 0 {
		t.Errorf("text events: %+v / %+v", evs[1], evs[2])
	}
	if u := evs[1].Fields["user"].(map[string]any)["name"]; u != "138****5678" {
		t.Errorf("user.name %v", u)
	}
}

func TestErrorsDoNotEchoPayload(t *testing.T) {
	e := newEnv(t, wal.Options{}, Options{Masker: newMasker(t)})
	for _, body := range []string{
		`{"id":"` + testID + `" "x":1}`,
		`[{"id":"` + testID + `"}, ` + testID + `]`,
		`{"id":` + testID + `}`,
		testID,
		`[{"a":1}, {"id":"` + testID + `"` + "\x01" + `}]`,
	} {
		rec := e.post(body)
		if rec.Code != http.StatusBadRequest {
			t.Errorf("status %d for %q", rec.Code, body)
		}
		if b := rec.Body.String(); strings.Contains(b, testID[:6]) || strings.Contains(b, "1949") {
			t.Errorf("error response echoes payload: %s", b)
		}
	}
}
