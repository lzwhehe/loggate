package httpingest

import (
	"bytes"
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
	if !bytes.Equal(ev.Raw, []byte(`{"msg":"hello","n":1}`)) {
		t.Fatalf("raw = %s", ev.Raw)
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
