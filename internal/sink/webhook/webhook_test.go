package webhook

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/lzwhehe/loggate/internal/delivery"
	"github.com/lzwhehe/loggate/internal/event"
	"github.com/lzwhehe/loggate/internal/sink"
)

func envs(n int) []*sink.Envelope {
	out := make([]*sink.Envelope, n)
	for i := range out {
		var k event.IdemKey
		k[0] = byte(i + 1)
		out[i] = &sink.Envelope{Seq: uint64(i + 1), Event: &event.LogEvent{Seq: uint64(i + 1), IdemKey: k}}
	}
	return out
}

func TestClassification(t *testing.T) {
	var mu sync.Mutex
	var status int
	var retryAfter string
	var gotKeys string
	var gotLen int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var arr []map[string]any
		_ = json.NewDecoder(r.Body).Decode(&arr)
		mu.Lock()
		defer mu.Unlock()
		gotKeys, gotLen = r.Header.Get("Idempotency-Keys"), len(arr)
		if retryAfter != "" {
			w.Header().Set("Retry-After", retryAfter)
		}
		w.WriteHeader(status)
	}))
	defer srv.Close()

	s := New()
	if err := s.Init(context.Background(), map[string]any{"url": srv.URL, "timeout": "2s"}); err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		code int
		ra   string
		kind delivery.Kind
		wait time.Duration
	}{
		{200, "", 0, 0},
		{503, "", delivery.Retryable, 0},
		{500, "", delivery.Retryable, 0},
		{400, "", delivery.NonRetryable, 0},
		{429, "3", delivery.Throttled, 3 * time.Second},
	}
	for _, c := range cases {
		mu.Lock()
		status, retryAfter = c.code, c.ra
		mu.Unlock()
		errs := s.Write(context.Background(), envs(3))
		if len(errs) != 3 {
			t.Fatalf("%d: %d results", c.code, len(errs))
		}
		for _, err := range errs {
			k, w := delivery.Classify(err)
			if (c.kind == 0) != (err == nil) || k != c.kind || w != c.wait {
				t.Fatalf("%d: err %v kind %v wait %v", c.code, err, k, w)
			}
		}
	}
	mu.Lock()
	if gotLen != 3 || strings.Count(gotKeys, ",") != 2 {
		t.Fatalf("batch len %d keys %q", gotLen, gotKeys)
	}
	mu.Unlock()

	// Connection refused: Retryable.
	srv.Close()
	for _, err := range s.Write(context.Background(), envs(1)) {
		if k, _ := delivery.Classify(err); err == nil || k != delivery.Retryable {
			t.Fatalf("closed server: %v", err)
		}
	}
}

func TestSingleModeSetsIdempotencyKey(t *testing.T) {
	var mu sync.Mutex
	var keys []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		keys = append(keys, r.Header.Get("Idempotency-Key"))
		mu.Unlock()
	}))
	defer srv.Close()
	s := New()
	if err := s.Init(context.Background(), map[string]any{"url": srv.URL, "batch": false}); err != nil {
		t.Fatal(err)
	}
	in := envs(2)
	for _, err := range s.Write(context.Background(), in) {
		if err != nil {
			t.Fatal(err)
		}
	}
	if len(keys) != 2 || keys[0] != in[0].Event.IdemKey.String() {
		t.Fatalf("keys %v", keys)
	}
}
