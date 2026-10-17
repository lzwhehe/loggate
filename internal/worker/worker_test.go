package worker

import (
	"context"
	"errors"
	"log/slog"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/lzwhehe/loggate/internal/clock"
	"github.com/lzwhehe/loggate/internal/delivery"
	"github.com/lzwhehe/loggate/internal/dlq"
	"github.com/lzwhehe/loggate/internal/event"
	"github.com/lzwhehe/loggate/internal/sink"
	"github.com/lzwhehe/loggate/internal/state"
)

type resolver struct {
	mu  sync.Mutex
	got map[uint64]bool
}

func (r *resolver) Resolve(_ event.DestID, seq uint64) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.got[seq] = true
	return true
}

func (r *resolver) n() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.got)
}

// loader returns synthetic events for any ref (seq = offset).
type loader struct{}

func (loader) Load(ref state.Ref) (*sink.Envelope, error) {
	seq := uint64(ref.Offset)
	return &sink.Envelope{Seq: seq, Event: &event.LogEvent{Seq: seq, ReceivedAt: 1}, SegID: ref.SegID, Offset: ref.Offset, Length: ref.Length}, nil
}

type scriptSink struct {
	mu   sync.Mutex
	errs func(seq uint64) error
	got  []uint64
}

func (s *scriptSink) Init(context.Context, map[string]any) error { return nil }
func (s *scriptSink) Health(context.Context) error               { return nil }
func (s *scriptSink) Close(context.Context) error                { return nil }
func (s *scriptSink) Idempotent() bool                           { return false }
func (s *scriptSink) Write(_ context.Context, b []*sink.Envelope) []error {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]error, len(b))
	for i, e := range b {
		if s.errs != nil {
			out[i] = s.errs(e.Seq)
		}
		if out[i] == nil {
			s.got = append(s.got, e.Seq)
		}
	}
	return out
}

func newDeps(t *testing.T, clk clock.Clock) (*Deps, *resolver) {
	t.Helper()
	dir := t.TempDir()
	st, err := state.Open(state.Options{Path: filepath.Join(dir, "s.db"), Clock: clk})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	d, err := dlq.Open(filepath.Join(dir, "dlq"), 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { d.Close() })
	r := &resolver{got: map[uint64]bool{}}
	return &Deps{
		L1: st, DLQ: d, Resolver: r, Claims: state.NewClaims(), Loader: loader{}, Clock: clk,
		Logger: nil, Backoff: delivery.NewBackoff(time.Second, time.Minute, 1),
		Budget: delivery.Budget{MaxAttempts: 5, MaxAge: time.Hour},
	}, r
}

func envAt(seq uint64) *sink.Envelope {
	return &sink.Envelope{Seq: seq, Event: &event.LogEvent{Seq: seq, ReceivedAt: 1}, Offset: int64(seq), Length: 100}
}

func flush(t *testing.T, d *Deps) {
	t.Helper()
	if err := d.L1.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestEnqueueRedeliverShare(t *testing.T) {
	clk := clock.NewFake(time.Unix(1000, 0))
	d, _ := newDeps(t, clk)
	d.Logger = discard()
	s := New(Config{ID: 0, Name: "a", MaxEvents: 10, RedeliverShare: 0.5}, &scriptSink{}, d)
	s.state = Active // no consumer: inspect the queue only
	for i := range 10 {
		if r := s.Enqueue(envAt(uint64(i + 1))); r != Enqueued {
			t.Fatalf("no backlog: enqueue %d = %v", i, r)
		}
	}
	if s.Enqueue(envAt(11)) != QueueFull {
		t.Fatal("over capacity accepted")
	}
	s.popN(10)
	// With an L1 backlog live traffic gets at most half of L2.
	d.ParkLive(envAt(50), state.ReasonQueueFull, 0, clk.Now(), nil)
	flush(t, d)
	for i := range 5 {
		if r := s.Enqueue(envAt(uint64(20 + i))); r != Enqueued {
			t.Fatalf("backlog: enqueue %d = %v", i, r)
		}
	}
	if r := s.Enqueue(envAt(30)); r != QueueFull {
		t.Fatalf("live exceeded its share: %v", r)
	}
	// Redelivery may use the reserved half.
	if n := s.RedeliverOnce(); n != 1 {
		t.Fatalf("redelivered %d", n)
	}
	if s.Depth() != 6 {
		t.Fatalf("depth %d", s.Depth())
	}
}

// Failures are parked with full-jitter NextAt; breaker-open postponements
// do not consume attempts; budget exhaustion goes to the DLQ.
func TestResultHandlingWithFakeClock(t *testing.T) {
	clk := clock.NewFake(time.Unix(1000, 0))
	d, res := newDeps(t, clk)
	d.Logger = discard()
	sk := &scriptSink{errs: func(uint64) error { return errors.New("down") }}
	s := New(Config{ID: 0, Name: "a", MaxEvents: 100, MaxBatch: 10, BreakerFailures: 2, BreakerOpenFor: 10 * time.Second}, sk, d)
	s.state = Active

	s.process([]*sink.Envelope{envAt(1)}) // attempt 1 fails -> parked SendFailed
	flush(t, d)
	if res.n() != 1 {
		t.Fatal("live failure not resolved after park")
	}
	r, _ := d.L1.Get(1)
	sl := r.Slot(0)
	if sl.Reason != state.ReasonSendFailed || sl.Attempts != 1 || sl.NextAt < clk.Now().UnixNano() || sl.NextAt >= clk.Now().Add(time.Second).UnixNano() {
		t.Fatalf("slot after first failure %+v", sl)
	}
	// Not due before NextAt; due after.
	clk.Advance(time.Second)
	if n := s.RedeliverOnce(); n != 1 {
		t.Fatalf("redeliver %d", n)
	}
	batch := s.popN(1)
	if batch[0].Origin != sink.OriginRedeliver || batch[0].Attempt != 1 {
		t.Fatalf("redelivered env %+v", batch[0])
	}
	s.process(batch) // attempt 2 fails; breaker (2 failures) opens
	flush(t, d)
	if s.br.State() != delivery.BreakerOpen {
		t.Fatal("breaker not open")
	}
	r, _ = d.L1.Get(1)
	if r.Slot(0).Attempts != 2 {
		t.Fatalf("attempts %d", r.Slot(0).Attempts)
	}
	if s.Enqueue(envAt(2)) != BreakerOpen {
		t.Fatal("enqueue while breaker open")
	}
	if s.RedeliverOnce() != 0 {
		t.Fatal("redeliver while breaker open")
	}
	// A batch dequeued while open is postponed without counting.
	d.Claims.TryClaim(1, 0)
	s.process([]*sink.Envelope{{Seq: 1, Dest: 0, Origin: sink.OriginRedeliver, Attempt: 2, Event: &event.LogEvent{Seq: 1}, Offset: 1, Length: 100}})
	flush(t, d)
	r, _ = d.L1.Get(1)
	if r.Slot(0).Attempts != 2 || r.Slot(0).NextAt != clk.Now().Add(10*time.Second).UnixNano() {
		t.Fatalf("postponed slot %+v", r.Slot(0))
	}
	if d.Claims.Len() != 0 {
		t.Fatal("claim not released")
	}

	// Exhaust the budget (max 5 attempts) -> DLQ, record deleted.
	clk.Advance(10 * time.Second)
	for range 3 {
		clk.Advance(time.Minute)
		if s.RedeliverOnce() != 1 {
			t.Fatal("not due")
		}
		s.process(s.popN(1))
		flush(t, d)
	}
	if r, _ := d.L1.Get(1); r != nil {
		t.Fatalf("record survived budget: %+v", r)
	}
	if d.DLQ.Total() != 1 {
		t.Fatal("not in DLQ")
	}
}

func discard() *slog.Logger { return slog.New(slog.DiscardHandler) }
