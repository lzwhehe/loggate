package dispatch

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/lzwhehe/loggate/internal/event"
	"github.com/lzwhehe/loggate/internal/sink"
	"github.com/lzwhehe/loggate/internal/wal"
)

// memSink records delivered seqs and fails the first failN envelopes it sees.
type memSink struct {
	mu    sync.Mutex
	got   []uint64
	failN int
}

func (m *memSink) Init(context.Context, map[string]any) error { return nil }
func (m *memSink) Health(context.Context) error               { return nil }
func (m *memSink) Close(context.Context) error                { return nil }
func (m *memSink) Idempotent() bool                           { return false }
func (m *memSink) Write(_ context.Context, b []*sink.Envelope) []error {
	m.mu.Lock()
	defer m.mu.Unlock()
	errs := make([]error, len(b))
	for i, e := range b {
		if m.failN > 0 {
			m.failN--
			errs[i] = errors.New("transient")
			continue
		}
		m.got = append(m.got, e.Seq)
	}
	return errs
}

func rec(seq uint64, targets event.DestMask) wal.Record {
	p, _ := json.Marshal(event.LogEvent{Seq: seq, Targets: targets})
	return wal.Record{Seq: seq, Targets: targets, Payload: p}
}

func TestDirectDeliversWithRetryAndRouting(t *testing.T) {
	a, b := &memSink{failN: 3}, &memSink{}
	d := NewDirect(map[event.DestID]sink.Sink{0: a, 1: b}, Options{RetryBase: time.Millisecond, MaxBatch: 4})
	ch := make(chan []wal.Record, 4)
	both := event.DestMask(0).Set(0).Set(1)
	onlyA := event.DestMask(0).Set(0)
	ch <- []wal.Record{rec(1, both), rec(2, onlyA), rec(3, both)}
	ch <- []wal.Record{rec(4, both), rec(5, both), rec(6, onlyA), rec(7, both), rec(8, both)}
	close(ch)
	d.Run(context.Background(), ch)

	count := func(s []uint64) map[uint64]int {
		m := map[uint64]int{}
		for _, x := range s {
			m[x]++
		}
		return m
	}
	ca, cb := count(a.got), count(b.got)
	for s := uint64(1); s <= 8; s++ {
		if ca[s] != 1 {
			t.Fatalf("dest a got seq %d %d times", s, ca[s])
		}
	}
	for _, s := range []uint64{2, 6} {
		if cb[s] != 0 {
			t.Fatalf("dest b got seq %d not routed to it", s)
		}
	}
	if len(b.got) != 6 {
		t.Fatalf("dest b got %v", b.got)
	}
	if d.Delivered() != 14 || d.Dropped() != 0 {
		t.Fatalf("delivered=%d dropped=%d", d.Delivered(), d.Dropped())
	}
}

func TestDirectCancelledStillDrains(t *testing.T) {
	a := &memSink{failN: 1 << 30} // always fails
	d := NewDirect(map[event.DestID]sink.Sink{0: a}, Options{RetryBase: time.Millisecond})
	ch := make(chan []wal.Record)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { d.Run(ctx, ch); close(done) }()
	ch <- []wal.Record{rec(1, 1)}
	cancel()
	ch <- []wal.Record{rec(2, 1), rec(3, 1)} // must not block after cancel
	close(ch)
	<-done
	if d.Dropped() != 3 {
		t.Fatalf("dropped = %d, want 3", d.Dropped())
	}
}
