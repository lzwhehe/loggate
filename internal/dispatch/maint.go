package dispatch

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lzwhehe/loggate/internal/clock"
	"github.com/lzwhehe/loggate/internal/dlq"
	"github.com/lzwhehe/loggate/internal/event"
	"github.com/lzwhehe/loggate/internal/state"
	"github.com/lzwhehe/loggate/internal/tracker"
	"github.com/lzwhehe/loggate/internal/wal"
)

// MaintOptions configures the maintenance loops.
type MaintOptions struct {
	L1       *state.Store
	WAL      *wal.WAL
	DLQ      *dlq.Log
	Trackers *tracker.Set
	Claims   *state.Claims
	Clock    clock.Clock
	Logger   *slog.Logger
	// IDs returns every DestID in the registry (configured, absent and
	// tombstoned). Checkpoints and the GC minimum range over these.
	IDs func() []event.DestID
	// Active reports whether a destination is currently Active.
	Active   func(event.DestID) bool
	DestName func(event.DestID) string

	CheckpointInterval time.Duration // default 1s
	SweepInterval      time.Duration // default 1s
	GCInterval         time.Duration // default 10s
	OrphanTTL          time.Duration
	MaxAge             time.Duration
	SweepBatch         int // max pairs moved to the DLQ per sweep; default 1024
}

// Maint runs checkpointing, the global L1 sweeper and segment GC.
type Maint struct {
	o   MaintOptions
	log *slog.Logger

	stop chan struct{}
	wg   sync.WaitGroup

	swept   atomic.Uint64
	removed atomic.Uint64
}

// NewMaint returns the maintenance runner (not started).
func NewMaint(o MaintOptions) *Maint {
	if o.CheckpointInterval <= 0 {
		o.CheckpointInterval = time.Second
	}
	if o.SweepInterval <= 0 {
		o.SweepInterval = time.Second
	}
	if o.GCInterval <= 0 {
		o.GCInterval = 10 * time.Second
	}
	if o.SweepBatch <= 0 {
		o.SweepBatch = 1024
	}
	if o.Logger == nil {
		o.Logger = slog.New(slog.DiscardHandler)
	}
	return &Maint{o: o, log: o.Logger.With("component", "maint"), stop: make(chan struct{})}
}

// Swept returns how many (seq, dest) pairs the sweeper moved to the DLQ.
func (m *Maint) Swept() uint64 { return m.swept.Load() }

// Removed returns how many WAL segments GC deleted.
func (m *Maint) Removed() uint64 { return m.removed.Load() }

// Start launches the three loops.
func (m *Maint) Start() {
	m.loop(m.o.CheckpointInterval, func() {
		if err := m.Checkpoint(context.Background()); err != nil {
			m.log.Error("checkpoint", "err", err)
		}
	})
	m.loop(m.o.SweepInterval, func() { m.Sweep() })
	m.loop(m.o.GCInterval, func() { m.GC() })
}

// Stop stops the loops and waits for them.
func (m *Maint) Stop() {
	select {
	case <-m.stop:
	default:
		close(m.stop)
	}
	m.wg.Wait()
}

func (m *Maint) loop(every time.Duration, fn func()) {
	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		for {
			t := m.o.Clock.NewTimer(every)
			select {
			case <-t.C():
				fn()
			case <-m.stop:
				t.Stop()
				return
			}
		}
	}()
}

// Checkpoint persists the current watermark of every registry destination
// and waits for the commit. Stored watermarks never decrease.
func (m *Maint) Checkpoint(ctx context.Context) error {
	w := map[event.DestID]uint64{}
	for _, id := range m.o.IDs() {
		w[id] = m.o.Trackers.Watermark(id)
	}
	ch := make(chan error, 1)
	m.o.L1.SaveCheckpoints(w, func(_ bool, err error) { ch <- err })
	select {
	case err := <-ch:
		if err != nil {
			return fmt.Errorf("dispatch: save checkpoints: %w", err)
		}
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Sweep moves expired L1 pairs to the DLQ (DESIGN §0.5 item 3): pairs of
// non-Active destinations parked longer than OrphanTTL and any pair whose
// event is older than MaxAge, regardless of destination state. Order: claim,
// read the body, DLQ append+fsync, then L1 Dead, then release the claim.
func (m *Maint) Sweep() int {
	items, err := m.o.L1.SweepCandidates(state.SweepPolicy{
		Now: m.o.Clock.Now(), OrphanTTL: m.o.OrphanTTL, MaxAge: m.o.MaxAge,
		Active: m.o.Active, Claim: m.o.Claims.TryClaim, Limit: m.o.SweepBatch,
	})
	if err != nil {
		m.log.Error("sweep scan", "err", err)
	}
	if len(items) == 0 {
		return 0
	}
	release := func() {
		for _, it := range items {
			m.o.Claims.Release(it.Seq, it.Dest)
		}
	}
	now := m.o.Clock.Now().UnixNano()
	entries := make([]dlq.Entry, 0, len(items))
	for _, it := range items {
		e := dlq.Entry{Seq: it.Seq, Dest: it.Dest, DestName: m.o.DestName(it.Dest), Reason: it.Why,
			Attempts: it.Attempts, At: now, LastError: fmt.Sprintf("parked: %s", it.Reason)}
		rec, err := m.o.WAL.ReadAt(it.Ref.SegID, it.Ref.Offset, it.Ref.Length)
		if err != nil {
			// Should not happen (segRef pins the segment); record what we can.
			m.log.Error("sweep: read body", "seq", it.Seq, "err", err)
			e.LastError += "; body unreadable"
			e.Body = json.RawMessage("null")
		} else {
			e.Body = rec.Payload
			var k struct {
				IdemKey event.IdemKey `json:"idem_key"`
			}
			if json.Unmarshal(rec.Payload, &k) == nil {
				e.IdemKey = k.IdemKey
			}
		}
		entries = append(entries, e)
	}
	if err := m.o.DLQ.Append(entries); err != nil {
		m.log.Error("sweep: dlq append failed; will retry", "count", len(entries), "err", err)
		release()
		return 0
	}
	var wg sync.WaitGroup
	for _, it := range items {
		wg.Add(1)
		m.o.L1.Dead(it.Seq, it.Dest, func(_ bool, err error) {
			defer wg.Done()
			m.o.Claims.Release(it.Seq, it.Dest)
			if err != nil {
				m.log.Error("sweep: l1 dead failed", "seq", it.Seq, "err", err)
			}
		})
	}
	wg.Wait()
	m.swept.Add(uint64(len(items)))
	m.log.Info("swept expired l1 entries to dlq", "count", len(items))
	return len(items)
}

// GC deletes WAL segments that nothing needs any more (DESIGN §4.8): every
// seq in the segment is <= the minimum persisted checkpoint over all
// registry destinations (so recovery never replays it), no L1 record points
// into it, and it is not the newest segment. A segment's max seq is bounded
// by the next segment's base − 1. Removal is deferred while a reader has the
// file open (Windows).
func (m *Maint) GC() []uint64 {
	ids := m.o.IDs()
	if len(ids) == 0 {
		return nil
	}
	minCkpt := ^uint64(0)
	for _, id := range ids {
		minCkpt = min(minCkpt, m.o.L1.Checkpoint(id))
	}
	segs := m.o.WAL.Segments()
	var removed []uint64
	for i := 0; i+1 < len(segs); i++ {
		if segs[i+1]-1 > minCkpt {
			break // later segments hold even higher seqs
		}
		if m.o.L1.SegRef(segs[i]) > 0 {
			continue
		}
		if err := m.o.WAL.RemoveSegment(segs[i]); err != nil {
			m.log.Warn("gc: remove segment", "segment", segs[i], "err", err)
			continue
		}
		removed = append(removed, segs[i])
	}
	if len(removed) > 0 {
		m.removed.Add(uint64(len(removed)))
		m.log.Info("gc removed wal segments", "count", len(removed), "min_ckpt", minCkpt)
	}
	return removed
}
