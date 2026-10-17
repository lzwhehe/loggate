// Package dispatch moves committed WAL records to destination slots and
// runs the maintenance loops that keep delivery state bounded:
// checkpointing, the global L1 sweeper, and WAL segment GC (DESIGN §4.1,
// §4.6–4.8, §0.5 items 1–4).
package dispatch

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync/atomic"

	"github.com/lzwhehe/loggate/internal/dlq"
	"github.com/lzwhehe/loggate/internal/event"
	"github.com/lzwhehe/loggate/internal/sink"
	"github.com/lzwhehe/loggate/internal/state"
	"github.com/lzwhehe/loggate/internal/tracker"
	"github.com/lzwhehe/loggate/internal/wal"
	"github.com/lzwhehe/loggate/internal/worker"
)

// Options configures a Dispatcher.
type Options struct {
	Deps     *worker.Deps
	Trackers *tracker.Set
	// Slot returns the slot of d, or nil if d has no sink (absent or
	// tombstoned destination).
	Slot func(event.DestID) *worker.Slot
	// MissingReason is the park reason for a destination without a slot
	// (ReasonNoDest for route-only names, ReasonNotActive otherwise).
	MissingReason func(event.DestID) state.Reason
	// BeforeBatch, if set, is called before each live batch (test hook).
	BeforeBatch func()
	Logger      *slog.Logger
	ReplayChunk int // records per L1 ownership lookup; default 1024
}

// Dispatcher is the single goroutine that fans committed records out to
// destinations. It only ever sees fsynced records (the WAL's Committed
// stream and, during recovery, a WAL iterator), in seq order.
type Dispatcher struct {
	o   Options
	log *slog.Logger

	dispatched atomic.Uint64
	replayed   atomic.Uint64
	parked     atomic.Uint64
	undecoded  atomic.Uint64
}

// New returns a dispatcher.
func New(o Options) *Dispatcher {
	if o.ReplayChunk <= 0 {
		o.ReplayChunk = 1024
	}
	if o.Logger == nil {
		o.Logger = slog.New(slog.DiscardHandler)
	}
	return &Dispatcher{o: o, log: o.Logger.With("component", "dispatch")}
}

// Stats returns counters.
func (d *Dispatcher) Stats() (dispatched, replayed, parked, undecoded uint64) {
	return d.dispatched.Load(), d.replayed.Load(), d.parked.Load(), d.undecoded.Load()
}

// Replay re-dispatches WAL records after a restart (DESIGN §4.7): for each
// record and each target d, skip if seq <= ckpt[d] (already resolved) or
// if L1 owns (seq, d) (L1 is responsible); otherwise dispatch with
// OriginReplay. Duplicates this may cause are absorbed by idempotent sinks.
// Must run before live dispatch and before the redeliver loops start.
func (d *Dispatcher) Replay(it *wal.Iterator, ckpt func(event.DestID) uint64) (int, error) {
	defer it.Close()
	n := 0
	chunk := make([]wal.Record, 0, d.o.ReplayChunk)
	flush := func() error {
		if len(chunk) == 0 {
			return nil
		}
		owned, err := d.o.Deps.L1.OwnedRange(chunk[0].Seq, chunk[len(chunk)-1].Seq)
		if err != nil {
			return fmt.Errorf("dispatch: replay ownership lookup: %w", err)
		}
		for _, r := range chunk {
			o := owned[r.Seq]
			d.dispatch(r, sink.OriginReplay, func(dst event.DestID) bool {
				return r.Seq <= ckpt(dst) || o.Has(dst)
			})
		}
		n += len(chunk)
		chunk = chunk[:0]
		return nil
	}
	for {
		r, err := it.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return n, fmt.Errorf("dispatch: replay: %w", err)
		}
		chunk = append(chunk, r)
		if len(chunk) == cap(chunk) {
			if err := flush(); err != nil {
				return n, err
			}
		}
	}
	if err := flush(); err != nil {
		return n, err
	}
	d.replayed.Add(uint64(n))
	return n, nil
}

// Run dispatches live batches until committed is closed.
func (d *Dispatcher) Run(committed <-chan []wal.Record) {
	for batch := range committed {
		if d.o.BeforeBatch != nil {
			d.o.BeforeBatch()
		}
		for _, r := range batch {
			d.dispatch(r, sink.OriginLive, nil)
		}
	}
}

func decodeEvent(p []byte) (*event.LogEvent, error) {
	ev := new(event.LogEvent)
	dec := json.NewDecoder(bytes.NewReader(p))
	dec.UseNumber()
	if err := dec.Decode(ev); err != nil {
		return nil, err
	}
	return ev, nil
}

// dispatch hands r to every target (DESIGN §4.1): tracker.Add first, then
// TryEnqueue into the slot's L2, else Park (NoDest / NotActive /
// BreakerOpen / QueueFull — none of which counts as an attempt). The
// dispatch cursor is published only after all Adds for r completed.
func (d *Dispatcher) dispatch(r wal.Record, origin sink.Origin, skip func(event.DestID) bool) {
	defer d.o.Trackers.Publish(r.Seq)
	if r.Targets == 0 {
		return
	}
	ev, err := decodeEvent(r.Payload)
	if err != nil {
		d.undecoded.Add(1)
		d.log.Error("undecodable WAL payload; sending to DLQ", "seq", r.Seq, "segment", r.SegID, "offset", r.Offset, "err", err)
		d.deadRaw(r, skip, err)
		return
	}
	for _, dst := range r.Targets.Bits() {
		if skip != nil && skip(dst) {
			continue
		}
		d.o.Trackers.Add(dst, r.Seq)
		env := &sink.Envelope{
			Seq: r.Seq, Dest: dst, Event: ev, Origin: origin,
			SegID: r.SegID, Offset: r.Offset, Length: r.Len(),
		}
		d.dispatched.Add(1)
		var reason state.Reason
		if slot := d.o.Slot(dst); slot == nil {
			reason = d.o.MissingReason(dst)
		} else if res := slot.Enqueue(env); res != worker.Enqueued {
			reason = res.Reason()
		} else {
			continue
		}
		d.parked.Add(1)
		d.o.Deps.ParkLive(env, reason, 0, d.o.Deps.Clock.Now(), nil)
	}
}

// deadRaw sends an undecodable record to the DLQ for every target. If the
// DLQ append fails the seqs stay unresolved (and are retried after restart).
func (d *Dispatcher) deadRaw(r wal.Record, skip func(event.DestID) bool, cause error) {
	body, _ := json.Marshal(r.Payload) // base64 string: the payload is not valid JSON
	var entries []dlq.Entry
	var dsts []event.DestID
	for _, dst := range r.Targets.Bits() {
		if skip != nil && skip(dst) {
			continue
		}
		d.o.Trackers.Add(dst, r.Seq)
		dsts = append(dsts, dst)
		e := d.o.Deps.Entry(&sink.Envelope{Seq: r.Seq, Dest: dst}, "undecodable", cause)
		e.Body = body
		entries = append(entries, e)
	}
	if err := d.o.Deps.DLQ.Append(entries); err != nil {
		d.log.Error("dlq append failed for undecodable record", "seq", r.Seq, "err", err)
		return
	}
	for _, dst := range dsts {
		d.o.Trackers.Resolve(dst, r.Seq)
	}
}
