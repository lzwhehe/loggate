// Package dispatch moves committed WAL records to destinations.
//
// TODO(M2): this is the M1 placeholder. It delivers directly and
// synchronously from the WAL's committed stream, retrying in place, with no
// per-destination L2 queues, no AckTracker/checkpoint, no L1 parking and no
// crash-recovery replay. One slow or failing destination therefore stalls
// every destination and, through the committed channel, back-pressures the
// WAL writer. M2 replaces it with Dispatcher + Worker/L2 + Tracker + L1.
package dispatch

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"slices"
	"sync/atomic"
	"time"

	"github.com/lzwhehe/loggate/internal/clock"
	"github.com/lzwhehe/loggate/internal/event"
	"github.com/lzwhehe/loggate/internal/sink"
	"github.com/lzwhehe/loggate/internal/wal"
)

// Options configures Direct.
type Options struct {
	MaxBatch    int           // envelopes per Sink.Write; default 256
	RetryBase   time.Duration // default 100ms
	RetryMax    time.Duration // default 5s
	Clock       clock.Clock
	Logger      *slog.Logger
	DestNames   func(event.DestID) string // for logs; optional
	OnDelivered func(dest event.DestID, n int)
}

// Direct is the M1 delivery loop.
type Direct struct {
	sinks map[event.DestID]sink.Sink
	dests []event.DestID // sorted
	opts  Options
	log   *slog.Logger

	delivered atomic.Uint64
	dropped   atomic.Uint64
	undecoded atomic.Uint64
}

// NewDirect creates a delivery loop for the given destinations.
func NewDirect(sinks map[event.DestID]sink.Sink, opts Options) *Direct {
	if opts.MaxBatch <= 0 {
		opts.MaxBatch = 256
	}
	if opts.RetryBase <= 0 {
		opts.RetryBase = 100 * time.Millisecond
	}
	if opts.RetryMax <= 0 {
		opts.RetryMax = 5 * time.Second
	}
	if opts.Clock == nil {
		opts.Clock = clock.Real()
	}
	if opts.Logger == nil {
		opts.Logger = slog.New(slog.DiscardHandler)
	}
	if opts.DestNames == nil {
		opts.DestNames = func(d event.DestID) string { return fmt.Sprint(d) }
	}
	d := &Direct{sinks: sinks, opts: opts, log: opts.Logger.With("component", "dispatch")}
	for id := range sinks {
		d.dests = append(d.dests, id)
	}
	slices.Sort(d.dests)
	return d
}

// Delivered returns the number of envelopes confirmed by sinks.
func (d *Direct) Delivered() uint64 { return d.delivered.Load() }

// Dropped returns envelopes abandoned because ctx was cancelled (they remain
// in the WAL; M1 has no replay, see package doc).
func (d *Direct) Dropped() uint64 { return d.dropped.Load() }

// Run consumes committed batches until the channel is closed. If ctx is
// cancelled, pending deliveries are abandoned but the channel is still
// drained so the WAL writer can shut down.
func (d *Direct) Run(ctx context.Context, committed <-chan []wal.Record) {
	for batch := range committed {
		if ctx.Err() != nil {
			d.dropped.Add(uint64(len(batch)))
			continue
		}
		envs := d.decode(batch)
		for _, dest := range d.dests {
			var out []*sink.Envelope
			for _, e := range envs {
				if e.Event.Targets.Has(dest) {
					c := *e
					c.Dest = dest
					out = append(out, &c)
				}
			}
			for len(out) > 0 {
				n := min(len(out), d.opts.MaxBatch)
				d.deliver(ctx, dest, out[:n])
				out = out[n:]
			}
		}
	}
}

func (d *Direct) decode(batch []wal.Record) []*sink.Envelope {
	envs := make([]*sink.Envelope, 0, len(batch))
	for _, r := range batch {
		ev := new(event.LogEvent)
		dec := json.NewDecoder(bytes.NewReader(r.Payload))
		dec.UseNumber()
		if err := dec.Decode(ev); err != nil {
			// Cannot happen for records this process wrote; never silently.
			d.undecoded.Add(1)
			d.log.Error("undecodable WAL payload", "seq", r.Seq, "segment", r.SegID, "offset", r.Offset, "err", err)
			continue
		}
		envs = append(envs, &sink.Envelope{
			Seq: r.Seq, Event: ev, Origin: sink.OriginLive,
			SegID: r.SegID, Offset: r.Offset, Length: r.Len(),
		})
	}
	return envs
}

// deliver writes envs to dest, retrying failed envelopes with exponential
// backoff until all succeed or ctx is cancelled.
func (d *Direct) deliver(ctx context.Context, dest event.DestID, envs []*sink.Envelope) {
	s := d.sinks[dest]
	backoff := d.opts.RetryBase
	for attempt := uint16(1); ; attempt++ {
		for _, e := range envs {
			e.Attempt = attempt
		}
		errs := s.Write(ctx, envs)
		var failed []*sink.Envelope
		var firstErr error
		for i, err := range errs {
			if err != nil {
				failed = append(failed, envs[i])
				if firstErr == nil {
					firstErr = err
				}
			}
		}
		ok := len(envs) - len(failed)
		d.delivered.Add(uint64(ok))
		if d.opts.OnDelivered != nil && ok > 0 {
			d.opts.OnDelivered(dest, ok)
		}
		if len(failed) == 0 {
			return
		}
		d.log.Warn("delivery failed, retrying", "dest", d.opts.DestNames(dest), "failed", len(failed), "attempt", attempt, "backoff", backoff, "err", firstErr)
		envs = failed
		t := d.opts.Clock.NewTimer(backoff)
		select {
		case <-t.C():
		case <-ctx.Done():
			t.Stop()
			d.dropped.Add(uint64(len(envs)))
			d.log.Error("delivery abandoned on shutdown", "dest", d.opts.DestNames(dest), "count", len(envs))
			return
		}
		backoff = min(2*backoff, d.opts.RetryMax)
	}
}
