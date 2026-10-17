// Package worker implements per-destination delivery slots: a bounded L2
// queue, a consumer that batches into Sink.Write through a circuit breaker,
// per-envelope result handling (resolve / park / retry / DLQ), the
// per-destination redeliver loop over L1, and the lifecycle
// Registered → Active → Draining → Unregistered (DESIGN §4.1–4.4, §5; no
// Replace in v1).
package worker

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/lzwhehe/loggate/internal/clock"
	"github.com/lzwhehe/loggate/internal/delivery"
	"github.com/lzwhehe/loggate/internal/dlq"
	"github.com/lzwhehe/loggate/internal/event"
	"github.com/lzwhehe/loggate/internal/sink"
	"github.com/lzwhehe/loggate/internal/state"
)

// Resolver resolves (dest, seq) in the AckTracker.
type Resolver interface {
	Resolve(d event.DestID, seq uint64) bool
}

// Loader reads an event back from the WAL for redelivery.
type Loader interface {
	Load(ref state.Ref) (*sink.Envelope, error)
}

// Quota policies (config l1.on_quota).
const (
	OnQuotaBlockAll    = "block_all"
	OnQuotaDLQOverflow = "dlq_overflow"
)

// Deps are the shared collaborators of every slot and of the dispatcher.
type Deps struct {
	L1       *state.Store
	DLQ      *dlq.Log
	Resolver Resolver
	Claims   *state.Claims
	Loader   Loader
	Clock    clock.Clock
	Logger   *slog.Logger
	Backoff  *delivery.Backoff
	Budget   delivery.Budget
	// Quota is the per-destination L1 record limit (0 = unlimited).
	Quota   int64
	OnQuota string
	// DestName maps IDs to names for logs and DLQ entries.
	DestName func(event.DestID) string
}

func (d *Deps) name(id event.DestID) string {
	if d.DestName != nil {
		if n := d.DestName(id); n != "" {
			return n
		}
	}
	return fmt.Sprintf("dest-%d", id)
}

// OverQuota reports whether some destination is at or above its L1 quota
// while the policy is block_all; ingest then answers 429 (DESIGN §0.5 item 9).
func (d *Deps) OverQuota() bool {
	if d.Quota <= 0 || d.OnQuota == OnQuotaDLQOverflow {
		return false
	}
	for i := range event.MaxDests {
		if d.L1.Pending(event.DestID(i)) >= d.Quota {
			return true
		}
	}
	return false
}

// ParkLive hands a live/replayed envelope (tracked in the AckTracker) to
// L1. The tracker is resolved only after the L1 commit. If the
// destination's L1 quota is exhausted and the policy is dlq_overflow, the
// envelope goes to the DLQ instead (fsync first, then resolve). wg (may be
// nil) is held until the operation completed.
func (d *Deps) ParkLive(env *sink.Envelope, reason state.Reason, lastErr delivery.Kind, nextAt time.Time, wg *sync.WaitGroup) {
	if d.Quota > 0 && d.OnQuota == OnQuotaDLQOverflow && d.L1.Pending(env.Dest) >= d.Quota {
		err := d.DLQ.Append([]dlq.Entry{d.Entry(env, "l1_quota_overflow", nil)})
		if err == nil {
			d.Resolver.Resolve(env.Dest, env.Seq)
			return
		}
		d.Logger.Error("dlq append failed, parking over quota", "dest", d.name(env.Dest), "seq", env.Seq, "err", err)
	}
	if wg != nil {
		wg.Add(1)
	}
	d.L1.Park(state.ParkReq{
		Seq: env.Seq, Dest: env.Dest,
		Ref:        state.Ref{SegID: env.SegID, Offset: env.Offset, Length: env.Length},
		ReceivedAt: env.Event.ReceivedAt, Reason: reason, Attempts: env.Attempt,
		LastErr: uint16(lastErr), NextAt: nextAt.UnixNano(),
	}, func(_ bool, err error) {
		if wg != nil {
			defer wg.Done()
		}
		if err != nil {
			// Responsibility was not transferred: leave the seq unresolved
			// so the watermark cannot pass it; recovery replays it.
			d.Logger.Error("l1 park failed; event stays unresolved until restart", "dest", d.name(env.Dest), "seq", env.Seq, "err", err)
			return
		}
		d.Resolver.Resolve(env.Dest, env.Seq)
	})
}

// Entry builds a DLQ entry for env. The body is the event as stored in the
// WAL (masked from M4 on); err text comes from sinks and must not carry
// payload data.
func (d *Deps) Entry(env *sink.Envelope, reason string, cause error) dlq.Entry {
	e := dlq.Entry{
		Seq: env.Seq, Dest: env.Dest, DestName: d.name(env.Dest), Reason: reason,
		Attempts: env.Attempt, At: d.Clock.Now().UnixNano(),
	}
	if env.Event != nil {
		e.IdemKey = env.Event.IdemKey
		if b, err := json.Marshal(env.Event); err == nil {
			e.Body = b
		}
	}
	if cause != nil {
		e.LastError = cause.Error()
	}
	return e
}
