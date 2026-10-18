package gateway

import (
	"time"

	"github.com/lzwhehe/loggate/internal/delivery"
	"github.com/lzwhehe/loggate/internal/event"
	"github.com/lzwhehe/loggate/internal/metrics"
	"github.com/lzwhehe/loggate/internal/sink"
)

// deliveryObserver forwards slot outcomes to the metrics.
type deliveryObserver struct{ g *Gateway }

func (o deliveryObserver) Delivered(env *sink.Envelope, now time.Time) {
	var recv time.Time
	if env.Event != nil && env.Event.ReceivedAt > 0 {
		recv = time.Unix(0, env.Event.ReceivedAt)
	} else {
		recv = now
	}
	o.g.met.Delivered(o.g.destName(env.Dest), env.Origin.String(), recv, now)
}

func (o deliveryObserver) DeliveryError(dest event.DestID, kind delivery.Kind) {
	o.g.met.DeliveryError(o.g.destName(dest), kind.String(), o.g.clk.Now())
}

// MetricsSnapshot is the scrape-time state for metrics.Metrics.SetSource.
// It returns ok=false before Start and after Shutdown/Crash began.
func (g *Gateway) MetricsSnapshot() (metrics.Snapshot, bool) {
	if !g.Running() {
		return metrics.Snapshot{}, false
	}
	st := g.Stats()
	s := metrics.Snapshot{WALBytes: g.wal.Bytes(), Backpressure: st.Backpressure}
	if st.Masking != nil {
		s.MaskHits = st.Masking.Hits
	}
	for _, d := range st.Destinations {
		s.Dests = append(s.Dests, metrics.DestSnapshot{
			Name: d.Name, State: d.State, Breaker: d.Breaker, L2Depth: d.L2Depth,
			L1Pending: d.L1Pending, Watermark: d.Watermark, LevelBlocked: d.RouteLevelBlocked,
		})
	}
	return s, true
}

// Running reports whether the gateway has started and is not stopping.
func (g *Gateway) Running() bool { return g.running.Load() }
