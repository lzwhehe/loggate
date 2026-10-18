// Package metrics exposes LogGate's Prometheus metrics (DESIGN §8).
//
// Event-driven metrics (counters and histograms) are updated through the
// observer methods (IngestEvents, WALFsync, Delivered, DeliveryError, DLQ),
// which the gateway wires into ingest, the WAL, the slots and the DLQ.
// Point-in-time metrics (queue depths, breaker and lifecycle states,
// watermarks, mask hits, max_level blocks) are read at scrape time from a
// Source, normally Gateway.MetricsSnapshot. One Metrics value can outlive a
// gateway instance (the demo's simulated crash replaces the gateway and
// keeps the counters).
//
// Label cardinality is bounded: every label value goes through a per-label
// limiter that admits a fixed number of distinct values and maps the rest
// to "other". Destination names are additionally bounded by the registry
// (at most 64), sources by the configured tokens, and reasons, kinds,
// origins and mask types by the code.
package metrics

import (
	"net/http"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Namespace prefixes every metric name.
const Namespace = "loggate"

// Other replaces label values beyond a limiter's capacity.
const Other = "other"

// Label value limits.
const (
	MaxDests   = 64 // event.MaxDests; further names map to Other
	MaxSources = 64
	MaxReasons = 32
	MaxKinds   = 8
	MaxTypes   = 16
	MaxResults = 16
)

// Snapshot is the point-in-time state read at scrape time.
type Snapshot struct {
	WALBytes     int64
	Backpressure bool
	// MaskHits are cumulative masked spans per detector type since the
	// gateway started.
	MaskHits map[string]uint64
	Dests    []DestSnapshot
}

// DestSnapshot is one destination's point-in-time state.
type DestSnapshot struct {
	Name      string
	State     string // lifecycle: absent | registered | active | draining | unregistered
	Breaker   string // closed | half_open | open ("" when absent)
	L2Depth   int
	L1Pending int64
	Watermark uint64
	// LevelBlocked is the cumulative number of events kept away by
	// max_level since the gateway started.
	LevelBlocked uint64
}

// Source returns the current snapshot; ok=false means "no gateway right
// now" (e.g. during a simulated restart) and the gauges are omitted.
type Source func() (s Snapshot, ok bool)

// BreakerValue maps a breaker state to the loggate_breaker_state value.
func BreakerValue(s string) float64 {
	switch s {
	case "closed":
		return 0
	case "half_open":
		return 1
	case "open":
		return 2
	default:
		return -1
	}
}

// PluginValue maps a lifecycle state to the loggate_plugin_state value.
func PluginValue(s string) float64 {
	switch s {
	case "absent":
		return 0
	case "registered":
		return 1
	case "active":
		return 2
	case "draining":
		return 3
	case "unregistered":
		return 4
	default:
		return -1
	}
}

// limiter admits up to max distinct values of one label.
type limiter struct {
	mu   sync.Mutex
	max  int
	seen map[string]struct{}
}

func newLimiter(max int) *limiter { return &limiter{max: max, seen: map[string]struct{}{}} }

func (l *limiter) value(v string) string {
	if v == "" {
		v = "unknown"
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if _, ok := l.seen[v]; ok {
		return v
	}
	if len(l.seen) >= l.max {
		return Other
	}
	l.seen[v] = struct{}{}
	return v
}

// latency keeps recent per-destination end-to-end latencies for the
// dashboard's p99 (a time window, independent of histogram buckets).
type latency struct {
	at  []int64
	d   []time.Duration
	pos int
	n   int
}

const latencyRing = 4096

func (r *latency) add(at time.Time, d time.Duration) {
	if r.at == nil {
		r.at, r.d = make([]int64, latencyRing), make([]time.Duration, latencyRing)
	}
	r.at[r.pos], r.d[r.pos] = at.UnixNano(), d
	r.pos = (r.pos + 1) % latencyRing
	r.n = min(r.n+1, latencyRing)
}

type destState struct {
	delivered atomic.Uint64
	errors    atomic.Uint64
	dlq       atomic.Uint64

	mu       sync.Mutex
	lat      latency
	lastKind string
	lastAt   time.Time
}

// Metrics holds the registry and all LogGate collectors.
type Metrics struct {
	reg *prometheus.Registry

	ingest    *prometheus.CounterVec
	fsync     prometheus.Histogram
	delivered *prometheus.CounterVec
	delivErr  *prometheus.CounterVec
	dlq       *prometheus.CounterVec
	e2e       *prometheus.HistogramVec

	lDest, lSource, lReason, lKind, lType, lResult, lOrigin *limiter

	src atomic.Pointer[Source]

	mu    sync.Mutex
	dests map[string]*destState

	accepted atomic.Uint64
	refused  atomic.Uint64
}

// New creates the metrics and registers them (plus Go runtime and process
// collectors) on a private registry.
func New() *Metrics {
	m := &Metrics{
		reg:   prometheus.NewRegistry(),
		lDest: newLimiter(MaxDests), lSource: newLimiter(MaxSources), lReason: newLimiter(MaxReasons),
		lKind: newLimiter(MaxKinds), lType: newLimiter(MaxTypes), lResult: newLimiter(MaxResults),
		lOrigin: newLimiter(MaxKinds),
		dests:   map[string]*destState{},
	}
	m.ingest = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: Namespace, Name: "ingest_events_total",
		Help: "Ingest outcomes by authenticated source and result. accepted/wal_error count events; results decided before the body is parsed (backpressure, unauthorized, bad_request, too_large, draining) count requests.",
	}, []string{"source", "result"})
	m.fsync = prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: Namespace, Name: "wal_fsync_seconds",
		Help:    "Duration of WAL group-commit fsyncs.",
		Buckets: prometheus.ExponentialBuckets(0.00025, 2, 15), // 0.25ms .. ~4s
	})
	m.delivered = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: Namespace, Name: "delivered_total",
		Help: "Envelopes confirmed by a sink, by destination and origin (live | replay | redeliver).",
	}, []string{"dest", "origin"})
	m.delivErr = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: Namespace, Name: "delivery_errors_total",
		Help: "Failed envelopes of real send attempts, by destination and error kind (retryable | non_retryable | throttled).",
	}, []string{"dest", "kind"})
	m.dlq = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: Namespace, Name: "dlq_total",
		Help: "Dead letters written (after fsync), by destination and reason.",
	}, []string{"dest", "reason"})
	m.e2e = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: Namespace, Name: "e2e_latency_seconds",
		Help:    "Time from ingest (received_at) to sink confirmation, per destination. Includes time parked in L1.",
		Buckets: prometheus.ExponentialBuckets(0.005, 2, 17), // 5ms .. ~5.5min
	}, []string{"dest"})
	m.reg.MustRegister(m.ingest, m.fsync, m.delivered, m.delivErr, m.dlq, m.e2e, &snapshotCollector{m: m},
		collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	return m
}

// Registry returns the private registry (tests use it with testutil).
func (m *Metrics) Registry() *prometheus.Registry { return m.reg }

// Handler serves the registry in the Prometheus exposition format.
func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.reg, promhttp.HandlerOpts{Registry: m.reg})
}

// SetSource sets (or, with nil, clears) the snapshot source.
func (m *Metrics) SetSource(s Source) {
	if s == nil {
		m.src.Store(nil)
		return
	}
	m.src.Store(&s)
}

func (m *Metrics) dest(name string) *destState {
	m.mu.Lock()
	defer m.mu.Unlock()
	d := m.dests[name]
	if d == nil {
		d = &destState{}
		m.dests[name] = d
	}
	return d
}

// IngestEvents counts n events (or requests, see the metric help) of one
// ingest result.
func (m *Metrics) IngestEvents(source, result string, n int) {
	if n <= 0 {
		return
	}
	m.ingest.WithLabelValues(m.lSource.value(source), m.lResult.value(result)).Add(float64(n))
	if result == "accepted" {
		m.accepted.Add(uint64(n))
	} else {
		m.refused.Add(uint64(n))
	}
}

// IngestTotals returns accepted events and refused events/requests.
func (m *Metrics) IngestTotals() (accepted, refused uint64) {
	return m.accepted.Load(), m.refused.Load()
}

// WALFsync records one fsync duration.
func (m *Metrics) WALFsync(d time.Duration) { m.fsync.Observe(d.Seconds()) }

// Delivered records one confirmed envelope and its end-to-end latency.
func (m *Metrics) Delivered(dest, origin string, receivedAt, now time.Time) {
	dest = m.lDest.value(dest)
	m.delivered.WithLabelValues(dest, m.lOrigin.value(origin)).Inc()
	lat := max(now.Sub(receivedAt), 0)
	m.e2e.WithLabelValues(dest).Observe(lat.Seconds())
	d := m.dest(dest)
	d.delivered.Add(1)
	d.mu.Lock()
	d.lat.add(now, lat)
	d.mu.Unlock()
}

// DeliveryError records one failed envelope.
func (m *Metrics) DeliveryError(dest, kind string, now time.Time) {
	dest, kind = m.lDest.value(dest), m.lKind.value(kind)
	m.delivErr.WithLabelValues(dest, kind).Inc()
	d := m.dest(dest)
	d.errors.Add(1)
	d.mu.Lock()
	d.lastKind, d.lastAt = kind, now
	d.mu.Unlock()
}

// DLQ records n dead letters.
func (m *Metrics) DLQ(dest, reason string, n int) {
	dest = m.lDest.value(dest)
	m.dlq.WithLabelValues(dest, m.lReason.value(reason)).Add(float64(n))
	m.dest(dest).dlq.Add(uint64(n))
}

// DestView is the dashboard's per-destination view of the counters, which
// survive gateway restarts within one process.
type DestView struct {
	Delivered uint64
	Errors    uint64
	DLQ       uint64
	// P99 is the 99th percentile end-to-end latency over the last
	// window (Samples deliveries); 0 if none.
	P99     time.Duration
	Samples int
	// LastErrorKind and LastErrorAt describe the most recent failure.
	LastErrorKind string
	LastErrorAt   time.Time
}

// View returns the dashboard view of dest; p99 is computed over
// deliveries confirmed within window before now.
func (m *Metrics) View(dest string, now time.Time, window time.Duration) DestView {
	d := m.dest(m.lDest.value(dest))
	v := DestView{Delivered: d.delivered.Load(), Errors: d.errors.Load(), DLQ: d.dlq.Load()}
	cut := now.Add(-window).UnixNano()
	d.mu.Lock()
	v.LastErrorKind, v.LastErrorAt = d.lastKind, d.lastAt
	var xs []time.Duration
	for i := 0; i < d.lat.n; i++ {
		if d.lat.at[i] >= cut {
			xs = append(xs, d.lat.d[i])
		}
	}
	d.mu.Unlock()
	if len(xs) > 0 {
		slices.Sort(xs)
		v.P99 = xs[min(len(xs)-1, (len(xs)*99+99)/100-1)]
		v.Samples = len(xs)
	}
	return v
}

// snapshotCollector turns a Source snapshot into gauges and counters.
type snapshotCollector struct{ m *Metrics }

var (
	descWALBytes = prometheus.NewDesc(Namespace+"_wal_bytes", "Durable (fsynced) bytes in WAL segments.", nil, nil)
	descL2       = prometheus.NewDesc(Namespace+"_l2_depth", "Envelopes queued in the destination's in-memory L2 queue.", []string{"dest"}, nil)
	descL1       = prometheus.NewDesc(Namespace+"_l1_pending", "Events parked in L1 (bbolt) awaiting redelivery to the destination.", []string{"dest"}, nil)
	descBreaker  = prometheus.NewDesc(Namespace+"_breaker_state", "Circuit breaker state: 0 closed, 1 half_open, 2 open.", []string{"dest"}, nil)
	descPlugin   = prometheus.NewDesc(Namespace+"_plugin_state", "Destination lifecycle: 0 absent, 1 registered, 2 active, 3 draining, 4 unregistered.", []string{"dest"}, nil)
	descWM       = prometheus.NewDesc(Namespace+"_watermark", "Per-destination resolved watermark (every seq at or below it is delivered, parked in L1 or dead).", []string{"dest"}, nil)
	descBlocked  = prometheus.NewDesc(Namespace+"_route_level_blocked_total", "Events kept away from the destination by max_level (since gateway start).", []string{"dest"}, nil)
	descMask     = prometheus.NewDesc(Namespace+"_mask_hits_total", "Masked PII spans by detector type (since gateway start).", []string{"type"}, nil)
	descBackp    = prometheus.NewDesc(Namespace+"_backpressure_active", "1 while ingest refuses new events with 429 (L1 or disk quota).", nil, nil)
)

func (c *snapshotCollector) Describe(ch chan<- *prometheus.Desc) {
	for _, d := range []*prometheus.Desc{descWALBytes, descL2, descL1, descBreaker, descPlugin, descWM, descBlocked, descMask, descBackp} {
		ch <- d
	}
}

func (c *snapshotCollector) Collect(ch chan<- prometheus.Metric) {
	p := c.m.src.Load()
	if p == nil {
		return
	}
	s, ok := (*p)()
	if !ok {
		return
	}
	g := func(d *prometheus.Desc, t prometheus.ValueType, v float64, labels ...string) {
		ch <- prometheus.MustNewConstMetric(d, t, v, labels...)
	}
	g(descWALBytes, prometheus.GaugeValue, float64(s.WALBytes))
	bp := 0.0
	if s.Backpressure {
		bp = 1
	}
	g(descBackp, prometheus.GaugeValue, bp)
	seen := map[string]bool{}
	for _, d := range s.Dests {
		name := c.m.lDest.value(d.Name)
		if seen[name] { // only possible for Other; never emit duplicates
			continue
		}
		seen[name] = true
		g(descL2, prometheus.GaugeValue, float64(d.L2Depth), name)
		g(descL1, prometheus.GaugeValue, float64(d.L1Pending), name)
		if d.Breaker != "" {
			g(descBreaker, prometheus.GaugeValue, BreakerValue(d.Breaker), name)
		}
		g(descPlugin, prometheus.GaugeValue, PluginValue(d.State), name)
		g(descWM, prometheus.GaugeValue, float64(d.Watermark), name)
		g(descBlocked, prometheus.CounterValue, float64(d.LevelBlocked), name)
	}
	types := make([]string, 0, len(s.MaskHits))
	for t := range s.MaskHits {
		types = append(types, t)
	}
	slices.Sort(types)
	seenT := map[string]bool{}
	for _, t := range types {
		lt := c.m.lType.value(t)
		if seenT[lt] {
			continue
		}
		seenT[lt] = true
		g(descMask, prometheus.CounterValue, float64(s.MaskHits[t]), lt)
	}
}
