package ui

import (
	"time"

	"github.com/lzwhehe/loggate/internal/gateway"
	"github.com/lzwhehe/loggate/internal/metrics"
)

// Snapshot is the JSON document pushed to the dashboard every second.
type Snapshot struct {
	At       int64             `json:"at"`   // unix ms
	Mode     string            `json:"mode"` // "demo" | "gateway"
	Gateway  GatewayView       `json:"gateway"`
	Dests    []DestRow         `json:"dests"`
	MaskHits map[string]uint64 `json:"mask_hits"`
	Demo     *DemoView         `json:"demo,omitempty"`
}

// GatewayView holds the headline counters.
type GatewayView struct {
	// Status: running | restarting | stopped.
	Status string `json:"status"`
	// Boots counts gateway instances started by this process (the demo's
	// simulated crash starts a new one on the same data directory).
	Boots int `json:"boots"`
	// Ingested / Acked / Refused are ingest totals of this process
	// (accepted events; refused events or requests), from the metrics.
	Ingested     uint64 `json:"ingested"`
	Acked        uint64 `json:"acked"`
	Refused      uint64 `json:"refused"`
	L1Pending    int64  `json:"l1_pending"`
	L1Records    int    `json:"l1_records"`
	DLQ          uint64 `json:"dlq"`
	Backpressure bool   `json:"backpressure"`
	NextSeq      uint64 `json:"next_seq"`
	Replayed     int    `json:"replayed"`
	WALSegments  int    `json:"wal_segments"`
	DiskBytes    int64  `json:"disk_bytes"`
}

// DestRow is one row of the destination table.
type DestRow struct {
	Name      string `json:"name"`
	Type      string `json:"type"`
	Lifecycle string `json:"lifecycle"`
	Breaker   string `json:"breaker"`
	L2        int    `json:"l2"`
	L1Pending int64  `json:"l1_pending"`
	InFlight  int64  `json:"in_flight"`
	// P99Ms is the p99 end-to-end latency over the last 30s of
	// confirmations (P99Samples of them); null without samples.
	P99Ms      *float64 `json:"p99_ms"`
	P99Samples int      `json:"p99_samples"`
	// LastErrKind / LastErrAgoS describe the latest failed send.
	LastErrKind string   `json:"last_err_kind,omitempty"`
	LastErrAgoS *float64 `json:"last_err_ago_s,omitempty"`
	// Delivered is cumulative for the process (survives simulated
	// restarts); DLQ likewise.
	Delivered    uint64     `json:"delivered"`
	DLQ          uint64     `json:"dlq"`
	LevelBlocked uint64     `json:"level_blocked"`
	Watermark    uint64     `json:"watermark"`
	Fault        *FaultView `json:"fault,omitempty"`
}

// FaultView is the injected fault of a demo receiver.
type FaultView struct {
	Mode    string `json:"mode"`
	DelayMS int64  `json:"delay_ms,omitempty"`
}

// DemoView carries the demo-only panels.
type DemoView struct {
	EPS      float64       `json:"eps"`
	Sent     uint64        `json:"sent"`
	Acked    uint64        `json:"acked"`
	Refused  uint64        `json:"refused"`
	Failed   uint64        `json:"failed"`
	LastErr  string        `json:"last_err,omitempty"`
	Crashes  int           `json:"crashes"`
	Recon    []ReconRow    `json:"recon"`
	Masked   []MaskedEvent `json:"masked"`
	Log      []LogLine     `json:"log"`
	DataDir  string        `json:"data_dir"`
	Settling bool          `json:"settling"`
}

// ReconRow is the reconciliation of one destination (see internal/demo).
type ReconRow struct {
	Dest        string  `json:"dest"`
	Expected    uint64  `json:"expected"`
	Received    uint64  `json:"received"`
	Outstanding uint64  `json:"outstanding"`
	Dup         uint64  `json:"dup"`
	StoredDup   int64   `json:"stored_dup"`
	DLQ         uint64  `json:"dlq"`
	Misrouted   uint64  `json:"misrouted"`
	Lost        *uint64 `json:"lost"` // null while settling
	Settled     bool    `json:"settled"`
}

// MaskedEvent is one event AFTER masking (the original is never kept).
type MaskedEvent struct {
	Seq       uint64   `json:"seq"`
	At        int64    `json:"at"`
	ClassUID  int32    `json:"class_uid"`
	DataClass []string `json:"data_class"`
	Level     string   `json:"level"`
	Hits      int      `json:"hits"`
	Masked    string   `json:"masked"`
}

// LogLine is one demo action.
type LogLine struct {
	At   int64  `json:"at"`
	Text string `json:"text"`
	OK   bool   `json:"ok"`
}

// P99Window is the window of the dashboard's p99 latency.
const P99Window = 30 * time.Second

// FromGateway builds the generic part of a snapshot. types maps
// destination names to sink types.
func FromGateway(g *gateway.Gateway, m *metrics.Metrics, types map[string]string, now time.Time) Snapshot {
	s := Snapshot{At: now.UnixMilli(), Mode: "gateway", MaskHits: map[string]uint64{}}
	if m != nil {
		s.Gateway.Acked, s.Gateway.Refused = m.IngestTotals()
		s.Gateway.Ingested = s.Gateway.Acked + s.Gateway.Refused
	}
	if g == nil || !g.Running() {
		s.Gateway.Status = "stopped"
		return s
	}
	st := g.Stats()
	s.Gateway.Status = "running"
	s.Gateway.L1Records, s.Gateway.DLQ, s.Gateway.Backpressure = st.L1Records, st.DLQTotal, st.Backpressure
	s.Gateway.NextSeq, s.Gateway.Replayed, s.Gateway.WALSegments, s.Gateway.DiskBytes = st.NextSeq, st.Replayed, st.WALSegments, st.DiskBytes
	if st.Masking != nil {
		s.MaskHits = st.Masking.Hits
	}
	for _, d := range st.Destinations {
		row := DestRow{
			Name: d.Name, Type: types[d.Name], Lifecycle: d.State, Breaker: d.Breaker,
			L2: d.L2Depth, L1Pending: d.L1Pending, InFlight: d.InFlight,
			Delivered: d.Delivered, DLQ: d.DLQ, LevelBlocked: d.RouteLevelBlocked, Watermark: d.Watermark,
		}
		if m != nil {
			v := m.View(d.Name, now, P99Window)
			row.Delivered, row.DLQ = v.Delivered, v.DLQ
			if v.Samples > 0 {
				ms := float64(v.P99.Microseconds()) / 1000
				row.P99Ms, row.P99Samples = &ms, v.Samples
			}
			if v.LastErrorKind != "" {
				ago := now.Sub(v.LastErrorAt).Seconds()
				row.LastErrKind, row.LastErrAgoS = v.LastErrorKind, &ago
			}
		}
		s.Gateway.L1Pending += d.L1Pending
		s.Dests = append(s.Dests, row)
	}
	if m != nil {
		var dl uint64
		for _, r := range s.Dests {
			dl += r.DLQ
		}
		s.Gateway.DLQ = dl
	}
	return s
}
