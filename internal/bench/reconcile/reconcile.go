// Package reconcile is the benchmark's independent oracle (DESIGN §10.1,
// REVIEW_01 item 12). For each destination d it computes the expected set
//
//	E_d = acked ∩ routed_to(d)
//
// where acked is the loadgen's promised set (HTTP 200) and routed_to is
// evaluated with the scenario's routing table by this package's own code,
// never the gateway's router. It then compares E_d with what the
// destination actually holds (mocksink journal, file sink lines, sqlite
// rows) and with the gateway's DLQ.
//
// Definitions (per destination):
//   - lost: in E_d but neither delivered nor dead-lettered;
//   - dup_raw: delivered copies beyond the first (raw, as written);
//   - dup_stored: duplicates that survive storage: sqlite rows minus unique
//     test_seqs; for a webhook, unique idem keys minus unique test_seqs (what
//     a receiver honouring Idempotency-Key would store); for a file, dup_raw;
//   - dlq: unique test_seqs in the DLQ for d;
//   - extra: delivered but never acked (not an error: the client saw a
//     failure for an event the gateway had in fact persisted);
//   - misrouted: delivered/dead-lettered at d although the oracle routes
//     the event elsewhere;
//   - recovery_s: with a fault window, the time from the window's end until
//     every event of E_d acked before that end (and not dead-lettered) had
//     arrived; 0 if they all arrived before the end; null if some never did
//     or arrival times are unknown;
//   - latency: first arrival − scheduled send time of each delivered
//     event of E_d, split by scheduled time into baseline / during / after
//     the fault window.
package reconcile

import (
	"cmp"
	"fmt"
	"math"
	"slices"
	"sort"
	"strings"

	"github.com/lzwhehe/loggate/internal/bench/loadgen"
	"github.com/lzwhehe/loggate/internal/bench/scenario"
	"github.com/lzwhehe/loggate/internal/bench/stats"
)

// Source holds what was read for one destination.
type Source struct {
	Dest string
	Type string // file | sqlite | webhook
	// Receipts: every delivered (or, for webhooks, rejected) copy.
	Receipts []Receipt
	// Observed: first-seen time per test_seq from the faultbench observer
	// (file/sqlite sinks have no receive clock of their own). Optional.
	Observed map[uint64]int64
	Torn     int // torn fragments / lines skipped
}

// Input is everything reconcile needs.
type Input struct {
	Scenario *scenario.Scenario // resolved (scaled) scenario
	RunID    string
	StartNS  int64 // load start = timeline offset 0
	Sent     []loadgen.Result
	Sources  []Source
	DLQ      []DLQEntry
}

// Latency is a percentile summary in milliseconds.
type Latency struct {
	N    int     `json:"n"`
	P50  float64 `json:"p50_ms"`
	P99  float64 `json:"p99_ms"`
	P999 float64 `json:"p999_ms"`
	Max  float64 `json:"max_ms"`
}

// DestResult is the per-destination outcome.
type DestResult struct {
	Dest               string         `json:"dest"`
	Type               string         `json:"type"`
	Expected           int            `json:"expected"`
	ReceivedUnique     int            `json:"received_unique"`
	Lost               int            `json:"lost"`
	LostSample         []uint64       `json:"lost_sample,omitempty"`
	DupRaw             int            `json:"dup_raw"`
	DupStored          int            `json:"dup_stored"`
	DLQ                int            `json:"dlq"`
	DLQReasons         map[string]int `json:"dlq_reasons,omitempty"`
	DLQAndDelivered    int            `json:"dlq_and_delivered"`
	Extra              int            `json:"extra"`
	Misrouted          int            `json:"misrouted"`
	Unknown            int            `json:"unknown"` // test_seq not in sent.csv / other run
	Rejected           int            `json:"rejected"`
	DLQMatchesRejected *bool          `json:"dlq_matches_rejected,omitempty"`
	Torn               int            `json:"torn"`
	RecoveryS          *float64       `json:"recovery_s"`
	TimesKnown         bool           `json:"arrival_times_known"`
	LatAll             *Latency       `json:"latency_all,omitempty"`
	LatBaseline        *Latency       `json:"latency_baseline,omitempty"`
	LatDuring          *Latency       `json:"latency_during,omitempty"`
	LatAfter           *Latency       `json:"latency_after,omitempty"`
}

// LoadResult summarises the loadgen side.
type LoadResult struct {
	Planned  int            `json:"planned"`
	Acked    int            `json:"acked"`
	ByStatus map[string]int `json:"by_status"`
}

// CheckResult is one evaluated pass criterion.
type CheckResult struct {
	Check  string  `json:"check"`
	Dest   string  `json:"dest"`
	Value  float64 `json:"value"`
	Pass   bool    `json:"pass"`
	Reason string  `json:"reason,omitempty"`
}

// Result is the reconcile output.
type Result struct {
	Scenario string        `json:"scenario"`
	Profile  string        `json:"profile"`
	RunID    string        `json:"run_id"`
	Load     LoadResult    `json:"load"`
	Window   []float64     `json:"fault_window_s,omitempty"`
	Dests    []DestResult  `json:"destinations"`
	DLQOther int           `json:"dlq_unattributed"`
	Checks   []CheckResult `json:"checks"`
	Pass     bool          `json:"pass"`
}

type seqInfo struct {
	res    *loadgen.Result
	routed []string
}

// Run computes the result.
func Run(in Input) (*Result, error) {
	sc := in.Scenario
	router := sc.Router()
	out := &Result{Scenario: sc.Name, Profile: sc.Profile, RunID: in.RunID,
		Load: LoadResult{Planned: len(in.Sent), ByStatus: map[string]int{}}}

	info := make(map[uint64]seqInfo, len(in.Sent))
	for i := range in.Sent {
		r := &in.Sent[i]
		info[r.TestSeq] = seqInfo{res: r, routed: router.RouteClass(r.Class)}
		key := "error"
		if r.Status != 0 {
			key = fmt.Sprint(r.Status)
		}
		out.Load.ByStatus[key]++
		if r.Acked() {
			out.Load.Acked++
		}
	}
	var winStart, winEnd int64
	hasWin := sc.FaultWindow != nil
	if hasWin {
		winStart = in.StartNS + sc.FaultWindow.Start.Nanoseconds()
		winEnd = in.StartNS + sc.FaultWindow.End.Nanoseconds()
		out.Window = []float64{sc.FaultWindow.Start.Seconds(), sc.FaultWindow.End.Seconds()}
	}

	dlqBy := map[string]map[uint64]string{}
	for _, e := range in.DLQ {
		if _, ok := sc.Dest(e.Dest); !ok || e.TestSeq == 0 || (in.RunID != "" && e.RunID != in.RunID) {
			out.DLQOther++
			continue
		}
		if dlqBy[e.Dest] == nil {
			dlqBy[e.Dest] = map[uint64]string{}
		}
		dlqBy[e.Dest][e.TestSeq] = e.Reason
	}

	for _, src := range in.Sources {
		d := DestResult{Dest: src.Dest, Type: src.Type, Torn: src.Torn, DLQReasons: map[string]int{}}
		copies := map[uint64]int{}
		firstRecv := map[uint64]int64{}
		idems := map[string]bool{}
		rejected4xx := map[uint64]bool{}
		rejected := map[uint64]bool{}
		for _, rc := range src.Receipts {
			if in.RunID != "" && rc.RunID != in.RunID {
				d.Unknown++
				continue
			}
			if !rc.Accepted() {
				rejected[rc.TestSeq] = true
				if rc.Status >= 400 && rc.Status < 500 && rc.Status != 408 && rc.Status != 429 {
					rejected4xx[rc.TestSeq] = true
				}
				continue
			}
			copies[rc.TestSeq]++
			idems[rc.IdemKey] = true
			if rc.RecvNS > 0 {
				if t, ok := firstRecv[rc.TestSeq]; !ok || rc.RecvNS < t {
					firstRecv[rc.TestSeq] = rc.RecvNS
				}
			}
		}
		for s, t := range src.Observed {
			if _, ok := copies[s]; ok {
				if old, ok := firstRecv[s]; !ok || t < old {
					firstRecv[s] = t
				}
			}
		}
		d.TimesKnown = src.Type == "webhook" || src.Observed != nil
		dl := dlqBy[src.Dest]
		for _, reason := range dl {
			d.DLQReasons[reason]++
		}
		d.DLQ = len(dl)
		d.Rejected = len(rejected)

		for s, n := range copies {
			d.DupRaw += n - 1
			if _, ok := dl[s]; ok {
				d.DLQAndDelivered++
			}
			si, known := info[s]
			switch {
			case !known:
				d.Unknown++
			case !si.res.Acked():
				d.Extra++
			}
			if known && !slices.Contains(si.routed, src.Dest) {
				d.Misrouted++
			}
		}
		for s := range dl {
			if si, ok := info[s]; ok && !slices.Contains(si.routed, src.Dest) {
				d.Misrouted++
			}
		}
		d.ReceivedUnique = len(copies)
		switch src.Type {
		case "sqlite", "file":
			d.DupStored = d.DupRaw // one row per stored copy
		case "webhook":
			d.DupStored = max(0, len(idems)-len(copies))
		}
		if len(rejected4xx) > 0 {
			match := len(rejected4xx) == len(dl)
			for s := range rejected4xx {
				if _, ok := dl[s]; !ok {
					match = false
					break
				}
			}
			d.DLQMatchesRejected = &match
		}

		// Expected set, loss, recovery, latency.
		var lats, base, during, after []float64
		recovered, recoveryKnown := int64(math.MinInt64), d.TimesKnown
		for s, si := range info {
			if !si.res.Acked() || !slices.Contains(si.routed, src.Dest) {
				continue
			}
			d.Expected++
			_, delivered := copies[s]
			_, dead := dl[s]
			if !delivered && !dead {
				d.Lost++
				if len(d.LostSample) < 20 {
					d.LostSample = append(d.LostSample, s)
				}
			}
			if !delivered {
				continue
			}
			t, ok := firstRecv[s]
			if !ok {
				if hasWin && si.res.DoneNS < winEnd && !dead {
					recoveryKnown = false
				}
				continue
			}
			ms := float64(t-si.res.ScheduledNS) / 1e6
			lats = append(lats, ms)
			if hasWin {
				switch sched := si.res.ScheduledNS; {
				case sched < winStart:
					base = append(base, ms)
				case sched < winEnd:
					during = append(during, ms)
				default:
					after = append(after, ms)
				}
				if si.res.DoneNS < winEnd && !dead {
					recovered = max(recovered, t)
				}
			}
		}
		if hasWin && recoveryKnown && d.Lost == 0 {
			r := 0.0
			if recovered > winEnd {
				r = float64(recovered-winEnd) / 1e9
			}
			d.RecoveryS = &r
		}
		d.LatAll = latency(lats)
		if hasWin {
			d.LatBaseline, d.LatDuring, d.LatAfter = latency(base), latency(during), latency(after)
		}
		slices.Sort(d.LostSample)
		out.Dests = append(out.Dests, d)
	}
	slices.SortFunc(out.Dests, func(a, b DestResult) int { return cmp.Compare(a.Dest, b.Dest) })
	out.Checks, out.Pass = Evaluate(sc.AllChecks(), out.Dests)
	return out, nil
}

func latency(ms []float64) *Latency {
	if len(ms) == 0 {
		return nil
	}
	sort.Float64s(ms)
	return &Latency{N: len(ms), P50: stats.Percentile(ms, 0.50), P99: stats.Percentile(ms, 0.99),
		P999: stats.Percentile(ms, 0.999), Max: ms[len(ms)-1]}
}

// Metric returns a named metric of d (ok=false if undefined for d).
func (d *DestResult) Metric(name string) (float64, bool) {
	b2f := func(b bool) float64 {
		if b {
			return 1
		}
		return 0
	}
	switch name {
	case "expected":
		return float64(d.Expected), true
	case "received_unique":
		return float64(d.ReceivedUnique), true
	case "lost":
		return float64(d.Lost), true
	case "dup_raw":
		return float64(d.DupRaw), true
	case "dup_stored":
		return float64(d.DupStored), true
	case "dlq":
		return float64(d.DLQ), true
	case "dlq_and_delivered":
		return float64(d.DLQAndDelivered), true
	case "extra":
		return float64(d.Extra), true
	case "misrouted":
		return float64(d.Misrouted), true
	case "unknown":
		return float64(d.Unknown), true
	case "dlq_matches_rejected":
		if d.DLQMatchesRejected == nil {
			return 0, false
		}
		return b2f(*d.DLQMatchesRejected), true
	case "recovery_s":
		if d.RecoveryS == nil {
			return 0, false
		}
		return *d.RecoveryS, true
	case "p99_during_ms":
		if d.LatDuring == nil {
			return 0, false
		}
		return d.LatDuring.P99, true
	case "p99_baseline_ms":
		if d.LatBaseline == nil {
			return 0, false
		}
		return d.LatBaseline.P99, true
	}
	if r, ok := strings.CutPrefix(name, "dlq_reason:"); ok {
		return float64(d.DLQReasons[r]), true
	}
	return 0, false
}

// Evaluate applies checks to the destinations.
func Evaluate(checks []scenario.Check, dests []DestResult) ([]CheckResult, bool) {
	pass := true
	var out []CheckResult
	for _, c := range checks {
		targets := c.Dests
		if len(targets) == 0 || slices.Contains(targets, "all") {
			targets = nil
			for _, d := range dests {
				targets = append(targets, d.Dest)
			}
		}
		for _, name := range targets {
			cr := CheckResult{Check: fmt.Sprintf("%s %s %g", c.Metric, c.Op, c.Value), Dest: name}
			i := slices.IndexFunc(dests, func(d DestResult) bool { return d.Dest == name })
			if i < 0 {
				cr.Reason = "destination missing"
			} else if v, ok := dests[i].Metric(c.Metric); !ok {
				cr.Reason = "metric undefined"
			} else {
				cr.Value, cr.Pass = v, c.Eval(v)
			}
			pass = pass && cr.Pass
			out = append(out, cr)
		}
	}
	return out, pass
}
