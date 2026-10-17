package faultbench

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"github.com/lzwhehe/loggate/internal/bench/reconcile"
	"github.com/lzwhehe/loggate/internal/bench/stats"
)

// SummaryMetrics are aggregated across repetitions (mean, 95% CI).
var SummaryMetrics = []string{
	"expected", "lost", "dup_raw", "dup_stored", "dlq", "extra", "misrouted",
	"recovery_s", "p99_baseline_ms", "p99_during_ms", "p99_after_ms",
}

// DestSummary maps metric -> summary for one destination.
type DestSummary map[string]stats.Summary

func metric(d *reconcile.DestResult, name string) (float64, bool) {
	if name == "p99_after_ms" {
		if d.LatAfter == nil {
			return 0, false
		}
		return d.LatAfter.P99, true
	}
	return d.Metric(name)
}

// Aggregate summarises repetitions per destination and metric.
func Aggregate(reps []*RepResult) (map[string]DestSummary, bool) {
	vals := map[string]map[string][]float64{}
	pass := len(reps) > 0
	for _, r := range reps {
		if r.Recon == nil {
			pass = false
			continue
		}
		pass = pass && r.Recon.Pass && r.Drained && len(r.Errors) == 0
		for i := range r.Recon.Dests {
			d := &r.Recon.Dests[i]
			if vals[d.Dest] == nil {
				vals[d.Dest] = map[string][]float64{}
			}
			for _, m := range SummaryMetrics {
				if v, ok := metric(d, m); ok {
					vals[d.Dest][m] = append(vals[d.Dest][m], v)
				}
			}
		}
	}
	out := map[string]DestSummary{}
	for dest, ms := range vals {
		out[dest] = DestSummary{}
		for m, xs := range ms {
			out[dest][m] = stats.Describe(xs)
		}
	}
	return out, pass
}

func fmtSummary(s stats.Summary, ok bool, prec int) string {
	if !ok || s.N == 0 {
		return "-"
	}
	if !s.HasCI {
		return fmt.Sprintf("%.*f", prec, s.Mean)
	}
	return fmt.Sprintf("%.*f ± %.*f", prec, s.Mean, prec, s.CI95)
}

// WriteReport writes fault.md and summary.csv for all scenarios.
func WriteReport(dir string, all []*ScenarioResult) error {
	var b strings.Builder
	b.WriteString("# LogGate fault-injection results\n\n")
	if len(all) > 0 && all[0].Env != nil {
		e := all[0].Env
		fmt.Fprintf(&b, "- Profile: **%s**\n", e.Profile)
		fmt.Fprintf(&b, "- Environment: %s/%s, %s, CPU %s (%d logical), %s\n", e.OS, e.Arch, e.OSVersion, e.CPU, e.NumCPU, e.GoVersion)
		dirty := ""
		if e.GitDirty {
			dirty = " (working tree dirty)"
		}
		fmt.Fprintf(&b, "- Commit: `%s`%s; started %s\n", e.GitCommit, dirty, e.StartedAt)
		for _, n := range e.Notes {
			fmt.Fprintf(&b, "- %s\n", n)
		}
		b.WriteString("\n")
	}
	b.WriteString("Values are mean ± 95% CI half-width (Student t) across repetitions; a single repetition shows the raw value. ")
	b.WriteString("lost = acked ∩ routed but neither delivered nor in the DLQ. Latency = first arrival at the sink − scheduled send time (ms). ")
	b.WriteString("recovery_s = time after the fault window ends until every event acked before that end has arrived. ")
	b.WriteString("file/sqlite arrival times come from an independent poller (50 ms resolution).\n\n")

	baseline := map[string]float64{} // dest -> S0 p99 during (reference window)
	for _, sr := range all {
		if sr.Scenario == "S0" {
			for dest, ds := range sr.Summary {
				if s, ok := ds["p99_during_ms"]; ok && s.N > 0 {
					baseline[dest] = s.Mean
				}
			}
		}
	}

	b.WriteString("## Overview\n\n| scenario | title | reps | verdict | lost (all dests) | notes |\n|---|---|---:|---|---:|---|\n")
	for _, sr := range all {
		if sr.Skipped != "" {
			fmt.Fprintf(&b, "| %s | %s | 0 | SKIPPED | - | %s |\n", sr.Scenario, sr.Title, sr.Skipped)
			continue
		}
		lost := 0.0
		for _, ds := range sr.Summary {
			lost += ds["lost"].Mean
		}
		verdict := "PASS"
		if !sr.PassAll {
			verdict = "FAIL"
		}
		var notes []string
		for _, r := range sr.Reps {
			notes = append(notes, r.Errors...)
		}
		fmt.Fprintf(&b, "| %s | %s | %d | %s | %g | %s |\n", sr.Scenario, sr.Title, len(sr.Reps), verdict, lost, strings.Join(notes, "; "))
	}
	b.WriteString("\n")

	for _, sr := range all {
		if sr.Skipped != "" {
			continue
		}
		fmt.Fprintf(&b, "## %s: %s\n\n", sr.Scenario, sr.Title)
		if sc := sr.Resolved; sc != nil {
			if sc.Description != "" {
				fmt.Fprintf(&b, "%s\n\n", strings.TrimSpace(sc.Description))
			}
			fmt.Fprintf(&b, "Load %g events/s for %v. Timeline:", sc.Rate, sc.Duration)
			if len(sc.Timeline) == 0 {
				b.WriteString(" none")
			}
			for _, a := range sc.Timeline {
				fmt.Fprintf(&b, " %v %s", a.At, a.Action)
				if a.Dest != "" {
					fmt.Fprintf(&b, " %s", a.Dest)
				}
				if a.Mode != "" {
					fmt.Fprintf(&b, "=%s", a.Mode)
				}
				if a.DelayMS > 0 {
					fmt.Fprintf(&b, "(%dms)", a.DelayMS)
				}
				if a.ErrorRate > 0 {
					fmt.Fprintf(&b, "(%d@%g)", a.ErrorCode, a.ErrorRate)
				}
				b.WriteString(";")
			}
			if w := sc.FaultWindow; w != nil {
				fmt.Fprintf(&b, " fault window %v..%v.", w.Start, w.End)
			}
			b.WriteString("\n\n")
		}
		b.WriteString("| dest | expected | lost | dup_raw | dup_stored | dlq | extra | recovery_s | p99 baseline ms | p99 during ms | p99 after ms | p99 during vs S0 |\n")
		b.WriteString("|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|\n")
		dests := make([]string, 0, len(sr.Summary))
		for d := range sr.Summary {
			dests = append(dests, d)
		}
		slices.Sort(dests)
		for _, d := range dests {
			ds := sr.Summary[d]
			get := func(m string, prec int) string { s, ok := ds[m]; return fmtSummary(s, ok, prec) }
			vs := "-"
			if base, ok := baseline[d]; ok && base > 0 && sr.Scenario != "S0" {
				if s, ok := ds["p99_during_ms"]; ok && s.N > 0 {
					vs = fmt.Sprintf("%+.0f%%", (s.Mean/base-1)*100)
				}
			}
			fmt.Fprintf(&b, "| %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s |\n", d,
				get("expected", 0), get("lost", 0), get("dup_raw", 0), get("dup_stored", 0), get("dlq", 0), get("extra", 0),
				get("recovery_s", 2), get("p99_baseline_ms", 1), get("p99_during_ms", 1), get("p99_after_ms", 1), vs)
		}
		b.WriteString("\n")
		for _, r := range sr.Reps {
			if r.Recon == nil {
				fmt.Fprintf(&b, "Rep %d: no reconcile result (%s)\n\n", r.Rep, strings.Join(r.Errors, "; "))
				continue
			}
			var failed []string
			for _, c := range r.Recon.Checks {
				if !c.Pass {
					failed = append(failed, fmt.Sprintf("%s [%s]=%g%s", c.Check, c.Dest, c.Value, reasonSuffix(c.Reason)))
				}
			}
			verdict := "pass"
			if len(failed) > 0 {
				verdict = "FAIL: " + strings.Join(failed, ", ")
			}
			var extras []string
			for _, d := range r.Recon.Dests {
				if len(d.DLQReasons) > 0 {
					extras = append(extras, fmt.Sprintf("%s dlq %v", d.Dest, d.DLQReasons))
				}
				if d.DLQMatchesRejected != nil {
					extras = append(extras, fmt.Sprintf("%s dlq==rejected4xx %v", d.Dest, *d.DLQMatchesRejected))
				}
				if d.Torn > 0 {
					extras = append(extras, fmt.Sprintf("%s torn %d", d.Dest, d.Torn))
				}
			}
			load := ""
			if r.Load != nil {
				load = fmt.Sprintf("acked %d/%d %v, client behind %d, inflight-cap hits %d, ack p99 %.1f ms",
					r.Load.Acked, r.Load.Planned, r.Load.ByStatus, r.Load.BehindCount, r.Load.InFlightCapHit, r.Load.AckLatencyMS.P99)
			}
			fmt.Fprintf(&b, "- rep %d (`%s`): %s; gateway starts %d; drained %v after %.1fs; %s", r.Rep, r.RunID, verdict, r.GatewayStarts, r.Drained, r.DrainS, load)
			if len(extras) > 0 {
				fmt.Fprintf(&b, "; %s", strings.Join(extras, "; "))
			}
			if len(r.Errors) > 0 {
				fmt.Fprintf(&b, "; errors: %s", strings.Join(r.Errors, "; "))
			}
			b.WriteString("\n")
		}
		b.WriteString("\n")
	}
	if err := os.WriteFile(filepath.Join(dir, "fault.md"), []byte(b.String()), 0o644); err != nil {
		return err
	}
	return writeSummaryCSV(filepath.Join(dir, "summary.csv"), all)
}

func reasonSuffix(r string) string {
	if r == "" {
		return ""
	}
	return " (" + r + ")"
}

// writeSummaryCSV writes one row per scenario × repetition × destination.
func writeSummaryCSV(path string, all []*ScenarioResult) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	w := bufio.NewWriter(f)
	fmt.Fprintln(w, "scenario,profile,rep,run_id,dest,type,expected,received_unique,lost,dup_raw,dup_stored,dlq,extra,misrouted,recovery_s,p50_all_ms,p99_all_ms,p999_all_ms,p99_baseline_ms,p99_during_ms,p99_after_ms,pass")
	opt := func(v float64, ok bool) string {
		if !ok {
			return ""
		}
		return fmt.Sprintf("%.3f", v)
	}
	for _, sr := range all {
		for _, r := range sr.Reps {
			if r.Recon == nil {
				continue
			}
			for i := range r.Recon.Dests {
				d := &r.Recon.Dests[i]
				var p50, p99, p999 string
				if d.LatAll != nil {
					p50, p99, p999 = opt(d.LatAll.P50, true), opt(d.LatAll.P99, true), opt(d.LatAll.P999, true)
				}
				rec, recOK := d.Metric("recovery_s")
				pb, pbOK := d.Metric("p99_baseline_ms")
				pd, pdOK := d.Metric("p99_during_ms")
				pa, paOK := metric(d, "p99_after_ms")
				fmt.Fprintf(w, "%s,%s,%d,%s,%s,%s,%d,%d,%d,%d,%d,%d,%d,%d,%s,%s,%s,%s,%s,%s,%s,%v\n",
					sr.Scenario, sr.Profile, r.Rep, r.RunID, d.Dest, d.Type, d.Expected, d.ReceivedUnique, d.Lost, d.DupRaw,
					d.DupStored, d.DLQ, d.Extra, d.Misrouted, opt(rec, recOK), p50, p99, p999,
					opt(pb, pbOK), opt(pd, pdOK), opt(pa, paOK), r.Recon.Pass)
			}
		}
	}
	return errors.Join(w.Flush(), f.Close())
}
