package reconcile

import (
	"fmt"
	"strings"
)

// FormatFloatPtr renders an optional number ("-" when nil).
func FormatFloatPtr(v *float64, prec int) string {
	if v == nil {
		return "-"
	}
	return fmt.Sprintf("%.*f", prec, *v)
}

func fmtLat(l *Latency) string {
	if l == nil {
		return "-"
	}
	return fmt.Sprintf("%.1f / %.1f / %.1f", l.P50, l.P99, l.P999)
}

// Markdown renders one reconcile result.
func Markdown(r *Result) string {
	var b strings.Builder
	verdict := "PASS"
	if !r.Pass {
		verdict = "FAIL"
	}
	fmt.Fprintf(&b, "### %s (%s) run %s: %s\n\n", r.Scenario, r.Profile, r.RunID, verdict)
	fmt.Fprintf(&b, "Load: planned %d, acked (HTTP 200) %d, by status %v", r.Load.Planned, r.Load.Acked, r.Load.ByStatus)
	if r.Window != nil {
		fmt.Fprintf(&b, "; fault window %.1fs..%.1fs", r.Window[0], r.Window[1])
	}
	b.WriteString("\n\n")
	b.WriteString("| dest | expected | received | lost | dup_raw | dup_stored | dlq | extra | misrouted | recovery_s | latency baseline p50/p99/p999 ms | during | after |\n")
	b.WriteString("|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|---|---|\n")
	for _, d := range r.Dests {
		fmt.Fprintf(&b, "| %s (%s) | %d | %d | %d | %d | %d | %d | %d | %d | %s | %s | %s | %s |\n",
			d.Dest, d.Type, d.Expected, d.ReceivedUnique, d.Lost, d.DupRaw, d.DupStored, d.DLQ, d.Extra, d.Misrouted,
			FormatFloatPtr(d.RecoveryS, 2), fmtLat(d.LatBaseline), fmtLat(d.LatDuring), fmtLat(d.LatAfter))
	}
	var notes []string
	for _, d := range r.Dests {
		if len(d.DLQReasons) > 0 {
			notes = append(notes, fmt.Sprintf("%s DLQ reasons %v", d.Dest, d.DLQReasons))
		}
		if d.DLQMatchesRejected != nil {
			notes = append(notes, fmt.Sprintf("%s DLQ set == sink 4xx-rejected set: %v", d.Dest, *d.DLQMatchesRejected))
		}
		if d.Torn > 0 {
			notes = append(notes, fmt.Sprintf("%s torn fragments skipped: %d", d.Dest, d.Torn))
		}
		if d.DLQAndDelivered > 0 {
			notes = append(notes, fmt.Sprintf("%s delivered AND dead-lettered: %d", d.Dest, d.DLQAndDelivered))
		}
		if d.Unknown > 0 {
			notes = append(notes, fmt.Sprintf("%s unknown test_seqs: %d", d.Dest, d.Unknown))
		}
		if len(d.LostSample) > 0 {
			notes = append(notes, fmt.Sprintf("%s lost sample: %v", d.Dest, d.LostSample))
		}
	}
	if r.DLQOther > 0 {
		notes = append(notes, fmt.Sprintf("DLQ entries not attributable to this run: %d", r.DLQOther))
	}
	if len(notes) > 0 {
		b.WriteString("\n")
		for _, n := range notes {
			fmt.Fprintf(&b, "- %s\n", n)
		}
	}
	var failed []string
	for _, c := range r.Checks {
		if !c.Pass {
			s := fmt.Sprintf("%s [%s] = %g", c.Check, c.Dest, c.Value)
			if c.Reason != "" {
				s += " (" + c.Reason + ")"
			}
			failed = append(failed, s)
		}
	}
	if len(failed) > 0 {
		b.WriteString("\nFailed checks:\n")
		for _, f := range failed {
			fmt.Fprintf(&b, "- %s\n", f)
		}
	}
	return b.String()
}
