package reconcile

import (
	"fmt"
	"math"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/lzwhehe/loggate/internal/bench/loadgen"
	"github.com/lzwhehe/loggate/internal/bench/scenario"
)

const run = "r1"

func testScenario(t *testing.T) *scenario.Scenario {
	t.Helper()
	sc, err := scenario.Parse([]byte(`
name: T
duration: 10s
classes: [{ name: a, weight: 1 }, { name: b, weight: 1 }]
destinations:
  - { name: f,  type: file }
  - { name: q,  type: sqlite }
  - { name: w,  type: webhook, mock: true }
routes:
  - { match: { class: a }, to: [f, q, w] }
  - { match: {}, to: [f] }
fault_window: { start: 2s, end: 5s }
checks:
  - { metric: lost, dests: [all], op: eq, value: 0 }
`))
	if err != nil {
		t.Fatal(err)
	}
	return sc
}

const sec = int64(time.Second)

// sent builds loadgen results: seq i scheduled at i*100ms, acked unless in
// notAcked, class "a" for odd seqs and "b" for even ones.
func sent(n int, notAcked ...uint64) []loadgen.Result {
	na := map[uint64]bool{}
	for _, s := range notAcked {
		na[s] = true
	}
	out := make([]loadgen.Result, n)
	for i := range out {
		s := uint64(i + 1)
		cls := "b"
		if s%2 == 1 {
			cls = "a"
		}
		st := 200
		if na[s] {
			st = 503
		}
		sched := int64(i) * sec / 10
		out[i] = loadgen.Result{TestSeq: s, Class: cls, ScheduledNS: sched, SentNS: sched, DoneNS: sched + sec/100, Status: st, Attempts: 1}
	}
	return out
}

func rc(seq uint64, recv int64) Receipt {
	return Receipt{TestSeq: seq, IdemKey: fmt.Sprintf("k%d", seq), RunID: run, RecvNS: recv, Status: 200}
}

func find(t *testing.T, r *Result, dest string) DestResult {
	t.Helper()
	for _, d := range r.Dests {
		if d.Dest == dest {
			return d
		}
	}
	t.Fatalf("no dest %s", dest)
	return DestResult{}
}

// The expected set is acked ∩ routed (by the oracle), per destination.
func TestExpectedSetAndLoss(t *testing.T) {
	sc := testScenario(t)
	s := sent(10, 3, 4) // 3 (class a) and 4 (class b) not acked
	var fileR, sqlR []Receipt
	for i := uint64(1); i <= 10; i++ {
		fileR = append(fileR, rc(i, 0)) // file gets everything, including unacked 3 and 4
		if i%2 == 1 && i != 5 && i != 3 {
			sqlR = append(sqlR, rc(i, 0)) // sqlite misses 5 (lost) and 3 (unacked: fine)
		}
	}
	res, err := Run(Input{Scenario: sc, RunID: run, Sent: s, Sources: []Source{
		{Dest: "f", Type: "file", Receipts: fileR},
		{Dest: "q", Type: "sqlite", Receipts: sqlR},
		{Dest: "w", Type: "webhook"},
	}, DLQ: []DLQEntry{{Dest: "w", TestSeq: 1, RunID: run, Reason: "non_retryable"}}})
	if err != nil {
		t.Fatal(err)
	}
	f, q, w := find(t, res, "f"), find(t, res, "q"), find(t, res, "w")
	if f.Expected != 8 || f.Lost != 0 || f.Extra != 2 || f.ReceivedUnique != 10 {
		t.Fatalf("file: %+v", f)
	}
	// q routes odd seqs; acked odd = 1,5,7,9 -> 4 expected; 5 lost.
	if q.Expected != 4 || q.Lost != 1 || q.LostSample[0] != 5 {
		t.Fatalf("sqlite: %+v", q)
	}
	// w: expected 1,5,7,9; nothing delivered; 1 dead-lettered -> lost 3.
	if w.Expected != 4 || w.Lost != 3 || w.DLQ != 1 || w.DLQReasons["non_retryable"] != 1 {
		t.Fatalf("webhook: %+v", w)
	}
	if res.Pass {
		t.Fatal("checks passed despite losses")
	}
	if res.Load.Acked != 8 || res.Load.ByStatus["503"] != 2 {
		t.Fatalf("load: %+v", res.Load)
	}
}

func TestMisroutedAndUnknown(t *testing.T) {
	sc := testScenario(t)
	s := sent(4)
	res, err := Run(Input{Scenario: sc, RunID: run, Sent: s, Sources: []Source{
		{Dest: "q", Type: "sqlite", Receipts: []Receipt{rc(1, 0), rc(2, 0), rc(3, 0), rc(99, 0),
			{TestSeq: 1, IdemKey: "x", RunID: "other-run", Status: 200}}},
	}})
	if err != nil {
		t.Fatal(err)
	}
	q := find(t, res, "q")
	if q.Misrouted != 1 || q.Unknown != 2 || q.Lost != 0 {
		t.Fatalf("%+v", q)
	}
}

// Duplicate counting: raw copies, sqlite rows vs unique test_seq, webhook
// idem keys vs unique test_seq.
func TestDupCounting(t *testing.T) {
	sc := testScenario(t)
	s := sent(4)
	w := []Receipt{
		{TestSeq: 1, IdemKey: "k1", RunID: run, Status: 200, RecvNS: 1},
		{TestSeq: 1, IdemKey: "k1", RunID: run, Status: 200, RecvNS: 2}, // redelivery: same key
		{TestSeq: 3, IdemKey: "k3", RunID: run, Status: 200, RecvNS: 3},
		{TestSeq: 3, IdemKey: "k3b", RunID: run, Status: 200, RecvNS: 4}, // different key: stored dup
		{TestSeq: 3, IdemKey: "k3", RunID: run, Status: 503, RecvNS: 5},  // rejected copy: not counted
	}
	q := []Receipt{
		{TestSeq: 1, IdemKey: "k1", RunID: run, Status: 200},
		{TestSeq: 3, IdemKey: "k3", RunID: run, Status: 200},
		{TestSeq: 3, IdemKey: "k3b", RunID: run, Status: 200}, // two rows for one event
	}
	f := []Receipt{rc(1, 0), rc(1, 0), rc(1, 0), rc(2, 0), rc(3, 0), rc(4, 0)}
	res, err := Run(Input{Scenario: sc, RunID: run, Sent: s, Sources: []Source{
		{Dest: "w", Type: "webhook", Receipts: w}, {Dest: "q", Type: "sqlite", Receipts: q}, {Dest: "f", Type: "file", Receipts: f},
	}})
	if err != nil {
		t.Fatal(err)
	}
	if d := find(t, res, "w"); d.DupRaw != 2 || d.DupStored != 1 || d.Rejected != 1 || d.Lost != 0 {
		t.Fatalf("webhook: %+v", d)
	}
	if d := find(t, res, "q"); d.DupRaw != 1 || d.DupStored != 1 {
		t.Fatalf("sqlite: %+v", d)
	}
	if d := find(t, res, "f"); d.DupRaw != 2 || d.DupStored != 2 {
		t.Fatalf("file: %+v", d)
	}
}

// recovery_s: time after the window end until all events acked before
// the end arrived; latency split by scheduled time.
func TestRecoveryAndLatencyWindows(t *testing.T) {
	sc := testScenario(t)
	sc.FaultWindow = &scenario.Window{Start: 2 * time.Second, End: 5 * time.Second}
	s := sent(80) // 8 s of load, seq i scheduled at (i-1)*100ms
	start := int64(1000) * sec
	for i := range s {
		s[i].ScheduledNS += start
		s[i].SentNS += start
		s[i].DoneNS += start
	}
	winEnd := start + 5*sec
	var recs []Receipt
	for _, r := range s {
		if r.Class != "a" {
			continue
		}
		recv := r.ScheduledNS + 20*int64(time.Millisecond)
		if r.ScheduledNS >= start+2*sec && r.ScheduledNS < winEnd {
			recv = winEnd + 1500*int64(time.Millisecond) // held during the fault
		}
		if r.TestSeq == 41 { // scheduled at 4.0s: the last to arrive
			recv = winEnd + 3*sec
		}
		recs = append(recs, Receipt{TestSeq: r.TestSeq, IdemKey: "k", RunID: run, RecvNS: recv, Status: 200})
	}
	res, err := Run(Input{Scenario: sc, RunID: run, StartNS: start, Sent: s,
		Sources: []Source{{Dest: "w", Type: "webhook", Receipts: recs}}})
	if err != nil {
		t.Fatal(err)
	}
	w := find(t, res, "w")
	if w.RecoveryS == nil || math.Abs(*w.RecoveryS-3.0) > 1e-9 {
		t.Fatalf("recovery = %v, want 3", FormatFloatPtr(w.RecoveryS, 3))
	}
	if w.LatBaseline == nil || w.LatBaseline.P99 != 20 || w.LatDuring == nil || w.LatDuring.P50 < 1500 || w.LatAfter.P99 != 20 {
		t.Fatalf("latency: base %+v during %+v after %+v", w.LatBaseline, w.LatDuring, w.LatAfter)
	}

	// An event acked before the end that never arrives: lost, recovery null.
	recs2 := make([]Receipt, 0, len(recs))
	for _, r := range recs {
		if r.TestSeq != 41 {
			recs2 = append(recs2, r)
		}
	}
	res, _ = Run(Input{Scenario: sc, RunID: run, StartNS: start, Sent: s, Sources: []Source{{Dest: "w", Type: "webhook", Receipts: recs2}}})
	if w := find(t, res, "w"); w.RecoveryS != nil || w.Lost < 1 {
		t.Fatalf("with loss: recovery %v lost %d", FormatFloatPtr(w.RecoveryS, 3), w.Lost)
	}

	// Healthy destination: everything arrives before the end -> 0.
	var healthy []Receipt
	for _, r := range s {
		if r.Class == "a" {
			healthy = append(healthy, Receipt{TestSeq: r.TestSeq, IdemKey: "k", RunID: run, RecvNS: r.ScheduledNS + 1e6, Status: 200})
		}
	}
	res, _ = Run(Input{Scenario: sc, RunID: run, StartNS: start, Sent: s, Sources: []Source{{Dest: "w", Type: "webhook", Receipts: healthy}}})
	if w := find(t, res, "w"); w.RecoveryS == nil || *w.RecoveryS != 0 {
		t.Fatalf("healthy recovery = %v", FormatFloatPtr(w.RecoveryS, 3))
	}

	// File sink without observer times: recovery unknown, not zero.
	var fr []Receipt
	for _, r := range s {
		fr = append(fr, Receipt{TestSeq: r.TestSeq, RunID: run, Status: 200})
	}
	res, _ = Run(Input{Scenario: sc, RunID: run, StartNS: start, Sent: s, Sources: []Source{{Dest: "f", Type: "file", Receipts: fr}}})
	if f := find(t, res, "f"); f.RecoveryS != nil || f.TimesKnown || f.LatAll != nil {
		t.Fatalf("file without times: %+v", f)
	}
}

// DLQ set must equal the set the mock rejected with a non-retryable 4xx.
func TestDLQMatchesRejected(t *testing.T) {
	sc := testScenario(t)
	s := sent(6)
	recs := []Receipt{
		{TestSeq: 1, IdemKey: "a", RunID: run, Status: 400, RecvNS: 1},
		{TestSeq: 3, IdemKey: "b", RunID: run, Status: 200, RecvNS: 1},
		{TestSeq: 5, IdemKey: "c", RunID: run, Status: 429, RecvNS: 1}, // throttled: retried, not DLQ
		{TestSeq: 5, IdemKey: "c", RunID: run, Status: 200, RecvNS: 2},
	}
	in := Input{Scenario: sc, RunID: run, Sent: s, Sources: []Source{{Dest: "w", Type: "webhook", Receipts: recs}},
		DLQ: []DLQEntry{{Dest: "w", TestSeq: 1, RunID: run, Reason: "non_retryable"}}}
	res, _ := Run(in)
	if w := find(t, res, "w"); w.DLQMatchesRejected == nil || !*w.DLQMatchesRejected || w.Lost != 0 || w.Rejected != 2 {
		t.Fatalf("match: %+v", w)
	}
	in.DLQ = append(in.DLQ, DLQEntry{Dest: "w", TestSeq: 3, RunID: run, Reason: "max_age"})
	res, _ = Run(in)
	w := find(t, res, "w")
	if *w.DLQMatchesRejected || w.DLQAndDelivered != 1 {
		t.Fatalf("mismatch not detected: %+v", w)
	}
	if v, ok := w.Metric("dlq_reason:max_age"); !ok || v != 1 {
		t.Fatalf("dlq_reason metric = %v %v", v, ok)
	}
}

func TestEvaluateUndefinedMetricFails(t *testing.T) {
	checks := []scenario.Check{{Metric: "recovery_s", Dests: []string{"all"}, Op: "le", Value: 10}}
	out, pass := Evaluate(checks, []DestResult{{Dest: "x"}})
	if pass || out[0].Reason != "metric undefined" {
		t.Fatalf("%+v pass=%v", out, pass)
	}
}

// Torn last line and a fragment glued to a record after a restart.
func TestFileSinkTornLines(t *testing.T) {
	line := func(seq, ts int) string {
		return `{"seq":` + strconv.Itoa(seq) + `,"idem_key":"k` + strconv.Itoa(seq) + `","source_id":"http:loadgen","received_at":1,"fields":{"unmapped":{"run_id":"r1","test_seq":` + strconv.Itoa(ts) + `}}}`
	}
	full := line(1, 11) + "\n" +
		line(2, 12)[:30] + line(3, 13) + "\n" + // killed mid-line, restarted writer appended
		line(4, 14) + "\n" +
		line(5, 15)[:40] + "\n" + // fragment terminated by the sink on restart
		line(6, 16) + "\n" +
		line(7, 17)[:25] // torn tail
	recs, st, err := ReadFileSink(strings.NewReader(full))
	if err != nil {
		t.Fatal(err)
	}
	var got []uint64
	for _, r := range recs {
		got = append(got, r.TestSeq)
	}
	want := []uint64{11, 13, 14, 16}
	if len(got) != len(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("got %v, want %v", got, want)
		}
	}
	if st.TornFragments != 3 {
		t.Fatalf("torn fragments = %d, want 3", st.TornFragments)
	}
}

func TestFileTailIncremental(t *testing.T) {
	path := t.TempDir() + "/a.jsonl"
	ft := NewFileTail(path)
	if recs, err := ft.Next(); err != nil || len(recs) != 0 {
		t.Fatalf("missing file: %v %v", recs, err)
	}
	l := func(ts int) string {
		return `{"seq":1,"idem_key":"k","fields":{"unmapped":{"run_id":"r1","test_seq":` + strconv.Itoa(ts) + `}}}`
	}
	writeFile(t, path, l(1)+"\n"+l(2)[:20])
	if recs, _ := ft.Next(); len(recs) != 1 || recs[0].TestSeq != 1 {
		t.Fatalf("first poll: %+v", recs)
	}
	appendFile(t, path, l(2)[20:]+"\n"+l(3)+"\n")
	if recs, _ := ft.Next(); len(recs) != 2 || recs[0].TestSeq != 2 || recs[1].TestSeq != 3 {
		t.Fatalf("second poll: %+v", recs)
	}
}

func writeFile(t *testing.T, path, s string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(s), 0o644); err != nil {
		t.Fatal(err)
	}
}

func appendFile(t *testing.T, path, s string) {
	t.Helper()
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if _, err := f.WriteString(s); err != nil {
		t.Fatal(err)
	}
}
