package piibench

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"regexp"
	"slices"
	"sort"
	"strings"
	"unicode/utf8"

	"github.com/lzwhehe/loggate/internal/mask"
)

// Pred is a predicted span.
type Pred struct {
	Start, End int
	Type       string
}

// System is a detector under evaluation.
type System struct {
	Name   string
	Detect func(string) []Pred
}

// EngineSystem wraps the LogGate detector.
func EngineSystem(e *mask.Engine) System {
	det := e.Detector()
	return System{Name: "loggate", Detect: func(s string) []Pred {
		var out []Pred
		for _, sp := range det.Detect(s, nil) {
			out = append(out, Pred{sp.Start, sp.End, sp.Kind.String()})
		}
		return out
	}}
}

var typePrio = map[string]int{TypeIDCard: 0, TypeBankCard: 1, TypePhone: 2, TypeEmail: 3}

// RegexSystem is the regex-only baseline: one pattern per type, overlaps
// resolved with the same priority as the engine (id > card > phone >
// email, longer first). bounded adds \b on both sides.
func RegexSystem(bounded bool) System {
	pats := map[string]string{
		TypeIDCard:   `\d{17}[\dXx]`,
		TypeBankCard: `\d{16,19}`,
		TypePhone:    `1[3-9]\d{9}`,
		TypeEmail:    `[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}`,
	}
	name := "regex_naive"
	if bounded {
		name = "regex_bounded"
	}
	res := map[string]*regexp.Regexp{}
	for t, p := range pats {
		if bounded {
			p = `\b` + p + `\b`
		}
		res[t] = regexp.MustCompile(p)
	}
	return System{Name: name, Detect: func(s string) []Pred {
		var all []Pred
		for _, t := range Types {
			for _, m := range res[t].FindAllStringIndex(s, -1) {
				all = append(all, Pred{m[0], m[1], t})
			}
		}
		return resolvePreds(all)
	}}
}

func resolvePreds(all []Pred) []Pred {
	sort.SliceStable(all, func(i, j int) bool {
		a, b := all[i], all[j]
		if typePrio[a.Type] != typePrio[b.Type] {
			return typePrio[a.Type] < typePrio[b.Type]
		}
		return a.End-a.Start > b.End-b.Start
	})
	var kept []Pred
	for _, p := range all {
		ok := true
		for _, k := range kept {
			if p.Start < k.End && k.Start < p.End {
				ok = false
				break
			}
		}
		if ok {
			kept = append(kept, p)
		}
	}
	sort.Slice(kept, func(i, j int) bool { return kept[i].Start < kept[j].Start })
	return kept
}

// Counts holds confusion counts and derived scores.
type Counts struct {
	TP        int     `json:"tp"`
	FP        int     `json:"fp"`
	FN        int     `json:"fn"`
	Precision float64 `json:"precision"`
	Recall    float64 `json:"recall"`
	F1        float64 `json:"f1"`
}

func (c *Counts) finish() {
	if c.TP+c.FP > 0 {
		c.Precision = float64(c.TP) / float64(c.TP+c.FP)
	}
	if c.TP+c.FN > 0 {
		c.Recall = float64(c.TP) / float64(c.TP+c.FN)
	}
	if c.Precision+c.Recall > 0 {
		c.F1 = 2 * c.Precision * c.Recall / (c.Precision + c.Recall)
	}
}

// Sample is an example error (synthetic data only).
type Sample struct {
	Record  int    `json:"record"`
	Type    string `json:"type"`
	Value   string `json:"value"`
	Cause   string `json:"cause"`
	Context string `json:"context"`
}

// LimStat counts known-limitation spans and how many a system found.
type LimStat struct {
	Total    int `json:"total"`
	Detected int `json:"detected"`
}

// SystemResult is the evaluation of one system.
type SystemResult struct {
	Name        string                    `json:"name"`
	PerType     map[string]*Counts        `json:"per_type"`
	Micro       Counts                    `json:"micro"`
	FPCauses    map[string]map[string]int `json:"fp_causes"`   // predicted type -> cause -> n
	FNVariants  map[string]map[string]int `json:"fn_variants"` // gold type -> variant -> n
	Limitations map[string]*LimStat       `json:"limitations"`
	FPSamples   []Sample                  `json:"fp_samples"`
	FNSamples   []Sample                  `json:"fn_samples"`
}

const maxSamples = 8

func context(text string, start, end int) string {
	a, b := max(0, start-24), min(len(text), end+24)
	for a > 0 && !utf8.RuneStart(text[a]) {
		a--
	}
	for b < len(text) && !utf8.RuneStart(text[b]) {
		b++
	}
	return strings.ToValidUTF8(text[a:start]+"⟦"+text[start:end]+"⟧"+text[end:b], "?")
}

func overlaps(a0, a1, b0, b1 int) bool { return a0 < b1 && b0 < a1 }

// Evaluate scores sys on recs with span-level exact match (start, end and
// type must all agree).
func Evaluate(sys System, recs []Record) *SystemResult {
	r := &SystemResult{Name: sys.Name, PerType: map[string]*Counts{}, FPCauses: map[string]map[string]int{},
		FNVariants: map[string]map[string]int{}, Limitations: map[string]*LimStat{}}
	for _, t := range Types {
		r.PerType[t] = &Counts{}
		r.FPCauses[t] = map[string]int{}
		r.FNVariants[t] = map[string]int{}
	}
	for _, rec := range recs {
		preds := sys.Detect(rec.Text)
		matched := make([]bool, len(preds))
		for _, g := range rec.Spans {
			hit := -1
			for i, p := range preds {
				if p.Start == g.Start && p.End == g.End && p.Type == g.Type {
					hit = i
				}
			}
			if g.Limitation != "" {
				ls := r.Limitations[g.Limitation]
				if ls == nil {
					ls = &LimStat{}
					r.Limitations[g.Limitation] = ls
				}
				ls.Total++
				if hit >= 0 {
					ls.Detected++
				}
				for i, p := range preds { // predictions on them are not scored
					if overlaps(p.Start, p.End, g.Start, g.End) {
						matched[i] = true
					}
				}
				continue
			}
			if hit >= 0 {
				matched[hit] = true
				r.PerType[g.Type].TP++
				continue
			}
			r.PerType[g.Type].FN++
			v := g.Variant
			if v == "" {
				v = "plain"
			}
			r.FNVariants[g.Type][v]++
			if len(r.FNSamples) < maxSamples {
				r.FNSamples = append(r.FNSamples, Sample{Record: rec.ID, Type: g.Type, Value: rec.Text[g.Start:g.End], Cause: v, Context: context(rec.Text, g.Start, g.End)})
			}
		}
		for i, p := range preds {
			if matched[i] {
				continue
			}
			c, ok := r.PerType[p.Type]
			if !ok {
				continue
			}
			c.FP++
			cause := fpCause(rec, p)
			r.FPCauses[p.Type][cause]++
			if len(r.FPSamples) < maxSamples {
				r.FPSamples = append(r.FPSamples, Sample{Record: rec.ID, Type: p.Type, Value: rec.Text[p.Start:p.End], Cause: cause, Context: context(rec.Text, p.Start, p.End)})
			}
		}
	}
	for _, t := range Types {
		c := r.PerType[t]
		c.finish()
		r.Micro.TP += c.TP
		r.Micro.FP += c.FP
		r.Micro.FN += c.FN
	}
	r.Micro.finish()
	return r
}

func fpCause(rec Record, p Pred) string {
	for _, g := range rec.Spans {
		if overlaps(p.Start, p.End, g.Start, g.End) {
			if g.Type == p.Type {
				return "boundary_mismatch"
			}
			return "inside_" + g.Type
		}
	}
	for _, n := range rec.Negatives {
		if overlaps(p.Start, p.End, n.Start, n.End) {
			return n.Kind
		}
	}
	return "unlabelled_text"
}

// LeakResult reports gold PII values still present verbatim after masking.
type LeakResult struct {
	Positives     int `json:"positives"`      // non-limitation gold spans
	StringLeaks   int `json:"string_leaks"`   // after MaskString on the line
	JSONRecords   int `json:"json_records"`   // app_json records run through MaskEvent
	PipelineLeaks int `json:"pipeline_leaks"` // after MaskEvent + JSON encoding
	MaskErrors    int `json:"mask_errors"`    // values replaced with [REDACTED:error]
}

// Leaks masks every record with e and counts surviving positives.
func Leaks(e *mask.Engine, recs []Record) (LeakResult, error) {
	var lr LeakResult
	for _, rec := range recs {
		out, res := e.MaskString(rec.Text)
		lr.MaskErrors += res.Errors
		var pipeline []byte
		if rec.Template == "app_json" {
			dec := json.NewDecoder(strings.NewReader(rec.Text))
			dec.UseNumber()
			var m map[string]any
			if err := dec.Decode(&m); err != nil {
				return lr, fmt.Errorf("record %d: invalid JSON: %w", rec.ID, err)
			}
			mres := e.MaskEvent(map[string]any{"unmapped": m})
			lr.MaskErrors += mres.Errors
			var err error
			if pipeline, err = json.Marshal(m); err != nil {
				return lr, err
			}
			lr.JSONRecords++
		}
		for _, g := range rec.Spans {
			if g.Limitation != "" {
				continue
			}
			lr.Positives++
			v := rec.Text[g.Start:g.End]
			if strings.Contains(out, v) {
				lr.StringLeaks++
			}
			if pipeline != nil && bytes.Contains(pipeline, []byte(v)) {
				lr.PipelineLeaks++
			}
		}
	}
	return lr, nil
}

// DatasetStats summarises a dataset.
type DatasetStats struct {
	Records     int            `json:"records"`
	Bytes       int            `json:"bytes"`
	Templates   map[string]int `json:"templates"`
	Positives   map[string]int `json:"positives"`
	Limitations map[string]int `json:"limitations"`
	Negatives   map[string]int `json:"negatives"`
}

// Stats summarises recs.
func Stats(recs []Record) DatasetStats {
	s := DatasetStats{Templates: map[string]int{}, Positives: map[string]int{}, Limitations: map[string]int{}, Negatives: map[string]int{}}
	for _, r := range recs {
		s.Records++
		s.Bytes += len(r.Text)
		s.Templates[r.Template]++
		for _, g := range r.Spans {
			if g.Limitation != "" {
				s.Limitations[g.Limitation]++
			} else {
				s.Positives[g.Type]++
			}
		}
		for _, n := range r.Negatives {
			s.Negatives[n.Kind]++
		}
	}
	return s
}

func sortedKeys(m map[string]int) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	slices.SortFunc(keys, func(a, b string) int {
		if m[a] != m[b] {
			return m[b] - m[a]
		}
		return strings.Compare(a, b)
	})
	return keys
}

func mdEscape(s string) string {
	s = strings.ReplaceAll(s, "|", `\|`)
	s = strings.ReplaceAll(s, "`", "'")
	return s
}

// WriteMarkdown renders the report.
func WriteMarkdown(w io.Writer, header string, ds DatasetStats, results []*SystemResult, leaks LeakResult) {
	fmt.Fprintf(w, "%s\n", header)
	fmt.Fprintf(w, "## Dataset\n\n%d records, %d bytes. Templates: ", ds.Records, ds.Bytes)
	for _, k := range sortedKeys(ds.Templates) {
		fmt.Fprintf(w, "%s %d; ", k, ds.Templates[k])
	}
	fmt.Fprintf(w, "\n\nScored positives: ")
	for _, t := range Types {
		fmt.Fprintf(w, "%s %d; ", t, ds.Positives[t])
	}
	fmt.Fprintf(w, "\n\nHard negatives: ")
	for _, k := range sortedKeys(ds.Negatives) {
		fmt.Fprintf(w, "%s %d; ", k, ds.Negatives[k])
	}
	fmt.Fprintf(w, "\n\nKnown-limitation spans (excluded from P/R/F1): ")
	for _, k := range sortedKeys(ds.Limitations) {
		fmt.Fprintf(w, "%s %d; ", k, ds.Limitations[k])
	}
	fmt.Fprintf(w, "\n\n## Span-level exact match\n\n| system | type | P | R | F1 | TP | FP | FN |\n|---|---|---|---|---|---|---|---|\n")
	for _, r := range results {
		for _, t := range Types {
			c := r.PerType[t]
			fmt.Fprintf(w, "| %s | %s | %.4f | %.4f | %.4f | %d | %d | %d |\n", r.Name, t, c.Precision, c.Recall, c.F1, c.TP, c.FP, c.FN)
		}
		c := r.Micro
		fmt.Fprintf(w, "| %s | **micro** | %.4f | %.4f | %.4f | %d | %d | %d |\n", r.Name, c.Precision, c.Recall, c.F1, c.TP, c.FP, c.FN)
	}
	fmt.Fprintf(w, "\n## False positives by cause\n\n`inside_<type>` = the prediction overlaps a gold span of another type; other causes are the hard-negative kind it overlaps; `unlabelled_text` = background text (IPs, ports, header fields).\n\n| system | predicted type | cause | n |\n|---|---|---|---|\n")
	for _, r := range results {
		for _, t := range Types {
			for _, k := range sortedKeys(r.FPCauses[t]) {
				fmt.Fprintf(w, "| %s | %s | %s | %d |\n", r.Name, t, k, r.FPCauses[t][k])
			}
		}
	}
	fmt.Fprintf(w, "\n## False negatives by surface variant\n\n| system | type | variant | n |\n|---|---|---|---|\n")
	for _, r := range results {
		for _, t := range Types {
			for _, k := range sortedKeys(r.FNVariants[t]) {
				fmt.Fprintf(w, "| %s | %s | %s | %d |\n", r.Name, t, k, r.FNVariants[t][k])
			}
		}
	}
	fmt.Fprintf(w, "\n## Known limitations (not scored)\n\n| system | limitation | found / total |\n|---|---|---|\n")
	for _, r := range results {
		keys := make([]string, 0, len(r.Limitations))
		for k := range r.Limitations {
			keys = append(keys, k)
		}
		slices.Sort(keys)
		for _, k := range keys {
			fmt.Fprintf(w, "| %s | %s | %d / %d |\n", r.Name, k, r.Limitations[k].Detected, r.Limitations[k].Total)
		}
	}
	fmt.Fprintf(w, "\n## Residual PII after LogGate masking\n\n- scored positives: %d\n- still present verbatim after `MaskString` on the whole line: %d\n- app_json records also run through `MaskEvent` (decoded JSON tree): %d; positives still present after re-encoding: %d\n- values replaced with `[REDACTED:error]`: %d\n",
		leaks.Positives, leaks.StringLeaks, leaks.JSONRecords, leaks.PipelineLeaks, leaks.MaskErrors)
	fmt.Fprintf(w, "\n## Sample errors (synthetic values)\n")
	for _, r := range results {
		fmt.Fprintf(w, "\n### %s\n\n| kind | type | cause/variant | context |\n|---|---|---|---|\n", r.Name)
		for _, s := range r.FPSamples {
			fmt.Fprintf(w, "| FP | %s | %s | `%s` |\n", s.Type, s.Cause, mdEscape(s.Context))
		}
		for _, s := range r.FNSamples {
			fmt.Fprintf(w, "| FN | %s | %s | `%s` |\n", s.Type, s.Cause, mdEscape(s.Context))
		}
	}
}
