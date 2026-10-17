// Command piieval scores the LogGate PII detector and two regex-only
// baselines on a piigen dataset with span-level exact match, and checks
// for residual PII after masking. It writes piieval.md and piieval.json.
//
//	piieval -in pii.jsonl -out bench/results/<ts>-pii
package main

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"time"

	"github.com/lzwhehe/loggate/internal/mask"
	"github.com/lzwhehe/loggate/internal/piibench"
)

func main() {
	in := flag.String("in", "pii.jsonl", "dataset written by piigen")
	out := flag.String("out", "", "output directory (default bench/results/<timestamp>-pii)")
	today := flag.String("today", "2026-10-17", "upper bound for ID birth dates (fixed for reproducibility)")
	flag.Parse()
	if *out == "" {
		*out = filepath.Join("bench", "results", time.Now().Format("20060102-150405")+"-pii")
	}
	if err := run(*in, *out, *today); err != nil {
		fmt.Fprintln(os.Stderr, "piieval:", err)
		os.Exit(1)
	}
}

type report struct {
	Dataset       string                   `json:"dataset"`
	DatasetSHA256 string                   `json:"dataset_sha256"`
	Today         string                   `json:"today"`
	GoVersion     string                   `json:"go_version"`
	Stats         piibench.DatasetStats    `json:"stats"`
	Systems       []*piibench.SystemResult `json:"systems"`
	Leaks         piibench.LeakResult      `json:"leaks"`
	DetectSeconds map[string]float64       `json:"detect_seconds"`
	Notes         []string                 `json:"notes"`
}

func run(in, out, todayS string) error {
	raw, err := os.ReadFile(in)
	if err != nil {
		return err
	}
	today, err := time.Parse("2006-01-02", todayS)
	if err != nil {
		return fmt.Errorf("-today: %w", err)
	}
	var recs []piibench.Record
	sc := bufio.NewScanner(bytes.NewReader(raw))
	sc.Buffer(make([]byte, 1<<20), 1<<24)
	for sc.Scan() {
		var r piibench.Record
		if err := json.Unmarshal(sc.Bytes(), &r); err != nil {
			return fmt.Errorf("record %d: %w", len(recs)+1, err)
		}
		recs = append(recs, r)
	}
	if err := sc.Err(); err != nil {
		return err
	}
	eng, err := mask.New(mask.Config{Detector: mask.DetectorConfig{Today: today}})
	if err != nil {
		return err
	}
	sum := sha256.Sum256(raw)
	rep := report{
		Dataset: filepath.Base(in), DatasetSHA256: hex.EncodeToString(sum[:]), Today: todayS,
		GoVersion: runtime.Version(), Stats: piibench.Stats(recs), DetectSeconds: map[string]float64{},
		Notes: []string{
			"Synthetic data only; the synthetic distribution is not a real-world log distribution.",
			"Exact match: start, end and type must all agree. Phone spans are the 11 national digits (a +86/86 prefix is context).",
			"Known-limitation spans (full-width digits, URL-encoded '@', digits glued to ASCII letters) are excluded from P/R/F1 and listed separately.",
			"Labels are technical detections, not legal compliance determinations.",
		},
	}
	for _, sys := range []piibench.System{piibench.EngineSystem(eng), piibench.RegexSystem(false), piibench.RegexSystem(true)} {
		t0 := time.Now()
		res := piibench.Evaluate(sys, recs)
		rep.DetectSeconds[sys.Name] = time.Since(t0).Seconds()
		rep.Systems = append(rep.Systems, res)
	}
	if rep.Leaks, err = piibench.Leaks(eng, recs); err != nil {
		return err
	}
	if err := os.MkdirAll(out, 0o755); err != nil {
		return err
	}
	jb, err := json.MarshalIndent(rep, "", "  ")
	if err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(out, "piieval.json"), jb, 0o644); err != nil {
		return err
	}
	var md bytes.Buffer
	header := fmt.Sprintf("# PII detection: LogGate vs regex-only baselines\n\nDataset `%s` (sha256 %s…), birth-date bound %s, %s.\n\n"+
		"- **loggate**: hand-written digit scanner with strict boundaries + ID MOD 11-2/date/province, card BIN+Luhn, phone prefix table, e-mail RE2 when '@' is present.\n"+
		"- **regex_naive**: `\\d{17}[\\dXx]`, `\\d{16,19}`, `1[3-9]\\d{9}`, e-mail regex; no boundaries, no validation.\n"+
		"- **regex_bounded**: the same patterns wrapped in `\\b`.\n\nBoth baselines resolve overlaps with the engine's priority (id_card > bank_card > phone > email, longer first).\n",
		rep.Dataset, rep.DatasetSHA256[:12], todayS, rep.GoVersion)
	piibench.WriteMarkdown(&md, header, rep.Stats, rep.Systems, rep.Leaks)
	fmt.Fprintf(&md, "\n## Notes\n\n")
	for _, n := range rep.Notes {
		fmt.Fprintf(&md, "- %s\n", n)
	}
	if err := os.WriteFile(filepath.Join(out, "piieval.md"), md.Bytes(), 0o644); err != nil {
		return err
	}
	fmt.Printf("piieval: %d records -> %s\n", len(recs), out)
	for _, s := range rep.Systems {
		fmt.Printf("  %-14s micro P=%.4f R=%.4f F1=%.4f\n", s.Name, s.Micro.Precision, s.Micro.Recall, s.Micro.F1)
	}
	return nil
}
