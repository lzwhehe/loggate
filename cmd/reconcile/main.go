// Command reconcile is the benchmark's independent oracle. It reads one
// faultbench repetition directory (load/sent.csv, mock/<dest>.csv,
// out/<dest>.jsonl|.db, data/dlq) and the resolved scenario, and prints
// per-destination expected / lost / dup / dlq / extra / recovery / latency
// as JSON and a markdown table. See internal/bench/reconcile.
//
//	reconcile -dir bench/results/<ts>/raw/S1/rep1
package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/lzwhehe/loggate/internal/bench/faultbench"
	"github.com/lzwhehe/loggate/internal/bench/reconcile"
	"github.com/lzwhehe/loggate/internal/bench/scenario"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "reconcile:", err)
		os.Exit(1)
	}
}

func run() error {
	dir := flag.String("dir", "", "repetition directory written by faultbench")
	scen := flag.String("scenario", "", "resolved scenario (default <dir>/scenario.resolved.yaml)")
	runID := flag.String("run-id", "", "only count events of this run id (default: from load/summary.json)")
	jsonOut := flag.String("json", "", "write JSON result here (default <dir>/reconcile.json)")
	mdOut := flag.String("md", "", "write markdown here (default <dir>/reconcile.md)")
	flag.Parse()
	if *dir == "" {
		return errors.New("-dir is required")
	}
	if *scen == "" {
		*scen = filepath.Join(*dir, "scenario.resolved.yaml")
	}
	b, err := os.ReadFile(*scen)
	if err != nil {
		return err
	}
	sc, err := scenario.Parse(b)
	if err != nil {
		return err
	}
	var summary struct {
		RunID   string `json:"run_id"`
		StartNS int64  `json:"start_ns"`
	}
	if sb, err := os.ReadFile(filepath.Join(*dir, "load", "summary.json")); err == nil {
		_ = json.Unmarshal(sb, &summary)
	}
	if *runID == "" {
		*runID = summary.RunID
	}
	start := summary.StartNS
	if tb, err := os.ReadFile(filepath.Join(*dir, "t0_ns.txt")); err == nil {
		if v, err := strconv.ParseInt(strings.TrimSpace(string(tb)), 10, 64); err == nil {
			start = v
		}
	}
	in, err := faultbench.Inputs(sc, *dir, *runID, start)
	if err != nil {
		return err
	}
	res, err := reconcile.Run(*in)
	if err != nil {
		return err
	}
	if *jsonOut == "" {
		*jsonOut = filepath.Join(*dir, "reconcile.json")
	}
	if *mdOut == "" {
		*mdOut = filepath.Join(*dir, "reconcile.md")
	}
	jb, err := json.MarshalIndent(res, "", "  ")
	if err != nil {
		return err
	}
	md := reconcile.Markdown(res)
	if err := errors.Join(os.WriteFile(*jsonOut, jb, 0o644), os.WriteFile(*mdOut, []byte(md), 0o644)); err != nil {
		return err
	}
	fmt.Print(md)
	if !res.Pass {
		os.Exit(2)
	}
	return nil
}
