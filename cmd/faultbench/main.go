// Command faultbench runs LogGate fault-injection scenarios end to end and
// writes bench/results/<timestamp>-<os>-<profile>/{<scenario>.json,
// fault.md, summary.csv, raw/}. See internal/bench/faultbench.
//
//	faultbench -bin bin -profile quick bench/scenarios/S1_sink_down.yaml ...
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"time"

	"github.com/lzwhehe/loggate/internal/bench/faultbench"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "faultbench:", err)
		os.Exit(1)
	}
}

func run() error {
	bin := flag.String("bin", "bin", "directory with loggate, mocksink, loadgen binaries")
	profile := flag.String("profile", "quick", "profile: quick (smoke) | full (DESIGN §10.3)")
	reps := flag.Int("reps", 0, "repetitions per scenario (0 = profile default: quick 1, full 5)")
	out := flag.String("out", "", "results directory (default bench/results/<timestamp>-<os>-<profile>)")
	seed := flag.Uint64("seed", 1, "load-mix seed")
	payload := flag.Int("payload", 512, "approximate event size in bytes")
	flag.Parse()
	if flag.NArg() == 0 {
		return errors.New("usage: faultbench [flags] scenario.yaml ...")
	}
	if *out == "" {
		*out = filepath.Join("bench", "results", fmt.Sprintf("%s-%s-%s", time.Now().Format("20060102-150405"), runtime.GOOS, *profile))
	}
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	res, err := faultbench.Run(ctx, faultbench.Options{
		Scenarios: flag.Args(), Profile: *profile, Reps: *reps, BinDir: *bin, ResultsDir: *out,
		Seed: *seed, Payload: *payload, Logger: log,
	})
	if err != nil {
		return err
	}
	failed := 0
	for _, r := range res {
		switch {
		case r.Skipped != "":
			fmt.Printf("%-4s SKIPPED (%s)\n", r.Scenario, r.Skipped)
		case r.PassAll:
			fmt.Printf("%-4s PASS\n", r.Scenario)
		default:
			fmt.Printf("%-4s FAIL\n", r.Scenario)
			failed++
		}
	}
	fmt.Printf("results: %s\n", filepath.Join(*out, "fault.md"))
	if failed > 0 {
		os.Exit(2)
	}
	return nil
}
