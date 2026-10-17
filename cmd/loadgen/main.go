// Command loadgen is the benchmark's open-loop load generator. See
// internal/bench/loadgen.
//
//	LOGGATE_BENCH_TOKEN=... loadgen -url http://127.0.0.1:8080/v1/events \
//	    -token-env LOGGATE_BENCH_TOKEN -rate 200 -duration 60s -out ./load
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"regexp"
	"time"

	"github.com/lzwhehe/loggate/internal/bench/loadgen"
)

var runIDRe = regexp.MustCompile(`^[A-Za-z0-9_.-]{1,64}$`)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "loadgen:", err)
		os.Exit(1)
	}
}

func run() error {
	url := flag.String("url", "http://127.0.0.1:8080/v1/events", "ingest URL")
	tokenEnv := flag.String("token-env", "LOGGATE_BENCH_TOKEN", "env var holding the bearer token (empty = none)")
	rate := flag.Float64("rate", 200, "events per second")
	dur := flag.Duration("duration", time.Minute, "load duration")
	startAt := flag.Int64("start-at-ns", 0, "unix ns of event 0's scheduled time (0 = now)")
	runID := flag.String("run-id", "manual", "run id stamped into every event")
	seed := flag.Uint64("seed", 1, "seed for the class mix")
	classes := flag.String("classes", "auth=0.5,net=0.3,app=0.2", "class mix name=weight,...")
	payload := flag.Int("payload", 512, "approximate event size in bytes")
	inflight := flag.Int("max-inflight", 4096, "max concurrent requests")
	timeout := flag.Duration("timeout", 10*time.Second, "per-request timeout")
	retries := flag.Int("retries", 0, "client retries on non-200 (default none)")
	out := flag.String("out", "./load", "output directory")
	flag.Parse()

	if !runIDRe.MatchString(*runID) {
		return errors.New("run-id must match [A-Za-z0-9_.-]{1,64}")
	}
	cls, err := loadgen.ParseClasses(*classes)
	if err != nil {
		return err
	}
	for _, c := range cls {
		if !runIDRe.MatchString(c.Name) {
			return fmt.Errorf("class name %q must match [A-Za-z0-9_.-]", c.Name)
		}
	}
	cfg := loadgen.Config{
		URL: *url, RunID: *runID, Rate: *rate, Duration: *dur, Seed: *seed, Classes: cls,
		PayloadSize: *payload, MaxInFlight: *inflight, Timeout: *timeout, Retries: *retries, OutDir: *out,
	}
	if *tokenEnv != "" {
		cfg.Token = os.Getenv(*tokenEnv)
	}
	if *startAt > 0 {
		cfg.Start = time.Unix(0, *startAt)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	sum, _, err := loadgen.Run(ctx, cfg)
	if err != nil {
		return err
	}
	fmt.Printf("planned %d acked %d by_status %v behind %d (max lag %.1f ms) inflight-cap hits %d achieved %.1f eps ack p50 %.2f p99 %.2f p999 %.2f ms\n",
		sum.Planned, sum.Acked, sum.ByStatus, sum.BehindCount, sum.MaxLagMS, sum.InFlightCapHit, sum.AchievedRate,
		sum.AckLatencyMS.P50, sum.AckLatencyMS.P99, sum.AckLatencyMS.P999)
	return nil
}
