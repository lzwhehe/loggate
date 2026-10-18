// Package faultbench runs fault-injection scenarios end to end (DESIGN
// §10): it starts the mocksinks and the gateway as separate OS processes,
// drives an open-loop loadgen process, executes the scenario timeline
// (sink faults, hard kills, graceful stops, restarts, admin
// unregister/register), waits for the gateway to drain, stops everything,
// and reconciles what every destination received against what the client
// was promised.
package faultbench

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/lzwhehe/loggate/internal/bench/loadgen"
	"github.com/lzwhehe/loggate/internal/bench/mocksink"
	"github.com/lzwhehe/loggate/internal/bench/reconcile"
	"github.com/lzwhehe/loggate/internal/bench/scenario"
)

// Options configures a benchmark run.
type Options struct {
	Scenarios  []string // scenario files
	Profile    string   // quick | full | custom name
	Reps       int      // 0 = profile default
	BinDir     string   // holds loggate, mocksink, loadgen (+.exe on Windows)
	ResultsDir string   // e.g. bench/results/20261017-1200-windows-quick
	Seed       uint64
	Payload    int           // approximate event size in bytes (default 512)
	Observe    time.Duration // observer poll interval (default 50ms)
	Logger     *slog.Logger
}

// TimelineEvent records when an action actually ran.
type TimelineEvent struct {
	Action    string  `json:"action"`
	Dest      string  `json:"dest,omitempty"`
	Mode      string  `json:"mode,omitempty"`
	PlannedS  float64 `json:"planned_s"`
	StartedS  float64 `json:"started_s"`
	FinishedS float64 `json:"finished_s"`
	Error     string  `json:"error,omitempty"`
}

// RepResult is one repetition.
type RepResult struct {
	Rep           int               `json:"rep"`
	RunID         string            `json:"run_id"`
	Dir           string            `json:"dir"`
	Recon         *reconcile.Result `json:"reconcile"`
	Load          *loadgen.Summary  `json:"loadgen"`
	Timeline      []TimelineEvent   `json:"timeline"`
	GatewayStarts int               `json:"gateway_starts"`
	Drained       bool              `json:"drained"`
	DrainS        float64           `json:"drain_s"` // from load end until drained
	Errors        []string          `json:"errors,omitempty"`
}

// ScenarioResult aggregates repetitions of one scenario.
type ScenarioResult struct {
	Scenario string                 `json:"scenario"`
	Title    string                 `json:"title"`
	Profile  string                 `json:"profile"`
	Skipped  string                 `json:"skipped,omitempty"`
	Resolved *scenario.Scenario     `json:"resolved,omitempty"`
	Reps     []*RepResult           `json:"reps,omitempty"`
	Summary  map[string]DestSummary `json:"summary,omitempty"`
	PassAll  bool                   `json:"pass_all"`
	Env      *Environment           `json:"environment"`
}

func exe(dir, name string) string {
	if runtime.GOOS == "windows" {
		name += ".exe"
	}
	return filepath.Join(dir, name)
}

func randHex(n int) string {
	b := make([]byte, n)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

// Run executes every scenario and writes results under opts.ResultsDir.
func Run(ctx context.Context, opts Options) ([]*ScenarioResult, error) {
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	if opts.Seed == 0 {
		opts.Seed = 1
	}
	if opts.Payload <= 0 {
		opts.Payload = 512
	}
	if opts.Observe <= 0 {
		opts.Observe = 50 * time.Millisecond
	}
	for _, b := range []string{"loggate", "mocksink", "loadgen"} {
		if _, err := os.Stat(exe(opts.BinDir, b)); err != nil {
			return nil, fmt.Errorf("faultbench: binary %s missing in %s (build first): %w", b, opts.BinDir, err)
		}
	}
	if err := os.MkdirAll(opts.ResultsDir, 0o755); err != nil {
		return nil, err
	}
	env := CollectEnvironment(opts.Profile)
	var all []*ScenarioResult
	for _, path := range opts.Scenarios {
		sc, err := scenario.Load(path)
		if err != nil {
			return all, err
		}
		sr := &ScenarioResult{Scenario: sc.Name, Title: sc.Title, Profile: opts.Profile, Env: env}
		all = append(all, sr)
		if !sc.Supported() {
			sr.Skipped = fmt.Sprintf("not supported on %s (platforms: %v)", runtime.GOOS, sc.Platforms)
			opts.Logger.Warn("scenario skipped", "scenario", sc.Name, "reason", sr.Skipped)
			if err := writeScenario(opts.ResultsDir, sr); err != nil {
				return all, err
			}
			continue
		}
		res, prof, err := sc.Resolve(opts.Profile)
		if err != nil {
			return all, err
		}
		sr.Resolved = res
		reps := opts.Reps
		if reps <= 0 {
			reps = prof.Reps
		}
		for rep := 1; rep <= reps; rep++ {
			opts.Logger.Info("scenario repetition", "scenario", sc.Name, "profile", opts.Profile, "rep", rep, "of", reps,
				"rate", res.Rate, "duration", res.Duration)
			rr, err := runRep(ctx, opts, res, prof, rep)
			if err != nil {
				return all, fmt.Errorf("%s rep %d: %w", sc.Name, rep, err)
			}
			sr.Reps = append(sr.Reps, rr)
			if rr.Recon != nil {
				opts.Logger.Info("repetition done", "scenario", sc.Name, "rep", rep, "pass", rr.Recon.Pass, "drained", rr.Drained)
			}
		}
		sr.Summary, sr.PassAll = Aggregate(sr.Reps)
		if err := writeScenario(opts.ResultsDir, sr); err != nil {
			return all, err
		}
	}
	if err := WriteReport(opts.ResultsDir, all); err != nil {
		return all, err
	}
	return all, nil
}

func writeScenario(dir string, sr *ScenarioResult) error {
	b, err := json.MarshalIndent(sr, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(dir, sr.Scenario+".json"), b, 0o644)
}

// BuildGatewayConfig generates the loggate config for a scenario.
func BuildGatewayConfig(sc *scenario.Scenario, dataDir, outDir, listen string, mockAddr map[string]string) (map[string]any, error) {
	var dests []any
	for _, d := range sc.Destinations {
		m := map[string]any{"name": d.Name, "type": d.Type}
		conf := scenario.DeepMerge(map[string]any{}, d.Config)
		switch d.Type {
		case "file":
			conf["path"] = filepath.ToSlash(filepath.Join(outDir, d.Name+".jsonl"))
		case "sqlite":
			conf["path"] = filepath.ToSlash(filepath.Join(outDir, d.Name+".db"))
		case "webhook":
			a, ok := mockAddr[d.Name]
			if !ok {
				return nil, fmt.Errorf("no mocksink address for %s", d.Name)
			}
			conf["url"] = "http://" + a + "/ingest"
		}
		m["config"] = conf
		if d.Queue != nil {
			m["queue"] = d.Queue
		}
		if d.Breaker != nil {
			m["breaker"] = d.Breaker
		}
		if o := sc.DestOverrides[d.Name]; o != nil {
			m = scenario.DeepMerge(m, o)
		}
		dests = append(dests, m)
	}
	var routes []any
	for _, r := range sc.Routes {
		match := map[string]any{}
		for k, v := range r.Match {
			match[k] = v
		}
		to := make([]any, len(r.To))
		for i, n := range r.To {
			to[i] = n
		}
		routes = append(routes, map[string]any{"match": match, "to": to})
	}
	cfg := map[string]any{
		"node_id":  "bench-gw",
		"data_dir": filepath.ToSlash(dataDir),
		"ingest": map[string]any{"http": map[string]any{
			"listen": listen, "max_body": "1MiB",
			"tokens": []any{map[string]any{"source": "loadgen", "token_env": "LOGGATE_BENCH_TOKEN"}},
		}},
		"idempotency": map[string]any{"key_env": "LOGGATE_IDEM_KEY"},
		// Metrics on an ephemeral loopback port; no dashboard (bench runs
		// must not collide with a developer's :9091/:8081).
		"admin": map[string]any{"token_env": "LOGGATE_ADMIN_TOKEN", "metrics": "127.0.0.1:0", "ui": "off"},
	}
	cfg = scenario.DeepMerge(cfg, sc.Gateway)
	cfg["destinations"] = dests
	cfg["routes"] = routes
	return cfg, nil
}

func runRep(ctx context.Context, opts Options, sc *scenario.Scenario, prof scenario.Profile, rep int) (*RepResult, error) {
	log := opts.Logger.With("scenario", sc.Name, "rep", rep)
	dir, err := filepath.Abs(filepath.Join(opts.ResultsDir, "raw", sc.Name, fmt.Sprintf("rep%d", rep)))
	if err != nil {
		return nil, err
	}
	_ = os.RemoveAll(dir)
	for _, sub := range []string{"data", "out", "mock", "load"} {
		if err := os.MkdirAll(filepath.Join(dir, sub), 0o755); err != nil {
			return nil, err
		}
	}
	// LOGGATE_BENCH_DATA_ROOT puts data_dir elsewhere, e.g. on a
	// size-limited tmpfs for a real ENOSPC run of S5.
	dataDir := filepath.Join(dir, "data")
	if root := os.Getenv("LOGGATE_BENCH_DATA_ROOT"); root != "" {
		dataDir = filepath.Join(root, sc.Name, fmt.Sprintf("rep%d", rep))
		_ = os.RemoveAll(dataDir)
		if err := os.MkdirAll(dataDir, 0o755); err != nil {
			return nil, err
		}
	}
	if err := os.WriteFile(filepath.Join(dir, "data_dir.txt"), []byte(dataDir+"\n"), 0o644); err != nil {
		return nil, err
	}
	runID := fmt.Sprintf("%s-r%d-%s", sc.Name, rep, randHex(4))
	rr := &RepResult{Rep: rep, RunID: runID, Dir: dir}
	fail := func(f string, a ...any) { rr.Errors = append(rr.Errors, fmt.Sprintf(f, a...)) }

	var mocks []string
	for _, d := range sc.Destinations {
		if d.Mock {
			mocks = append(mocks, d.Name)
		}
	}
	ports, err := freePorts(2 + len(mocks))
	if err != nil {
		return nil, err
	}
	control := "127.0.0.1:" + strconv.Itoa(ports[0])
	gwAddr := "127.0.0.1:" + strconv.Itoa(ports[1])
	mockAddr := map[string]string{}
	margs := []string{"-control", control, "-journal", filepath.Join(dir, "mock")}
	for i, n := range mocks {
		mockAddr[n] = "127.0.0.1:" + strconv.Itoa(ports[2+i])
		margs = append(margs, "-sink", n+"="+mockAddr[n])
	}

	// 1. Mock sinks.
	mp, err := startProc("mocksink", exe(opts.BinDir, "mocksink"), margs, nil, filepath.Join(dir, "mocksink.log"))
	if err != nil {
		return nil, err
	}
	stopMock := func() {
		if mp.exited() {
			return
		}
		httpStatus(context.Background(), http.MethodPost, "http://"+control+"/shutdown", "", nil)
		if !mp.wait(30 * time.Second) {
			fail("mocksink did not shut down; killed")
			_ = mp.kill()
		}
	}
	defer stopMock()
	if err := waitHTTP(ctx, "http://"+control+"/stats", 20*time.Second); err != nil {
		return nil, fmt.Errorf("mocksink: %w", err)
	}

	// 2. Gateway.
	cfg, err := BuildGatewayConfig(sc, dataDir, filepath.Join(dir, "out"), gwAddr, mockAddr)
	if err != nil {
		return nil, err
	}
	cfgYAML, err := yaml.Marshal(cfg)
	if err != nil {
		return nil, err
	}
	cfgPath := filepath.Join(dir, "gateway.yaml")
	if err := os.WriteFile(cfgPath, cfgYAML, 0o644); err != nil {
		return nil, err
	}
	benchToken, adminToken := randHex(16), randHex(16)
	gw := &gateway{
		bin: exe(opts.BinDir, "loggate"), cfg: cfgPath, addr: gwAddr, adminToken: adminToken,
		logPath: filepath.Join(dir, "gateway.log"),
		env: []string{"LOGGATE_BENCH_TOKEN=" + benchToken, "LOGGATE_ADMIN_TOKEN=" + adminToken,
			"LOGGATE_IDEM_KEY=" + randHex(16)},
	}
	if err := gw.start(ctx); err != nil {
		return nil, err
	}
	defer func() { _ = gw.kill() }()

	// 3. Observer for sinks without their own receive clock.
	obs := newObserver(opts.Observe, log)
	for _, d := range sc.Destinations {
		switch d.Type {
		case "file":
			err = obs.add(d.Name, d.Type, filepath.Join(dir, "out", d.Name+".jsonl"), filepath.Join(dir, "out", d.Name+".observed.csv"))
		case "sqlite":
			err = obs.add(d.Name, d.Type, filepath.Join(dir, "out", d.Name+".db"), filepath.Join(dir, "out", d.Name+".observed.csv"))
		}
		if err != nil {
			return nil, err
		}
	}
	obs.start()
	obsClosed := false
	defer func() {
		if !obsClosed {
			obs.close()
		}
	}()

	// 4. Load + timeline, anchored at t0.
	t0 := time.Now().Add(1500 * time.Millisecond)
	var classes []string
	for _, c := range sc.Classes {
		classes = append(classes, fmt.Sprintf("%s=%g", c.Name, c.Weight))
	}
	largs := []string{
		"-url", "http://" + gwAddr + "/v1/events", "-token-env", "LOGGATE_BENCH_TOKEN",
		"-rate", fmt.Sprint(sc.Rate), "-duration", sc.Duration.String(),
		"-start-at-ns", strconv.FormatInt(t0.UnixNano(), 10), "-run-id", runID,
		"-seed", strconv.FormatUint(opts.Seed, 10), "-classes", strings.Join(classes, ","),
		"-payload", strconv.Itoa(opts.Payload), "-out", filepath.Join(dir, "load"),
	}
	lp, err := startProc("loadgen", exe(opts.BinDir, "loadgen"), largs, []string{"LOGGATE_BENCH_TOKEN=" + benchToken}, filepath.Join(dir, "loadgen.log"))
	if err != nil {
		return nil, err
	}
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		rr.Timeline = runTimeline(ctx, log, sc, t0, gw, control)
	}()
	loadDeadline := time.Until(t0) + sc.Duration + 2*time.Minute
	if !lp.wait(loadDeadline) {
		fail("loadgen did not finish in time; killed")
		_ = lp.kill()
	} else if lp.err != nil {
		fail("loadgen: %v", lp.err)
	}
	loadEnd := time.Now()
	wg.Wait()

	// 5. Make sure the gateway runs (a timeline may end with it down), then
	//    wait for every acked event to be delivered or dead-lettered.
	if !gw.running() {
		log.Info("gateway not running after timeline; starting it for drain")
		if err := gw.start(ctx); err != nil {
			fail("final start: %v", err)
		}
	}
	rr.Drained = waitDrain(ctx, gw, prof.DrainTimeout)
	rr.DrainS = time.Since(loadEnd).Seconds()
	if !rr.Drained {
		fail("gateway did not drain within %v", prof.DrainTimeout)
	}
	if _, raw, err := gw.stats(ctx); err == nil {
		_ = os.WriteFile(filepath.Join(dir, "gateway-stats.json"), raw, 0o644)
	}
	if err := gw.stop(ctx, 90*time.Second); err != nil {
		fail("final graceful stop: %v", err)
	}
	rr.GatewayStarts = gw.starts
	obs.close()
	obsClosed = true
	stopMock()

	// 6. Reconcile.
	if b, err := os.ReadFile(filepath.Join(dir, "load", "summary.json")); err == nil {
		var s loadgen.Summary
		if json.Unmarshal(b, &s) == nil {
			rr.Load = &s
		}
	}
	in, err := Inputs(sc, dir, runID, t0.UnixNano())
	if err != nil {
		return rr, err
	}
	rr.Recon, err = reconcile.Run(*in)
	if err != nil {
		return rr, err
	}
	if b, err := json.MarshalIndent(rr.Recon, "", "  "); err == nil {
		_ = os.WriteFile(filepath.Join(dir, "reconcile.json"), b, 0o644)
	}
	_ = os.WriteFile(filepath.Join(dir, "reconcile.md"), []byte(reconcile.Markdown(rr.Recon)), 0o644)
	rb, _ := yaml.Marshal(sc)
	_ = os.WriteFile(filepath.Join(dir, "scenario.resolved.yaml"), rb, 0o644)
	_ = os.WriteFile(filepath.Join(dir, "t0_ns.txt"), []byte(strconv.FormatInt(t0.UnixNano(), 10)+"\n"), 0o644)
	return rr, nil
}

// Inputs reads a repetition directory into reconcile inputs (also used by
// cmd/reconcile).
func Inputs(sc *scenario.Scenario, dir, runID string, startNS int64) (*reconcile.Input, error) {
	sent, err := loadgen.ReadSent(filepath.Join(dir, "load", "sent.csv"))
	if err != nil {
		return nil, fmt.Errorf("read sent.csv: %w", err)
	}
	in := &reconcile.Input{Scenario: sc, RunID: runID, StartNS: startNS, Sent: sent}
	for _, d := range sc.Destinations {
		src := reconcile.Source{Dest: d.Name, Type: d.Type}
		switch d.Type {
		case "webhook":
			src.Receipts, src.Torn, err = reconcile.ReadJournal(filepath.Join(dir, "mock", d.Name+".csv"))
		case "file":
			var fs reconcile.FileStats
			src.Receipts, fs, err = reconcile.ReadFileSinkPath(filepath.Join(dir, "out", d.Name+".jsonl"))
			src.Torn = fs.TornFragments
		case "sqlite":
			src.Receipts, err = reconcile.SQLiteRows(context.Background(), filepath.Join(dir, "out", d.Name+".db"))
		}
		if err != nil {
			return nil, fmt.Errorf("read %s: %w", d.Name, err)
		}
		if d.Type != "webhook" {
			if src.Observed, err = reconcile.ReadObserved(filepath.Join(dir, "out", d.Name+".observed.csv")); err != nil {
				return nil, err
			}
		}
		in.Sources = append(in.Sources, src)
	}
	dataDir := filepath.Join(dir, "data")
	if b, err := os.ReadFile(filepath.Join(dir, "data_dir.txt")); err == nil {
		dataDir = strings.TrimSpace(string(b))
	}
	in.DLQ, err = reconcile.ReadDLQ(filepath.Join(dataDir, "dlq"))
	if err != nil {
		return nil, fmt.Errorf("read dlq: %w", err)
	}
	return in, nil
}

func runTimeline(ctx context.Context, log *slog.Logger, sc *scenario.Scenario, t0 time.Time, gw *gateway, control string) []TimelineEvent {
	var out []TimelineEvent
	for _, a := range sc.Timeline {
		at := t0.Add(a.At)
		select {
		case <-ctx.Done():
			return out
		case <-time.After(time.Until(at)):
		}
		ev := TimelineEvent{Action: a.Action, Dest: a.Dest, Mode: a.Mode, PlannedS: a.At.Seconds(), StartedS: time.Since(t0).Seconds()}
		var err error
		switch a.Action {
		case scenario.ActFault:
			f := mocksink.Fault{Dest: a.Dest, Mode: a.Mode, DelayMS: a.DelayMS, ErrorCode: a.ErrorCode, ErrorRate: a.ErrorRate, Seed: a.Seed}
			b, _ := json.Marshal(f)
			if c := httpStatus(ctx, http.MethodPost, "http://"+control+"/fault", "", b); c != http.StatusOK {
				err = fmt.Errorf("mocksink /fault: status %d", c)
			}
		case scenario.ActKill:
			err = gw.kill()
		case scenario.ActStop:
			err = gw.stop(ctx, 90*time.Second)
		case scenario.ActStart:
			err = gw.start(ctx)
		case scenario.ActUnregister:
			err = gw.admin(ctx, "/v1/admin/unregister", map[string]string{"name": a.Dest})
		case scenario.ActRegister:
			err = gw.admin(ctx, "/v1/admin/register", map[string]string{"name": a.Dest})
		}
		ev.FinishedS = time.Since(t0).Seconds()
		if err != nil {
			ev.Error = err.Error()
			log.Error("timeline action failed", "action", a.Action, "dest", a.Dest, "err", err)
		} else {
			log.Info("timeline action", "action", a.Action, "dest", a.Dest, "mode", a.Mode, "at_s", fmt.Sprintf("%.2f", ev.StartedS))
		}
		out = append(out, ev)
	}
	return out
}

func waitHTTP(ctx context.Context, url string, d time.Duration) error {
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if httpStatus(ctx, http.MethodGet, url, "", nil) == http.StatusOK {
			return nil
		}
		time.Sleep(20 * time.Millisecond)
	}
	return errors.New("not reachable: " + url)
}

// waitDrain polls /v1/stats until the gateway has dispatched everything
// and holds nothing in L2, in flight, unresolved or in L1, on two
// consecutive polls.
func waitDrain(ctx context.Context, gw *gateway, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	ok := 0
	for time.Now().Before(deadline) {
		s, _, err := gw.stats(ctx)
		if err == nil && s.drained() {
			ok++
			if ok >= 2 {
				return true
			}
		} else {
			ok = 0
		}
		time.Sleep(250 * time.Millisecond)
	}
	return false
}
