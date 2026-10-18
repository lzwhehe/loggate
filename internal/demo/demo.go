// Package demo implements `loggate --demo`: an in-process LogGate with a
// mock SOAR webhook receiver, file and sqlite destinations, an embedded
// load generator that emits SYNTHETIC events only, live reconciliation,
// fault injection and a simulated crash, all driven from the dashboard.
//
// Demo-only HTTP endpoints (mounted under /demo/ on the dashboard listener
// only when the demo runs; see Handler):
//
//	POST /demo/fault         {"dest":"soar","mode":"down|slow|ok","delay_ms":500}
//	POST /demo/kill-restart  SIMULATED crash: Gateway.Crash (drop in-memory
//	                         state, no graceful shutdown) and reopen the same
//	                         data directory
//	POST /demo/unregister    {"name":"soar"}
//	POST /demo/register      {"name":"soar"}
//
// They accept requests only from loopback addresses, with a loopback Host
// header (DNS rebinding) and the header X-LogGate-Demo: 1 (a custom header
// forces a CORS preflight, which is never answered, so other web pages
// cannot trigger them).
package demo

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/lzwhehe/loggate/internal/config"
	"github.com/lzwhehe/loggate/internal/gateway"
	"github.com/lzwhehe/loggate/internal/metrics"
	"github.com/lzwhehe/loggate/internal/ui"
)

// Options configures the demo.
type Options struct {
	// Dir is the working directory (data/, out/). Required.
	Dir string
	// EPS is the synthetic load rate; default 50.
	EPS float64
	// Seed makes the synthetic values reproducible; default 20261018.
	Seed uint64
	// IngestAddr is the gateway's HTTP ingest address; default
	// 127.0.0.1:0 (the resolved port is kept across simulated restarts).
	IngestAddr string
	// ReceiverAddr is the mock SOAR receiver's address; default 127.0.0.1:0.
	ReceiverAddr string
	Logger       *slog.Logger
}

// Runner owns the demo: receivers, gateway instance(s), load and
// reconciliation.
type Runner struct {
	opts Options
	log  *slog.Logger
	cfg  *config.Config // base config (all destinations)
	met  *metrics.Metrics
	idem []byte
	mkey []byte

	soar  *Receiver
	load  *loadGen
	recon *reconciler
	ring  maskRing
	types map[string]string

	actMu   sync.Mutex // serialises actions
	mu      sync.RWMutex
	gw      *gateway.Gateway
	status  string
	boots   int
	crashes int
	unreg   map[string]bool
	logs    []ui.LogLine
	eps     float64

	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// New prepares the demo (starts the mock receiver and builds the config).
func New(o Options) (*Runner, error) {
	if o.Dir == "" {
		return nil, errors.New("demo: Dir is required")
	}
	// Reconciliation is per process: a data directory from an earlier run
	// would replay events the load generator never saw.
	if _, err := os.Stat(filepath.Join(o.Dir, "data")); err == nil {
		return nil, fmt.Errorf("demo: %s already holds a data directory; use a new or empty directory", o.Dir)
	}
	if o.EPS <= 0 {
		o.EPS = 50
	}
	if o.EPS > 2000 {
		return nil, errors.New("demo: eps must be <= 2000")
	}
	if o.Seed == 0 {
		o.Seed = 20261018
	}
	if o.IngestAddr == "" {
		o.IngestAddr = "127.0.0.1:0"
	}
	if o.ReceiverAddr == "" {
		o.ReceiverAddr = "127.0.0.1:0"
	}
	if o.Logger == nil {
		o.Logger = slog.New(slog.DiscardHandler)
	}
	r := &Runner{opts: o, log: o.Logger, met: metrics.New(), unreg: map[string]bool{}, status: "stopped"}
	r.idem, r.mkey = randKey(), randKey()
	var err error
	if r.soar, err = NewReceiver("soar", o.ReceiverAddr, o.Logger); err != nil {
		return nil, err
	}
	if r.cfg, err = Config(o.Dir, o.IngestAddr, r.soar.URL()); err != nil {
		_ = r.soar.Close()
		return nil, err
	}
	r.types = map[string]string{}
	for _, d := range r.cfg.Destinations {
		r.types[d.Name] = d.Type
	}
	r.recon = &reconciler{
		file:   newFileObserver(filepath.Join(o.Dir, "out", "archive.jsonl")),
		sqlite: newSQLiteObserver(filepath.Join(o.Dir, "out", "siem.db")),
		soar:   r.soar, dlqDir: filepath.Join(o.Dir, "data", "dlq"),
	}
	r.met.SetSource(func() (metrics.Snapshot, bool) {
		r.mu.RLock()
		defer r.mu.RUnlock()
		if r.gw == nil {
			return metrics.Snapshot{}, false
		}
		return r.gw.MetricsSnapshot()
	})
	return r, nil
}

func randKey() []byte {
	b := make([]byte, 32)
	_, _ = rand.Read(b)
	return b
}

// Config returns the demo configuration: archive (file), siem (sqlite) and
// soar (webhook → in-process mock receiver, max_level general). Short
// backoff and breaker timings make faults and recovery visible within a
// few seconds; the retry budget is large so a demo outage never exhausts
// it.
func Config(dir, ingestAddr, soarURL string) (*config.Config, error) {
	data := filepath.ToSlash(filepath.Join(dir, "data"))
	out := filepath.ToSlash(filepath.Join(dir, "out"))
	y := fmt.Sprintf(`
node_id: demo-gw
data_dir: %q
wal: { segment_bytes: 4MiB, fsync_interval: 5ms }
ingest: { http: { listen: %q, max_body: 1MiB } }
retry: { base: 100ms, max: 2s, max_attempts: 100000, max_age: 24h, redeliver_interval: 200ms }
l1: { checkpoint_interval: 1s, orphan_ttl: 10m, sweep_interval: 1s, gc_interval: 5s }
admin: { metrics: off, ui: off }
masking:
  default: scan
  fields: { user.name: hash, src_endpoint.ip: keep }
destinations:
  - { name: archive, type: file, config: { path: %q } }
  - { name: siem, type: sqlite, config: { path: %q } }
  - name: soar
    type: webhook
    max_level: general
    breaker: { failures: 3, open_for: 3s }
    config: { url: %q, timeout: 2s }
routes:
  - { match: { class_uid: 3002 }, to: [archive, siem, soar] }
  - { match: {}, to: [archive, siem] }
`, data, ingestAddr, out+"/archive.jsonl", out+"/siem.db", soarURL)
	return config.Parse([]byte(y))
}

// Metrics returns the demo's metrics (served by the caller).
func (r *Runner) Metrics() *metrics.Metrics { return r.met }

// IngestAddr returns the gateway ingest address (resolved after Start).
func (r *Runner) IngestAddr() string { return r.cfg.Ingest.HTTP.Listen }

// Start starts the gateway, the load generator and the reconciliation
// loop.
func (r *Runner) Start(ctx context.Context) error {
	if err := os.MkdirAll(filepath.Join(r.opts.Dir, "out"), 0o755); err != nil {
		return fmt.Errorf("demo: %w", err)
	}
	g, err := r.startGateway(ctx)
	if err != nil {
		return err
	}
	// Keep the resolved port for simulated restarts.
	r.cfg.Ingest.HTTP.Listen = g.Addr()
	r.mu.Lock()
	r.gw, r.status = g, "running"
	r.mu.Unlock()
	r.load = newLoadGen("http://"+g.Addr()+"/v1/events", r.opts.EPS, r.opts.Seed)
	rctx, cancel := context.WithCancel(context.Background())
	r.cancel = cancel
	r.wg.Add(2)
	go func() { defer r.wg.Done(); r.load.run(rctx) }()
	go func() { defer r.wg.Done(); r.reconLoop(rctx) }()
	r.addLog(true, "演示启动 · demo started (%.0f eps, synthetic data only)", r.opts.EPS)
	return nil
}

// startGateway opens a gateway on the demo data directory. Destinations
// unregistered through the demo stay unregistered across simulated
// restarts (they are left out of the config, so their events stay parked
// as NoDest until registered again or orphan_ttl).
func (r *Runner) startGateway(ctx context.Context) (*gateway.Gateway, error) {
	cfg := *r.cfg
	cfg.Destinations = slices.DeleteFunc(slices.Clone(r.cfg.Destinations), func(d config.Destination) bool { return r.unreg[d.Name] })
	g, err := gateway.New(ctx, &cfg, gateway.Options{
		Logger: r.log, Metrics: r.met, IdemKey: r.idem, MaskKey: r.mkey,
		OnAccepted: r.ring.add,
	})
	if err != nil {
		return nil, err
	}
	if err := g.Start(); err != nil {
		g.Crash()
		return nil, err
	}
	r.mu.Lock()
	r.boots++
	r.mu.Unlock()
	return g, nil
}

func (r *Runner) reconLoop(ctx context.Context) {
	t := time.NewTicker(time.Second)
	defer t.Stop()
	lastSent, lastAt := uint64(0), time.Now()
	type backlog struct {
		since time.Time
		peak  int64
	}
	open := map[string]*backlog{}
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-t.C:
			gv := r.reconcileOnce(ctx)
			// Log when a destination's L1 backlog appears and drains, so
			// the recovery time is visible on the dashboard.
			for name, n := range gv.l1 {
				b := open[name]
				switch {
				case n > 0 && b == nil:
					open[name] = &backlog{since: now, peak: n}
				case n > 0:
					b.peak = max(b.peak, n)
				case b != nil:
					delete(open, name)
					r.addLog(true, "L1 积压已清空 · %s backlog drained: peak %d events, %.1fs from first park to empty", name, b.peak, now.Sub(b.since).Seconds())
				}
			}
			sent := r.load.sent.Load()
			eps := float64(sent-lastSent) / now.Sub(lastAt).Seconds()
			lastSent, lastAt = sent, now
			r.mu.Lock()
			r.eps = eps
			r.mu.Unlock()
		}
	}
}

// reconcileOnce reads the gateway view, then polls every receiver.
func (r *Runner) reconcileOnce(ctx context.Context) gatewayView {
	gv := gatewayView{watermark: map[string]uint64{}, l1: map[string]int64{}}
	r.mu.RLock()
	if r.gw != nil && r.gw.Running() {
		gv.up = true
		for _, d := range r.gw.Stats().Destinations { // watermark is read before L1 pending
			gv.watermark[d.Name], gv.l1[d.Name] = d.Watermark, d.L1Pending
		}
	}
	r.mu.RUnlock()
	r.recon.update(ctx, gv, r.load.expectedCopy())
	return gv
}

// Stop stops load, reconciliation, the gateway (gracefully) and the
// receiver.
func (r *Runner) Stop(ctx context.Context) error {
	if r.cancel != nil {
		r.cancel()
		r.wg.Wait()
	}
	r.actMu.Lock()
	defer r.actMu.Unlock()
	r.mu.Lock()
	g := r.gw
	r.gw, r.status = nil, "stopped"
	r.mu.Unlock()
	var err error
	if g != nil {
		err = g.Shutdown(ctx)
	}
	r.recon.sqlite.close()
	return errors.Join(err, r.soar.Close())
}

// Fault injects a receiver fault. Only "soar" has a mock receiver.
func (r *Runner) Fault(dest, mode string, delay time.Duration) error {
	if dest != "soar" {
		return fmt.Errorf("only soar has a mock receiver (got %q)", dest)
	}
	err := r.soar.SetFault(mode, delay)
	switch {
	case err != nil:
		r.addLog(false, "注入故障失败 · fault %s %s: %v", dest, mode, err)
	case mode == ModeDown:
		r.addLog(true, "停止接收端 · %s receiver DOWN (connections refused)", dest)
	case mode == ModeSlow:
		r.addLog(true, "接收端变慢 · %s receiver SLOW %dms", dest, delay.Milliseconds())
	default:
		r.addLog(true, "恢复接收端 · %s receiver restored", dest)
	}
	return err
}

// KillRestart simulates a crash: Gateway.Crash drops in-memory state
// (uncommitted L1 operations, L2 queues, in-flight results, no final
// checkpoint) and a new gateway reopens the same data directory, which
// runs WAL recovery and replay. This is an IN-PROCESS SIMULATION; a real
// kill -9 is the faultbench S3 scenario.
func (r *Runner) KillRestart(ctx context.Context) error {
	r.actMu.Lock()
	defer r.actMu.Unlock()
	r.mu.Lock()
	g := r.gw
	r.gw, r.status = nil, "restarting"
	r.crashes++
	r.mu.Unlock()
	if g != nil {
		g.Crash()
	}
	start := time.Now()
	ng, err := r.startGateway(ctx)
	if err != nil {
		r.mu.Lock()
		r.status = "stopped"
		r.mu.Unlock()
		r.addLog(false, "模拟崩溃后重启失败 · restart after simulated crash failed: %v", err)
		return err
	}
	r.mu.Lock()
	r.gw, r.status = ng, "running"
	r.mu.Unlock()
	r.addLog(true, "模拟崩溃并重启 · simulated crash + restart in %dms, replayed %d WAL records",
		time.Since(start).Milliseconds(), ng.Replayed())
	return nil
}

// Unregister drains and removes a destination.
func (r *Runner) Unregister(ctx context.Context, name string) error {
	r.actMu.Lock()
	defer r.actMu.Unlock()
	r.mu.RLock()
	g := r.gw
	r.mu.RUnlock()
	if g == nil {
		return errors.New("gateway is restarting")
	}
	err := g.Unregister(ctx, name)
	if err == nil {
		r.mu.Lock()
		r.unreg[name] = true
		r.mu.Unlock()
		r.addLog(true, "注销目的地 · unregistered %s (its events park in L1, orphan_ttl %s)", name, r.cfg.L1.OrphanTTL)
	} else {
		r.addLog(false, "注销失败 · unregister %s: %v", name, err)
	}
	return err
}

// Register re-registers a destination with its demo configuration.
func (r *Runner) Register(ctx context.Context, name string) error {
	r.actMu.Lock()
	defer r.actMu.Unlock()
	i := slices.IndexFunc(r.cfg.Destinations, func(d config.Destination) bool { return d.Name == name })
	if i < 0 {
		return fmt.Errorf("unknown destination %q", name)
	}
	r.mu.RLock()
	g := r.gw
	r.mu.RUnlock()
	if g == nil {
		return errors.New("gateway is restarting")
	}
	err := g.Register(ctx, r.cfg.Destinations[i])
	if err == nil {
		r.mu.Lock()
		delete(r.unreg, name)
		r.mu.Unlock()
		r.addLog(true, "重新注册 · registered %s; parked events are redelivered", name)
	} else {
		r.addLog(false, "注册失败 · register %s: %v", name, err)
	}
	return err
}

func (r *Runner) addLog(ok bool, format string, args ...any) {
	line := ui.LogLine{At: time.Now().UnixMilli(), Text: fmt.Sprintf(format, args...), OK: ok}
	r.mu.Lock()
	r.logs = append(r.logs, line)
	if len(r.logs) > 30 {
		r.logs = r.logs[len(r.logs)-30:]
	}
	r.mu.Unlock()
	r.log.Info("demo", "action", line.Text)
}

// Snapshot builds the dashboard state.
func (r *Runner) Snapshot() ui.Snapshot {
	now := time.Now()
	r.mu.RLock()
	s := ui.FromGateway(r.gw, r.met, r.types, now)
	status, boots, crashes, eps := r.status, r.boots, r.crashes, r.eps
	logs := slices.Clone(r.logs)
	r.mu.RUnlock()
	s.Mode = "demo"
	s.Gateway.Status, s.Gateway.Boots = status, boots
	if status != "running" {
		// Keep the table: show configured destinations as unknown.
		for _, d := range r.cfg.Destinations {
			s.Dests = append(s.Dests, ui.DestRow{Name: d.Name, Type: d.Type, Lifecycle: status})
		}
	}
	mode, delay := r.soar.Fault()
	for i := range s.Dests {
		if s.Dests[i].Name == "soar" {
			s.Dests[i].Fault = &ui.FaultView{Mode: mode, DelayMS: delay.Milliseconds()}
		}
	}
	slices.Reverse(logs)
	dv := &ui.DemoView{
		EPS: eps, Crashes: crashes, Recon: r.recon.snapshot(), Masked: r.ring.latest(), Log: logs,
		DataDir: r.opts.Dir,
	}
	if r.load != nil {
		dv.Sent, dv.Acked, dv.Refused, dv.Failed = r.load.sent.Load(), r.load.acked.Load(), r.load.refused.Load(), r.load.failed.Load()
		r.load.mu.Lock()
		dv.LastErr = r.load.lastErr
		r.load.mu.Unlock()
	}
	for _, row := range dv.Recon {
		if !row.Settled {
			dv.Settling = true
		}
	}
	s.Demo = dv
	return s
}

// Handler serves the demo actions (mount at /demo/).
func (r *Runner) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /demo/fault", func(w http.ResponseWriter, req *http.Request) {
		var body struct {
			Dest    string `json:"dest"`
			Mode    string `json:"mode"`
			DelayMS int64  `json:"delay_ms"`
		}
		if !decode(w, req, &body) {
			return
		}
		reply(w, r.Fault(body.Dest, body.Mode, time.Duration(body.DelayMS)*time.Millisecond))
	})
	mux.HandleFunc("POST /demo/kill-restart", func(w http.ResponseWriter, req *http.Request) {
		ctx, cancel := context.WithTimeout(req.Context(), 30*time.Second)
		defer cancel()
		reply(w, r.KillRestart(ctx))
	})
	mux.HandleFunc("POST /demo/unregister", func(w http.ResponseWriter, req *http.Request) {
		var body struct {
			Name string `json:"name"`
		}
		if !decode(w, req, &body) {
			return
		}
		ctx, cancel := context.WithTimeout(req.Context(), 30*time.Second)
		defer cancel()
		reply(w, r.Unregister(ctx, body.Name))
	})
	mux.HandleFunc("POST /demo/register", func(w http.ResponseWriter, req *http.Request) {
		var body struct {
			Name string `json:"name"`
		}
		if !decode(w, req, &body) {
			return
		}
		reply(w, r.Register(req.Context(), body.Name))
	})
	return guard(mux)
}

// guard admits only loopback clients that address the server by a
// loopback host name and send the anti-CSRF header.
func guard(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		host, _, err := net.SplitHostPort(req.RemoteAddr)
		ip := net.ParseIP(host)
		if err != nil || ip == nil || !ip.IsLoopback() {
			writeJSON(w, http.StatusForbidden, map[string]any{"ok": false, "error": "demo actions are loopback-only"})
			return
		}
		if !loopbackHost(req.Host) {
			writeJSON(w, http.StatusForbidden, map[string]any{"ok": false, "error": "Host must be localhost or a loopback address"})
			return
		}
		if req.Header.Get("X-LogGate-Demo") != "1" {
			writeJSON(w, http.StatusForbidden, map[string]any{"ok": false, "error": "missing X-LogGate-Demo: 1 header"})
			return
		}
		h.ServeHTTP(w, req)
	})
}

func loopbackHost(hostport string) bool {
	h := hostport
	if hh, _, err := net.SplitHostPort(hostport); err == nil {
		h = hh
	}
	h = strings.Trim(h, "[]")
	if strings.EqualFold(h, "localhost") {
		return true
	}
	ip := net.ParseIP(h)
	return ip != nil && ip.IsLoopback()
}

func decode(w http.ResponseWriter, req *http.Request, v any) bool {
	ct := req.Header.Get("Content-Type")
	if ct != "" && !strings.HasPrefix(ct, "application/json") {
		writeJSON(w, http.StatusUnsupportedMediaType, map[string]any{"ok": false, "error": "want application/json"})
		return false
	}
	if err := json.NewDecoder(io.LimitReader(req.Body, 4096)).Decode(v); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"ok": false, "error": "invalid JSON body"})
		return false
	}
	return true
}

func reply(w http.ResponseWriter, err error) {
	if err != nil {
		writeJSON(w, http.StatusConflict, map[string]any{"ok": false, "error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
