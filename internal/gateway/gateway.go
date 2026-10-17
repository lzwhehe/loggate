// Package gateway wires WAL, state, ingest, routing, dispatch, workers and
// sinks into a running LogGate instance. cmd/loggate and end-to-end tests
// both use it.
package gateway

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lzwhehe/loggate/internal/clock"
	"github.com/lzwhehe/loggate/internal/config"
	"github.com/lzwhehe/loggate/internal/delivery"
	"github.com/lzwhehe/loggate/internal/dispatch"
	"github.com/lzwhehe/loggate/internal/dlq"
	"github.com/lzwhehe/loggate/internal/event"
	httpingest "github.com/lzwhehe/loggate/internal/ingest/http"
	"github.com/lzwhehe/loggate/internal/mask"
	"github.com/lzwhehe/loggate/internal/route"
	"github.com/lzwhehe/loggate/internal/sink"
	filesink "github.com/lzwhehe/loggate/internal/sink/file"
	sqlitesink "github.com/lzwhehe/loggate/internal/sink/sqlite"
	webhooksink "github.com/lzwhehe/loggate/internal/sink/webhook"
	"github.com/lzwhehe/loggate/internal/state"
	"github.com/lzwhehe/loggate/internal/tracker"
	"github.com/lzwhehe/loggate/internal/wal"
	"github.com/lzwhehe/loggate/internal/worker"
)

// Options carries dependencies that are not part of the YAML config.
type Options struct {
	Logger *slog.Logger
	Clock  clock.Clock
	// WALFS overrides the WAL filesystem (fault injection in tests).
	WALFS wal.FS
	// IdemKey overrides the HMAC key otherwise read from the environment.
	IdemKey []byte
	// MaskKey overrides the masking hash key otherwise read from the
	// environment.
	MaskKey []byte
	// Sinks overrides the sink type registry (tests register fakes).
	Sinks *sink.Registry
	// BackoffSeed makes retry jitter reproducible; 0 = random.
	BackoffSeed uint64
	// BeforeDispatch is called before each live batch is dispatched (test
	// hook for "committed but not yet dispatched" crash points).
	BeforeDispatch func()
}

type slotTable [event.MaxDests]*worker.Slot

// Gateway is one running LogGate instance.
type Gateway struct {
	cfg *config.Config
	log *slog.Logger
	clk clock.Clock
	reg *sink.Registry

	st       *state.Store
	wal      *wal.WAL
	dlq      *dlq.Log
	table    *route.Table
	masker   *mask.Engine
	trackers *tracker.Set
	claims   *state.Claims
	deps     *worker.Deps
	disp     *dispatch.Dispatcher
	maint    *dispatch.Maint
	http     *httpingest.Handler
	srv      *http.Server
	ln       net.Listener

	replayFrom uint64
	replayed   int

	adminMu      sync.Mutex // serialises Register/Unregister
	slots        atomic.Pointer[slotTable]
	regMu        sync.RWMutex
	entries      map[string]state.RegEntry
	names        map[event.DestID]string
	unregistered map[event.DestID]bool
	dests        map[string]config.Destination

	origDests   map[string]config.Destination // startup config, for admin re-register
	adminDigest []byte                        // sha256 of the admin token; nil = admin API off

	shutdownReq  chan struct{}
	shutdownOnce sync.Once

	diskBytes    atomic.Int64
	diskOver     atomic.Bool
	diskStop     chan struct{}
	diskDone     chan struct{}
	diskStopOnce sync.Once

	dispDone chan struct{}
	started  bool
	stopOnce sync.Once
	shutErr  error
}

// BuiltinSinks returns the registry of compiled-in sink types.
func BuiltinSinks() *sink.Registry {
	r := sink.NewRegistry()
	_ = r.Register(filesink.Type, filesink.New)
	_ = r.Register(sqlitesink.Type, sqlitesink.New)
	_ = r.Register(webhooksink.Type, webhooksink.New)
	return r
}

// New runs recovery up to (not including) replay: it opens the state store
// and the WAL (seq floor = max(all ckpt, max L1 seq), scan from min ckpt),
// reconciles the destination registry, builds the routing table, and
// initialises sinks. Nothing is delivered or served until Start.
func New(ctx context.Context, cfg *config.Config, opts Options) (g *Gateway, err error) {
	if opts.Logger == nil {
		opts.Logger = slog.New(slog.DiscardHandler)
	}
	if opts.Clock == nil {
		opts.Clock = clock.Real()
	}
	if opts.Sinks == nil {
		opts.Sinks = BuiltinSinks()
	}
	g = &Gateway{
		cfg: cfg, log: opts.Logger, clk: opts.Clock, reg: opts.Sinks,
		claims: state.NewClaims(), unregistered: map[event.DestID]bool{}, dests: map[string]config.Destination{},
		origDests: map[string]config.Destination{}, shutdownReq: make(chan struct{}),
	}
	g.slots.Store(&slotTable{})
	defer func() {
		if err != nil {
			g.closeResources(context.Background(), true)
		}
	}()

	key, err := idemKey(cfg, opts)
	if err != nil {
		return nil, err
	}
	keyer, err := event.NewIdemKeyer(key, cfg.NodeID, uint64(opts.Clock.Now().UnixNano()))
	if err != nil {
		return nil, fmt.Errorf("gateway: %w", err)
	}
	if err := os.MkdirAll(cfg.DataDir, 0o755); err != nil {
		return nil, fmt.Errorf("gateway: create data dir: %w", err)
	}

	// 1. State: registry, ckpt, L1 indexes.
	g.st, err = state.Open(state.Options{Path: filepath.Join(cfg.DataDir, "state.db"), Clock: opts.Clock, Logger: opts.Logger})
	if err != nil {
		return nil, fmt.Errorf("gateway: %w", err)
	}
	floor, scanFrom := g.st.MaxL1Seq(), uint64(0)
	if reg := g.st.Registry(); len(reg) > 0 {
		scanFrom = ^uint64(0)
		for _, e := range reg {
			c := g.st.Checkpoint(e.ID)
			floor, scanFrom = max(floor, c), min(scanFrom, c)
		}
	}

	// 2. WAL: torn-tail truncation; seqs are never reused (§0.5 item 1).
	g.wal, err = wal.Open(wal.Options{
		Dir:            filepath.Join(cfg.DataDir, "wal"),
		SegmentBytes:   cfg.WAL.SegmentBytes.Int64(),
		FsyncInterval:  cfg.WAL.FsyncInterval,
		FsyncMaxEvents: cfg.WAL.FsyncMaxEvents,
		SeqFloor:       floor,
		ScanFrom:       scanFrom,
		EmitCommitted:  true,
		FS:             opts.WALFS,
		Clock:          opts.Clock,
		Logger:         opts.Logger,
	})
	if err != nil {
		return nil, fmt.Errorf("gateway: %w", err)
	}

	// 3. Registry: stable IDs, absent and tombstoned destinations.
	var configured []string
	for _, d := range cfg.Destinations {
		configured = append(configured, d.Name)
		g.dests[d.Name] = d
		g.origDests[d.Name] = d
	}
	var referenced []string
	for _, r := range cfg.Routes {
		referenced = append(referenced, r.To...)
	}
	if abs := cfg.AbsentDestinations(); len(abs) > 0 {
		g.log.Warn("routes reference unconfigured destinations; their events are parked until configured or orphan_ttl", "names", abs, "orphan_ttl", cfg.L1.OrphanTTL)
	}
	rec, err := g.st.Reconcile(configured, referenced, g.wal.NextSeq()-1, cfg.L1.TombstoneTTL)
	if err != nil {
		return nil, fmt.Errorf("gateway: %w", err)
	}
	g.setEntries(rec.Entries)
	ids := map[string]event.DestID{}
	for n, e := range rec.Entries {
		if e.State != state.RegTombstone {
			ids[n] = e.ID
		}
	}
	g.table, err = route.New(ids, cfg.Routes)
	if err != nil {
		return nil, fmt.Errorf("gateway: %w", err)
	}
	for _, d := range cfg.Destinations {
		if err := g.setMaxLevel(rec.Entries[d.Name].ID, d); err != nil {
			return nil, err
		}
	}
	if !cfg.Masking.Disabled {
		if g.masker, err = newMasker(cfg, opts); err != nil {
			return nil, err
		}
	} else {
		g.log.Warn("masking disabled: the WAL, L1 and DLQ hold unmasked input")
	}

	// 4. Delivery machinery.
	g.replayFrom = 1
	if len(rec.Entries) > 0 {
		m := ^uint64(0)
		for _, e := range rec.Entries {
			m = min(m, g.st.Checkpoint(e.ID))
		}
		g.replayFrom = m + 1
	}
	g.trackers = tracker.NewSet(g.replayFrom - 1)
	g.dlq, err = dlq.Open(filepath.Join(cfg.DataDir, "dlq"), 0)
	if err != nil {
		return nil, fmt.Errorf("gateway: %w", err)
	}
	seed := opts.BackoffSeed
	if seed == 0 {
		var b [8]byte
		_, _ = rand.Read(b[:])
		seed = binary.LittleEndian.Uint64(b[:])
	}
	quota := cfg.L1.MaxRecordsPerDest
	g.deps = &worker.Deps{
		L1: g.st, DLQ: g.dlq, Resolver: g.trackers, Claims: g.claims, Loader: walLoader{g.wal},
		Clock: opts.Clock, Logger: opts.Logger,
		Backoff: delivery.NewBackoff(cfg.Retry.Base, cfg.Retry.Max, seed),
		Budget:  delivery.Budget{MaxAttempts: cfg.Retry.MaxAttempts, MaxAge: cfg.Retry.MaxAge},
		Quota:   quota, OnQuota: cfg.L1.OnQuota, DestName: g.destName,
	}
	g.disp = dispatch.New(dispatch.Options{
		Deps: g.deps, Trackers: g.trackers, Slot: g.slot, MissingReason: g.missingReason,
		BeforeBatch: opts.BeforeDispatch, Logger: opts.Logger,
	})
	g.maint = dispatch.NewMaint(dispatch.MaintOptions{
		L1: g.st, WAL: g.wal, DLQ: g.dlq, Trackers: g.trackers, Claims: g.claims,
		Clock: opts.Clock, Logger: opts.Logger, IDs: g.ids, Active: g.active, DestName: g.destName,
		CheckpointInterval: cfg.L1.CheckpointInterval, SweepInterval: cfg.L1.SweepInterval,
		GCInterval: cfg.L1.GCInterval, OrphanTTL: cfg.L1.OrphanTTL, MaxAge: cfg.Retry.MaxAge,
	})

	// 5. Sinks and slots (Registered; started in Start).
	tbl := &slotTable{}
	for _, d := range cfg.Destinations {
		s, err := g.reg.New(ctx, d.Type, d.Config)
		if err != nil {
			g.slots.Store(tbl)
			return nil, fmt.Errorf("gateway: destination %s: %w", d.Name, err)
		}
		id := rec.Entries[d.Name].ID
		tbl[id] = worker.New(g.slotConfig(id, d), s, g.deps)
	}
	g.slots.Store(tbl)

	// 6. Ingest.
	tokens := map[string]string{}
	for _, t := range cfg.Ingest.HTTP.Tokens {
		v, err := t.Resolve()
		if err != nil {
			return nil, fmt.Errorf("gateway: %w", err)
		}
		if _, dup := tokens[v]; dup {
			return nil, fmt.Errorf("gateway: sources share a token (source %q)", t.Source)
		}
		tokens[v] = t.Source
	}
	g.http, err = httpingest.New(g.wal, g.table, httpingest.Options{
		MaxBody: cfg.Ingest.HTTP.MaxBody.Int64(), Tokens: tokens, Keyer: keyer,
		Clock: opts.Clock, Logger: opts.Logger,
		Backpressure: g.backpressure, Stats: func() any { return g.Stats() },
		Masker: g.masker,
	})
	if err != nil {
		return nil, fmt.Errorf("gateway: %w", err)
	}
	if cfg.Admin.Enabled() {
		tok, err := cfg.Admin.Resolve()
		if err != nil {
			return nil, fmt.Errorf("gateway: %w", err)
		}
		d := sha256.Sum256([]byte(tok))
		g.adminDigest = d[:]
	}
	return g, nil
}

func (g *Gateway) setMaxLevel(id event.DestID, d config.Destination) error {
	l, err := event.ParseLevel(d.MaxLevel)
	if err != nil {
		return fmt.Errorf("gateway: destination %s: %w", d.Name, err)
	}
	g.table.SetMaxLevel(id, l)
	return nil
}

func newMasker(cfg *config.Config, opts Options) (*mask.Engine, error) {
	mc, err := cfg.Masking.EngineConfig()
	if err != nil {
		return nil, fmt.Errorf("gateway: %w", err)
	}
	mc.HMACKey = opts.MaskKey
	if mc.HMACKey == nil {
		if v := os.Getenv(cfg.Masking.HMACKeyEnv); v != "" {
			if len(v) < 16 {
				return nil, fmt.Errorf("gateway: %s must be at least 16 bytes", cfg.Masking.HMACKeyEnv)
			}
			mc.HMACKey = []byte(v)
		} else {
			mc.HMACKey = make([]byte, 32)
			if _, err := rand.Read(mc.HMACKey); err != nil {
				return nil, fmt.Errorf("gateway: random mask key: %w", err)
			}
			for _, p := range mc.Fields {
				if p == mask.PolicyHash {
					opts.Logger.Warn("masking HMAC key not set; hash values use a random per-process key and are not stable across restarts", "env", cfg.Masking.HMACKeyEnv)
					break
				}
			}
		}
	}
	m, err := mask.New(mc)
	if err != nil {
		return nil, fmt.Errorf("gateway: %w", err)
	}
	return m, nil
}

func (g *Gateway) slotConfig(id event.DestID, d config.Destination) worker.Config {
	return worker.Config{
		ID: id, Name: d.Name,
		MaxEvents: d.Queue.MaxEvents, MaxBytes: d.Queue.MaxBytes.Int64(),
		MaxBatch: d.Queue.MaxBatch, BatchWait: d.Queue.BatchWait,
		RedeliverShare: g.cfg.Retry.RedeliverShare, RedeliverInterval: g.cfg.Retry.RedeliverInterval,
		BreakerFailures: d.Breaker.Failures, BreakerOpenFor: d.Breaker.OpenFor,
	}
}

func (g *Gateway) setEntries(entries map[string]state.RegEntry) {
	g.regMu.Lock()
	defer g.regMu.Unlock()
	g.entries = entries
	g.names = map[event.DestID]string{}
	for n, e := range entries {
		g.names[e.ID] = n
	}
}

func (g *Gateway) destName(id event.DestID) string {
	g.regMu.RLock()
	defer g.regMu.RUnlock()
	return g.names[id]
}

func (g *Gateway) ids() []event.DestID {
	g.regMu.RLock()
	defer g.regMu.RUnlock()
	out := make([]event.DestID, 0, len(g.entries))
	for _, e := range g.entries {
		out = append(out, e.ID)
	}
	slices.Sort(out)
	return out
}

func (g *Gateway) slot(id event.DestID) *worker.Slot { return g.slots.Load()[id&63] }

func (g *Gateway) active(id event.DestID) bool {
	s := g.slot(id)
	return s != nil && s.State() == worker.Active
}

func (g *Gateway) missingReason(id event.DestID) state.Reason {
	g.regMu.RLock()
	defer g.regMu.RUnlock()
	if g.unregistered[id] {
		return state.ReasonNotActive
	}
	if e, ok := g.entries[g.names[id]]; ok && e.State == state.RegAbsent {
		return state.ReasonNoDest
	}
	return state.ReasonNotActive
}

func idemKey(cfg *config.Config, opts Options) ([]byte, error) {
	if opts.IdemKey != nil {
		return opts.IdemKey, nil
	}
	if v := os.Getenv(cfg.Idempotency.KeyEnv); v != "" {
		if len(v) < event.MinIdemKeyLen {
			return nil, fmt.Errorf("gateway: %s must be at least %d bytes", cfg.Idempotency.KeyEnv, event.MinIdemKeyLen)
		}
		return []byte(v), nil
	}
	opts.Logger.Warn("idempotency HMAC key not set; using a random per-process key", "env", cfg.Idempotency.KeyEnv)
	k := make([]byte, 32)
	if _, err := rand.Read(k); err != nil {
		return nil, fmt.Errorf("gateway: random idem key: %w", err)
	}
	return k, nil
}

// walLoader reads parked events back from the WAL.
type walLoader struct{ w *wal.WAL }

func (l walLoader) Load(ref state.Ref) (*sink.Envelope, error) {
	r, err := l.w.ReadAt(ref.SegID, ref.Offset, ref.Length)
	if err != nil {
		return nil, err
	}
	ev := new(event.LogEvent)
	dec := json.NewDecoder(bytes.NewReader(r.Payload))
	dec.UseNumber()
	if err := dec.Decode(ev); err != nil {
		return nil, fmt.Errorf("decode seq %d: %w", r.Seq, err)
	}
	return &sink.Envelope{Seq: r.Seq, Event: ev, SegID: ref.SegID, Offset: ref.Offset, Length: ref.Length}, nil
}

// Start finishes recovery and opens ingest (DESIGN §4.7): start the slot
// consumers, replay the WAL from min(ckpt)+1, then start the redeliver
// loops and maintenance (checkpoint, sweeper, GC), then live dispatch, and
// only then listen for HTTP.
func (g *Gateway) Start() error {
	tbl := g.slots.Load()
	for _, s := range tbl {
		if s != nil {
			if err := s.Start(false); err != nil {
				return fmt.Errorf("gateway: %w", err)
			}
		}
	}
	n, err := g.disp.Replay(g.wal.NewIterator(g.replayFrom), g.st.Checkpoint)
	if err != nil {
		return fmt.Errorf("gateway: %w", err)
	}
	g.replayed = n
	for _, s := range tbl {
		if s != nil {
			s.StartRedeliver()
		}
	}
	g.maint.Start()
	g.checkDisk() // the quota gate is correct before the first request
	g.diskStop, g.diskDone = make(chan struct{}), make(chan struct{})
	go g.diskLoop()
	g.dispDone = make(chan struct{})
	go func() {
		defer close(g.dispDone)
		g.disp.Run(g.wal.Committed())
	}()
	g.started = true

	ln, err := net.Listen("tcp", g.cfg.Ingest.HTTP.Listen)
	if err != nil {
		return fmt.Errorf("gateway: listen %s: %w", g.cfg.Ingest.HTTP.Listen, err)
	}
	g.ln = ln
	g.srv = &http.Server{
		Handler:           g.handler(),
		ReadHeaderTimeout: 10 * time.Second,
		ErrorLog:          slog.NewLogLogger(g.log.Handler(), slog.LevelWarn),
	}
	go func() {
		if err := g.srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			g.log.Error("http server stopped", "err", err)
		}
	}()
	g.log.Info("loggate started", "http", ln.Addr().String(), "data_dir", g.cfg.DataDir,
		"next_seq", g.wal.NextSeq(), "replay_from", g.replayFrom, "replayed", n, "l1_records", g.st.Count())
	return nil
}

// Addr returns the HTTP listen address (useful with port 0).
func (g *Gateway) Addr() string {
	if g.ln == nil {
		return ""
	}
	return g.ln.Addr().String()
}

// Delivered returns the number of envelopes confirmed by sinks.
func (g *Gateway) Delivered() uint64 {
	var n uint64
	for _, s := range g.slots.Load() {
		if s != nil {
			n += s.Delivered()
		}
	}
	return n
}

// Replayed returns how many WAL records recovery re-dispatched.
func (g *Gateway) Replayed() int { return g.replayed }

// Register adds (or re-activates) a destination at runtime. Events parked
// for it while it was absent or unregistered are picked up by its
// redeliver loop (DESIGN §5 step 3).
func (g *Gateway) Register(ctx context.Context, d config.Destination) error {
	d.ApplyDefaults()
	if err := config.ValidateDestination(d, func(t string) bool { return slices.Contains(g.reg.Types(), t) }); err != nil {
		return fmt.Errorf("gateway: register: %w", err)
	}
	g.adminMu.Lock()
	defer g.adminMu.Unlock()
	if s := g.slotByName(d.Name); s != nil {
		return fmt.Errorf("gateway: register %s: already registered", d.Name)
	}
	if _, ok := g.st.Registry()[d.Name]; ok {
		if err := g.st.SetState(d.Name, state.RegConfigured); err != nil {
			return fmt.Errorf("gateway: register: %w", err)
		}
	} else {
		var configured []string
		for n := range g.dests {
			configured = append(configured, n)
		}
		configured = append(configured, d.Name)
		var referenced []string
		for _, r := range g.cfg.Routes {
			referenced = append(referenced, r.To...)
		}
		if _, err := g.st.Reconcile(configured, referenced, g.wal.NextSeq()-1, g.cfg.L1.TombstoneTTL); err != nil {
			return fmt.Errorf("gateway: register: %w", err)
		}
	}
	entries := g.st.Registry()
	id := entries[d.Name].ID
	s, err := g.reg.New(ctx, d.Type, d.Config)
	if err != nil {
		return fmt.Errorf("gateway: register %s: %w", d.Name, err)
	}
	if err := g.setMaxLevel(id, d); err != nil {
		return err
	}
	slot := worker.New(g.slotConfig(id, d), s, g.deps)
	if err := slot.Start(g.started); err != nil {
		return err
	}
	g.setEntries(entries)
	g.regMu.Lock()
	delete(g.unregistered, id)
	g.dests[d.Name] = d
	g.regMu.Unlock()
	tbl := *g.slots.Load()
	tbl[id] = slot
	g.slots.Store(&tbl)
	g.log.Info("destination registered", "dest", d.Name, "id", id, "l1_pending", g.st.Pending(id))
	return nil
}

func (g *Gateway) slotByName(name string) *worker.Slot {
	for _, s := range g.slots.Load() {
		if s != nil && s.Name() == name {
			return s
		}
	}
	return nil
}

// Unregister drains a destination (in-flight batch completes, the rest of
// L2 is parked as NotActive) and removes it. Its parked events wait for
// re-registration until orphan_ttl, then go to the DLQ.
func (g *Gateway) Unregister(ctx context.Context, name string) error {
	g.adminMu.Lock()
	defer g.adminMu.Unlock()
	s := g.slotByName(name)
	if s == nil {
		return fmt.Errorf("gateway: unregister %s: not registered", name)
	}
	if err := s.Drain(ctx, state.ReasonNotActive); err != nil {
		return fmt.Errorf("gateway: unregister %s: %w", name, err)
	}
	g.regMu.Lock()
	g.unregistered[s.ID()] = true
	delete(g.dests, name)
	g.regMu.Unlock()
	tbl := *g.slots.Load()
	tbl[s.ID()] = nil
	g.slots.Store(&tbl)
	// A routed name keeps its ID reserved (absent); otherwise tombstone.
	st := state.RegTombstone
	if _, routed := g.table.ID(name); routed {
		st = state.RegAbsent
	}
	if err := g.st.SetState(name, st); err != nil {
		return fmt.Errorf("gateway: unregister %s: %w", name, err)
	}
	g.setEntries(g.st.Registry())
	if err := s.Sink().Close(ctx); err != nil {
		g.log.Warn("close sink", "dest", name, "err", err)
	}
	g.log.Info("destination unregistered", "dest", name, "l1_pending", g.st.Pending(s.ID()))
	return nil
}

// Stats is the /v1/stats payload.
type Stats struct {
	NextSeq      uint64      `json:"next_seq"`
	Cursor       uint64      `json:"dispatch_cursor"`
	Replayed     int         `json:"replayed"`
	L1Records    int         `json:"l1_records"`
	DLQTotal     uint64      `json:"dlq_total"`
	Swept        uint64      `json:"swept"`
	WALSegments  int         `json:"wal_segments"`
	SegmentsGCed uint64      `json:"segments_gced"`
	Backpressure bool        `json:"backpressure"`
	Rejected     uint64      `json:"ingest_rejected_429"`
	Destinations []DestStats `json:"destinations"`
	Undecodable  uint64      `json:"undecodable"`
	DiskBytes    int64       `json:"disk_bytes"`
	DiskOver     bool        `json:"disk_over_quota"`
	Masking      *mask.Stats `json:"masking,omitempty"`
}

// DestStats is per-destination state.
type DestStats struct {
	worker.Stats
	Registry   string `json:"registry"`
	Watermark  uint64 `json:"watermark"`
	Checkpoint uint64 `json:"checkpoint"`
	Unresolved int    `json:"unresolved"`
	DLQ        uint64 `json:"dlq"`
	// RouteLevelBlocked counts events kept away by max_level.
	RouteLevelBlocked uint64 `json:"route_level_blocked"`
}

// Stats returns a snapshot of delivery state.
func (g *Gateway) Stats() Stats {
	_, _, _, undec := g.disp.Stats()
	s := Stats{
		NextSeq: g.wal.NextSeq(), Cursor: g.trackers.Cursor(), Replayed: g.replayed,
		L1Records: g.st.Count(), DLQTotal: g.dlq.Total(), Swept: g.maint.Swept(),
		WALSegments: len(g.wal.Segments()), SegmentsGCed: g.maint.Removed(),
		Backpressure: g.backpressure(), Rejected: g.http.Rejected(), Undecodable: undec,
		DiskBytes: g.diskBytes.Load(), DiskOver: g.diskOver.Load(),
	}
	if g.masker != nil {
		ms := g.masker.Stats()
		s.Masking = &ms
	}
	g.regMu.RLock()
	entries := make([]state.RegEntry, 0, len(g.entries))
	for n, e := range g.entries {
		e.Name = n
		entries = append(entries, e)
	}
	g.regMu.RUnlock()
	slices.SortFunc(entries, func(a, b state.RegEntry) int { return int(a.ID) - int(b.ID) })
	for _, e := range entries {
		ds := DestStats{
			Registry: string(e.State), Watermark: g.trackers.Watermark(e.ID), Checkpoint: g.st.Checkpoint(e.ID),
			Unresolved: g.trackers.Unresolved(e.ID), DLQ: g.dlq.Count(e.ID),
			RouteLevelBlocked: g.table.LevelBlocked(e.ID),
		}
		if sl := g.slot(e.ID); sl != nil {
			ds.Stats = sl.Stats()
		} else {
			ds.Stats = worker.Stats{ID: e.ID, Name: e.Name, State: "absent", L1Pending: g.st.Pending(e.ID)}
		}
		s.Destinations = append(s.Destinations, ds)
	}
	return s
}

// Shutdown stops the gateway gracefully (DESIGN §4.10): stop ingest, flush
// the WAL, let the dispatcher finish committed records, drain every slot
// (in-flight batches complete; the rest of L2 is parked with reason
// Shutdown), write a final checkpoint, close everything. After a graceful
// shutdown every acked event is either delivered or in L1, and the final
// checkpoint covers everything dispatched, so a restart replays nothing.
func (g *Gateway) Shutdown(ctx context.Context) error {
	g.stopOnce.Do(func() {
		var errs []error
		g.http.SetDraining(true)
		if g.srv != nil {
			if err := g.srv.Shutdown(ctx); err != nil {
				errs = append(errs, fmt.Errorf("http shutdown: %w", err))
			}
		}
		if err := g.wal.Close(); err != nil {
			errs = append(errs, err)
		}
		if g.dispDone != nil {
			<-g.dispDone // never blocks for long: dispatch is non-blocking
		}
		g.maint.Stop()
		g.stopDiskLoop()
		var wg sync.WaitGroup
		for _, s := range g.slots.Load() {
			if s == nil || s.State() != worker.Active {
				continue
			}
			wg.Add(1)
			go func() {
				defer wg.Done()
				if err := s.Drain(ctx, state.ReasonShutdown); err != nil {
					g.log.Warn("drain", "dest", s.Name(), "err", err)
				}
			}()
		}
		wg.Wait()
		if g.started {
			fctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			if err := g.st.Flush(fctx); err != nil {
				errs = append(errs, fmt.Errorf("flush l1: %w", err))
			} else if err := g.maint.Checkpoint(fctx); err != nil {
				errs = append(errs, err)
			}
			cancel()
		}
		if err := g.closeResources(ctx, false); err != nil {
			errs = append(errs, err)
		}
		g.shutErr = errors.Join(errs...)
		g.log.Info("loggate stopped", "delivered", g.Delivered(), "err", g.shutErr)
	})
	return g.shutErr
}

// crash stops the gateway as abruptly as an in-process test can: pending
// L1 operations are discarded uncommitted, no final checkpoint is written,
// in-flight sink results are ignored and L2 contents are dropped. (The WAL
// itself already fsynced everything that was acknowledged.)
func (g *Gateway) crash() {
	g.stopOnce.Do(func() {
		if g.st != nil {
			_ = g.st.Abort()
		}
		if g.srv != nil {
			_ = g.srv.Close()
		}
		g.maint.Stop()
		g.stopDiskLoop()
		for _, s := range g.slots.Load() {
			if s != nil {
				s.Abort()
			}
		}
		_ = g.wal.Close()
		if g.dispDone != nil {
			<-g.dispDone
		}
		_ = g.closeResources(context.Background(), true)
	})
}

// closeResources closes state, WAL, DLQ and sinks (whatever is open).
func (g *Gateway) closeResources(ctx context.Context, quiet bool) error {
	var errs []error
	if g.st != nil {
		if err := g.st.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	if g.wal != nil {
		// Drain the committed stream so Close cannot block on it.
		if c := g.wal.Committed(); c != nil && g.dispDone == nil {
			go func() {
				for range c {
				}
			}()
		}
		if err := g.wal.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	if g.dlq != nil {
		if err := g.dlq.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	if tbl := g.slots.Load(); tbl != nil {
		for _, s := range tbl {
			if s == nil {
				continue
			}
			if err := s.Sink().Close(ctx); err != nil {
				errs = append(errs, fmt.Errorf("close sink %s: %w", s.Name(), err))
			}
		}
	}
	if quiet {
		return nil
	}
	return errors.Join(errs...)
}
