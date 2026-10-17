// Package gateway wires WAL, ingest, routing, dispatch, and sinks into a
// running LogGate instance. cmd/loggate and end-to-end tests both use it.
package gateway

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/lzwhehe/loggate/internal/clock"
	"github.com/lzwhehe/loggate/internal/config"
	"github.com/lzwhehe/loggate/internal/dispatch"
	"github.com/lzwhehe/loggate/internal/event"
	httpingest "github.com/lzwhehe/loggate/internal/ingest/http"
	"github.com/lzwhehe/loggate/internal/route"
	"github.com/lzwhehe/loggate/internal/sink"
	filesink "github.com/lzwhehe/loggate/internal/sink/file"
	"github.com/lzwhehe/loggate/internal/wal"
)

// Options carries dependencies that are not part of the YAML config.
type Options struct {
	Logger *slog.Logger
	Clock  clock.Clock
	// WALFS overrides the WAL filesystem (fault injection in tests).
	WALFS wal.FS
	// IdemKey overrides the HMAC key otherwise read from the environment.
	IdemKey []byte
}

// Gateway is one running LogGate instance.
type Gateway struct {
	cfg   *config.Config
	log   *slog.Logger
	wal   *wal.WAL
	table *route.Table
	sinks map[event.DestID]sink.Sink
	disp  *dispatch.Direct
	http  *httpingest.Handler
	srv   *http.Server
	ln    net.Listener

	dispCancel context.CancelFunc
	dispDone   chan struct{}
	shutdown   sync.Once
	shutErr    error
}

// BuiltinSinks returns the registry of compiled-in sink types.
func BuiltinSinks() *sink.Registry {
	r := sink.NewRegistry()
	_ = r.Register(filesink.Type, filesink.New)
	return r
}

// New opens the WAL (running recovery), initialises sinks, and prepares the
// HTTP handler. Nothing listens until Start.
func New(ctx context.Context, cfg *config.Config, opts Options) (g *Gateway, err error) {
	if opts.Logger == nil {
		opts.Logger = slog.New(slog.DiscardHandler)
	}
	if opts.Clock == nil {
		opts.Clock = clock.Real()
	}
	g = &Gateway{cfg: cfg, log: opts.Logger, sinks: map[event.DestID]sink.Sink{}}
	defer func() {
		if err != nil {
			g.closeResources(context.Background())
		}
	}()

	key, err := idemKey(cfg, opts)
	if err != nil {
		return nil, err
	}
	bootEpoch := uint64(opts.Clock.Now().UnixNano())
	keyer, err := event.NewIdemKeyer(key, cfg.NodeID, bootEpoch)
	if err != nil {
		return nil, fmt.Errorf("gateway: %w", err)
	}

	g.table, err = route.New(cfg.Destinations, cfg.Routes)
	if err != nil {
		return nil, fmt.Errorf("gateway: %w", err)
	}
	reg := BuiltinSinks()
	for _, d := range cfg.Destinations {
		id, _ := g.table.ID(d.Name)
		s, err := reg.New(ctx, d.Type, d.Config)
		if err != nil {
			return nil, fmt.Errorf("gateway: destination %s: %w", d.Name, err)
		}
		g.sinks[id] = s
	}

	g.wal, err = wal.Open(wal.Options{
		Dir:            filepath.Join(cfg.DataDir, "wal"),
		SegmentBytes:   cfg.WAL.SegmentBytes.Int64(),
		FsyncInterval:  cfg.WAL.FsyncInterval,
		FsyncMaxEvents: cfg.WAL.FsyncMaxEvents,
		EmitCommitted:  true,
		FS:             opts.WALFS,
		Clock:          opts.Clock,
		Logger:         opts.Logger,
	})
	if err != nil {
		return nil, fmt.Errorf("gateway: %w", err)
	}

	g.disp = dispatch.NewDirect(g.sinks, dispatch.Options{
		Clock: opts.Clock, Logger: opts.Logger, DestNames: g.table.Name,
	})

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
	})
	if err != nil {
		return nil, fmt.Errorf("gateway: %w", err)
	}
	return g, nil
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

// Start begins delivering committed events and serving HTTP.
func (g *Gateway) Start() error {
	ln, err := net.Listen("tcp", g.cfg.Ingest.HTTP.Listen)
	if err != nil {
		return fmt.Errorf("gateway: listen %s: %w", g.cfg.Ingest.HTTP.Listen, err)
	}
	g.ln = ln
	dctx, cancel := context.WithCancel(context.Background())
	g.dispCancel = cancel
	g.dispDone = make(chan struct{})
	go func() {
		defer close(g.dispDone)
		g.disp.Run(dctx, g.wal.Committed())
	}()
	g.srv = &http.Server{
		Handler:           g.http,
		ReadHeaderTimeout: 10 * time.Second,
		ErrorLog:          slog.NewLogLogger(g.log.Handler(), slog.LevelWarn),
	}
	go func() {
		if err := g.srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			g.log.Error("http server stopped", "err", err)
		}
	}()
	g.log.Info("loggate started", "http", ln.Addr().String(), "data_dir", g.cfg.DataDir, "next_seq", g.wal.NextSeq())
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
func (g *Gateway) Delivered() uint64 { return g.disp.Delivered() }

// Shutdown stops the gateway (DESIGN §4.10): stop ingest, flush the WAL,
// finish delivering committed events, close sinks. If ctx expires before
// delivery finishes, remaining deliveries are abandoned (they stay in the WAL).
func (g *Gateway) Shutdown(ctx context.Context) error {
	g.shutdown.Do(func() {
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
			select {
			case <-g.dispDone:
			case <-ctx.Done():
				g.dispCancel()
				<-g.dispDone
				errs = append(errs, fmt.Errorf("delivery incomplete at shutdown deadline (%d abandoned)", g.disp.Dropped()))
			}
			g.dispCancel()
		}
		g.wal = nil
		if err := g.closeResources(ctx); err != nil {
			errs = append(errs, err)
		}
		g.shutErr = errors.Join(errs...)
		g.log.Info("loggate stopped", "delivered", g.disp.Delivered(), "err", g.shutErr)
	})
	return g.shutErr
}

// closeResources closes the WAL (if still open) and sinks.
func (g *Gateway) closeResources(ctx context.Context) error {
	var errs []error
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
		g.wal = nil
	}
	for id, s := range g.sinks {
		if err := s.Close(ctx); err != nil {
			errs = append(errs, fmt.Errorf("close sink %s: %w", g.table.Name(id), err))
		}
	}
	return errors.Join(errs...)
}
