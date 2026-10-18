// Command loggate runs the LogGate security log gateway.
//
//	loggate -config configs/example.yaml
//	loggate --demo        in-process demo: mock receivers, synthetic load,
//	                      dashboard on http://127.0.0.1:8081/ui/
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/lzwhehe/loggate/internal/config"
	"github.com/lzwhehe/loggate/internal/gateway"
	"github.com/lzwhehe/loggate/internal/metrics"
	"github.com/lzwhehe/loggate/internal/ui"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "loggate:", err)
		os.Exit(1)
	}
}

func run() error {
	cfgPath := flag.String("config", "configs/example.yaml", "path to the YAML config")
	shutdownTimeout := flag.Duration("shutdown-timeout", 30*time.Second, "max time to finish delivery on SIGINT/SIGTERM")
	var d demoFlags
	flag.BoolVar(&d.on, "demo", false, "run the self-contained demo (synthetic data, mock receivers, dashboard); ignores -config")
	flag.StringVar(&d.dir, "demo-dir", "", "demo working directory (default: a new temp dir, removed on exit unless -demo-keep)")
	flag.BoolVar(&d.keep, "demo-keep", false, "keep the demo directory on exit")
	flag.Float64Var(&d.eps, "demo-eps", 50, "demo synthetic load (events per second)")
	flag.StringVar(&d.ui, "demo-ui", "127.0.0.1:8081", "demo dashboard listen address")
	flag.StringVar(&d.metrics, "demo-metrics", "127.0.0.1:9091", `demo Prometheus listen address ("off" disables)`)
	flag.StringVar(&d.ingest, "demo-ingest", "127.0.0.1:8080", "demo gateway ingest listen address")
	flag.StringVar(&d.soar, "demo-soar", "127.0.0.1:9100", "demo mock SOAR receiver listen address")
	flag.Parse()

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))

	// SIGTERM is not delivered on Windows; there the admin API
	// (POST /v1/admin/shutdown) triggers the same graceful path.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if d.on {
		return runDemo(ctx, stop, d, logger)
	}
	cfg, err := config.Load(*cfgPath)
	if err != nil {
		return err
	}

	m := metrics.New()
	g, err := gateway.New(ctx, cfg, gateway.Options{Logger: logger, Metrics: m})
	if err != nil {
		return err
	}
	if err := g.Start(); err != nil {
		_ = g.Shutdown(context.Background())
		return err
	}
	m.SetSource(g.MetricsSnapshot)
	aux, err := startMetrics(cfg.Admin.Metrics, m, logger)
	if err != nil {
		_ = g.Shutdown(context.Background())
		return err
	}
	if cfg.Admin.UI != config.Off {
		types := map[string]string{}
		for _, dst := range cfg.Destinations {
			types[dst.Name] = dst.Type
		}
		h := ui.Handler(ui.Options{Logger: logger, Snapshot: func() ui.Snapshot {
			return ui.FromGateway(g, m, types, time.Now())
		}})
		s, err := serve(cfg.Admin.UI, h, logger, "dashboard")
		if err != nil {
			closeAll(aux)
			_ = g.Shutdown(context.Background())
			return err
		}
		aux = append(aux, s)
	}

	select {
	case <-ctx.Done():
	case <-g.ShutdownRequested(): // POST /v1/admin/shutdown (Windows has no SIGTERM)
	}
	stop() // a second signal kills the process immediately
	logger.Info("shutting down", "timeout", *shutdownTimeout)
	sctx, cancel := context.WithTimeout(context.Background(), *shutdownTimeout)
	defer cancel()
	err = g.Shutdown(sctx)
	closeAll(aux)
	return err
}

// startMetrics serves GET /metrics on addr ("off" disables it).
func startMetrics(addr string, m *metrics.Metrics, log *slog.Logger) ([]*http.Server, error) {
	if addr == config.Off {
		return nil, nil
	}
	mux := http.NewServeMux()
	mux.Handle("GET /metrics", m.Handler())
	s, err := serve(addr, mux, log, "metrics")
	if err != nil {
		return nil, err
	}
	return []*http.Server{s}, nil
}

// serve listens on addr and serves h in the background.
func serve(addr string, h http.Handler, log *slog.Logger, what string) (*http.Server, error) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("%s listen %s: %w", what, addr, err)
	}
	s := &http.Server{Handler: h, ReadHeaderTimeout: 10 * time.Second, ErrorLog: slog.NewLogLogger(log.Handler(), slog.LevelWarn)}
	go func() {
		if err := s.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error(what+" server stopped", "err", err)
		}
	}()
	log.Info(what+" listening", "addr", ln.Addr().String())
	return s, nil
}

// closeAll closes auxiliary listeners immediately (dashboard SSE streams
// would otherwise hold a graceful Shutdown open).
func closeAll(ss []*http.Server) {
	for _, s := range ss {
		_ = s.Close()
	}
}
