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
	flag.Parse()

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	cfg, err := config.Load(*cfgPath)
	if err != nil {
		return err
	}

	// SIGTERM is not delivered on Windows; there the admin API
	// (POST /v1/admin/shutdown) triggers the same graceful path.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

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

func closeAll(ss []*http.Server) {
	for _, s := range ss {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		_ = s.Shutdown(ctx)
		cancel()
	}
}
