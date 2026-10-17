// Command loggate runs the LogGate security log gateway.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/lzwhehe/loggate/internal/config"
	"github.com/lzwhehe/loggate/internal/gateway"
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

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	g, err := gateway.New(ctx, cfg, gateway.Options{Logger: logger})
	if err != nil {
		return err
	}
	if err := g.Start(); err != nil {
		_ = g.Shutdown(context.Background())
		return err
	}

	<-ctx.Done()
	stop() // a second signal kills the process immediately
	logger.Info("shutting down", "timeout", *shutdownTimeout)
	sctx, cancel := context.WithTimeout(context.Background(), *shutdownTimeout)
	defer cancel()
	return g.Shutdown(sctx)
}
