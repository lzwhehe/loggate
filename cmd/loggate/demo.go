package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/lzwhehe/loggate/internal/config"
	"github.com/lzwhehe/loggate/internal/demo"
	"github.com/lzwhehe/loggate/internal/ui"
)

type demoFlags struct {
	on                        bool
	dir                       string
	keep                      bool
	eps                       float64
	ui, metrics, ingest, soar string
}

// runDemo runs `loggate --demo` until ctx is cancelled (Ctrl+C).
func runDemo(ctx context.Context, stop context.CancelFunc, f demoFlags, log *slog.Logger) (err error) {
	dir, temp := f.dir, false
	if dir == "" {
		if dir, err = os.MkdirTemp("", "loggate-demo-"); err != nil {
			return err
		}
		temp = true
	}
	if temp && !f.keep {
		defer func() {
			if rerr := os.RemoveAll(dir); rerr != nil {
				log.Warn("remove demo dir", "dir", dir, "err", rerr)
			}
		}()
	}
	r, err := demo.New(demo.Options{
		Dir: dir, EPS: f.eps, IngestAddr: f.ingest, ReceiverAddr: f.soar, Logger: log,
	})
	if err != nil {
		return err
	}
	if err := r.Start(ctx); err != nil {
		_ = r.Stop(context.Background())
		return err
	}
	aux, err := startMetrics(f.metrics, r.Metrics(), log)
	if err == nil {
		h := ui.Handler(ui.Options{Logger: log, Snapshot: r.Snapshot, Demo: r.Handler()})
		var s *http.Server
		if s, err = serve(f.ui, h, log, "dashboard"); err == nil {
			aux = append(aux, s)
		}
	}
	if err != nil {
		closeAll(aux)
		_ = r.Stop(context.Background())
		return err
	}
	fmt.Fprintf(os.Stderr, "\nLogGate demo (synthetic data only)\n  dashboard  http://%s/ui/\n  metrics    %s\n  ingest     http://%s/v1/events\n  data dir   %s\nPress Ctrl+C to stop.\n\n",
		f.ui, metricsURL(f.metrics), r.IngestAddr(), dir)

	<-ctx.Done()
	stop()
	log.Info("stopping demo")
	closeAll(aux)
	sctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	return r.Stop(sctx)
}

func metricsURL(addr string) string {
	if addr == config.Off {
		return "(off)"
	}
	return "http://" + addr + "/metrics"
}
