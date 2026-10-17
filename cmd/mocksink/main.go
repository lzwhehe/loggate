// Command mocksink hosts one or more mock webhook receivers for the
// fault-injection benchmark, each on its own address, with a control API
// for fault modes and a CSV journal per sink. See internal/bench/mocksink.
//
//	mocksink -control 127.0.0.1:9199 -journal ./mock \
//	         -sink soar=127.0.0.1:9100 -sink probe=127.0.0.1:9101
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
	"strings"
	"syscall"
	"time"

	"github.com/lzwhehe/loggate/internal/bench/mocksink"
)

type sinkFlags map[string]string

func (s sinkFlags) String() string { return fmt.Sprint(map[string]string(s)) }
func (s sinkFlags) Set(v string) error {
	name, addr, ok := strings.Cut(v, "=")
	if !ok || name == "" || addr == "" {
		return errors.New("want name=host:port")
	}
	if _, dup := s[name]; dup {
		return fmt.Errorf("duplicate sink %q", name)
	}
	s[name] = addr
	return nil
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "mocksink:", err)
		os.Exit(1)
	}
}

func run() error {
	sinks := sinkFlags{}
	control := flag.String("control", "127.0.0.1:9199", "control API listen address")
	jdir := flag.String("journal", "./mock", "journal directory (<sink>.csv)")
	flag.Var(sinks, "sink", "name=host:port of a mock sink (repeatable)")
	flag.Parse()
	if len(sinks) == 0 {
		return errors.New("at least one -sink is required")
	}
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	srv, err := mocksink.Start(mocksink.Config{Sinks: sinks, JournalDir: *jdir, Logger: log})
	if err != nil {
		return err
	}
	ln, err := net.Listen("tcp", *control)
	if err != nil {
		_ = srv.Close()
		return err
	}
	hs := &http.Server{Handler: srv.ControlHandler(), ReadHeaderTimeout: 10 * time.Second}
	go func() { _ = hs.Serve(ln) }()
	for name := range sinks {
		log.Info("mock sink up", "sink", name, "addr", srv.Addr(name))
	}
	log.Info("control API up", "addr", ln.Addr().String())

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	select {
	case <-ctx.Done():
		_ = srv.Close()
	case <-srv.Done():
	}
	sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = hs.Shutdown(sctx)
	for _, st := range srv.Stats() {
		log.Info("final", "sink", st.Name, "requests", st.Requests, "accepted", st.Accepted, "rejected", st.Rejected)
	}
	return nil
}
