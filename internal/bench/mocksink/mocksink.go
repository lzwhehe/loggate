// Package mocksink is the benchmark's independent receiver for webhook
// destinations (DESIGN §10.1). One Server hosts several named sinks, each
// on its own listener, plus a control API:
//
//	POST /fault    {"dest": "soar", "mode": "ok|down|slow", "delay_ms": 500,
//	                "error_code": 400, "error_rate": 0.1, "seed": 1}
//	               (dest "" = every sink)
//	GET  /stats    per-sink counters
//	POST /flush    flush journals to disk
//	POST /shutdown flush, close, and signal Done
//
// Fault modes:
//   - ok:   accept immediately (subject to error injection);
//   - slow: hold every request for delay_ms, then answer. The delay is not
//     cut short when the client gives up: like a real receiver, the event
//     is processed (journaled) anyway, which is what makes "timeout
//     shorter than the sink" produce duplicates;
//   - down: close the listener and every open connection, so clients see
//     connection refused/reset, not an HTTP error. Leaving "down" re-binds
//     the same address.
//
// Error injection (ok and slow modes): request number n (counted per sink
// since the fault spec was set) is answered with error_code when
// u(seed, n) < error_rate, u being a fixed hash to [0,1). The decision is a
// pure function of (seed, n), so a run is reproducible given the request
// order.
//
// Every event of every request is journaled (see package journal) with the
// HTTP status it was answered with.
package mocksink

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lzwhehe/loggate/internal/bench/journal"
)

// Fault is a fault specification.
type Fault struct {
	Dest      string  `json:"dest,omitempty"`
	Mode      string  `json:"mode"`
	DelayMS   int     `json:"delay_ms,omitempty"`
	ErrorCode int     `json:"error_code,omitempty"`
	ErrorRate float64 `json:"error_rate,omitempty"`
	Seed      int64   `json:"seed,omitempty"`
}

// Validate checks a fault spec.
func (f Fault) Validate() error {
	switch f.Mode {
	case "ok", "down", "slow":
	default:
		return fmt.Errorf("mocksink: mode must be ok|down|slow, got %q", f.Mode)
	}
	if f.DelayMS < 0 || f.ErrorRate < 0 || f.ErrorRate > 1 {
		return errors.New("mocksink: delay_ms >= 0 and error_rate in [0,1] required")
	}
	if f.ErrorRate > 0 && (f.ErrorCode < 400 || f.ErrorCode > 599) {
		return errors.New("mocksink: error_code must be 4xx/5xx when error_rate > 0")
	}
	return nil
}

// Uniform maps (seed, n) to [0,1) with SplitMix64; the error-injection
// decision for request n is Uniform(seed, n) < error_rate.
func Uniform(seed int64, n uint64) float64 {
	z := uint64(seed) + (n+1)*0x9E3779B97F4A7C15
	z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9
	z = (z ^ (z >> 27)) * 0x94D049BB133111EB
	z ^= z >> 31
	return float64(z>>11) / float64(1<<53)
}

// SinkStats are per-sink counters.
type SinkStats struct {
	Name        string `json:"name"`
	Addr        string `json:"addr"`
	Mode        string `json:"mode"`
	Up          bool   `json:"up"`
	Requests    uint64 `json:"requests"`
	Events      uint64 `json:"events"`
	Accepted    uint64 `json:"accepted_events"`
	Rejected    uint64 `json:"rejected_events"`
	BadRequests uint64 `json:"bad_requests"`
	InFlight    int64  `json:"in_flight"`
}

type sinkEP struct {
	name, addr string
	log        *slog.Logger
	jw         *journal.Writer

	mu    sync.Mutex
	fault Fault
	reqN  uint64 // requests since the fault spec was set
	srv   *http.Server
	ln    net.Listener

	requests, events, accepted, rejected, bad atomic.Uint64
	inflight                                  atomic.Int64
}

// Config configures a Server.
type Config struct {
	Sinks      map[string]string // name -> listen address
	JournalDir string
	Logger     *slog.Logger
	// FlushInterval flushes journals periodically (default 500ms).
	FlushInterval time.Duration
}

// Server hosts the sinks.
type Server struct {
	sinks map[string]*sinkEP
	log   *slog.Logger
	done  chan struct{}
	once  sync.Once
	stop  chan struct{}
	wg    sync.WaitGroup
}

// Start opens every sink in mode ok.
func Start(cfg Config) (*Server, error) {
	if cfg.Logger == nil {
		cfg.Logger = slog.New(slog.DiscardHandler)
	}
	if cfg.FlushInterval <= 0 {
		cfg.FlushInterval = 500 * time.Millisecond
	}
	if err := os.MkdirAll(cfg.JournalDir, 0o755); err != nil {
		return nil, err
	}
	s := &Server{sinks: map[string]*sinkEP{}, log: cfg.Logger, done: make(chan struct{}), stop: make(chan struct{})}
	for name, addr := range cfg.Sinks {
		jw, err := journal.Create(filepath.Join(cfg.JournalDir, name+".csv"))
		if err != nil {
			s.Close()
			return nil, err
		}
		ep := &sinkEP{name: name, addr: addr, log: cfg.Logger.With("sink", name), jw: jw, fault: Fault{Mode: "ok"}}
		s.sinks[name] = ep
		if err := ep.up(); err != nil {
			s.Close()
			return nil, err
		}
	}
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		t := time.NewTicker(cfg.FlushInterval)
		defer t.Stop()
		for {
			select {
			case <-s.stop:
				return
			case <-t.C:
				_ = s.Flush()
			}
		}
	}()
	return s, nil
}

// Addr returns a sink's listen address (resolved, useful with port 0).
func (s *Server) Addr(name string) string {
	ep := s.sinks[name]
	if ep == nil {
		return ""
	}
	ep.mu.Lock()
	defer ep.mu.Unlock()
	return ep.addr
}

// up (re)binds the sink's address. Re-binding right after a close can
// briefly fail on some platforms, so it retries for a few seconds.
func (ep *sinkEP) up() error {
	var ln net.Listener
	var err error
	for i := 0; i < 100; i++ {
		ln, err = net.Listen("tcp", ep.addr)
		if err == nil {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if err != nil {
		return fmt.Errorf("mocksink %s: listen %s: %w", ep.name, ep.addr, err)
	}
	ep.ln = ln
	ep.addr = ln.Addr().String()
	ep.srv = &http.Server{Handler: http.HandlerFunc(ep.handle), ReadHeaderTimeout: 10 * time.Second}
	go func(srv *http.Server, ln net.Listener) {
		if err := srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			ep.log.Warn("serve", "err", err)
		}
	}(ep.srv, ln)
	return nil
}

// down closes the listener and all connections (clients get refused/reset).
func (ep *sinkEP) down() {
	if ep.srv != nil {
		_ = ep.srv.Close()
		ep.srv, ep.ln = nil, nil
	}
}

func (ep *sinkEP) setFault(f Fault) error {
	ep.mu.Lock()
	defer ep.mu.Unlock()
	if f.ErrorRate > 0 && f.ErrorCode == 0 {
		f.ErrorCode = http.StatusInternalServerError
	}
	switch {
	case f.Mode == "down" && ep.srv != nil:
		ep.down()
	case f.Mode != "down" && ep.srv == nil:
		if err := ep.up(); err != nil {
			return err
		}
	}
	f.Dest = ep.name
	ep.fault, ep.reqN = f, 0
	ep.log.Info("fault set", "mode", f.Mode, "delay_ms", f.DelayMS, "error_code", f.ErrorCode, "error_rate", f.ErrorRate, "seed", f.Seed)
	return nil
}

// wireEvent is the subset of a gateway LogEvent the mock reads.
type wireEvent struct {
	Seq     uint64 `json:"seq"`
	IdemKey string `json:"idem_key"`
	Fields  struct {
		Unmapped struct {
			RunID       string `json:"run_id"`
			TestSeq     uint64 `json:"test_seq"`
			ScheduledNS int64  `json:"scheduled_send_ns"`
		} `json:"unmapped"`
	} `json:"fields"`
}

// ParseBody decodes a webhook body: a JSON array of events or one event.
func ParseBody(body []byte) ([]journal.Record, error) {
	body = bytes.TrimSpace(body)
	var evs []wireEvent
	switch {
	case len(body) > 0 && body[0] == '[':
		if err := json.Unmarshal(body, &evs); err != nil {
			return nil, err
		}
	case len(body) > 0 && body[0] == '{':
		var ev wireEvent
		if err := json.Unmarshal(body, &ev); err != nil {
			return nil, err
		}
		evs = []wireEvent{ev}
	default:
		return nil, errors.New("body is not JSON")
	}
	out := make([]journal.Record, len(evs))
	for i, ev := range evs {
		u := ev.Fields.Unmapped
		out[i] = journal.Record{TestSeq: u.TestSeq, IdemKey: ev.IdemKey, ScheduledNS: u.ScheduledNS, GWSeq: ev.Seq, RunID: u.RunID}
	}
	return out, nil
}

func (ep *sinkEP) handle(w http.ResponseWriter, r *http.Request) {
	ep.inflight.Add(1)
	defer ep.inflight.Add(-1)
	body, err := io.ReadAll(io.LimitReader(r.Body, 256<<20))
	if err != nil {
		ep.bad.Add(1)
		return // connection broke; nothing was received
	}
	recs, err := ParseBody(body)
	if err != nil {
		ep.bad.Add(1)
		http.Error(w, "bad body", http.StatusBadRequest)
		return
	}
	ep.mu.Lock()
	f := ep.fault
	n := ep.reqN
	ep.reqN++
	ep.mu.Unlock()
	ep.requests.Add(1)

	if f.Mode == "slow" && f.DelayMS > 0 {
		time.Sleep(time.Duration(f.DelayMS) * time.Millisecond)
	}
	status := http.StatusOK
	if f.ErrorRate > 0 && Uniform(f.Seed, n) < f.ErrorRate {
		status = f.ErrorCode
	}
	now := time.Now().UnixNano()
	for i := range recs {
		recs[i].Dest, recs[i].RecvNS, recs[i].Status = ep.name, now, status
	}
	if err := ep.jw.Append(recs); err != nil {
		ep.log.Error("journal append", "err", err)
		status = http.StatusInternalServerError
	}
	ep.events.Add(uint64(len(recs)))
	if status == http.StatusOK {
		ep.accepted.Add(uint64(len(recs)))
	} else {
		ep.rejected.Add(uint64(len(recs)))
	}
	w.WriteHeader(status)
}

// SetFault applies f to f.Dest (or every sink when empty).
func (s *Server) SetFault(f Fault) error {
	if err := f.Validate(); err != nil {
		return err
	}
	if f.Dest != "" {
		ep := s.sinks[f.Dest]
		if ep == nil {
			return fmt.Errorf("mocksink: unknown sink %q", f.Dest)
		}
		return ep.setFault(f)
	}
	var errs []error
	for _, ep := range s.sinks {
		errs = append(errs, ep.setFault(f))
	}
	return errors.Join(errs...)
}

// Stats returns per-sink counters sorted by name.
func (s *Server) Stats() []SinkStats {
	var out []SinkStats
	for _, ep := range s.sinks {
		ep.mu.Lock()
		st := SinkStats{Name: ep.name, Addr: ep.addr, Mode: ep.fault.Mode, Up: ep.srv != nil}
		ep.mu.Unlock()
		st.Requests, st.Events = ep.requests.Load(), ep.events.Load()
		st.Accepted, st.Rejected, st.BadRequests = ep.accepted.Load(), ep.rejected.Load(), ep.bad.Load()
		st.InFlight = ep.inflight.Load()
		out = append(out, st)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

// Flush flushes every journal.
func (s *Server) Flush() error {
	var errs []error
	for _, ep := range s.sinks {
		errs = append(errs, ep.jw.Flush())
	}
	return errors.Join(errs...)
}

// Done is closed after Close (or POST /shutdown).
func (s *Server) Done() <-chan struct{} { return s.done }

// Close stops every sink (waiting briefly for in-flight requests so their
// journal lines are written) and closes the journals.
func (s *Server) Close() error {
	var errs []error
	s.once.Do(func() {
		close(s.stop)
		s.wg.Wait()
		for _, ep := range s.sinks {
			ep.mu.Lock()
			if ep.srv != nil {
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				if err := ep.srv.Shutdown(ctx); err != nil {
					_ = ep.srv.Close()
				}
				cancel()
				ep.srv = nil
			}
			ep.mu.Unlock()
			// Requests still sleeping in slow mode on closed connections.
			for deadline := time.Now().Add(15 * time.Second); ep.inflight.Load() > 0 && time.Now().Before(deadline); {
				time.Sleep(10 * time.Millisecond)
			}
			errs = append(errs, ep.jw.Close())
		}
		close(s.done)
	})
	return errors.Join(errs...)
}

// ControlHandler serves the control API.
func (s *Server) ControlHandler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /fault", func(w http.ResponseWriter, r *http.Request) {
		var f Fault
		if err := json.NewDecoder(io.LimitReader(r.Body, 1<<20)).Decode(&f); err != nil {
			http.Error(w, "bad json", http.StatusBadRequest)
			return
		}
		if err := s.SetFault(f); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		writeJSON(w, s.Stats())
	})
	mux.HandleFunc("GET /stats", func(w http.ResponseWriter, _ *http.Request) { writeJSON(w, s.Stats()) })
	mux.HandleFunc("POST /flush", func(w http.ResponseWriter, _ *http.Request) {
		if err := s.Flush(); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		writeJSON(w, s.Stats())
	})
	mux.HandleFunc("POST /shutdown", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, s.Stats())
		go func() { _ = s.Close() }()
	})
	return mux
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}
