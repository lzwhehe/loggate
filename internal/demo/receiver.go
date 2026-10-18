package demo

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"sync"
	"time"
)

// Fault modes of the mock receiver (same semantics as cmd/mocksink).
const (
	ModeOK   = "ok"
	ModeDown = "down" // listener closed: the gateway sees connection refused
	ModeSlow = "slow" // every request is delayed, then accepted
)

// Receiver is the in-process mock webhook receiver standing in for a SOAR
// platform. It records every accepted copy per gateway seq (and idem key)
// so the reconciliation panel can count lost and duplicated events
// independently of the gateway's own statistics.
type Receiver struct {
	name string
	addr string // fixed after the first listen (port 0 resolved)
	log  *slog.Logger

	mu      sync.Mutex
	srv     *http.Server
	mode    string
	delay   time.Duration
	copies  map[uint64]int
	keys    map[string]struct{}
	total   uint64
	reqs    uint64
	badReqs uint64
	stopped bool
}

// NewReceiver listens on addr ("127.0.0.1:0" picks a port).
func NewReceiver(name, addr string, log *slog.Logger) (*Receiver, error) {
	r := &Receiver{name: name, addr: addr, log: log, mode: ModeOK, copies: map[uint64]int{}, keys: map[string]struct{}{}}
	if err := r.up(); err != nil {
		return nil, err
	}
	return r, nil
}

// URL is the ingest URL to configure on the webhook destination.
func (r *Receiver) URL() string { return "http://" + r.addr + "/ingest" }

// up (re)listens on the receiver address. r.mu must not be held.
func (r *Receiver) up() error {
	var ln net.Listener
	var err error
	// After "down" the port may need a moment to be reusable.
	for i := 0; i < 50; i++ {
		if ln, err = net.Listen("tcp", r.addr); err == nil {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if err != nil {
		return fmt.Errorf("demo receiver %s: listen %s: %w", r.name, r.addr, err)
	}
	r.addr = ln.Addr().String()
	srv := &http.Server{Handler: http.HandlerFunc(r.handle), ReadHeaderTimeout: 5 * time.Second}
	r.srv = srv
	go func() {
		if err := srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			r.log.Warn("demo receiver stopped", "dest", r.name, "err", err)
		}
	}()
	return nil
}

type wireEvent struct {
	Seq     uint64 `json:"seq"`
	IdemKey string `json:"idem_key"`
}

func (r *Receiver) handle(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodPost || req.URL.Path != "/ingest" {
		http.NotFound(w, req)
		return
	}
	body, err := io.ReadAll(io.LimitReader(req.Body, 32<<20))
	r.mu.Lock()
	delay := time.Duration(0)
	if r.mode == ModeSlow {
		delay = r.delay
	}
	r.reqs++
	r.mu.Unlock()
	if delay > 0 {
		time.Sleep(delay) // like a slow receiver: client timeouts do not abort processing
	}
	var evs []wireEvent
	if err == nil {
		body = bytes.TrimSpace(body)
		if len(body) > 0 && body[0] == '{' {
			var one wireEvent
			err = json.Unmarshal(body, &one)
			evs = []wireEvent{one}
		} else {
			err = json.Unmarshal(body, &evs)
		}
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if err != nil {
		r.badReqs++
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	for _, e := range evs {
		r.copies[e.Seq]++
		r.keys[e.IdemKey] = struct{}{}
		r.total++
	}
	w.WriteHeader(http.StatusOK)
}

// SetFault switches the mode. delay applies to ModeSlow.
func (r *Receiver) SetFault(mode string, delay time.Duration) error {
	switch mode {
	case ModeOK, ModeDown, ModeSlow:
	default:
		return fmt.Errorf("mode must be ok, down or slow, got %q", mode)
	}
	if mode == ModeSlow && (delay <= 0 || delay > 30*time.Second) {
		return errors.New("slow needs 0 < delay_ms <= 30000")
	}
	r.mu.Lock()
	if r.stopped {
		r.mu.Unlock()
		return errors.New("receiver stopped")
	}
	prev := r.mode
	r.mode, r.delay = mode, delay
	srv := r.srv
	r.mu.Unlock()
	switch {
	case mode == ModeDown && prev != ModeDown:
		_ = srv.Close() // closes the listener and all connections
	case mode != ModeDown && prev == ModeDown:
		if err := r.up(); err != nil {
			r.mu.Lock()
			r.mode = ModeDown
			r.mu.Unlock()
			return err
		}
	}
	return nil
}

// Fault returns the current mode and delay.
func (r *Receiver) Fault() (string, time.Duration) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.mode, r.delay
}

// Copies returns a copy of seq -> accepted copies, and the number of
// distinct idem keys.
func (r *Receiver) Copies() (map[uint64]int, int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make(map[uint64]int, len(r.copies))
	for k, v := range r.copies {
		out[k] = v
	}
	return out, len(r.keys)
}

// Close stops the receiver.
func (r *Receiver) Close() error {
	r.mu.Lock()
	r.stopped = true
	srv, down := r.srv, r.mode == ModeDown
	r.mu.Unlock()
	if down || srv == nil {
		return nil
	}
	return srv.Close()
}
