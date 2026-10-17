package faultbench

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"runtime"
	"sync"
	"syscall"
	"time"
)

// proc is a child process with its output appended to a log file.
type proc struct {
	name string
	cmd  *exec.Cmd
	done chan struct{}
	err  error
}

func startProc(name, bin string, args, env []string, logPath string) (*proc, error) {
	lf, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	cmd := exec.Command(bin, args...)
	cmd.Env = append(os.Environ(), env...)
	cmd.Stdout, cmd.Stderr = lf, lf
	if err := cmd.Start(); err != nil {
		lf.Close()
		return nil, fmt.Errorf("start %s: %w", name, err)
	}
	p := &proc{name: name, cmd: cmd, done: make(chan struct{})}
	go func() {
		p.err = cmd.Wait()
		lf.Close()
		close(p.done)
	}()
	return p, nil
}

func (p *proc) exited() bool {
	select {
	case <-p.done:
		return true
	default:
		return false
	}
}

// kill hard-kills the process: SIGKILL on Unix, TerminateProcess on Windows.
func (p *proc) kill() error {
	if p.exited() {
		return nil
	}
	err := p.cmd.Process.Kill()
	select {
	case <-p.done:
	case <-time.After(30 * time.Second):
		return fmt.Errorf("%s did not exit after kill", p.name)
	}
	if err != nil && !errors.Is(err, os.ErrProcessDone) {
		return err
	}
	return nil
}

func (p *proc) wait(d time.Duration) bool {
	select {
	case <-p.done:
		return true
	case <-time.After(d):
		return false
	}
}

// gateway manages the loggate process across kills and restarts.
type gateway struct {
	mu         sync.Mutex
	bin, cfg   string
	addr       string
	adminToken string
	env        []string
	logPath    string
	p          *proc
	starts     int
}

func (g *gateway) baseURL() string { return "http://" + g.addr }

func (g *gateway) start(ctx context.Context) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.p != nil && !g.p.exited() {
		return errors.New("gateway already running")
	}
	p, err := startProc("loggate", g.bin, []string{"-config", g.cfg, "-shutdown-timeout", "60s"}, g.env, g.logPath)
	if err != nil {
		return err
	}
	g.p = p
	g.starts++
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		if p.exited() {
			return fmt.Errorf("gateway exited during startup: %v (see %s)", p.err, g.logPath)
		}
		if httpStatus(ctx, http.MethodGet, g.baseURL()+"/healthz", "", nil) == http.StatusOK {
			return nil
		}
		time.Sleep(20 * time.Millisecond)
	}
	return errors.New("gateway did not become healthy within 60s")
}

func (g *gateway) kill() error {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.p == nil {
		return nil
	}
	return g.p.kill()
}

// stop shuts the gateway down gracefully: SIGTERM on Unix, the admin API on
// Windows (no SIGTERM there). It returns an error (and kills the process)
// if the graceful path does not finish within timeout.
func (g *gateway) stop(ctx context.Context, timeout time.Duration) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.p == nil || g.p.exited() {
		return nil
	}
	if runtime.GOOS == "windows" {
		if c := httpStatus(ctx, http.MethodPost, g.baseURL()+"/v1/admin/shutdown", g.adminToken, nil); c != http.StatusAccepted {
			return fmt.Errorf("admin shutdown: status %d", c)
		}
	} else if err := g.p.cmd.Process.Signal(syscall.SIGTERM); err != nil {
		return fmt.Errorf("sigterm: %w", err)
	}
	if !g.p.wait(timeout) {
		_ = g.p.kill()
		return fmt.Errorf("graceful stop did not finish in %v; killed", timeout)
	}
	if g.p.err != nil {
		return fmt.Errorf("gateway exit: %w", g.p.err)
	}
	return nil
}

func (g *gateway) running() bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.p != nil && !g.p.exited()
}

func (g *gateway) admin(ctx context.Context, path string, body any) error {
	b, _ := json.Marshal(body)
	if c := httpStatus(ctx, http.MethodPost, g.baseURL()+path, g.adminToken, b); c != http.StatusOK {
		return fmt.Errorf("admin %s: status %d", path, c)
	}
	return nil
}

// gwStats is the subset of /v1/stats used to detect drain.
type gwStats struct {
	NextSeq      uint64 `json:"next_seq"`
	Cursor       uint64 `json:"dispatch_cursor"`
	L1Records    int    `json:"l1_records"`
	DLQTotal     uint64 `json:"dlq_total"`
	Destinations []struct {
		Name       string `json:"name"`
		State      string `json:"state"`
		L2Depth    int    `json:"l2_depth"`
		InFlight   int64  `json:"in_flight"`
		L1Pending  int64  `json:"l1_pending"`
		Unresolved int    `json:"unresolved"`
	} `json:"destinations"`
}

func (s gwStats) drained() bool {
	if s.Cursor+1 < s.NextSeq || s.L1Records != 0 {
		return false
	}
	for _, d := range s.Destinations {
		if d.L2Depth != 0 || d.InFlight != 0 || d.L1Pending != 0 || d.Unresolved != 0 {
			return false
		}
	}
	return true
}

func (g *gateway) stats(ctx context.Context) (gwStats, json.RawMessage, error) {
	var s gwStats
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, g.baseURL()+"/v1/stats", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return s, nil, err
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return s, nil, err
	}
	return s, raw, json.Unmarshal(raw, &s)
}

func httpStatus(ctx context.Context, method, url, token string, body []byte) int {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, method, url, bytes.NewReader(body))
	if err != nil {
		return 0
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	return resp.StatusCode
}

// freePorts reserves n distinct loopback ports (closed again before use;
// a small race with other processes is accepted).
func freePorts(n int) ([]int, error) {
	var lns []net.Listener
	defer func() {
		for _, l := range lns {
			l.Close()
		}
	}()
	var out []int
	for range n {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			return nil, err
		}
		lns = append(lns, l)
		out = append(out, l.Addr().(*net.TCPAddr).Port)
	}
	return out, nil
}
