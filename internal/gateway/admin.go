package gateway

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"io"
	"io/fs"
	"net/http"
	"path/filepath"
	"strings"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/lzwhehe/loggate/internal/config"
)

// Admin API (token-protected, disabled unless admin.token/token_env is set):
//
//	POST /v1/admin/shutdown     graceful shutdown (for platforms without SIGTERM)
//	POST /v1/admin/unregister   {"name": "soar"}
//	POST /v1/admin/register     {"name": "soar"} re-registers the destination as
//	                            it was configured at startup; a full destination
//	                            spec ({"name","type","config",...}) registers a
//	                            new one.
//
// These exist for the fault-injection benchmark and for operators; the
// gRPC admin of DESIGN §8 is out of v1 scope.

// ShutdownRequested is closed when POST /v1/admin/shutdown was accepted.
func (g *Gateway) ShutdownRequested() <-chan struct{} { return g.shutdownReq }

func (g *Gateway) requestShutdown() { g.shutdownOnce.Do(func() { close(g.shutdownReq) }) }

func (g *Gateway) handler() http.Handler {
	if g.adminDigest == nil {
		return g.http
	}
	mux := http.NewServeMux()
	mux.Handle("/", g.http)
	mux.HandleFunc("POST /v1/admin/shutdown", g.admin(func(w http.ResponseWriter, _ *http.Request) {
		g.log.Info("graceful shutdown requested via admin API")
		writeJSON(w, http.StatusAccepted, map[string]string{"status": "shutting down"})
		g.requestShutdown()
	}))
	mux.HandleFunc("POST /v1/admin/unregister", g.admin(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Name string `json:"name"`
		}
		if err := json.NewDecoder(io.LimitReader(r.Body, 64<<10)).Decode(&req); err != nil || req.Name == "" {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "want {\"name\": ...}"})
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
		defer cancel()
		if err := g.Unregister(ctx, req.Name); err != nil {
			writeJSON(w, http.StatusConflict, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"status": "unregistered", "name": req.Name})
	}))
	mux.HandleFunc("POST /v1/admin/register", g.admin(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
		if err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "read body"})
			return
		}
		// JSON is valid YAML, and YAML decoding understands the config
		// types (durations, byte sizes).
		var d config.Destination
		dec := yaml.NewDecoder(strings.NewReader(string(body)))
		dec.KnownFields(true)
		if err := dec.Decode(&d); err != nil || d.Name == "" {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "want a destination spec with at least a name"})
			return
		}
		if d.Type == "" {
			orig, ok := g.origDests[d.Name]
			if !ok {
				writeJSON(w, http.StatusNotFound, map[string]string{"error": "destination not in startup config; send a full spec"})
				return
			}
			d = orig
		}
		if err := g.Register(r.Context(), d); err != nil {
			writeJSON(w, http.StatusConflict, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"status": "registered", "name": d.Name})
	}))
	return mux
}

// admin wraps h with bearer-token authentication (constant-time compare of
// SHA-256 digests).
func (g *Gateway) admin(h http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		auth := r.Header.Get("Authorization")
		const prefix = "bearer "
		ok := len(auth) > len(prefix) && strings.EqualFold(auth[:len(prefix)], prefix)
		if ok {
			d := sha256.Sum256([]byte(strings.TrimSpace(auth[len(prefix):])))
			ok = subtle.ConstantTimeCompare(d[:], g.adminDigest) == 1
		}
		if !ok {
			w.Header().Set("WWW-Authenticate", `Bearer realm="loggate-admin"`)
			writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "admin token required"})
			return
		}
		h(w, r)
	}
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

// dirUsage returns the total size of regular files under dir. Files that
// disappear during the walk (GC, rolling) are skipped.
func dirUsage(dir string) (int64, error) {
	var total int64
	err := filepath.WalkDir(dir, func(_ string, d fs.DirEntry, err error) error {
		if err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				return nil
			}
			return err
		}
		if d.IsDir() {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				return nil
			}
			return err
		}
		if info.Mode().IsRegular() {
			total += info.Size()
		}
		return nil
	})
	return total, err
}

// checkDisk refreshes the disk-usage gauge and the over-quota flag.
func (g *Gateway) checkDisk() {
	n, err := dirUsage(g.cfg.DataDir)
	if err != nil {
		g.log.Warn("disk usage", "err", err)
		return
	}
	g.diskBytes.Store(n)
	over := g.cfg.Disk.MaxBytes > 0 && n > g.cfg.Disk.MaxBytes.Int64()
	if prev := g.diskOver.Swap(over); prev != over {
		g.log.Warn("disk quota state changed", "over", over, "bytes", n, "max_bytes", g.cfg.Disk.MaxBytes.Int64())
	}
}

func (g *Gateway) diskLoop() {
	defer close(g.diskDone)
	t := time.NewTicker(g.cfg.Disk.CheckInterval)
	defer t.Stop()
	for {
		select {
		case <-g.diskStop:
			return
		case <-t.C:
			g.checkDisk()
		}
	}
}

func (g *Gateway) stopDiskLoop() {
	if g.diskStop != nil {
		g.diskStopOnce.Do(func() { close(g.diskStop) })
		<-g.diskDone
	}
}

// backpressure is the ingest gate: L1 quota (block_all) or disk quota.
func (g *Gateway) backpressure() bool { return g.deps.OverQuota() || g.diskOver.Load() }
