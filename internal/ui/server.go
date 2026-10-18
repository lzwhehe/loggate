// Package ui serves the web dashboard (DESIGN §16): static assets embedded
// with go:embed (vanilla JS, no CDN, no external requests) and a
// Server-Sent Events stream that pushes a Snapshot every second.
//
//	GET /ui/              dashboard
//	GET /ui/api/snapshot  one Snapshot as JSON
//	GET /ui/api/stream    text/event-stream, one "snapshot" event per interval
//	/demo/...             demo actions, mounted only when Options.Demo is set
//
// The dashboard itself is read-only; the demo actions are guarded by the
// demo package (loopback only plus an anti-CSRF header).
package ui

import (
	"embed"
	"encoding/json"
	"io/fs"
	"log/slog"
	"net/http"
	"time"
)

//go:embed assets
var assets embed.FS

// Assets returns the embedded dashboard files (index.html, app.js, app.css).
func Assets() fs.FS {
	sub, err := fs.Sub(assets, "assets")
	if err != nil {
		panic(err)
	}
	return sub
}

// Options configures the dashboard handler.
type Options struct {
	// Snapshot builds the current state; required.
	Snapshot func() Snapshot
	// Demo, if non-nil, is mounted at /demo/ (demo mode only).
	Demo http.Handler
	// Interval between SSE events; default 1s.
	Interval time.Duration
	Logger   *slog.Logger
}

// Handler returns the dashboard handler.
func Handler(o Options) http.Handler {
	if o.Interval <= 0 {
		o.Interval = time.Second
	}
	if o.Logger == nil {
		o.Logger = slog.New(slog.DiscardHandler)
	}
	mux := http.NewServeMux()
	static := http.FileServerFS(Assets())
	mux.Handle("GET /ui/", secure(http.StripPrefix("/ui/", static)))
	mux.HandleFunc("GET /{$}", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/ui/", http.StatusFound)
	})
	mux.HandleFunc("GET /ui", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/ui/", http.StatusFound)
	})
	mux.Handle("GET /ui/api/snapshot", secure(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "no-store")
		_ = json.NewEncoder(w).Encode(o.Snapshot())
	})))
	mux.Handle("GET /ui/api/stream", secure(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		stream(w, r, o)
	})))
	if o.Demo != nil {
		mux.Handle("/demo/", o.Demo)
	}
	return mux
}

// secure adds headers that keep the page self-contained.
func secure(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hd := w.Header()
		hd.Set("Content-Security-Policy", "default-src 'self'; script-src 'self'; style-src 'self'; img-src 'self' data:; connect-src 'self'; frame-ancestors 'none'; base-uri 'none'; form-action 'none'")
		hd.Set("X-Content-Type-Options", "nosniff")
		hd.Set("Referrer-Policy", "no-referrer")
		h.ServeHTTP(w, r)
	})
}

func stream(w http.ResponseWriter, r *http.Request, o Options) {
	fl, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}
	h := w.Header()
	h.Set("Content-Type", "text/event-stream")
	h.Set("Cache-Control", "no-store")
	h.Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)
	send := func() bool {
		b, err := json.Marshal(o.Snapshot())
		if err != nil {
			o.Logger.Warn("dashboard snapshot", "err", err)
			return true
		}
		if _, err := w.Write([]byte("event: snapshot\ndata: ")); err != nil {
			return false
		}
		if _, err := w.Write(b); err != nil {
			return false
		}
		if _, err := w.Write([]byte("\n\n")); err != nil {
			return false
		}
		fl.Flush()
		return true
	}
	if _, err := w.Write([]byte("retry: 2000\n\n")); err != nil {
		return
	}
	if !send() {
		return
	}
	t := time.NewTicker(o.Interval)
	defer t.Stop()
	for {
		select {
		case <-r.Context().Done():
			return
		case <-t.C:
			if !send() {
				return
			}
		}
	}
}
