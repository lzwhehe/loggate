// Package httpingest implements the HTTP JSON input:
//
//	POST /v1/events   body: one JSON object or an array of objects
//	GET  /healthz
//	GET  /v1/stats    delivery statistics (if configured)
//
// A 200 response is sent only after every event in the request has been
// fsynced to the WAL; any WAL failure yields 503 and nothing is
// acknowledged (the client must retry).
package httpingest

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/lzwhehe/loggate/internal/clock"
	"github.com/lzwhehe/loggate/internal/event"
	"github.com/lzwhehe/loggate/internal/wal"
)

// AnonymousSource is the SourceID used when no tokens are configured.
const AnonymousSource = "http:anonymous"

// Appender is the WAL surface the handler needs.
type Appender interface {
	Append(ctx context.Context, items []wal.Item) (wal.Result, error)
}

// Router assigns destinations to an event.
type Router interface {
	Route(ev *event.LogEvent) event.DestMask
}

// Options configures the handler.
type Options struct {
	MaxBody int64
	// Tokens maps bearer token -> source name. SourceID becomes
	// "http:<source>". Empty means unauthenticated ("http:anonymous").
	Tokens map[string]string
	Keyer  *event.IdemKeyer
	Clock  clock.Clock
	Logger *slog.Logger
	// Backpressure, if set, is consulted per request; while it returns true
	// new events are refused with 429 and Retry-After (DESIGN §4.9).
	Backpressure func() bool
	// RetryAfter is the Retry-After value sent with 429; default 1s.
	RetryAfter time.Duration
	// Stats, if set, serves GET /v1/stats as JSON.
	Stats func() any
}

type tokenEntry struct {
	digest   [sha256.Size]byte
	sourceID string
}

// Handler serves the HTTP ingest API.
type Handler struct {
	wal      Appender
	router   Router
	keyer    *event.IdemKeyer
	clock    clock.Clock
	log      *slog.Logger
	maxBody  int64
	tokens   []tokenEntry
	draining atomic.Bool
	mux      *http.ServeMux
	backp    func() bool
	retryAft string
	stats    func() any
	rejected atomic.Uint64
}

// New returns a handler.
func New(app Appender, router Router, opts Options) (*Handler, error) {
	if app == nil || router == nil || opts.Keyer == nil {
		return nil, errors.New("httpingest: appender, router and keyer are required")
	}
	if opts.MaxBody <= 0 {
		return nil, errors.New("httpingest: MaxBody must be > 0")
	}
	if opts.Clock == nil {
		opts.Clock = clock.Real()
	}
	if opts.Logger == nil {
		opts.Logger = slog.New(slog.DiscardHandler)
	}
	if opts.RetryAfter <= 0 {
		opts.RetryAfter = time.Second
	}
	h := &Handler{
		wal: app, router: router, keyer: opts.Keyer, clock: opts.Clock,
		log: opts.Logger.With("component", "ingest.http"), maxBody: opts.MaxBody,
		backp: opts.Backpressure, stats: opts.Stats,
		retryAft: strconv.Itoa(int(max(1, opts.RetryAfter.Round(time.Second)/time.Second))),
	}
	for tok, src := range opts.Tokens {
		if tok == "" || src == "" {
			return nil, errors.New("httpingest: empty token or source")
		}
		h.tokens = append(h.tokens, tokenEntry{digest: sha256.Sum256([]byte(tok)), sourceID: "http:" + src})
	}
	h.mux = http.NewServeMux()
	h.mux.HandleFunc("POST /v1/events", h.handleEvents)
	h.mux.HandleFunc("GET /healthz", h.handleHealth)
	if h.stats != nil {
		h.mux.HandleFunc("GET /v1/stats", func(w http.ResponseWriter, _ *http.Request) { writeJSON(w, http.StatusOK, h.stats()) })
	}
	return h, nil
}

// Rejected returns how many requests were refused with 429.
func (h *Handler) Rejected() uint64 { return h.rejected.Load() }

// ServeHTTP implements http.Handler.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) { h.mux.ServeHTTP(w, r) }

// SetDraining makes /healthz and new ingest requests return 503 (first step
// of graceful shutdown, DESIGN §4.10).
func (h *Handler) SetDraining(v bool) { h.draining.Store(v) }

type acceptResponse struct {
	Accepted int    `json:"accepted"`
	FirstSeq uint64 `json:"first_seq"`
}

type errorResponse struct {
	Error string `json:"error"`
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func (h *Handler) handleHealth(w http.ResponseWriter, _ *http.Request) {
	if h.draining.Load() {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"status": "draining"})
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// authenticate returns the SourceID bound to the request's bearer token.
// Clients cannot choose their SourceID (REVIEW_01 item 10).
func (h *Handler) authenticate(r *http.Request) (string, bool) {
	if len(h.tokens) == 0 {
		return AnonymousSource, true
	}
	auth := r.Header.Get("Authorization")
	const prefix = "bearer "
	if len(auth) <= len(prefix) || !strings.EqualFold(auth[:len(prefix)], prefix) {
		return "", false
	}
	d := sha256.Sum256([]byte(strings.TrimSpace(auth[len(prefix):])))
	src, found := "", 0
	for _, t := range h.tokens { // constant-time over all entries
		if subtle.ConstantTimeCompare(d[:], t.digest[:]) == 1 {
			src, found = t.sourceID, 1
		}
	}
	return src, found == 1
}

func (h *Handler) handleEvents(w http.ResponseWriter, r *http.Request) {
	if h.draining.Load() {
		writeJSON(w, http.StatusServiceUnavailable, errorResponse{"shutting down"})
		return
	}
	sourceID, ok := h.authenticate(r)
	if !ok {
		w.Header().Set("WWW-Authenticate", `Bearer realm="loggate"`)
		writeJSON(w, http.StatusUnauthorized, errorResponse{"missing or invalid bearer token"})
		return
	}
	if h.backp != nil && h.backp() {
		h.rejected.Add(1)
		w.Header().Set("Retry-After", h.retryAft)
		writeJSON(w, http.StatusTooManyRequests, errorResponse{"delivery backlog over quota, retry later"})
		return
	}
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, h.maxBody))
	if err != nil {
		var tooBig *http.MaxBytesError
		if errors.As(err, &tooBig) {
			writeJSON(w, http.StatusRequestEntityTooLarge, errorResponse{fmt.Sprintf("body exceeds %d bytes", h.maxBody)})
			return
		}
		writeJSON(w, http.StatusBadRequest, errorResponse{"read body failed"})
		return
	}
	elems, err := splitEvents(body)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, errorResponse{err.Error()})
		return
	}

	now := h.clock.Now().UnixNano()
	items := make([]wal.Item, len(elems))
	for i, el := range elems {
		ev := &event.LogEvent{
			SourceID:   sourceID,
			ReceivedAt: now,
			Time:       now,
			// TODO(M4): normalise to OCSF and mask PII before this point;
			// the WAL must only ever hold masked events.
			Fields: map[string]any{"unmapped": el.fields},
			Raw:    el.raw,
		}
		ev.Targets = h.router.Route(ev)
		items[i] = wal.Item{Targets: ev.Targets, Encode: h.encoder(ev)}
	}

	res, err := h.wal.Append(r.Context(), items)
	if err != nil {
		// Detailed cause goes to the log only; WAL errors may contain paths.
		h.log.Warn("wal append failed", "source", sourceID, "events", len(items), "err", err)
		writeJSON(w, http.StatusServiceUnavailable, errorResponse{"not persisted, retry later"})
		return
	}
	writeJSON(w, http.StatusOK, acceptResponse{Accepted: res.Count, FirstSeq: res.FirstSeq})
}

// encoder stamps the WAL-assigned seq and gateway-assigned IdemKey, then
// serialises the event. It runs on the WAL writer goroutine.
func (h *Handler) encoder(ev *event.LogEvent) func(seq uint64) ([]byte, error) {
	return func(seq uint64) ([]byte, error) {
		ev.Seq = seq
		ev.IdemKey = h.keyer.Key(seq)
		return json.Marshal(ev)
	}
}

type element struct {
	fields map[string]any
	raw    []byte
}

// splitEvents accepts a single JSON object or an array of objects.
func splitEvents(body []byte) ([]element, error) {
	body = bytes.TrimSpace(body)
	if len(body) == 0 {
		return nil, errors.New("empty body")
	}
	var raws []json.RawMessage
	switch body[0] {
	case '{':
		raws = []json.RawMessage{body}
	case '[':
		if err := json.Unmarshal(body, &raws); err != nil {
			return nil, fmt.Errorf("invalid JSON array: %w", err)
		}
		if len(raws) == 0 {
			return nil, errors.New("empty event array")
		}
	default:
		return nil, errors.New("body must be a JSON object or an array of objects")
	}
	out := make([]element, len(raws))
	for i, raw := range raws {
		raw = bytes.TrimSpace(raw)
		if len(raw) == 0 || raw[0] != '{' {
			return nil, fmt.Errorf("event %d: not a JSON object", i)
		}
		dec := json.NewDecoder(bytes.NewReader(raw))
		dec.UseNumber()
		var m map[string]any
		if err := dec.Decode(&m); err != nil {
			return nil, fmt.Errorf("event %d: invalid JSON: %w", i, err)
		}
		if dec.More() {
			return nil, fmt.Errorf("event %d: trailing data", i)
		}
		out[i] = element{fields: m, raw: raw}
	}
	return out, nil
}
