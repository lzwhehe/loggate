// Package webhook implements an HTTP webhook sink.
//
// Batch mode (default) POSTs a JSON array of events; every element carries
// its idem_key, and the request carries the keys in an Idempotency-Keys
// header (comma-separated, in array order). Single mode (batch: false)
// sends one request per event with an Idempotency-Key header. The response
// status is classified with delivery.ClassifyHTTP: 2xx success, 429
// Throttled (Retry-After honoured), 408/5xx Retryable, other 4xx
// NonRetryable. A whole batch shares its request's outcome.
package webhook

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/lzwhehe/loggate/internal/delivery"
	"github.com/lzwhehe/loggate/internal/sink"
)

// Type is the destination type name used in configuration.
const Type = "webhook"

// Sink posts events to an HTTP endpoint.
type Sink struct {
	url     string
	batch   bool
	headers map[string]string
	client  *http.Client
	now     func() time.Time
}

var _ sink.Sink = (*Sink)(nil)

// New returns an uninitialised sink.
func New() sink.Sink { return &Sink{} }

// Init reads cfg: url (required), timeout (duration string, default 5s),
// batch (bool, default true), headers (map).
func (s *Sink) Init(_ context.Context, cfg map[string]any) error {
	u, _ := cfg["url"].(string)
	if u == "" {
		return errors.New("config.url is required")
	}
	if pu, err := url.Parse(u); err != nil || (pu.Scheme != "http" && pu.Scheme != "https") {
		return fmt.Errorf("config.url %q is not an http(s) URL", u)
	}
	timeout := 5 * time.Second
	if v, ok := cfg["timeout"]; ok {
		str, _ := v.(string)
		d, err := time.ParseDuration(str)
		if err != nil || d <= 0 {
			return fmt.Errorf("config.timeout %v: want a positive duration like 2s", v)
		}
		timeout = d
	}
	s.batch = true
	if v, ok := cfg["batch"]; ok {
		b, ok := v.(bool)
		if !ok {
			return fmt.Errorf("config.batch must be a bool")
		}
		s.batch = b
	}
	s.headers = map[string]string{}
	if h, ok := cfg["headers"].(map[string]any); ok {
		for k, v := range h {
			s.headers[k] = fmt.Sprint(v)
		}
	}
	s.url = u
	s.client = &http.Client{Timeout: timeout}
	s.now = time.Now
	return nil
}

// Write posts the batch (or each event in single mode).
func (s *Sink) Write(ctx context.Context, batch []*sink.Envelope) []error {
	if !s.batch {
		errs := make([]error, len(batch))
		for i, env := range batch {
			errs[i] = s.post(ctx, batch[i:i+1], env.Event.IdemKey.String(), "Idempotency-Key")
		}
		return errs
	}
	keys := make([]string, len(batch))
	for i, env := range batch {
		keys[i] = env.Event.IdemKey.String()
	}
	return sink.FillErrors(len(batch), s.post(ctx, batch, strings.Join(keys, ","), "Idempotency-Keys"))
}

func (s *Sink) post(ctx context.Context, envs []*sink.Envelope, keys, keyHeader string) error {
	evs := make([]any, len(envs))
	for i, env := range envs {
		evs[i] = env.Event
	}
	var body []byte
	var err error
	if len(evs) == 1 && !s.batch {
		body, err = json.Marshal(evs[0])
	} else {
		body, err = json.Marshal(evs)
	}
	if err != nil {
		return delivery.NewError(delivery.NonRetryable, fmt.Errorf("webhook: marshal: %w", err))
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.url, bytes.NewReader(body))
	if err != nil {
		return delivery.NewError(delivery.NonRetryable, fmt.Errorf("webhook: build request: %w", err))
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(keyHeader, keys)
	for k, v := range s.headers {
		req.Header.Set(k, v)
	}
	resp, err := s.client.Do(req)
	if err != nil {
		// Transport errors (refused, reset, timeout) are Retryable. The
		// error text contains the URL but no payload.
		return fmt.Errorf("webhook: %w", err)
	}
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 64<<10))
	resp.Body.Close()
	if err := delivery.ClassifyHTTP(resp.StatusCode, resp.Header.Get("Retry-After"), s.now()); err != nil {
		return fmt.Errorf("webhook: %w", err)
	}
	return nil
}

// Health is a no-op (a webhook has no cheap health probe); failures show
// up through the breaker.
func (s *Sink) Health(context.Context) error { return nil }

// Close releases idle connections.
func (s *Sink) Close(context.Context) error {
	if s.client != nil {
		s.client.CloseIdleConnections()
	}
	return nil
}

// Idempotent is false: dedup depends on the receiver honouring the keys.
func (*Sink) Idempotent() bool { return false }
