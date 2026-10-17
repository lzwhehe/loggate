// Package delivery holds the policy pieces shared by the delivery path:
// error classification, retry backoff and budget, and the per-destination
// circuit breaker (DESIGN §3 "错误分类", §4.3).
package delivery

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strconv"
	"time"
)

// Kind classifies a delivery failure.
type Kind uint8

const (
	// Retryable failures (network errors, timeouts, 5xx, anything unknown)
	// are retried with backoff and count against the retry budget.
	Retryable Kind = iota + 1
	// NonRetryable failures (4xx except 408/429, invalid payloads) go
	// straight to the DLQ.
	NonRetryable
	// Throttled failures (429) honour RetryAfter and do not count as
	// breaker failures.
	Throttled
)

func (k Kind) String() string {
	switch k {
	case Retryable:
		return "retryable"
	case NonRetryable:
		return "non_retryable"
	case Throttled:
		return "throttled"
	default:
		return fmt.Sprintf("kind(%d)", uint8(k))
	}
}

// Error is a classified delivery error. Sinks return it (wrapped or not) to
// state the kind explicitly; unclassified errors are treated as Retryable.
type Error struct {
	Kind       Kind
	RetryAfter time.Duration // only meaningful for Throttled (and optionally Retryable)
	Err        error
}

func (e *Error) Error() string {
	if e.RetryAfter > 0 {
		return fmt.Sprintf("%s (retry after %s): %v", e.Kind, e.RetryAfter, e.Err)
	}
	return fmt.Sprintf("%s: %v", e.Kind, e.Err)
}

func (e *Error) Unwrap() error { return e.Err }

// NewError wraps err with an explicit kind.
func NewError(kind Kind, err error) error { return &Error{Kind: kind, Err: err} }

// Classify returns the kind of err and, for throttling, how long to wait.
// Unknown errors are Retryable (DESIGN §3).
func Classify(err error) (Kind, time.Duration) {
	if err == nil {
		return 0, 0
	}
	var de *Error
	if errors.As(err, &de) {
		return de.Kind, de.RetryAfter
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return Retryable, 0
	}
	var ne net.Error
	if errors.As(err, &ne) {
		return Retryable, 0
	}
	return Retryable, 0
}

// StatusError is returned for non-2xx HTTP responses.
type StatusError struct {
	Code int
}

func (e *StatusError) Error() string { return fmt.Sprintf("http status %d", e.Code) }

// ClassifyHTTP maps an HTTP response status to a delivery error (nil for
// 2xx). retryAfter is the raw Retry-After header value; now is used to
// resolve the HTTP-date form.
//
//   - 2xx: success
//   - 429: Throttled, honouring Retry-After
//   - 408: Retryable
//   - other 4xx: NonRetryable
//   - 5xx and anything else: Retryable (Retry-After honoured if present)
func ClassifyHTTP(code int, retryAfter string, now time.Time) error {
	if code >= 200 && code < 300 {
		return nil
	}
	ra := ParseRetryAfter(retryAfter, now)
	se := &StatusError{Code: code}
	switch {
	case code == http.StatusTooManyRequests:
		return &Error{Kind: Throttled, RetryAfter: ra, Err: se}
	case code == http.StatusRequestTimeout:
		return &Error{Kind: Retryable, RetryAfter: ra, Err: se}
	case code >= 400 && code < 500:
		return &Error{Kind: NonRetryable, Err: se}
	default:
		return &Error{Kind: Retryable, RetryAfter: ra, Err: se}
	}
}

// ParseRetryAfter parses delay-seconds or an HTTP-date. Invalid or past
// values yield 0.
func ParseRetryAfter(v string, now time.Time) time.Duration {
	if v == "" {
		return 0
	}
	if s, err := strconv.Atoi(v); err == nil {
		if s < 0 {
			return 0
		}
		return time.Duration(s) * time.Second
	}
	if t, err := http.ParseTime(v); err == nil {
		if d := t.Sub(now); d > 0 {
			return d
		}
	}
	return 0
}
