package delivery

import (
	"fmt"
	"sync"
	"time"

	"github.com/lzwhehe/loggate/internal/clock"
)

// BreakerState is the circuit breaker state.
type BreakerState uint8

const (
	BreakerClosed BreakerState = iota
	BreakerOpen
	BreakerHalfOpen
)

func (s BreakerState) String() string {
	switch s {
	case BreakerClosed:
		return "closed"
	case BreakerOpen:
		return "open"
	case BreakerHalfOpen:
		return "half_open"
	default:
		return fmt.Sprintf("breaker(%d)", uint8(s))
	}
}

// Outcome is the breaker-relevant result of one Sink.Write call.
type Outcome uint8

const (
	OutcomeNeutral Outcome = iota // only NonRetryable/Throttled failures: says nothing about health
	OutcomeSuccess                // at least one envelope succeeded
	OutcomeFailure                // no success and at least one Retryable failure
)

// Breaker is a small consecutive-failure circuit breaker.
//
// Closed: everything passes; Failures consecutive failed writes open it.
// Open: nothing is sent until OpenFor has elapsed.
// HalfOpen: entered lazily once OpenFor has elapsed. Writes are allowed again
// (fresh events too, DESIGN §0.5 item 9); the destination's single consumer
// makes the next write the probe. Success closes, failure re-opens.
type Breaker struct {
	failures int
	openFor  time.Duration
	clk      clock.Clock

	mu        sync.Mutex
	state     BreakerState
	consec    int
	openUntil time.Time
}

// NewBreaker returns a closed breaker. failures <= 0 disables it.
func NewBreaker(failures int, openFor time.Duration, clk clock.Clock) *Breaker {
	return &Breaker{failures: failures, openFor: openFor, clk: clk}
}

func (b *Breaker) stateLocked(now time.Time) BreakerState {
	if b.state == BreakerOpen && !now.Before(b.openUntil) {
		b.state = BreakerHalfOpen
	}
	return b.state
}

// State returns the current state.
func (b *Breaker) State() BreakerState {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.stateLocked(b.clk.Now())
}

// Allow reports whether a write may be attempted now.
func (b *Breaker) Allow() bool { return b.State() != BreakerOpen }

// OpenUntil returns when an open breaker becomes half-open (zero if not
// open).
func (b *Breaker) OpenUntil() time.Time {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.stateLocked(b.clk.Now()) != BreakerOpen {
		return time.Time{}
	}
	return b.openUntil
}

// Record feeds the outcome of one write.
func (b *Breaker) Record(o Outcome) {
	if b.failures <= 0 {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	now := b.clk.Now()
	st := b.stateLocked(now)
	switch o {
	case OutcomeSuccess:
		b.state, b.consec = BreakerClosed, 0
	case OutcomeFailure:
		b.consec++
		if st == BreakerHalfOpen || b.consec >= b.failures {
			b.state = BreakerOpen
			b.openUntil = now.Add(b.openFor)
		}
	}
}
