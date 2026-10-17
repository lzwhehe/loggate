package delivery

import (
	"math/rand/v2"
	"sync"
	"time"
)

// Backoff computes retry delays with full jitter (DESIGN §4.3):
//
//	delay(n) = rand[0, min(Max, Base·2^(n-1)))
//
// where n >= 1 is the number of real send attempts so far. For Throttled
// errors the delay is max(RetryAfter, delay(n)).
type Backoff struct {
	Base time.Duration
	Max  time.Duration

	mu  sync.Mutex
	rnd *rand.Rand
}

// NewBackoff returns a Backoff. seed makes the jitter reproducible.
func NewBackoff(base, maxDelay time.Duration, seed uint64) *Backoff {
	return &Backoff{Base: base, Max: maxDelay, rnd: rand.New(rand.NewPCG(seed, seed^0x9e3779b97f4a7c15))}
}

// Ceiling returns the upper bound of the jitter window for attempt n.
func (b *Backoff) Ceiling(n int) time.Duration {
	if n < 1 {
		n = 1
	}
	d := b.Base
	for i := 1; i < n; i++ {
		if d >= b.Max/2 {
			return b.Max
		}
		d *= 2
	}
	return min(d, b.Max)
}

// Delay returns the delay before the next attempt after n attempts.
func (b *Backoff) Delay(n int, kind Kind, retryAfter time.Duration) time.Duration {
	c := b.Ceiling(n)
	var d time.Duration
	if c > 0 {
		b.mu.Lock()
		d = time.Duration(b.rnd.Int64N(int64(c)))
		b.mu.Unlock()
	}
	if retryAfter > d && (kind == Throttled || kind == Retryable) {
		d = retryAfter
	}
	return d
}

// Budget bounds retries: an event/destination pair is dead once it has
// had MaxAttempts real send attempts or is older than MaxAge, whichever
// comes first (DESIGN §4.3).
type Budget struct {
	MaxAttempts int
	MaxAge      time.Duration
}

// Exhausted reports whether the budget is used up and, if so, why
// ("max_attempts" or "max_age").
func (b Budget) Exhausted(attempts int, receivedAt int64, now time.Time) (bool, string) {
	if b.MaxAttempts > 0 && attempts >= b.MaxAttempts {
		return true, "max_attempts"
	}
	if b.MaxAge > 0 && receivedAt > 0 && now.Sub(time.Unix(0, receivedAt)) >= b.MaxAge {
		return true, "max_age"
	}
	return false, ""
}
