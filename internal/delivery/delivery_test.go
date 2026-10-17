package delivery

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/lzwhehe/loggate/internal/clock"
)

func TestClassifyHTTP(t *testing.T) {
	now := time.Date(2026, 10, 17, 0, 0, 0, 0, time.UTC)
	cases := []struct {
		code int
		ra   string
		kind Kind
		wait time.Duration
	}{
		{200, "", 0, 0},
		{204, "", 0, 0},
		{400, "", NonRetryable, 0},
		{404, "", NonRetryable, 0},
		{413, "", NonRetryable, 0},
		{408, "", Retryable, 0},
		{429, "7", Throttled, 7 * time.Second},
		{429, now.Add(3 * time.Second).Format(http.TimeFormat), Throttled, 3 * time.Second},
		{429, "garbage", Throttled, 0},
		{500, "", Retryable, 0},
		{503, "2", Retryable, 2 * time.Second},
	}
	for _, c := range cases {
		err := ClassifyHTTP(c.code, c.ra, now)
		if c.kind == 0 {
			if err != nil {
				t.Errorf("%d: unexpected error %v", c.code, err)
			}
			continue
		}
		k, w := Classify(fmt.Errorf("wrapped: %w", err))
		if k != c.kind || w != c.wait {
			t.Errorf("%d %q: got %v/%v, want %v/%v", c.code, c.ra, k, w, c.kind, c.wait)
		}
	}
}

type tempNetErr struct{}

func (tempNetErr) Error() string   { return "conn reset" }
func (tempNetErr) Timeout() bool   { return false }
func (tempNetErr) Temporary() bool { return true }

func TestClassifyUnknownIsRetryable(t *testing.T) {
	var _ net.Error = tempNetErr{}
	for _, err := range []error{errors.New("mystery"), context.DeadlineExceeded, tempNetErr{}, &net.OpError{Op: "dial", Err: errors.New("refused")}} {
		if k, _ := Classify(err); k != Retryable {
			t.Errorf("%v: got %v", err, k)
		}
	}
	if k, _ := Classify(NewError(NonRetryable, errors.New("schema"))); k != NonRetryable {
		t.Fatalf("explicit kind lost: %v", k)
	}
}

func TestBackoffBounds(t *testing.T) {
	b := NewBackoff(200*time.Millisecond, 30*time.Second, 1)
	wantCeil := []time.Duration{200 * time.Millisecond, 400 * time.Millisecond, 800 * time.Millisecond, 1600 * time.Millisecond}
	for i, w := range wantCeil {
		if c := b.Ceiling(i + 1); c != w {
			t.Fatalf("ceiling(%d) = %v want %v", i+1, c, w)
		}
	}
	if c := b.Ceiling(100); c != 30*time.Second {
		t.Fatalf("ceiling(100) = %v", c)
	}
	for n := 1; n < 40; n++ {
		for range 200 {
			d := b.Delay(n, Retryable, 0)
			if d < 0 || d >= b.Ceiling(n) {
				t.Fatalf("delay(%d) = %v outside [0,%v)", n, d, b.Ceiling(n))
			}
		}
	}
	// Retry-After dominates a smaller jittered delay.
	if d := b.Delay(1, Throttled, 5*time.Second); d != 5*time.Second {
		t.Fatalf("throttled delay = %v", d)
	}
	// Same seed, same sequence.
	b1, b2 := NewBackoff(time.Second, time.Minute, 42), NewBackoff(time.Second, time.Minute, 42)
	for n := 1; n < 10; n++ {
		if b1.Delay(n, Retryable, 0) != b2.Delay(n, Retryable, 0) {
			t.Fatal("jitter not reproducible")
		}
	}
}

func TestBudget(t *testing.T) {
	now := time.Unix(1000, 0)
	b := Budget{MaxAttempts: 3, MaxAge: time.Minute}
	if ex, _ := b.Exhausted(2, now.UnixNano(), now); ex {
		t.Fatal("2 attempts exhausted")
	}
	if ex, why := b.Exhausted(3, now.UnixNano(), now); !ex || why != "max_attempts" {
		t.Fatal("3 attempts not exhausted")
	}
	if ex, why := b.Exhausted(1, now.Add(-time.Minute).UnixNano(), now); !ex || why != "max_age" {
		t.Fatal("max_age not exhausted")
	}
}

func TestBreaker(t *testing.T) {
	clk := clock.NewFake(time.Unix(0, 0))
	b := NewBreaker(3, 10*time.Second, clk)
	b.Record(OutcomeFailure)
	b.Record(OutcomeFailure)
	b.Record(OutcomeNeutral) // throttled / non-retryable do not count
	if b.State() != BreakerClosed {
		t.Fatal("opened early")
	}
	b.Record(OutcomeFailure)
	if b.State() != BreakerOpen || b.Allow() {
		t.Fatal("not open after 3 failures")
	}
	if got := b.OpenUntil(); !got.Equal(time.Unix(10, 0)) {
		t.Fatalf("open until %v", got)
	}
	clk.Advance(10 * time.Second)
	if b.State() != BreakerHalfOpen || !b.Allow() {
		t.Fatal("not half-open after open_for")
	}
	b.Record(OutcomeFailure) // probe fails: reopen immediately
	if b.State() != BreakerOpen {
		t.Fatal("half-open failure did not reopen")
	}
	clk.Advance(10 * time.Second)
	b.Record(OutcomeSuccess)
	if b.State() != BreakerClosed {
		t.Fatal("probe success did not close")
	}
}
