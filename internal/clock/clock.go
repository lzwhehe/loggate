// Package clock abstracts time so that timing-dependent logic (group commit,
// retry backoff, TTLs) can be tested deterministically with FakeClock.
package clock

import (
	"sort"
	"sync"
	"time"
)

// Clock is the time source used throughout LogGate.
type Clock interface {
	Now() time.Time
	NewTimer(d time.Duration) Timer
}

// Timer mirrors the subset of *time.Timer that LogGate uses.
type Timer interface {
	C() <-chan time.Time
	// Stop prevents the timer from firing. It reports whether the call
	// stopped the timer (false if it already fired or was stopped).
	Stop() bool
}

// Real returns a Clock backed by the time package.
func Real() Clock { return realClock{} }

type realClock struct{}

func (realClock) Now() time.Time { return time.Now() }

func (realClock) NewTimer(d time.Duration) Timer { return realTimer{time.NewTimer(d)} }

type realTimer struct{ t *time.Timer }

func (r realTimer) C() <-chan time.Time { return r.t.C }
func (r realTimer) Stop() bool          { return r.t.Stop() }

// Fake is a manually advanced Clock for tests.
type Fake struct {
	mu      sync.Mutex
	now     time.Time
	timers  []*fakeTimer
	changed *sync.Cond
}

// NewFake returns a fake clock starting at start.
func NewFake(start time.Time) *Fake {
	f := &Fake{now: start}
	f.changed = sync.NewCond(&f.mu)
	return f
}

// Now returns the current fake time.
func (f *Fake) Now() time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.now
}

// NewTimer creates a timer that fires once the fake time reaches now+d.
func (f *Fake) NewTimer(d time.Duration) Timer {
	f.mu.Lock()
	defer f.mu.Unlock()
	t := &fakeTimer{clock: f, when: f.now.Add(d), ch: make(chan time.Time, 1)}
	if d <= 0 {
		t.fired = true
		t.ch <- f.now
		return t
	}
	f.timers = append(f.timers, t)
	f.changed.Broadcast()
	return t
}

// Advance moves the clock forward by d and fires every due timer in order.
func (f *Fake) Advance(d time.Duration) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.now = f.now.Add(d)
	sort.SliceStable(f.timers, func(i, j int) bool { return f.timers[i].when.Before(f.timers[j].when) })
	kept := f.timers[:0]
	for _, t := range f.timers {
		if !t.when.After(f.now) {
			t.fired = true
			t.ch <- t.when
			continue
		}
		kept = append(kept, t)
	}
	f.timers = kept
	f.changed.Broadcast()
}

// PendingTimers returns the number of armed timers.
func (f *Fake) PendingTimers() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.timers)
}

// BlockUntilTimers blocks until at least n timers are armed. Tests use it to
// avoid racing Advance against a goroutine that is about to create a timer.
func (f *Fake) BlockUntilTimers(n int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for len(f.timers) < n {
		f.changed.Wait()
	}
}

type fakeTimer struct {
	clock *Fake
	when  time.Time
	ch    chan time.Time
	fired bool
}

func (t *fakeTimer) C() <-chan time.Time { return t.ch }

func (t *fakeTimer) Stop() bool {
	f := t.clock
	f.mu.Lock()
	defer f.mu.Unlock()
	if t.fired {
		return false
	}
	for i, x := range f.timers {
		if x == t {
			f.timers = append(f.timers[:i], f.timers[i+1:]...)
			f.changed.Broadcast()
			return true
		}
	}
	return false
}
