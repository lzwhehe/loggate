package clock

import (
	"testing"
	"time"
)

func TestFakeTimerFiresOnAdvance(t *testing.T) {
	start := time.Unix(1000, 0)
	f := NewFake(start)
	tm := f.NewTimer(10 * time.Millisecond)
	f.Advance(9 * time.Millisecond)
	select {
	case <-tm.C():
		t.Fatal("fired early")
	default:
	}
	f.Advance(time.Millisecond)
	select {
	case got := <-tm.C():
		if want := start.Add(10 * time.Millisecond); !got.Equal(want) {
			t.Fatalf("fired at %v, want %v", got, want)
		}
	default:
		t.Fatal("did not fire")
	}
	if tm.Stop() {
		t.Fatal("Stop after fire returned true")
	}
}

func TestFakeTimerStop(t *testing.T) {
	f := NewFake(time.Unix(0, 0))
	tm := f.NewTimer(time.Second)
	if !tm.Stop() {
		t.Fatal("Stop returned false for armed timer")
	}
	f.Advance(2 * time.Second)
	select {
	case <-tm.C():
		t.Fatal("stopped timer fired")
	default:
	}
	if f.PendingTimers() != 0 {
		t.Fatal("timer still pending")
	}
}

func TestBlockUntilTimers(t *testing.T) {
	f := NewFake(time.Unix(0, 0))
	done := make(chan struct{})
	go func() {
		tm := f.NewTimer(time.Second)
		<-tm.C()
		close(done)
	}()
	f.BlockUntilTimers(1)
	f.Advance(time.Second)
	<-done
}
