package tracker

import (
	"math/rand/v2"
	"testing"
)

func TestOutOfOrderResolve(t *testing.T) {
	s := NewSet(0)
	for seq := uint64(1); seq <= 5; seq++ {
		s.Add(0, seq)
		s.Publish(seq)
	}
	if w := s.Watermark(0); w != 0 {
		t.Fatalf("wm = %d, want 0", w)
	}
	s.Resolve(0, 3)
	s.Resolve(0, 2)
	if w := s.Watermark(0); w != 0 {
		t.Fatalf("wm after resolving 2,3 = %d, want 0 (1 still open)", w)
	}
	s.Resolve(0, 1)
	if w := s.Watermark(0); w != 3 {
		t.Fatalf("wm = %d, want 3", w)
	}
	if s.Resolve(0, 3) {
		t.Fatal("double resolve reported true")
	}
	if s.Resolve(0, 99) {
		t.Fatal("unknown seq resolved")
	}
	s.Resolve(0, 5)
	if w := s.Watermark(0); w != 3 {
		t.Fatalf("wm = %d, want 3", w)
	}
	s.Resolve(0, 4)
	if w := s.Watermark(0); w != 5 {
		t.Fatalf("wm = %d, want 5", w)
	}
}

// A destination with no traffic follows the cursor, but never past a seq
// whose Adds have not all completed.
func TestNoTrafficFollowsCursor(t *testing.T) {
	s := NewSet(10)
	if w := s.Watermark(7); w != 10 {
		t.Fatalf("idle wm = %d, want 10", w)
	}
	// seq 11 goes to dest 0 only; Add done but not yet published.
	s.Add(0, 11)
	if w := s.Watermark(7); w != 10 {
		t.Fatalf("idle wm moved before publish: %d", w)
	}
	if w := s.Watermark(0); w != 10 {
		t.Fatalf("busy wm = %d", w)
	}
	s.Publish(11)
	if w := s.Watermark(7); w != 11 {
		t.Fatalf("idle wm = %d, want 11", w)
	}
	if w := s.Watermark(0); w != 10 {
		t.Fatalf("busy wm = %d, want 10", w)
	}
	s.Publish(5) // never backwards
	if s.Cursor() != 11 {
		t.Fatal("cursor moved back")
	}
}

// Randomised: watermark always equals the brute-force definition.
func TestWatermarkMatchesModel(t *testing.T) {
	r := rand.New(rand.NewPCG(1, 2))
	s := NewSet(0)
	open := map[uint64]bool{}
	var next uint64 = 1
	for step := 0; step < 20000; step++ {
		if r.IntN(3) > 0 {
			s.Add(3, next)
			open[next] = true
			s.Publish(next)
			next++
		} else if len(open) > 0 {
			for seq := range open { // random-ish pick
				s.Resolve(3, seq)
				delete(open, seq)
				break
			}
		}
		want := next - 1
		for seq := range open {
			want = min(want, seq-1)
		}
		if w := s.Watermark(3); w != want {
			t.Fatalf("step %d: wm %d want %d", step, w, want)
		}
	}
	if s.Unresolved(3) != len(open) {
		t.Fatal("unresolved count mismatch")
	}
}
