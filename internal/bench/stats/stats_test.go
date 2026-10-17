package stats

import (
	"math"
	"testing"
)

func TestDescribe(t *testing.T) {
	s := Describe([]float64{10, 12, 14})
	if s.Mean != 12 || math.Abs(s.SD-2) > 1e-12 {
		t.Fatalf("%+v", s)
	}
	// t(0.975, 2) = 4.303; half-width = 4.303 * 2 / sqrt(3)
	if want := 4.303 * 2 / math.Sqrt(3); math.Abs(s.CI95-want) > 1e-9 || !s.HasCI {
		t.Fatalf("ci %v want %v", s.CI95, want)
	}
	one := Describe([]float64{5})
	if one.HasCI || one.Mean != 5 || one.CI95 != 0 {
		t.Fatalf("%+v", one)
	}
	if z := Describe(nil); z.N != 0 {
		t.Fatalf("%+v", z)
	}
}

func TestTCrit(t *testing.T) {
	if TCrit(4) != 2.776 || TCrit(1000) != 1.96 || !math.IsNaN(TCrit(0)) {
		t.Fatal("tcrit")
	}
}

func TestPercentile(t *testing.T) {
	xs := make([]float64, 1000)
	for i := range xs {
		xs[i] = float64(i + 1)
	}
	if Percentile(xs, 0.5) != 500 || Percentile(xs, 0.99) != 990 || Percentile(xs, 0.999) != 999 || Percentile(xs, 1) != 1000 {
		t.Fatal("percentiles")
	}
	if !math.IsNaN(Percentile(nil, 0.5)) {
		t.Fatal("empty")
	}
}
