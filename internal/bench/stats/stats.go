// Package stats has the small amount of statistics the benchmark reports:
// mean and a two-sided 95% confidence interval from Student's t
// distribution, and exact percentiles.
package stats

import (
	"math"
	"slices"
)

// t975 holds t_{0.975, df} for df = 1..30.
var t975 = [...]float64{
	12.706, 4.303, 3.182, 2.776, 2.571, 2.447, 2.365, 2.306, 2.262, 2.228,
	2.201, 2.179, 2.160, 2.145, 2.131, 2.120, 2.110, 2.101, 2.093, 2.086,
	2.080, 2.074, 2.069, 2.064, 2.060, 2.056, 2.052, 2.048, 2.045, 2.042,
}

// TCrit returns t_{0.975, df} (1.96 for df > 30, NaN for df < 1).
func TCrit(df int) float64 {
	switch {
	case df < 1:
		return math.NaN()
	case df <= len(t975):
		return t975[df-1]
	case df <= 60:
		return 2.000
	case df <= 120:
		return 1.980
	default:
		return 1.960
	}
}

// Summary is mean ± half-width of the 95% CI.
type Summary struct {
	N      int       `json:"n"`
	Mean   float64   `json:"mean"`
	SD     float64   `json:"sd"`
	CI95   float64   `json:"ci95_half"` // 0 when n < 2 (HasCI false)
	Min    float64   `json:"min"`
	Max    float64   `json:"max"`
	HasCI  bool      `json:"has_ci"`
	Values []float64 `json:"values"`
}

// Describe summarises xs (sample SD, t-based CI).
func Describe(xs []float64) Summary {
	s := Summary{N: len(xs), Values: slices.Clone(xs)}
	if s.N == 0 {
		return s
	}
	s.Min, s.Max = xs[0], xs[0]
	var sum float64
	for _, x := range xs {
		sum += x
		s.Min, s.Max = min(s.Min, x), max(s.Max, x)
	}
	s.Mean = sum / float64(s.N)
	if s.N < 2 {
		return s
	}
	var ss float64
	for _, x := range xs {
		ss += (x - s.Mean) * (x - s.Mean)
	}
	s.SD = math.Sqrt(ss / float64(s.N-1))
	s.CI95 = TCrit(s.N-1) * s.SD / math.Sqrt(float64(s.N))
	s.HasCI = true
	return s
}

// Percentile returns the q-quantile (0..1) of sorted xs by nearest rank.
func Percentile(sorted []float64, q float64) float64 {
	if len(sorted) == 0 {
		return math.NaN()
	}
	if q <= 0 {
		return sorted[0]
	}
	if q >= 1 {
		return sorted[len(sorted)-1]
	}
	rank := int(math.Ceil(q*float64(len(sorted)))) - 1
	return sorted[max(0, min(rank, len(sorted)-1))]
}
