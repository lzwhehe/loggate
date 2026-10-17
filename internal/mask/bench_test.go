package mask

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/lzwhehe/loggate/internal/normalize"
)

// benchEvent builds an OCSF Base Event of about size bytes of string
// content: unmapped holds 32-byte string fields, and a fraction density of
// them (rounded, at least one if density > 0) contains one PII value
// (rotating ID card, bank card, phone, e-mail). It returns the event, the
// boxed original values (to restore after masking), the number of string
// bytes scanned, and the PII-field count / field count.
func benchEvent(size int, density float64) (map[string]any, map[string]any, int, string) {
	pii := []string{"11010519491231002X", "4111 1111 1111 1111", "13812345678", "zhang.wei@example.com"}
	n := max(1, size/32)
	withPII := int(float64(n)*density + 0.5)
	if density > 0 {
		withPII = max(1, withPII)
	}
	um := map[string]any{}
	total := 0
	for i := 0; i < n; i++ {
		v := fmt.Sprintf("req %04d ts=1760688000000 h%02d", i, i%50)
		if i < withPII {
			v = fmt.Sprintf("u %s r%d", pii[i%len(pii)], i%10)
		}
		if len(v) < 32 {
			v += strings.Repeat(".", 32-len(v))
		}
		v = v[:32]
		um[fmt.Sprintf("f%03d", i)] = v
		total += len(v)
	}
	orig := map[string]any{}
	for k, v := range um {
		orig[k] = v
	}
	ev := normalize.JSON(um, time.Unix(1_760_688_000, 0)).Fields
	return ev, orig, total, fmt.Sprintf("%d/%d", withPII, n)
}

func BenchmarkMaskEvent(b *testing.B) {
	e, err := New(Config{Detector: DetectorConfig{Today: testToday}})
	if err != nil {
		b.Fatal(err)
	}
	for _, size := range []int{200, 1024, 4096} {
		for _, d := range []float64{0, 0.1, 0.5} {
			ev, orig, n, frac := benchEvent(size, d)
			b.Run(fmt.Sprintf("size=%d/pii=%d%%(%s)", size, int(d*100), frac), func(b *testing.B) {
				um := ev["unmapped"].(map[string]any)
				b.SetBytes(int64(n))
				b.ReportAllocs()
				for b.Loop() {
					if d > 0 {
						for k, v := range orig { // restore masked fields
							um[k] = v
						}
					}
					e.MaskEvent(ev)
				}
			})
		}
	}
}

func BenchmarkMaskString(b *testing.B) {
	e, err := New(Config{Detector: DetectorConfig{Today: testToday}})
	if err != nil {
		b.Fatal(err)
	}
	for _, size := range []int{200, 1024, 4096} {
		for _, withPII := range []bool{false, true} {
			line := strings.Repeat("GET /api/v1/orders/20261017000123?ts=1760688000000&trace=4bf92f3577b34da6 200 ", size/40+2)[:size]
			if withPII {
				line = line[:size/2] + " tel 13812345678 id 11010519491231002X " + line[size/2+40:]
			}
			b.Run(fmt.Sprintf("size=%d/pii=%v", size, withPII), func(b *testing.B) {
				b.SetBytes(int64(len(line)))
				b.ReportAllocs()
				for b.Loop() {
					e.MaskString(line)
				}
			})
		}
	}
}

// BenchmarkPipeline1KB is the ingest-path cost per 1 KB JSON event:
// decode, normalise, (mask), encode — masking on vs off.
func BenchmarkPipeline1KB(b *testing.B) {
	e, err := New(Config{Detector: DetectorConfig{Today: testToday}})
	if err != nil {
		b.Fatal(err)
	}
	ev, _, _, _ := benchEvent(1024, 0.1)
	raw, err := json.Marshal(ev["unmapped"])
	if err != nil {
		b.Fatal(err)
	}
	for _, on := range []bool{false, true} {
		b.Run(fmt.Sprintf("mask=%v", on), func(b *testing.B) {
			b.SetBytes(int64(len(raw)))
			b.ReportAllocs()
			for b.Loop() {
				var m map[string]any
				dec := json.NewDecoder(strings.NewReader(string(raw)))
				dec.UseNumber()
				if err := dec.Decode(&m); err != nil {
					b.Fatal(err)
				}
				f := normalize.JSON(m, time.Unix(1_760_688_000, 0)).Fields
				if on {
					e.MaskEvent(f)
				}
				if _, err := json.Marshal(f); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
