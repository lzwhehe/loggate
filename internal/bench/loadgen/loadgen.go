// Package loadgen is the benchmark's open-loop load generator (DESIGN
// §10.1).
//
// Event i (0-based, test_seq = i+1) is scheduled at start + i/rate. The
// sender never waits for a response before sending the next event: each
// send runs in its own goroutine, bounded only by MaxInFlight. Latency is
// measured from the scheduled send time, not the actual one, so a stalled
// server cannot hide queueing delay (no coordinated omission). If the
// client itself falls behind (dispatch later than Behind after schedule, or
// the in-flight bound is hit) that is counted and reported, because it
// would make the offered load lower than requested.
//
// The "promised set" is every test_seq whose request returned HTTP 200.
// 429/503/other statuses and transport errors are recorded separately. No
// client-side retries by default.
package loadgen

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/HdrHistogram/hdrhistogram-go"
)

// Class is one weighted event class.
type Class struct {
	Name   string
	Weight float64
}

// Config configures a run.
type Config struct {
	URL      string // e.g. http://127.0.0.1:8080/v1/events
	Token    string // bearer token bound to the bench source
	RunID    string
	Rate     float64 // events per second
	Duration time.Duration
	// Start is the scheduled time of event 0; zero = now.
	Start       time.Time
	Seed        uint64
	Classes     []Class
	PayloadSize int // approximate bytes of synthetic message text
	MaxInFlight int
	Timeout     time.Duration
	// Retries is the number of client-side retries on a non-200 response
	// (default 0). A retried event keeps its test_seq and scheduled time.
	Retries    int
	RetryDelay time.Duration
	// Behind is the dispatch lag above which the client counts itself as
	// falling behind (default 10ms).
	Behind time.Duration
	OutDir string
}

// Count returns how many events a run of d at rate sends.
func Count(rate float64, d time.Duration) int {
	return int(math.Floor(rate * d.Seconds()))
}

// ScheduledAt returns the scheduled send time of event i (0-based).
func ScheduledAt(start time.Time, rate float64, i int) time.Time {
	return start.Add(time.Duration(math.Round(float64(i) * 1e9 / rate)))
}

// mix64 is SplitMix64's finaliser.
func mix64(z uint64) uint64 {
	z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9
	z = (z ^ (z >> 27)) * 0x94D049BB133111EB
	return z ^ (z >> 31)
}

// ClassOf deterministically picks the class of test_seq from the seed, so
// the load mix is identical across repetitions and independent of
// goroutine scheduling.
func ClassOf(classes []Class, seed uint64, testSeq uint64) string {
	var total float64
	for _, c := range classes {
		total += c.Weight
	}
	u := float64(mix64(seed+testSeq*0x9E3779B97F4A7C15)>>11) / float64(1<<53) * total
	for _, c := range classes {
		if u < c.Weight {
			return c.Name
		}
		u -= c.Weight
	}
	return classes[len(classes)-1].Name
}

// Result of one event.
type Result struct {
	TestSeq     uint64
	Class       string
	ScheduledNS int64
	SentNS      int64 // first dispatch
	DoneNS      int64 // final response (or error)
	Status      int   // final HTTP status; 0 = transport error
	Attempts    int
}

// Acked reports whether the event is in the promised set.
func (r Result) Acked() bool { return r.Status == http.StatusOK }

// Summary is written to summary.json.
type Summary struct {
	RunID          string         `json:"run_id"`
	Rate           float64        `json:"rate"`
	DurationS      float64        `json:"duration_s"`
	StartNS        int64          `json:"start_ns"`
	Planned        int            `json:"planned"`
	Acked          int            `json:"acked"`
	ByStatus       map[string]int `json:"by_status"` // "200", "429", "503", "error", ...
	BehindCount    int            `json:"behind_count"`
	MaxLagMS       float64        `json:"max_dispatch_lag_ms"`
	InFlightCapHit int            `json:"inflight_cap_hits"`
	MaxInFlight    int64          `json:"max_inflight_observed"`
	Retries        int            `json:"retries_configured"`
	AckLatencyMS   Percentiles    `json:"ack_latency_ms"` // acked events, from scheduled time
	AchievedRate   float64        `json:"achieved_rate"`
}

// Percentiles of a latency distribution in milliseconds.
type Percentiles struct {
	Count int64   `json:"count"`
	P50   float64 `json:"p50"`
	P90   float64 `json:"p90"`
	P99   float64 `json:"p99"`
	P999  float64 `json:"p999"`
	Max   float64 `json:"max"`
}

// FromHist reads percentiles from a microsecond histogram.
func FromHist(h *hdrhistogram.Histogram) Percentiles {
	ms := func(v int64) float64 { return float64(v) / 1000 }
	return Percentiles{
		Count: h.TotalCount(),
		P50:   ms(h.ValueAtQuantile(50)), P90: ms(h.ValueAtQuantile(90)),
		P99: ms(h.ValueAtQuantile(99)), P999: ms(h.ValueAtQuantile(99.9)), Max: ms(h.Max()),
	}
}

// Body builds the JSON object for one event, padded to about payload
// bytes with synthetic text (no PII).
func Body(runID string, testSeq uint64, scheduledNS int64, class string, payload int) []byte {
	var b bytes.Buffer
	b.Grow(payload + 160)
	b.WriteString(`{"run_id":"`)
	b.WriteString(runID)
	b.WriteString(`","test_seq":`)
	b.WriteString(strconv.FormatUint(testSeq, 10))
	b.WriteString(`,"scheduled_send_ns":`)
	b.WriteString(strconv.FormatInt(scheduledNS, 10))
	b.WriteString(`,"class":"`)
	b.WriteString(class)
	b.WriteString(`","msg":"synthetic bench event `)
	b.WriteString(class)
	b.WriteString(` `)
	pad := payload - b.Len() - 2 // closing quote and brace
	for i := 0; i < pad; i++ {
		b.WriteByte('a' + byte((testSeq+uint64(i))%26))
	}
	b.WriteString(`"}`)
	return b.Bytes()
}

// Run executes the load and writes sent.csv, acked.bitmap and
// summary.json to cfg.OutDir.
func Run(ctx context.Context, cfg Config) (*Summary, []Result, error) {
	if cfg.Rate <= 0 || cfg.Duration <= 0 || cfg.URL == "" || len(cfg.Classes) == 0 {
		return nil, nil, errors.New("loadgen: url, rate, duration and classes are required")
	}
	if cfg.MaxInFlight <= 0 {
		cfg.MaxInFlight = 4096
	}
	if cfg.Timeout <= 0 {
		cfg.Timeout = 10 * time.Second
	}
	if cfg.Behind <= 0 {
		cfg.Behind = 10 * time.Millisecond
	}
	if cfg.RetryDelay <= 0 {
		cfg.RetryDelay = 100 * time.Millisecond
	}
	if cfg.Start.IsZero() {
		cfg.Start = time.Now()
	}
	n := Count(cfg.Rate, cfg.Duration)
	results := make([]Result, n)
	tr := &http.Transport{
		MaxIdleConns: cfg.MaxInFlight, MaxIdleConnsPerHost: cfg.MaxInFlight,
		IdleConnTimeout: 30 * time.Second, DisableCompression: true,
	}
	defer tr.CloseIdleConnections()
	client := &http.Client{Transport: tr, Timeout: cfg.Timeout}

	sem := make(chan struct{}, cfg.MaxInFlight)
	var wg sync.WaitGroup
	var inflight, maxInflight atomic.Int64
	var behind, capHits int
	var maxLag time.Duration

	for i := 0; i < n; i++ {
		sched := ScheduledAt(cfg.Start, cfg.Rate, i)
		if d := time.Until(sched); d > 0 {
			select {
			case <-ctx.Done():
				return nil, nil, ctx.Err()
			case <-time.After(d):
			}
		}
		select {
		case sem <- struct{}{}:
		default:
			capHits++ // the client is the bottleneck: report it
			sem <- struct{}{}
		}
		now := time.Now()
		if lag := now.Sub(sched); lag > cfg.Behind {
			behind++
			maxLag = max(maxLag, lag)
		} else {
			maxLag = max(maxLag, max(lag, 0))
		}
		seq := uint64(i + 1)
		class := ClassOf(cfg.Classes, cfg.Seed, seq)
		results[i] = Result{TestSeq: seq, Class: class, ScheduledNS: sched.UnixNano(), SentNS: now.UnixNano()}
		if v := inflight.Add(1); v > maxInflight.Load() {
			maxInflight.Store(v)
		}
		wg.Add(1)
		go func(r *Result) {
			defer func() { inflight.Add(-1); <-sem; wg.Done() }()
			body := Body(cfg.RunID, r.TestSeq, r.ScheduledNS, r.Class, cfg.PayloadSize)
			for attempt := 0; ; attempt++ {
				r.Attempts = attempt + 1
				r.Status = send(ctx, client, cfg.URL, cfg.Token, body)
				r.DoneNS = time.Now().UnixNano()
				if r.Status == http.StatusOK || attempt >= cfg.Retries || ctx.Err() != nil {
					return
				}
				time.Sleep(cfg.RetryDelay)
			}
		}(&results[i])
	}
	wg.Wait()

	sum := &Summary{
		RunID: cfg.RunID, Rate: cfg.Rate, DurationS: cfg.Duration.Seconds(), StartNS: cfg.Start.UnixNano(),
		Planned: n, ByStatus: map[string]int{}, BehindCount: behind, MaxLagMS: float64(maxLag) / 1e6,
		InFlightCapHit: capHits, MaxInFlight: maxInflight.Load(), Retries: cfg.Retries,
	}
	h := hdrhistogram.New(1, 3_600_000_000, 3) // 1µs .. 1h
	var lastSent int64
	for _, r := range results {
		key := "error"
		if r.Status != 0 {
			key = strconv.Itoa(r.Status)
		}
		sum.ByStatus[key]++
		if r.Acked() {
			sum.Acked++
			_ = h.RecordValue(max(1, (r.DoneNS-r.ScheduledNS)/1000))
		}
		lastSent = max(lastSent, r.SentNS)
	}
	if n > 1 && lastSent > results[0].SentNS {
		sum.AchievedRate = float64(n-1) / (float64(lastSent-results[0].SentNS) / 1e9)
	}
	sum.AckLatencyMS = FromHist(h)
	if cfg.OutDir != "" {
		if err := WriteOutputs(cfg.OutDir, sum, results); err != nil {
			return sum, results, err
		}
	}
	return sum, results, nil
}

func send(ctx context.Context, c *http.Client, url, token string, body []byte) int {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return 0
	}
	req.Header.Set("Content-Type", "application/json")
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := c.Do(req)
	if err != nil {
		return 0
	}
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 1<<16))
	resp.Body.Close()
	return resp.StatusCode
}

// SentHeader is the sent.csv header.
const SentHeader = "test_seq,class,scheduled_ns,sent_ns,done_ns,status,attempts"

// WriteOutputs writes sent.csv, acked.bitmap (bit test_seq-1 set when
// acked) and summary.json.
func WriteOutputs(dir string, sum *Summary, results []Result) error {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	f, err := os.Create(filepath.Join(dir, "sent.csv"))
	if err != nil {
		return err
	}
	w := bufio.NewWriterSize(f, 1<<20)
	fmt.Fprintln(w, SentHeader)
	bitmap := make([]byte, (len(results)+7)/8)
	for i, r := range results {
		fmt.Fprintf(w, "%d,%s,%d,%d,%d,%d,%d\n", r.TestSeq, r.Class, r.ScheduledNS, r.SentNS, r.DoneNS, r.Status, r.Attempts)
		if r.Acked() {
			bitmap[i/8] |= 1 << (i % 8)
		}
	}
	if err := errors.Join(w.Flush(), f.Close()); err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(dir, "acked.bitmap"), bitmap, 0o644); err != nil {
		return err
	}
	b, err := json.MarshalIndent(sum, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(dir, "summary.json"), b, 0o644)
}

// ReadSent parses sent.csv.
func ReadSent(path string) ([]Result, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<16), 1<<20)
	var out []Result
	line := 0
	for sc.Scan() {
		line++
		t := sc.Text()
		if line == 1 && t == SentHeader {
			continue
		}
		p := strings.Split(t, ",")
		if len(p) != 7 {
			return nil, fmt.Errorf("sent.csv line %d: want 7 fields", line)
		}
		var r Result
		var errs [6]error
		r.TestSeq, errs[0] = strconv.ParseUint(p[0], 10, 64)
		r.Class = p[1]
		r.ScheduledNS, errs[1] = strconv.ParseInt(p[2], 10, 64)
		r.SentNS, errs[2] = strconv.ParseInt(p[3], 10, 64)
		r.DoneNS, errs[3] = strconv.ParseInt(p[4], 10, 64)
		r.Status, errs[4] = strconv.Atoi(p[5])
		r.Attempts, errs[5] = strconv.Atoi(p[6])
		if err := errors.Join(errs[:]...); err != nil {
			return nil, fmt.Errorf("sent.csv line %d: %w", line, err)
		}
		out = append(out, r)
	}
	return out, sc.Err()
}

// ParseClasses parses "auth=0.5,net=0.3,app=0.2".
func ParseClasses(s string) ([]Class, error) {
	var out []Class
	for _, part := range strings.Split(s, ",") {
		name, w, ok := strings.Cut(strings.TrimSpace(part), "=")
		if !ok || name == "" {
			return nil, fmt.Errorf("class %q: want name=weight", part)
		}
		wt, err := strconv.ParseFloat(w, 64)
		if err != nil || wt <= 0 {
			return nil, fmt.Errorf("class %q: bad weight", part)
		}
		out = append(out, Class{Name: name, Weight: wt})
	}
	return out, nil
}
