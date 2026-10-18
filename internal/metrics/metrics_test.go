package metrics

import (
	"fmt"
	"io"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestObserverCounters(t *testing.T) {
	m := New()
	now := time.Unix(1000, 0)
	m.IngestEvents("http:app", "accepted", 5)
	m.IngestEvents("http:app", "backpressure", 1)
	m.IngestEvents("http:app", "accepted", 0) // ignored
	m.Delivered("soar", "live", now.Add(-50*time.Millisecond), now)
	m.Delivered("soar", "redeliver", now.Add(-2*time.Second), now)
	m.DeliveryError("soar", "retryable", now)
	m.DLQ("soar", "non_retryable", 3)
	m.WALFsync(3 * time.Millisecond)

	if v := testutil.ToFloat64(m.ingest.WithLabelValues("http:app", "accepted")); v != 5 {
		t.Fatalf("ingest accepted = %v", v)
	}
	if v := testutil.ToFloat64(m.delivered.WithLabelValues("soar", "redeliver")); v != 1 {
		t.Fatalf("delivered redeliver = %v", v)
	}
	if v := testutil.ToFloat64(m.delivErr.WithLabelValues("soar", "retryable")); v != 1 {
		t.Fatalf("errors = %v", v)
	}
	if v := testutil.ToFloat64(m.dlq.WithLabelValues("soar", "non_retryable")); v != 3 {
		t.Fatalf("dlq = %v", v)
	}
	if n := testutil.CollectAndCount(m.e2e); n != 1 {
		t.Fatalf("e2e series = %d", n)
	}
	if a, r := m.IngestTotals(); a != 5 || r != 1 {
		t.Fatalf("totals = %d %d", a, r)
	}
	v := m.View("soar", now, time.Minute)
	if v.Delivered != 2 || v.Errors != 1 || v.DLQ != 3 || v.LastErrorKind != "retryable" || v.Samples != 2 {
		t.Fatalf("view = %+v", v)
	}
	if v.P99 != 2*time.Second {
		t.Fatalf("p99 = %v", v.P99)
	}
	if v := m.View("soar", now.Add(2*time.Minute), time.Minute); v.Samples != 0 || v.P99 != 0 {
		t.Fatalf("window not applied: %+v", v)
	}
}

func TestSnapshotGauges(t *testing.T) {
	m := New()
	if n := testutil.CollectAndCount(&snapshotCollector{m: m}); n != 0 {
		t.Fatalf("no source: %d series", n)
	}
	m.SetSource(func() (Snapshot, bool) {
		return Snapshot{
			WALBytes: 4096, Backpressure: true,
			MaskHits: map[string]uint64{"phone": 7, "id_card": 2},
			Dests: []DestSnapshot{
				{Name: "soar", State: "active", Breaker: "open", L2Depth: 3, L1Pending: 42, Watermark: 99, LevelBlocked: 5},
				{Name: "ghost", State: "absent", L1Pending: 1},
			},
		}, true
	})
	want := `
# HELP loggate_l1_pending Events parked in L1 (bbolt) awaiting redelivery to the destination.
# TYPE loggate_l1_pending gauge
loggate_l1_pending{dest="ghost"} 1
loggate_l1_pending{dest="soar"} 42
# HELP loggate_breaker_state Circuit breaker state: 0 closed, 1 half_open, 2 open.
# TYPE loggate_breaker_state gauge
loggate_breaker_state{dest="soar"} 2
# HELP loggate_backpressure_active 1 while ingest refuses new events with 429 (L1 or disk quota).
# TYPE loggate_backpressure_active gauge
loggate_backpressure_active 1
# HELP loggate_mask_hits_total Masked PII spans by detector type (since gateway start).
# TYPE loggate_mask_hits_total counter
loggate_mask_hits_total{type="id_card"} 2
loggate_mask_hits_total{type="phone"} 7
# HELP loggate_wal_bytes Durable (fsynced) bytes in WAL segments.
# TYPE loggate_wal_bytes gauge
loggate_wal_bytes 4096
`
	if err := testutil.GatherAndCompare(m.Registry(), strings.NewReader(want),
		"loggate_l1_pending", "loggate_breaker_state", "loggate_backpressure_active", "loggate_mask_hits_total", "loggate_wal_bytes"); err != nil {
		t.Fatal(err)
	}
	m.SetSource(func() (Snapshot, bool) { return Snapshot{}, false })
	if n := testutil.CollectAndCount(&snapshotCollector{m: m}); n != 0 {
		t.Fatalf("source not ready: %d series", n)
	}
}

func TestLabelCardinalityBounded(t *testing.T) {
	m := New()
	now := time.Now()
	for i := range 500 {
		m.Delivered(fmt.Sprintf("dest-%d", i), "live", now, now)
		m.IngestEvents(fmt.Sprintf("http:src-%d", i), "accepted", 1)
		m.DLQ("dest-0", fmt.Sprintf("reason-%d", i), 1)
	}
	if n := testutil.CollectAndCount(m.e2e); n != MaxDests+1 {
		t.Fatalf("e2e series = %d, want %d (limit + other)", n, MaxDests+1)
	}
	if n := testutil.CollectAndCount(m.ingest); n != MaxSources+1 {
		t.Fatalf("ingest series = %d", n)
	}
	if n := testutil.CollectAndCount(m.dlq); n != MaxReasons+1 {
		t.Fatalf("dlq series = %d", n)
	}
	if v := m.View(Other, now, time.Minute); v.Delivered != 500-MaxDests {
		t.Fatalf("other delivered = %d", v.Delivered)
	}
}

func TestHandlerServesExposition(t *testing.T) {
	m := New()
	m.WALFsync(time.Millisecond)
	srv := httptest.NewServer(m.Handler())
	defer srv.Close()
	resp, err := srv.Client().Get(srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	for _, want := range []string{"loggate_wal_fsync_seconds_count 1", "go_goroutines"} {
		if !strings.Contains(string(b), want) {
			t.Fatalf("exposition lacks %q", want)
		}
	}
}
