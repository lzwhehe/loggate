package gateway

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/lzwhehe/loggate/internal/mask"
	"github.com/lzwhehe/loggate/internal/metrics"
	"github.com/lzwhehe/loggate/internal/sink"
)

// After traffic every metric family of DESIGN §8 is exposed with the
// expected label sets, and the counters agree with what was sent.
func TestMetricsAfterTraffic(t *testing.T) {
	dir := t.TempDir()
	cfg := testCfg(t, dir, `
destinations:
  - { name: archive, type: file, config: { path: "$OUT/archive.jsonl" } }
  - { name: soar, type: file, max_level: general, breaker: { failures: 1000 }, config: { path: "fake:soar" } }
routes:
  - { match: {}, to: [archive, soar] }
`)
	soar := newFake()
	soar.setFail(func(env *sink.Envelope) error {
		if env.Seq%10 == 0 && env.Attempt == 1 {
			return errDown
		}
		return nil
	})
	m := metrics.New()
	g := start(t, cfg, map[string]*fakeSink{"soar": soar}, func(o *Options) { o.Metrics = m })
	m.SetSource(g.MetricsSnapshot)

	id := "11010519491231002"
	id += string(mask.IDChecksum(id))
	card := "622202123456789012"
	card += string(mask.LuhnDigit([]byte(card)))
	acked := post(t, g.Addr(), 40, func(i int) string {
		if i%4 == 0 { // id card + bank card -> important -> blocked for soar
			return `{"msg":"id ` + id + ` card ` + card + `"}`
		}
		return `{"msg":"call 13812345678"}`
	})
	eventually(t, "delivery", func() bool {
		return g.Delivered() == uint64(len(acked)+len(acked)*3/4)
	})

	if a, _ := m.IngestTotals(); a != 40 {
		t.Fatalf("ingest accepted = %v", a)
	}
	if n, err := testutil.GatherAndCount(m.Registry(), "loggate_e2e_latency_seconds"); err != nil || n != 2 {
		t.Fatalf("e2e series = %d (%v), want 2", n, err)
	}
	body := scrape(t, m)
	for _, want := range []string{
		`loggate_ingest_events_total{result="accepted",source="http:anonymous"} 40`,
		`loggate_delivered_total{dest="archive",origin="live"} 40`,
		`loggate_delivery_errors_total{dest="soar",kind="retryable"}`,
		`loggate_delivered_total{dest="soar",origin="redeliver"}`,
		`loggate_route_level_blocked_total{dest="soar"} 10`,
		`loggate_mask_hits_total{type="phone"} 30`,
		`loggate_mask_hits_total{type="id_card"} 10`,
		`loggate_l1_pending{dest="soar"}`,
		`loggate_l2_depth{dest="archive"}`,
		`loggate_breaker_state{dest="soar"} 0`,
		`loggate_plugin_state{dest="archive"} 2`,
		`loggate_watermark{dest="archive"}`,
		`loggate_backpressure_active 0`,
		`loggate_wal_bytes `,
		`loggate_wal_fsync_seconds_count`,
		`loggate_e2e_latency_seconds_bucket{dest="soar"`,
	} {
		if !strings.Contains(body, want) {
			t.Errorf("metrics lack %q", want)
		}
	}
	shutdown(t, g)
	// After shutdown the snapshot source reports "not running": gauges
	// disappear, counters stay.
	body = scrape(t, m)
	if strings.Contains(body, "loggate_l1_pending") || !strings.Contains(body, "loggate_delivered_total") {
		t.Fatalf("unexpected exposition after shutdown")
	}
}

func scrape(t *testing.T, m *metrics.Metrics) string {
	t.Helper()
	rec := httptest.NewRecorder()
	m.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("scrape status %d", rec.Code)
	}
	return rec.Body.String()
}
