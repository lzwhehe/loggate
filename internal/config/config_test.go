package config

import (
	"strings"
	"testing"
	"time"
)

const validYAML = `
node_id: gw-test
data_dir: /tmp/lg
wal: { segment_bytes: 4MiB, fsync_interval: 5ms, fsync_max_events: 128 }
ingest:
  http:
    listen: "127.0.0.1:0"
    max_body: 512KiB
    tokens:
      - { source: app1, token: "s3cret" }
idempotency: { key_env: MY_KEY }
destinations:
  - { name: archive, type: file, config: { path: ./out/archive.jsonl } }
routes:
  - { match: {}, to: [archive] }
`

func TestParseValid(t *testing.T) {
	c, err := Parse([]byte(validYAML))
	if err != nil {
		t.Fatal(err)
	}
	if c.WAL.SegmentBytes != 4<<20 || c.WAL.FsyncInterval != 5*time.Millisecond || c.WAL.FsyncMaxEvents != 128 {
		t.Fatalf("wal = %+v", c.WAL)
	}
	if c.Ingest.HTTP.MaxBody != 512<<10 {
		t.Fatalf("max_body = %d", c.Ingest.HTTP.MaxBody)
	}
	if c.Destinations[0].Config["path"] != "./out/archive.jsonl" {
		t.Fatalf("dest config = %v", c.Destinations[0].Config)
	}
	if c.Idempotency.KeyEnv != "MY_KEY" {
		t.Fatalf("key env = %q", c.Idempotency.KeyEnv)
	}
}

func TestDefaults(t *testing.T) {
	c, err := Parse([]byte(`
destinations: [{ name: a, type: file, config: { path: a.jsonl } }]
routes: [{ match: {}, to: [a] }]
`))
	if err != nil {
		t.Fatal(err)
	}
	if c.NodeID != DefaultNodeID || c.DataDir != DefaultDataDir || c.WAL.SegmentBytes != DefaultSegmentBytes ||
		c.WAL.FsyncInterval != DefaultFsyncInterval || c.WAL.FsyncMaxEvents != DefaultFsyncMaxEvents ||
		c.Ingest.HTTP.Listen != DefaultHTTPListen || c.Ingest.HTTP.MaxBody != DefaultMaxBody {
		t.Fatalf("defaults not applied: %+v", c)
	}
}

func TestValidationErrors(t *testing.T) {
	cases := map[string]struct{ yaml, want string }{
		"unknown field": {`foo: 1`, "field foo not found"},
		"bad type": {`
destinations: [{ name: a, type: kafka }]
routes: [{ match: {}, to: [a] }]`, "unsupported type"},
		"missing path": {`
destinations: [{ name: a, type: file }]
routes: [{ match: {}, to: [a] }]`, "config.path is required"},
		"non-scalar match": {`
destinations: [{ name: a, type: file, config: { path: x } }]
routes: [{ match: { class_uid: [1, 2] }, to: [a] }]`, "scalar equality"},
		"share too small": {`
retry: { redeliver_share: 0.1 }
destinations: [{ name: a, type: file, config: { path: x } }]
routes: [{ match: {}, to: [a] }]`, "redeliver_share"},
		"bad on_quota": {`
l1: { on_quota: drop_oldest_to_dlq }
destinations: [{ name: a, type: file, config: { path: x } }]
routes: [{ match: {}, to: [a] }]`, "on_quota"},
		"webhook without url": {`
destinations: [{ name: a, type: webhook }]
routes: [{ match: {}, to: [a] }]`, "config.url is required"},
		"duplicate dest": {`
destinations: [{ name: a, type: file, config: { path: x } }, { name: a, type: file, config: { path: y } }]
routes: [{ match: {}, to: [a] }]`, "duplicate name"},
		"tiny segment": {`
wal: { segment_bytes: 1KiB }
destinations: [{ name: a, type: file, config: { path: x } }]
routes: [{ match: {}, to: [a] }]`, "segment_bytes"},
		"bad size": {`
wal: { segment_bytes: 12XB }
destinations: [{ name: a, type: file, config: { path: x } }]
routes: [{ match: {}, to: [a] }]`, "invalid size"},
		"token both": {`
ingest: { http: { tokens: [{ source: s, token: t, token_env: E }] } }
destinations: [{ name: a, type: file, config: { path: x } }]
routes: [{ match: {}, to: [a] }]`, "exactly one of token"},
		"admin both": {`
admin: { token: "0123456789abcdef", token_env: E }
destinations: [{ name: a, type: file, config: { path: x } }]
routes: [{ match: {}, to: [a] }]`, "only one of token"},
		"admin short": {`
admin: { token: short }
destinations: [{ name: a, type: file, config: { path: x } }]
routes: [{ match: {}, to: [a] }]`, "at least 16 bytes"},
		"disk negative interval": {`
disk: { max_bytes: 1MiB, check_interval: -1s }
destinations: [{ name: a, type: file, config: { path: x } }]
routes: [{ match: {}, to: [a] }]`, "disk:"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := Parse([]byte(tc.yaml))
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err = %v, want containing %q", err, tc.want)
			}
		})
	}
}

func TestParseByteSize(t *testing.T) {
	cases := map[string]ByteSize{
		"64MiB": 64 << 20, "1MB": 1_000_000, "512KiB": 512 << 10, "4096": 4096,
		"5_000_000": 5_000_000, "8GiB": 8 << 30, "10 k": 10 << 10, "7b": 7,
	}
	for in, want := range cases {
		got, err := ParseByteSize(in)
		if err != nil || got != want {
			t.Errorf("ParseByteSize(%q) = %d, %v; want %d", in, got, err, want)
		}
	}
	for _, bad := range []string{"", "MiB", "-1", "1.5MiB", "99999999999999GiB"} {
		if _, err := ParseByteSize(bad); err == nil {
			t.Errorf("ParseByteSize(%q) succeeded", bad)
		}
	}
}

func TestExampleConfigLoads(t *testing.T) {
	if _, err := Load("../../configs/example.yaml"); err != nil {
		t.Fatal(err)
	}
}

func TestM2KeysAndAbsentDestinations(t *testing.T) {
	c, err := Parse([]byte(`
retry: { base: 100ms, max: 5s, max_attempts: 3, max_age: 1h, redeliver_share: 0.25 }
l1: { checkpoint_interval: 500ms, orphan_ttl: 30s, max_records_per_dest: 100, on_quota: dlq_overflow }
destinations:
  - { name: archive, type: file, queue: { max_events: 50, max_bytes: 1MiB }, config: { path: x } }
  - { name: siem, type: sqlite, breaker: { failures: 2, open_for: 1s }, config: { path: y.db } }
  - { name: soar, type: webhook, config: { url: "http://127.0.0.1:9100/ingest", timeout: 2s } }
routes:
  - { match: { class_uid: 3002 }, to: [archive, siem, soar] }
  - { match: {}, to: [archive, ghost] }
`))
	if err != nil {
		t.Fatal(err)
	}
	if c.Retry.MaxAttempts != 3 || c.L1.OrphanTTL.Seconds() != 30 || c.L1.OnQuota != "dlq_overflow" {
		t.Fatalf("retry/l1 not parsed: %+v %+v", c.Retry, c.L1)
	}
	if c.Destinations[0].Queue.MaxEvents != 50 || c.Destinations[0].Queue.MaxBatch != DefaultMaxBatch {
		t.Fatalf("queue: %+v", c.Destinations[0].Queue)
	}
	if c.Destinations[1].Breaker.Failures != 2 || c.Destinations[2].Breaker.Failures != DefaultBreakerFailures {
		t.Fatal("breaker defaults")
	}
	if abs := c.AbsentDestinations(); len(abs) != 1 || abs[0] != "ghost" {
		t.Fatalf("absent = %v", abs)
	}
}
