// Package config loads and validates the LogGate YAML configuration.
//
// Only the subset needed by the current milestone is modelled; unknown keys
// are rejected so that typos (or not-yet-implemented features) fail loudly
// instead of being silently ignored.
package config

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/lzwhehe/loggate/internal/event"
)

// Config is the root configuration.
type Config struct {
	NodeID       string        `yaml:"node_id"`
	DataDir      string        `yaml:"data_dir"`
	WAL          WAL           `yaml:"wal"`
	Ingest       Ingest        `yaml:"ingest"`
	Idempotency  Idempotency   `yaml:"idempotency"`
	Retry        Retry         `yaml:"retry"`
	L1           L1            `yaml:"l1"`
	Disk         Disk          `yaml:"disk"`
	Admin        Admin         `yaml:"admin"`
	Destinations []Destination `yaml:"destinations"`
	Routes       []Route       `yaml:"routes"`
}

// WAL configures the write-ahead log.
type WAL struct {
	SegmentBytes   ByteSize      `yaml:"segment_bytes"`
	FsyncInterval  time.Duration `yaml:"fsync_interval"`
	FsyncMaxEvents int           `yaml:"fsync_max_events"`
}

// Ingest groups all input listeners.
type Ingest struct {
	HTTP HTTPIngest `yaml:"http"`
}

// HTTPIngest configures POST /v1/events.
type HTTPIngest struct {
	Listen  string   `yaml:"listen"`
	MaxBody ByteSize `yaml:"max_body"`
	// Tokens binds bearer tokens to source names. If empty, all requests are
	// accepted as source "http:anonymous".
	Tokens []Token `yaml:"tokens"`
}

// Token maps one bearer token (inline or from an environment variable) to a
// source name. Exactly one of Token and TokenEnv must be set.
type Token struct {
	Source   string `yaml:"source"`
	Token    string `yaml:"token"`
	TokenEnv string `yaml:"token_env"`
}

// Resolve returns the token value.
func (t Token) Resolve() (string, error) {
	if t.TokenEnv != "" {
		v := os.Getenv(t.TokenEnv)
		if v == "" {
			return "", fmt.Errorf("token for source %q: env %s is empty", t.Source, t.TokenEnv)
		}
		return v, nil
	}
	return t.Token, nil
}

// Idempotency configures the gateway-assigned IdemKey HMAC.
type Idempotency struct {
	// KeyEnv names the environment variable holding the HMAC key
	// (>= 16 bytes). If the variable is unset a random per-process key is
	// generated; keys already written to the WAL stay valid either way.
	KeyEnv string `yaml:"key_env"`
}

// Retry configures redelivery backoff and the retry budget (DESIGN §4.3).
type Retry struct {
	Base        time.Duration `yaml:"base"`         // full-jitter base; default 200ms
	Max         time.Duration `yaml:"max"`          // backoff cap; default 30s
	MaxAttempts int           `yaml:"max_attempts"` // real send attempts before DLQ; default 20
	MaxAge      time.Duration `yaml:"max_age"`      // event age before DLQ; default 24h
	// RedeliverShare is the fraction of each L2 queue reserved for
	// redelivery while the destination has an L1 backlog (>= 0.2).
	RedeliverShare    float64       `yaml:"redeliver_share"`
	RedeliverInterval time.Duration `yaml:"redeliver_interval"` // default 200ms
}

// L1 configures the parking area, checkpoints, sweeper and GC.
type L1 struct {
	CheckpointInterval time.Duration `yaml:"checkpoint_interval"` // default 1s
	OrphanTTL          time.Duration `yaml:"orphan_ttl"`          // default 24h
	// MaxRecordsPerDest is the per-destination L1 quota (0 = unlimited).
	MaxRecordsPerDest int64         `yaml:"max_records_per_dest"`
	OnQuota           string        `yaml:"on_quota"`       // block_all (default) | dlq_overflow
	SweepInterval     time.Duration `yaml:"sweep_interval"` // default 1s
	GCInterval        time.Duration `yaml:"gc_interval"`    // default 10s
	TombstoneTTL      time.Duration `yaml:"tombstone_ttl"`  // default 24h
}

// Disk configures the data_dir disk quota (DESIGN §4.9, §0.5 item 10).
// Usage counts everything under data_dir: WAL segments, state.db (L1,
// checkpoints, registry) and the DLQ. While usage exceeds MaxBytes, ingest
// answers 429 with Retry-After. Already-acked events are never dropped.
type Disk struct {
	MaxBytes      ByteSize      `yaml:"max_bytes"`      // 0 = unlimited (default)
	CheckInterval time.Duration `yaml:"check_interval"` // default 1s
}

// Admin enables the token-protected admin endpoints under /v1/admin/
// (register, unregister, shutdown). They are disabled unless a token is set.
// Exactly one of Token and TokenEnv may be set.
type Admin struct {
	Token    string `yaml:"token"`
	TokenEnv string `yaml:"token_env"`
}

// Enabled reports whether an admin token is configured.
func (a Admin) Enabled() bool { return a.Token != "" || a.TokenEnv != "" }

// Resolve returns the admin token.
func (a Admin) Resolve() (string, error) {
	if a.TokenEnv != "" {
		v := os.Getenv(a.TokenEnv)
		if len(v) < 16 {
			return "", fmt.Errorf("admin token: env %s must hold at least 16 bytes", a.TokenEnv)
		}
		return v, nil
	}
	if len(a.Token) < 16 {
		return "", errors.New("admin.token must be at least 16 bytes")
	}
	return a.Token, nil
}

// Queue bounds a destination's L2 queue and its batching.
type Queue struct {
	MaxEvents int           `yaml:"max_events"` // default 10000
	MaxBytes  ByteSize      `yaml:"max_bytes"`  // default 64MiB
	MaxBatch  int           `yaml:"max_batch"`  // default 256
	BatchWait time.Duration `yaml:"batch_wait"` // default 20ms
}

// Breaker configures a destination's circuit breaker.
type Breaker struct {
	Failures int           `yaml:"failures"` // consecutive failed writes; default 5
	OpenFor  time.Duration `yaml:"open_for"` // default 10s
}

// Destination is one output.
type Destination struct {
	Name    string         `yaml:"name"`
	Type    string         `yaml:"type"`
	Queue   Queue          `yaml:"queue"`
	Breaker Breaker        `yaml:"breaker"`
	Config  map[string]any `yaml:"config"`
}

// Route maps matching events to destinations. Match is a set of top-level
// field equalities on the ingested JSON object ({} matches everything); the
// first matching route wins. A route may name a destination that is not
// configured: its ID is reserved (registry state "absent") and its events
// are parked until it is configured or orphan_ttl expires (DESIGN §5).
type Route struct {
	Match map[string]any `yaml:"match"`
	To    []string       `yaml:"to"`
}

// Defaults.
const (
	DefaultNodeID         = "gw-1"
	DefaultDataDir        = "./data"
	DefaultSegmentBytes   = 64 << 20
	DefaultFsyncInterval  = 10 * time.Millisecond
	DefaultFsyncMaxEvents = 512
	DefaultHTTPListen     = ":8080"
	DefaultMaxBody        = 1 << 20
	DefaultIdemKeyEnv     = "LOGGATE_IDEM_KEY"

	DefaultRetryBase          = 200 * time.Millisecond
	DefaultRetryMax           = 30 * time.Second
	DefaultMaxAttempts        = 20
	DefaultMaxAge             = 24 * time.Hour
	DefaultRedeliverShare     = 0.5
	MinRedeliverShare         = 0.2
	DefaultRedeliverInterval  = 200 * time.Millisecond
	DefaultCheckpointInterval = time.Second
	DefaultOrphanTTL          = 24 * time.Hour
	DefaultMaxRecordsPerDest  = 1_000_000
	DefaultOnQuota            = "block_all"
	DefaultSweepInterval      = time.Second
	DefaultGCInterval         = 10 * time.Second
	DefaultTombstoneTTL       = 24 * time.Hour
	DefaultDiskCheckInterval  = time.Second
	DefaultQueueMaxEvents     = 10000
	DefaultQueueMaxBytes      = 64 << 20
	DefaultMaxBatch           = 256
	DefaultBatchWait          = 20 * time.Millisecond
	DefaultBreakerFailures    = 5
	DefaultBreakerOpenFor     = 10 * time.Second
)

// supportedSinkTypes lists the destination types implemented so far.
var supportedSinkTypes = map[string]bool{"file": true, "sqlite": true, "webhook": true}

// Load reads, parses, defaults, and validates the file at path.
func Load(path string) (*Config, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("config: read %s: %w", path, err)
	}
	c, err := Parse(b)
	if err != nil {
		return nil, fmt.Errorf("config: %s: %w", path, err)
	}
	return c, nil
}

// Parse decodes YAML, applies defaults, and validates.
func Parse(b []byte) (*Config, error) {
	var c Config
	dec := yaml.NewDecoder(bytes.NewReader(b))
	dec.KnownFields(true)
	if err := dec.Decode(&c); err != nil {
		return nil, fmt.Errorf("parse yaml: %w", err)
	}
	c.ApplyDefaults()
	if err := c.Validate(); err != nil {
		return nil, err
	}
	return &c, nil
}

func setDur(d *time.Duration, def time.Duration) {
	if *d == 0 {
		*d = def
	}
}

// ApplyDefaults fills zero values with defaults.
func (c *Config) ApplyDefaults() {
	if c.NodeID == "" {
		c.NodeID = DefaultNodeID
	}
	if c.DataDir == "" {
		c.DataDir = DefaultDataDir
	}
	if c.WAL.SegmentBytes == 0 {
		c.WAL.SegmentBytes = DefaultSegmentBytes
	}
	if c.WAL.FsyncInterval == 0 {
		c.WAL.FsyncInterval = DefaultFsyncInterval
	}
	if c.WAL.FsyncMaxEvents == 0 {
		c.WAL.FsyncMaxEvents = DefaultFsyncMaxEvents
	}
	if c.Ingest.HTTP.Listen == "" {
		c.Ingest.HTTP.Listen = DefaultHTTPListen
	}
	if c.Ingest.HTTP.MaxBody == 0 {
		c.Ingest.HTTP.MaxBody = DefaultMaxBody
	}
	if c.Idempotency.KeyEnv == "" {
		c.Idempotency.KeyEnv = DefaultIdemKeyEnv
	}
	r := &c.Retry
	setDur(&r.Base, DefaultRetryBase)
	setDur(&r.Max, DefaultRetryMax)
	setDur(&r.MaxAge, DefaultMaxAge)
	setDur(&r.RedeliverInterval, DefaultRedeliverInterval)
	if r.MaxAttempts == 0 {
		r.MaxAttempts = DefaultMaxAttempts
	}
	if r.RedeliverShare == 0 {
		r.RedeliverShare = DefaultRedeliverShare
	}
	l := &c.L1
	setDur(&l.CheckpointInterval, DefaultCheckpointInterval)
	setDur(&l.OrphanTTL, DefaultOrphanTTL)
	setDur(&l.SweepInterval, DefaultSweepInterval)
	setDur(&l.GCInterval, DefaultGCInterval)
	setDur(&l.TombstoneTTL, DefaultTombstoneTTL)
	if l.MaxRecordsPerDest == 0 {
		l.MaxRecordsPerDest = DefaultMaxRecordsPerDest
	}
	if l.OnQuota == "" {
		l.OnQuota = DefaultOnQuota
	}
	setDur(&c.Disk.CheckInterval, DefaultDiskCheckInterval)
	for i := range c.Destinations {
		c.Destinations[i].ApplyDefaults()
	}
}

// ApplyDefaults fills a destination's zero values.
func (d *Destination) ApplyDefaults() {
	q, b := &d.Queue, &d.Breaker
	if q.MaxEvents == 0 {
		q.MaxEvents = DefaultQueueMaxEvents
	}
	if q.MaxBytes == 0 {
		q.MaxBytes = DefaultQueueMaxBytes
	}
	if q.MaxBatch == 0 {
		q.MaxBatch = DefaultMaxBatch
	}
	setDur(&q.BatchWait, DefaultBatchWait)
	if b.Failures == 0 {
		b.Failures = DefaultBreakerFailures
	}
	setDur(&b.OpenFor, DefaultBreakerOpenFor)
}

// AbsentDestinations returns names referenced by routes that are not
// configured (they get reserved IDs and their events are parked).
func (c *Config) AbsentDestinations() []string {
	conf := map[string]bool{}
	for _, d := range c.Destinations {
		conf[d.Name] = true
	}
	seen := map[string]bool{}
	var out []string
	for _, r := range c.Routes {
		for _, n := range r.To {
			if !conf[n] && !seen[n] {
				seen[n] = true
				out = append(out, n)
			}
		}
	}
	return out
}

// BuiltinType reports whether typ is a compiled-in sink type.
func BuiltinType(typ string) bool { return supportedSinkTypes[typ] }

// ValidateDestination checks one destination (also used for runtime
// registration). knownType decides which sink types exist (BuiltinType
// for configuration files).
func ValidateDestination(d Destination, knownType func(string) bool) error {
	var errs []error
	add := func(format string, args ...any) { errs = append(errs, fmt.Errorf(format, args...)) }
	if d.Name == "" {
		add("name is required")
	}
	if !knownType(d.Type) {
		add("%s: unsupported type %q (supported: file, sqlite, webhook)", d.Name, d.Type)
		return errors.Join(errs...)
	}
	switch d.Type {
	case "file", "sqlite":
		if p, _ := d.Config["path"].(string); p == "" {
			add("%s: config.path is required for type %s", d.Name, d.Type)
		}
	case "webhook":
		if u, _ := d.Config["url"].(string); u == "" {
			add("%s: config.url is required for type webhook", d.Name)
		}
	}
	if d.Queue.MaxEvents < 1 || d.Queue.MaxBytes < 1 || d.Queue.MaxBatch < 1 || d.Queue.BatchWait < 0 {
		add("%s: queue limits must be positive", d.Name)
	}
	if d.Breaker.Failures < 1 || d.Breaker.OpenFor <= 0 {
		add("%s: breaker.failures and breaker.open_for must be positive", d.Name)
	}
	return errors.Join(errs...)
}

// Validate checks semantic constraints. It reports all problems at once.
func (c *Config) Validate() error {
	var errs []error
	add := func(format string, args ...any) { errs = append(errs, fmt.Errorf(format, args...)) }

	if c.WAL.SegmentBytes < 4<<10 {
		add("wal.segment_bytes must be >= 4KiB, got %d", c.WAL.SegmentBytes)
	}
	if c.WAL.FsyncInterval < 0 {
		add("wal.fsync_interval must be >= 0")
	}
	if c.WAL.FsyncMaxEvents < 1 {
		add("wal.fsync_max_events must be >= 1")
	}
	if c.Ingest.HTTP.MaxBody < 1 {
		add("ingest.http.max_body must be > 0")
	}
	sources := map[string]bool{}
	for i, t := range c.Ingest.HTTP.Tokens {
		if t.Source == "" {
			add("ingest.http.tokens[%d].source is required", i)
		}
		if sources[t.Source] {
			add("ingest.http.tokens[%d]: duplicate source %q", i, t.Source)
		}
		sources[t.Source] = true
		if (t.Token == "") == (t.TokenEnv == "") {
			add("ingest.http.tokens[%d]: exactly one of token and token_env must be set", i)
		}
	}

	r := c.Retry
	if r.Base <= 0 || r.Max < r.Base {
		add("retry: need 0 < base <= max")
	}
	if r.MaxAttempts < 1 || r.MaxAge <= 0 || r.RedeliverInterval <= 0 {
		add("retry: max_attempts, max_age and redeliver_interval must be positive")
	}
	if r.RedeliverShare < MinRedeliverShare || r.RedeliverShare >= 1 {
		add("retry.redeliver_share must be in [%.1f, 1), got %v", MinRedeliverShare, r.RedeliverShare)
	}
	l := c.L1
	if l.CheckpointInterval <= 0 || l.OrphanTTL <= 0 || l.SweepInterval <= 0 || l.GCInterval <= 0 || l.TombstoneTTL <= 0 {
		add("l1: intervals and TTLs must be positive")
	}
	if l.MaxRecordsPerDest < 0 {
		add("l1.max_records_per_dest must be >= 0 (0 = unlimited)")
	}
	if l.OnQuota != "block_all" && l.OnQuota != "dlq_overflow" {
		add("l1.on_quota must be block_all or dlq_overflow, got %q", l.OnQuota)
	}

	if c.Disk.MaxBytes < 0 || c.Disk.CheckInterval <= 0 {
		add("disk: max_bytes must be >= 0 and check_interval > 0")
	}
	if c.Admin.Token != "" && c.Admin.TokenEnv != "" {
		add("admin: set only one of token and token_env")
	}
	if c.Admin.Token != "" && len(c.Admin.Token) < 16 {
		add("admin.token must be at least 16 bytes")
	}

	names := map[string]bool{}
	if len(c.Destinations) == 0 {
		add("at least one destination is required")
	}
	for i, d := range c.Destinations {
		if d.Name != "" && names[d.Name] {
			add("destinations[%d]: duplicate name %q", i, d.Name)
		}
		names[d.Name] = true
		if err := ValidateDestination(d, BuiltinType); err != nil {
			add("destinations[%d]: %w", i, err)
		}
	}

	if len(c.Routes) == 0 {
		add("at least one route is required")
	}
	for i, r := range c.Routes {
		for k, v := range r.Match {
			switch v.(type) {
			case string, int, int64, float64, bool:
			default:
				add("routes[%d].match.%s: only scalar equality is supported", i, k)
			}
		}
		if len(r.To) == 0 {
			add("routes[%d].to must not be empty", i)
		}
		for _, n := range r.To {
			if n == "" {
				add("routes[%d]: empty destination name", i)
			}
		}
	}
	if n := len(names) + len(c.AbsentDestinations()); n > event.MaxDests {
		add("at most %d destinations (configured + route-only) are supported, got %d", event.MaxDests, n)
	}
	return errors.Join(errs...)
}
