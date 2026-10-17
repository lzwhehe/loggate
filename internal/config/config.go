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

// Destination is one output.
type Destination struct {
	Name   string         `yaml:"name"`
	Type   string         `yaml:"type"`
	Config map[string]any `yaml:"config"`
}

// Route maps matching events to destinations. In M1 only the catch-all
// match ({}) is supported.
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
)

// SupportedSinkTypes lists the destination types implemented so far.
var supportedSinkTypes = map[string]bool{"file": true}

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

	names := map[string]bool{}
	if len(c.Destinations) == 0 {
		add("at least one destination is required")
	}
	if len(c.Destinations) > event.MaxDests {
		add("at most %d destinations are supported, got %d", event.MaxDests, len(c.Destinations))
	}
	for i, d := range c.Destinations {
		if d.Name == "" {
			add("destinations[%d].name is required", i)
		} else if names[d.Name] {
			add("destinations[%d]: duplicate name %q", i, d.Name)
		}
		names[d.Name] = true
		if !supportedSinkTypes[d.Type] {
			add("destinations[%d] (%s): unsupported type %q (supported: file)", i, d.Name, d.Type)
			continue
		}
		if d.Type == "file" {
			if p, _ := d.Config["path"].(string); p == "" {
				add("destinations[%d] (%s): config.path is required for type file", i, d.Name)
			}
		}
	}

	if len(c.Routes) == 0 {
		add("at least one route is required")
	}
	for i, r := range c.Routes {
		if len(r.Match) != 0 {
			add("routes[%d]: only the catch-all match {} is supported in this version", i)
		}
		if len(r.To) == 0 {
			add("routes[%d].to must not be empty", i)
		}
		for _, n := range r.To {
			if !names[n] {
				add("routes[%d]: unknown destination %q", i, n)
			}
		}
	}
	return errors.Join(errs...)
}
