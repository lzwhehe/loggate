// Package scenario defines fault-injection benchmark scenarios (DESIGN §10):
// the destination topology, the routing table, the load mix, the fault
// timeline and the pass criteria.
//
// A scenario file may name a base file ("base: _base.yaml"); the two are
// deep-merged (maps merge recursively, everything else is replaced) before
// decoding, so scenarios only state what differs from the shared topology.
//
// The routing table here is the benchmark's independent oracle: reconcile
// evaluates it with its own code (Router), never with the gateway's router.
package scenario

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"time"

	"gopkg.in/yaml.v3"
)

// Scenario is one benchmark scenario.
type Scenario struct {
	Name        string   `yaml:"name"`
	Title       string   `yaml:"title"`
	Description string   `yaml:"description,omitempty"`
	Platforms   []string `yaml:"platforms,omitempty"` // GOOS values; empty = all
	// Duration is the load duration at full scale.
	Duration time.Duration `yaml:"duration"`
	// Classes is the event mix; each event carries "class" for routing.
	Classes      []Class `yaml:"classes"`
	Destinations []Dest  `yaml:"destinations"`
	// Routes are evaluated first-match-wins on the event's "class" field.
	Routes []Route `yaml:"routes"`
	// Gateway is deep-merged into the generated gateway config (not scaled).
	Gateway map[string]any `yaml:"gateway,omitempty"`
	// DestOverrides is deep-merged into one generated destination entry.
	DestOverrides map[string]map[string]any `yaml:"dest_overrides,omitempty"`
	Timeline      []Action                  `yaml:"timeline,omitempty"`
	// FaultWindow is where latency is split into baseline/during/after and
	// from whose end recovery_s is measured. Optional.
	FaultWindow *Window `yaml:"fault_window,omitempty"`
	// Checks are pass criteria; a base file usually holds the common ones
	// and scenarios add theirs in ExtraChecks (lists are not merged).
	Checks      []Check            `yaml:"checks,omitempty"`
	ExtraChecks []Check            `yaml:"extra_checks,omitempty"`
	Profiles    map[string]Profile `yaml:"profiles,omitempty"`

	// Filled by Resolve.
	Profile string  `yaml:"profile,omitempty"`
	Rate    float64 `yaml:"rate,omitempty"`
}

// Class is one event class of the load mix.
type Class struct {
	Name   string  `yaml:"name"`
	Weight float64 `yaml:"weight"`
}

// Dest is one benchmark destination. Mock destinations are webhooks
// pointed at a mocksink sink of the same name.
type Dest struct {
	Name    string         `yaml:"name"`
	Type    string         `yaml:"type"` // file | sqlite | webhook
	Mock    bool           `yaml:"mock,omitempty"`
	Config  map[string]any `yaml:"config,omitempty"`
	Queue   map[string]any `yaml:"queue,omitempty"`
	Breaker map[string]any `yaml:"breaker,omitempty"`
}

// Route maps events whose fields equal Match to destinations.
type Route struct {
	Match map[string]string `yaml:"match"`
	To    []string          `yaml:"to"`
}

// Action kinds.
const (
	ActFault      = "fault"      // mocksink fault mode (dest, mode, delay_ms, error_code, error_rate, seed)
	ActKill       = "kill"       // hard-kill the gateway (SIGKILL / TerminateProcess)
	ActStop       = "stop"       // graceful stop (SIGTERM, or the admin API on Windows)
	ActStart      = "start"      // (re)start the gateway on the same data dir
	ActUnregister = "unregister" // admin API
	ActRegister   = "register"   // admin API
)

// Action is one timeline step at offset At from load start.
type Action struct {
	At        time.Duration `yaml:"at"`
	Action    string        `yaml:"action"`
	Dest      string        `yaml:"dest,omitempty"`
	Mode      string        `yaml:"mode,omitempty"` // ok | down | slow
	DelayMS   int           `yaml:"delay_ms,omitempty"`
	ErrorCode int           `yaml:"error_code,omitempty"`
	ErrorRate float64       `yaml:"error_rate,omitempty"`
	Seed      int64         `yaml:"seed,omitempty"`
}

// Window is a time window relative to load start.
type Window struct {
	Start time.Duration `yaml:"start"`
	End   time.Duration `yaml:"end"`
	Label string        `yaml:"label,omitempty"`
}

// Profile scales a scenario: Rate events/s, timeline offsets × Scale.
type Profile struct {
	Rate         float64       `yaml:"rate"`
	Scale        float64       `yaml:"scale"`
	DrainTimeout time.Duration `yaml:"drain_timeout"`
	Reps         int           `yaml:"reps"`
}

// DefaultProfiles: "quick" is a local smoke test, "full" is DESIGN §10.3.
var DefaultProfiles = map[string]Profile{
	"quick": {Rate: 200, Scale: 0.2, DrainTimeout: 150 * time.Second, Reps: 1},
	"full":  {Rate: 2000, Scale: 1, DrainTimeout: 10 * time.Minute, Reps: 5},
	// smoke is for developing the harness itself (~30 s per scenario).
	"smoke": {Rate: 100, Scale: 0.1, DrainTimeout: 90 * time.Second, Reps: 1},
}

// Check is one pass criterion: metric(dest) op value for each listed dest
// ("all" = every destination).
type Check struct {
	Metric string   `yaml:"metric"`
	Dests  []string `yaml:"dests"`
	Op     string   `yaml:"op"` // eq | le | ge | lt | gt
	Value  float64  `yaml:"value"`
	Note   string   `yaml:"note,omitempty"`
}

// Eval applies the comparison.
func (c Check) Eval(v float64) bool {
	switch c.Op {
	case "eq":
		return v == c.Value
	case "le":
		return v <= c.Value
	case "ge":
		return v >= c.Value
	case "lt":
		return v < c.Value
	case "gt":
		return v > c.Value
	}
	return false
}

// Load reads a scenario file, merging its base (if any).
func Load(path string) (*Scenario, error) {
	m, err := loadMap(path, 0)
	if err != nil {
		return nil, err
	}
	b, err := yaml.Marshal(m)
	if err != nil {
		return nil, err
	}
	s, err := Parse(b)
	if err != nil {
		return nil, fmt.Errorf("scenario %s: %w", path, err)
	}
	return s, nil
}

// Parse decodes an already-merged scenario (e.g. a resolved copy).
func Parse(b []byte) (*Scenario, error) {
	var s Scenario
	dec := yaml.NewDecoder(bytes.NewReader(b))
	dec.KnownFields(true)
	if err := dec.Decode(&s); err != nil {
		return nil, err
	}
	return &s, s.Validate()
}

func loadMap(path string, depth int) (map[string]any, error) {
	if depth > 4 {
		return nil, errors.New("scenario: base chain too deep")
	}
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("scenario: %w", err)
	}
	var m map[string]any
	if err := yaml.Unmarshal(b, &m); err != nil {
		return nil, fmt.Errorf("scenario %s: %w", path, err)
	}
	base, _ := m["base"].(string)
	delete(m, "base")
	if base == "" {
		return m, nil
	}
	bm, err := loadMap(filepath.Join(filepath.Dir(path), base), depth+1)
	if err != nil {
		return nil, err
	}
	return DeepMerge(bm, m), nil
}

// DeepMerge merges over into dst (maps recursively; other values replace)
// and returns dst.
func DeepMerge(dst, over map[string]any) map[string]any {
	if dst == nil {
		dst = map[string]any{}
	}
	for k, v := range over {
		if om, ok := v.(map[string]any); ok {
			if dm, ok := dst[k].(map[string]any); ok {
				dst[k] = DeepMerge(dm, om)
				continue
			}
			dst[k] = DeepMerge(map[string]any{}, om)
			continue
		}
		dst[k] = v
	}
	return dst
}

// Validate checks internal consistency.
func (s *Scenario) Validate() error {
	var errs []error
	add := func(f string, a ...any) { errs = append(errs, fmt.Errorf(f, a...)) }
	if s.Name == "" {
		add("name is required")
	}
	if s.Duration <= 0 {
		add("duration must be positive")
	}
	if len(s.Classes) == 0 {
		add("at least one class is required")
	}
	for _, c := range s.Classes {
		if c.Name == "" || c.Weight <= 0 {
			add("class %q: name and positive weight required", c.Name)
		}
	}
	names := map[string]Dest{}
	for _, d := range s.Destinations {
		if _, dup := names[d.Name]; dup || d.Name == "" {
			add("destination %q: empty or duplicate name", d.Name)
		}
		switch d.Type {
		case "file", "sqlite":
			if d.Mock {
				add("destination %s: only webhooks can be mock", d.Name)
			}
		case "webhook":
			if !d.Mock {
				add("destination %s: webhook destinations must be mock (mocksink)", d.Name)
			}
		default:
			add("destination %s: unsupported type %q", d.Name, d.Type)
		}
		names[d.Name] = d
	}
	if len(s.Routes) == 0 {
		add("at least one route is required")
	}
	for i, r := range s.Routes {
		for k := range r.Match {
			if k != "class" {
				add("routes[%d]: only the class field can be matched, got %q", i, k)
			}
		}
		for _, n := range r.To {
			if _, ok := names[n]; !ok {
				add("routes[%d]: unknown destination %q", i, n)
			}
		}
	}
	for n := range s.DestOverrides {
		if _, ok := names[n]; !ok {
			add("dest_overrides: unknown destination %q", n)
		}
	}
	for i, a := range s.Timeline {
		if a.At < 0 {
			add("timeline[%d]: negative offset", i)
		}
		switch a.Action {
		case ActFault:
			d, ok := names[a.Dest]
			if !ok || !d.Mock {
				add("timeline[%d]: fault needs a mock destination, got %q", i, a.Dest)
			}
			if a.Mode != "ok" && a.Mode != "down" && a.Mode != "slow" {
				add("timeline[%d]: mode must be ok|down|slow", i)
			}
			if a.ErrorRate < 0 || a.ErrorRate > 1 {
				add("timeline[%d]: error_rate must be in [0,1]", i)
			}
		case ActUnregister, ActRegister:
			if _, ok := names[a.Dest]; !ok {
				add("timeline[%d]: unknown destination %q", i, a.Dest)
			}
		case ActKill, ActStop, ActStart:
		default:
			add("timeline[%d]: unknown action %q", i, a.Action)
		}
		if i > 0 && a.At < s.Timeline[i-1].At {
			add("timeline[%d]: offsets must be non-decreasing", i)
		}
	}
	if w := s.FaultWindow; w != nil && (w.Start < 0 || w.End < w.Start) {
		add("fault_window: need 0 <= start <= end")
	}
	for i, c := range s.AllChecks() {
		if !slices.Contains([]string{"eq", "le", "ge", "lt", "gt"}, c.Op) {
			add("checks[%d]: bad op %q", i, c.Op)
		}
		for _, d := range c.Dests {
			if _, ok := names[d]; !ok && d != "all" {
				add("checks[%d]: unknown destination %q", i, d)
			}
		}
	}
	return errors.Join(errs...)
}

// AllChecks returns Checks followed by ExtraChecks.
func (s *Scenario) AllChecks() []Check {
	return append(slices.Clone(s.Checks), s.ExtraChecks...)
}

// SupportedOn reports whether the scenario runs on goos.
func (s *Scenario) SupportedOn(goos string) bool {
	return len(s.Platforms) == 0 || slices.Contains(s.Platforms, goos)
}

// Supported reports whether the scenario runs on this OS.
func (s *Scenario) Supported() bool { return s.SupportedOn(runtime.GOOS) }

// LookupProfile returns the named profile (scenario overrides first).
func (s *Scenario) LookupProfile(name string) (Profile, error) {
	p, ok := DefaultProfiles[name]
	if o, ok2 := s.Profiles[name]; ok2 {
		if o.Rate > 0 {
			p.Rate = o.Rate
		}
		if o.Scale > 0 {
			p.Scale = o.Scale
		}
		if o.DrainTimeout > 0 {
			p.DrainTimeout = o.DrainTimeout
		}
		if o.Reps > 0 {
			p.Reps = o.Reps
		}
		ok = true
	}
	if !ok || p.Rate <= 0 || p.Scale <= 0 {
		return Profile{}, fmt.Errorf("scenario %s: unknown or invalid profile %q", s.Name, name)
	}
	if p.Reps <= 0 {
		p.Reps = 3
	}
	if p.DrainTimeout <= 0 {
		p.DrainTimeout = 5 * time.Minute
	}
	return p, nil
}

// Resolve returns a copy scaled by the profile: duration, timeline offsets
// and the fault window are multiplied by Scale; the rate is set. Gateway
// settings (timeouts, TTLs) are not scaled.
func (s *Scenario) Resolve(profile string) (*Scenario, Profile, error) {
	p, err := s.LookupProfile(profile)
	if err != nil {
		return nil, p, err
	}
	r := *s
	scale := func(d time.Duration) time.Duration { return time.Duration(math.Round(float64(d) * p.Scale)) }
	r.Duration = scale(s.Duration)
	r.Timeline = make([]Action, len(s.Timeline))
	for i, a := range s.Timeline {
		a.At = scale(a.At)
		r.Timeline[i] = a
	}
	if s.FaultWindow != nil {
		w := *s.FaultWindow
		w.Start, w.End = scale(w.Start), scale(w.End)
		r.FaultWindow = &w
	}
	r.Profile, r.Rate = profile, p.Rate
	r.Profiles = nil
	return &r, p, nil
}

// Dest returns the named destination.
func (s *Scenario) Dest(name string) (Dest, bool) {
	for _, d := range s.Destinations {
		if d.Name == name {
			return d, true
		}
	}
	return Dest{}, false
}

// Router is the independent routing oracle: first matching route wins.
type Router struct{ routes []Route }

// Router returns the oracle for s.
func (s *Scenario) Router() Router { return Router{routes: s.Routes} }

// RouteClass returns the destinations of an event with the given class.
func (r Router) RouteClass(class string) []string {
	fields := map[string]string{"class": class}
	for _, rt := range r.routes {
		ok := true
		for k, v := range rt.Match {
			if fields[k] != v {
				ok = false
				break
			}
		}
		if ok {
			return rt.To
		}
	}
	return nil
}
