// Package mask implements the PII masking engine (DESIGN §7, §0.5 item 11).
//
// Every field of an event is looked up in a policy trie built once from the
// configuration: keep | drop | hash | mask | scan (default scan). "scan"
// runs the detectors (ID card, bank card, phone, e-mail) over every string
// and json.Number in the subtree, including raw_data and unmapped, and
// rewrites the hits in place. If masking a value fails or panics, the value
// is replaced with "[REDACTED:error]" (fail-closed on error) and counted.
//
// The engine also derives DataClass tags (pii.id_card, ...) and a
// configurable DataLevel; these are labels, not legal determinations.
package mask

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync/atomic"

	"github.com/lzwhehe/loggate/internal/event"
)

// RedactedError replaces a value whose masking failed.
const RedactedError = "[REDACTED:error]"

// maxPasses bounds the re-scan loop: masking is repeated until a scan of
// the output finds nothing (a card window may only become visible after a
// neighbouring card was masked). Exceeding it is treated as an error.
const maxPasses = 4

// Policy is a field policy.
type Policy uint8

// Field policies. PolicyUnset inherits from the nearest ancestor (or the
// default).
const (
	PolicyUnset Policy = iota
	PolicyKeep
	PolicyDrop
	PolicyHash
	PolicyMask
	PolicyScan
)

var policyNames = []string{"", "keep", "drop", "hash", "mask", "scan"}

func (p Policy) String() string {
	if int(p) < len(policyNames) {
		return policyNames[p]
	}
	return fmt.Sprintf("policy(%d)", uint8(p))
}

// ParsePolicy parses keep|drop|hash|mask|scan.
func ParsePolicy(s string) (Policy, error) {
	for i, n := range policyNames {
		if i > 0 && n == s {
			return Policy(i), nil
		}
	}
	return PolicyUnset, fmt.Errorf("unknown field policy %q (want keep|drop|hash|mask|scan)", s)
}

// Config configures an Engine.
type Config struct {
	// Default is the policy of fields without one (default scan).
	Default Policy
	// Fields maps dotted paths ("user.name", "unmapped.*.token") to
	// policies. "*" matches any single key. Arrays are transparent.
	Fields map[string]Policy
	// Detector configures the detectors.
	Detector DetectorConfig
	// HMACKey keys the hash policy (HMAC-SHA256, first 16 bytes as hex).
	HMACKey []byte
	// Levels maps "pii.<kind>" or "combo:<kind>+<kind>..." to a level.
	// Entries override DefaultLevels; LevelUnset removes one.
	Levels map[string]event.Level
}

// DefaultLevels: any PII is general (一般); an ID card together with a bank
// card in one event is important (重要). Core (核心) is only ever set by
// explicit configuration.
func DefaultLevels() map[string]event.Level {
	return map[string]event.Level{
		"pii.id_card": event.LevelGeneral, "pii.bank_card": event.LevelGeneral,
		"pii.phone": event.LevelGeneral, "pii.email": event.LevelGeneral,
		"combo:id_card+bank_card": event.LevelImportant,
	}
}

// builtinKeep lists gateway-generated OCSF fields that never carry
// ingested content.
var builtinKeep = []string{
	"class_uid", "class_name", "category_uid", "category_name", "activity_id", "activity_name",
	"type_uid", "type_name", "status_id", "severity_id", "time", "metadata",
}

type node struct {
	children map[string]*node
	wildcard *node
	policy   Policy
	hasDesc  bool // some descendant has an explicit policy
}

func (n *node) child(k string) *node {
	if n == nil {
		return nil
	}
	if c, ok := n.children[k]; ok {
		return c
	}
	return n.wildcard
}

func (n *node) insert(path string, p Policy) error {
	cur := n
	for seg := range strings.SplitSeq(path, ".") {
		if seg == "" {
			return fmt.Errorf("mask: invalid field path %q", path)
		}
		cur.hasDesc = true
		var next *node
		if seg == "*" {
			if cur.wildcard == nil {
				cur.wildcard = &node{}
			}
			next = cur.wildcard
		} else {
			if cur.children == nil {
				cur.children = map[string]*node{}
			}
			if next = cur.children[seg]; next == nil {
				next = &node{}
				cur.children[seg] = next
			}
		}
		cur = next
	}
	cur.policy = p
	return nil
}

type combo struct {
	kinds uint8 // bitset of Kind
	level event.Level
}

// Engine masks events. It is immutable after New and safe for concurrent
// use.
type Engine struct {
	det      *Detector
	root     *node
	def      Policy
	key      []byte
	kindLvl  [NumKinds]event.Level
	combos   []combo
	hits     [NumKinds]atomic.Uint64
	errs     atomic.Uint64
	events   atomic.Uint64
	piiEvts  atomic.Uint64
	testHook func(string) // called before scanning each value (tests inject panics)
}

// New builds an engine.
func New(c Config) (*Engine, error) {
	det, err := NewDetector(c.Detector)
	if err != nil {
		return nil, err
	}
	e := &Engine{det: det, root: &node{}, def: c.Default, key: c.HMACKey}
	if e.def == PolicyUnset {
		e.def = PolicyScan
	}
	for _, f := range builtinKeep {
		if err := e.root.insert(f, PolicyKeep); err != nil {
			return nil, err
		}
	}
	paths := make([]string, 0, len(c.Fields))
	for p := range c.Fields {
		paths = append(paths, p)
	}
	slices.Sort(paths)
	usesHash := e.def == PolicyHash
	for _, p := range paths {
		pol := c.Fields[p]
		if pol == PolicyUnset || pol > PolicyScan {
			return nil, fmt.Errorf("mask: field %q: invalid policy", p)
		}
		usesHash = usesHash || pol == PolicyHash
		if err := e.root.insert(p, pol); err != nil {
			return nil, err
		}
	}
	if usesHash && len(e.key) < 16 {
		return nil, errors.New("mask: hash policy needs an HMAC key of at least 16 bytes")
	}
	levels := DefaultLevels()
	for k, v := range c.Levels {
		levels[k] = v
	}
	for k, v := range levels {
		if v == event.LevelUnset {
			continue
		}
		switch {
		case strings.HasPrefix(k, "pii."):
			kd, err := ParseKind(strings.TrimPrefix(k, "pii."))
			if err != nil {
				return nil, fmt.Errorf("mask: levels: %w", err)
			}
			e.kindLvl[kd] = v
		case strings.HasPrefix(k, "combo:"):
			var cb combo
			for part := range strings.SplitSeq(strings.TrimPrefix(k, "combo:"), "+") {
				kd, err := ParseKind(part)
				if err != nil {
					return nil, fmt.Errorf("mask: levels: %w", err)
				}
				cb.kinds |= 1 << kd
			}
			cb.level = v
			e.combos = append(e.combos, cb)
		default:
			return nil, fmt.Errorf("mask: levels: key %q must start with pii. or combo:", k)
		}
	}
	return e, nil
}

// Detector returns the engine's detector.
func (e *Engine) Detector() *Detector { return e.det }

// Result summarises masking of one event.
type Result struct {
	Kinds  uint8 // bitset of Kind found
	Hits   int   // spans masked
	Errors int   // values replaced with RedactedError
	Level  event.Level
}

// Classes returns the DataClass tags in Kind order (nil if none).
func (r Result) Classes() []string {
	if r.Kinds == 0 {
		return nil
	}
	var out []string
	for k := Kind(0); k < NumKinds; k++ {
		if r.Kinds&(1<<k) != 0 {
			out = append(out, k.Class())
		}
	}
	return out
}

// Has reports whether kind k was found.
func (r Result) Has(k Kind) bool { return r.Kinds&(1<<k) != 0 }

type accum struct {
	kinds  uint8
	hits   int
	errors int
}

// MaskEvent masks the OCSF field tree m in place and returns what it found.
// If the walk itself panics, every field is removed and replaced by
// {"redacted": RedactedError}.
func (e *Engine) MaskEvent(m map[string]any) (res Result) {
	var acc accum
	defer func() {
		if r := recover(); r != nil {
			clear(m)
			m["redacted"] = RedactedError
			acc.errors++
			e.errs.Add(1)
		}
		res = e.finish(acc)
	}()
	e.walk(m, e.root, e.def, &acc)
	return
}

func (e *Engine) finish(acc accum) Result {
	e.events.Add(1)
	r := Result{Kinds: acc.kinds, Hits: acc.hits, Errors: acc.errors}
	if acc.kinds == 0 {
		return r
	}
	e.piiEvts.Add(1)
	for k := Kind(0); k < NumKinds; k++ {
		if acc.kinds&(1<<k) != 0 {
			r.Level = max(r.Level, e.kindLvl[k])
		}
	}
	for _, c := range e.combos {
		if acc.kinds&c.kinds == c.kinds {
			r.Level = max(r.Level, c.level)
		}
	}
	return r
}

func (e *Engine) walk(m map[string]any, n *node, inherited Policy, acc *accum) {
	for k, v := range m {
		c := n.child(k)
		p := inherited
		if c != nil && c.policy != PolicyUnset {
			p = c.policy
		}
		// An explicit drop/hash (or one on a node without configured
		// descendants) applies to the whole subtree; an inherited one on a
		// node with configured descendants is applied per leaf instead.
		whole := c == nil || !c.hasDesc || c.policy == p
		switch {
		case p == PolicyDrop && whole:
			delete(m, k)
			continue
		case p == PolicyHash && whole:
			m[k] = e.hashValue(v, acc)
			continue
		case p == PolicyKeep && (c == nil || !c.hasDesc):
			continue
		}
		if nv, changed := e.value(v, c, p, acc); changed {
			m[k] = nv
		}
	}
}

func (e *Engine) value(v any, n *node, p Policy, acc *accum) (any, bool) {
	switch x := v.(type) {
	case map[string]any:
		e.walk(x, n, p, acc)
	case []any:
		for i, el := range x {
			if nv, ch := e.value(el, n, p, acc); ch {
				x[i] = nv
			}
		}
	case string:
		return e.leaf(v, x, p, acc)
	case json.Number:
		return e.leaf(v, string(x), p, acc)
	default:
		if p == PolicyDrop {
			return nil, true
		}
		if p == PolicyHash && x != nil {
			return e.hashValue(x, acc), true
		}
	}
	return nil, false
}

// leaf applies p to a scalar whose text is s.
func (e *Engine) leaf(v any, s string, p Policy, acc *accum) (any, bool) {
	switch p {
	case PolicyScan:
		if out := e.scan(s, acc); out != s {
			return out, true
		}
	case PolicyMask:
		if s != "" {
			return maskFormat(s), true
		}
	case PolicyDrop:
		return nil, true
	case PolicyHash:
		return e.hashValue(v, acc), true
	}
	return nil, false
}

// MaskString scans s and returns it with all detected PII masked, plus the
// kinds found. It is the per-value path of MaskEvent.
func (e *Engine) MaskString(s string) (string, Result) {
	var acc accum
	out := e.scan(s, &acc)
	return out, Result{Kinds: acc.kinds, Hits: acc.hits, Errors: acc.errors}
}

// scan masks s. On panic or if masking does not converge, it returns
// RedactedError (fail-closed) and counts an error.
func (e *Engine) scan(s string, acc *accum) (out string) {
	defer func() {
		if r := recover(); r != nil {
			out = RedactedError
			acc.errors++
			e.errs.Add(1)
		}
	}()
	if e.testHook != nil {
		e.testHook(s)
	}
	var buf [16]Span
	for pass := 0; pass < maxPasses; pass++ {
		sp := e.det.Detect(s, buf[:0])
		if len(sp) == 0 {
			return s
		}
		for _, x := range sp {
			acc.kinds |= 1 << x.Kind
			e.hits[x.Kind].Add(1)
		}
		acc.hits += len(sp)
		s = rewrite(s, sp)
	}
	if len(e.det.Detect(s, buf[:0])) == 0 {
		return s
	}
	acc.errors++
	e.errs.Add(1)
	return RedactedError
}

func rewrite(s string, sp []Span) string {
	var b strings.Builder
	b.Grow(len(s) + 4*len(sp))
	prev := 0
	for _, x := range sp {
		b.WriteString(s[prev:x.Start])
		v := s[x.Start:x.End]
		switch x.Kind {
		case IDCard: // 110105********123X
			b.WriteString(v[:6])
			b.WriteString("********")
			b.WriteString(v[14:])
		case BankCard: // keep first 4 and last 4 digits, keep separators
			nd := 0
			for i := 0; i < len(v); i++ {
				if isDigit(v[i]) {
					nd++
				}
			}
			seen := 0
			for i := 0; i < len(v); i++ {
				c := v[i]
				if isDigit(c) {
					seen++
					if seen > 4 && seen <= nd-4 {
						c = '*'
					}
				}
				b.WriteByte(c)
			}
		case Phone: // 138****5678
			b.WriteString(v[:3])
			b.WriteString("****")
			b.WriteString(v[7:])
		case Email: // z***@example.com
			at := strings.IndexByte(v, '@')
			b.WriteByte(v[0])
			b.WriteString("***")
			b.WriteString(v[at:])
		}
		prev = x.End
	}
	b.WriteString(s[prev:])
	return b.String()
}

// maskFormat replaces every ASCII letter and digit with '*'.
func maskFormat(s string) string {
	b := []byte(s)
	for i, c := range b {
		if isWord(c) {
			b[i] = '*'
		}
	}
	return string(b)
}

// hashValue returns hex(HMAC-SHA256(key, value)[:16]). Strings and
// numbers hash their text; other values hash their JSON encoding.
func (e *Engine) hashValue(v any, acc *accum) (out any) {
	defer func() {
		if r := recover(); r != nil {
			out = RedactedError
			acc.errors++
			e.errs.Add(1)
		}
	}()
	var msg []byte
	switch x := v.(type) {
	case nil:
		return nil
	case string:
		msg = []byte(x)
	case json.Number:
		msg = []byte(x)
	default:
		b, err := json.Marshal(x)
		if err != nil {
			acc.errors++
			e.errs.Add(1)
			return RedactedError
		}
		msg = b
	}
	h := hmac.New(sha256.New, e.key)
	h.Write(msg)
	sum := h.Sum(nil)
	return hex.EncodeToString(sum[:16])
}

// Stats is a snapshot of engine counters.
type Stats struct {
	Events    uint64            `json:"events"`
	PIIEvents uint64            `json:"pii_events"`
	Hits      map[string]uint64 `json:"hits"`
	Errors    uint64            `json:"errors"`
}

// Stats returns the counters.
func (e *Engine) Stats() Stats {
	s := Stats{Events: e.events.Load(), PIIEvents: e.piiEvts.Load(), Errors: e.errs.Load(), Hits: map[string]uint64{}}
	for k := Kind(0); k < NumKinds; k++ {
		s.Hits[k.String()] = e.hits[k].Load()
	}
	return s
}
