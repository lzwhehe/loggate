// Package route maps events to destination bitmaps.
//
// Rules are evaluated in order and the first match wins. A match is a set
// of field equalities (values are compared by their canonical string form,
// so YAML 3002 matches JSON 3002 and "3002"); an empty match is a
// catch-all. A key is looked up among the top-level OCSF fields first
// (class_uid, activity_id, ...) and then among the top-level keys of
// "unmapped" (the ingested JSON object for Base Events). Events that match
// no rule get an empty mask (they are stored in the WAL but delivered
// nowhere).
//
// After matching, destinations whose max_level is below the event's
// DataLevel are removed from the mask and counted (route_level_blocked).
package route

import (
	"encoding/json"
	"fmt"
	"strconv"
	"sync/atomic"

	"github.com/lzwhehe/loggate/internal/config"
	"github.com/lzwhehe/loggate/internal/event"
)

type rule struct {
	match map[string]string
	to    event.DestMask
}

// Table is an immutable routing table.
type Table struct {
	rules []rule
	ids   map[string]event.DestID
	names map[event.DestID]string
	// maxLevel[d] is the highest DataLevel d accepts (LevelUnset = any).
	maxLevel [event.MaxDests]atomic.Uint32
	blocked  [event.MaxDests]atomic.Uint64
	limited  atomic.Uint64 // DestMask of destinations with a max level
}

// New builds a table. ids maps every destination name (configured or
// route-only) to its persisted DestID.
func New(ids map[string]event.DestID, routes []config.Route) (*Table, error) {
	t := &Table{ids: map[string]event.DestID{}, names: map[event.DestID]string{}}
	for n, id := range ids {
		if !id.Valid() {
			return nil, fmt.Errorf("route: destination %q has invalid id %d", n, id)
		}
		t.ids[n] = id
		t.names[id] = n
	}
	for i, r := range routes {
		ru := rule{match: map[string]string{}}
		for k, v := range r.Match {
			ru.match[k] = canon(v)
		}
		for _, n := range r.To {
			id, ok := t.ids[n]
			if !ok {
				return nil, fmt.Errorf("route %d: unknown destination %q", i, n)
			}
			ru.to = ru.to.Set(id)
		}
		t.rules = append(t.rules, ru)
	}
	return t, nil
}

func canon(v any) string {
	switch x := v.(type) {
	case string:
		return x
	case json.Number:
		return x.String()
	case float64:
		return strconv.FormatFloat(x, 'f', -1, 64)
	case int:
		return strconv.Itoa(x)
	case int64:
		return strconv.FormatInt(x, 10)
	case bool:
		return strconv.FormatBool(x)
	case nil:
		return "null"
	default:
		return fmt.Sprint(x)
	}
}

// lookup returns the routing value of key k: a top-level OCSF field, else
// a top-level key of unmapped.
func lookup(ev *event.LogEvent, k string) (any, bool) {
	if v, ok := ev.Fields[k]; ok && k != "unmapped" {
		return v, true
	}
	if m, ok := ev.Fields["unmapped"].(map[string]any); ok {
		v, ok := m[k]
		return v, ok
	}
	return nil, false
}

// Route returns the destinations for ev, minus those whose max_level is
// below ev.DataLevel.
func (t *Table) Route(ev *event.LogEvent) event.DestMask {
	var to event.DestMask
next:
	for _, r := range t.rules {
		for k, want := range r.match {
			v, ok := lookup(ev, k)
			if !ok {
				continue next
			}
			switch v.(type) {
			case map[string]any, []any:
				continue next
			}
			if canon(v) != want {
				continue next
			}
		}
		to = r.to
		break
	}
	if ev.DataLevel == event.LevelUnset {
		return to
	}
	for _, d := range (to & event.DestMask(t.limited.Load())).Bits() {
		if lim := event.Level(t.maxLevel[d].Load()); lim != event.LevelUnset && ev.DataLevel > lim {
			to = to.Clear(d)
			t.blocked[d].Add(1)
		}
	}
	return to
}

// SetMaxLevel sets the highest DataLevel destination id accepts
// (LevelUnset removes the limit).
func (t *Table) SetMaxLevel(id event.DestID, l event.Level) {
	if !id.Valid() {
		return
	}
	t.maxLevel[id].Store(uint32(l))
	for {
		old := t.limited.Load()
		nw := uint64(event.DestMask(old).Clear(id))
		if l != event.LevelUnset {
			nw = uint64(event.DestMask(old).Set(id))
		}
		if t.limited.CompareAndSwap(old, nw) {
			return
		}
	}
}

// LevelBlocked returns how many events were kept away from id by its
// max_level.
func (t *Table) LevelBlocked(id event.DestID) uint64 {
	if !id.Valid() {
		return 0
	}
	return t.blocked[id].Load()
}

// ID returns the DestID for a destination name.
func (t *Table) ID(name string) (event.DestID, bool) {
	id, ok := t.ids[name]
	return id, ok
}

// Name returns the destination name for id ("" if unknown).
func (t *Table) Name(id event.DestID) string { return t.names[id] }
