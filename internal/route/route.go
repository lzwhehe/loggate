// Package route maps events to destination bitmaps.
//
// Rules are evaluated in order and the first match wins. A match is a set
// of top-level field equalities on the ingested JSON object (values are
// compared by their canonical string form, so YAML 3002 matches JSON 3002
// and "3002"); an empty match is a catch-all. Events that match no rule get
// an empty mask (they are stored in the WAL but delivered nowhere).
// OCSF-aware matching (class_uid after normalisation, max_level) is M4.
package route

import (
	"encoding/json"
	"fmt"
	"strconv"

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

// fields returns the ingested top-level object of ev.
func fields(ev *event.LogEvent) map[string]any {
	if m, ok := ev.Fields["unmapped"].(map[string]any); ok {
		return m
	}
	return ev.Fields
}

// Route returns the destinations for ev.
func (t *Table) Route(ev *event.LogEvent) event.DestMask {
	f := fields(ev)
next:
	for _, r := range t.rules {
		for k, want := range r.match {
			v, ok := f[k]
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
		return r.to
	}
	return 0
}

// ID returns the DestID for a destination name.
func (t *Table) ID(name string) (event.DestID, bool) {
	id, ok := t.ids[name]
	return id, ok
}

// Name returns the destination name for id ("" if unknown).
func (t *Table) Name(id event.DestID) string { return t.names[id] }
