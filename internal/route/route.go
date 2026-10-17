// Package route maps events to destination bitmaps.
//
// M1 supports only catch-all routes; match expressions (class_uid, source,
// max_level) arrive in M4.
package route

import (
	"fmt"

	"github.com/lzwhehe/loggate/internal/config"
	"github.com/lzwhehe/loggate/internal/event"
)

// Table is an immutable routing table.
type Table struct {
	names    []string // index = DestID
	ids      map[string]event.DestID
	catchAll event.DestMask
}

// New builds a table. DestIDs are assigned by position in dests.
//
// TODO(M2): persist name→DestID in the registry bucket so IDs survive
// config reordering and are not reused during tombstone periods.
func New(dests []config.Destination, routes []config.Route) (*Table, error) {
	if len(dests) > event.MaxDests {
		return nil, fmt.Errorf("route: %d destinations exceed max %d", len(dests), event.MaxDests)
	}
	t := &Table{ids: make(map[string]event.DestID, len(dests))}
	for i, d := range dests {
		if _, dup := t.ids[d.Name]; dup {
			return nil, fmt.Errorf("route: duplicate destination %q", d.Name)
		}
		t.ids[d.Name] = event.DestID(i)
		t.names = append(t.names, d.Name)
	}
	for i, r := range routes {
		if len(r.Match) != 0 {
			return nil, fmt.Errorf("route %d: match expressions are not supported yet", i)
		}
		for _, n := range r.To {
			id, ok := t.ids[n]
			if !ok {
				return nil, fmt.Errorf("route %d: unknown destination %q", i, n)
			}
			t.catchAll = t.catchAll.Set(id)
		}
	}
	return t, nil
}

// Route returns the destinations for ev.
func (t *Table) Route(*event.LogEvent) event.DestMask { return t.catchAll }

// ID returns the DestID for a destination name.
func (t *Table) ID(name string) (event.DestID, bool) {
	id, ok := t.ids[name]
	return id, ok
}

// Name returns the destination name for id ("" if unknown).
func (t *Table) Name(id event.DestID) string {
	if int(id) < len(t.names) {
		return t.names[id]
	}
	return ""
}
