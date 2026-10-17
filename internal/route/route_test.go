package route

import (
	"encoding/json"
	"testing"

	"github.com/lzwhehe/loggate/internal/config"
	"github.com/lzwhehe/loggate/internal/event"
)

func ev(fields map[string]any) *event.LogEvent {
	return &event.LogEvent{Fields: map[string]any{"unmapped": fields}}
}

func TestFirstMatchWins(t *testing.T) {
	ids := map[string]event.DestID{"archive": 0, "siem": 4, "soar": 9}
	tb, err := New(ids, []config.Route{
		{Match: map[string]any{"class_uid": 3002}, To: []string{"archive", "siem", "soar"}},
		{Match: map[string]any{"class_uid": 4001, "kind": "net"}, To: []string{"archive", "siem"}},
		{To: []string{"archive"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		f    map[string]any
		want event.DestMask
	}{
		{map[string]any{"class_uid": json.Number("3002")}, event.DestMask(0).Set(0).Set(4).Set(9)},
		{map[string]any{"class_uid": "3002"}, event.DestMask(0).Set(0).Set(4).Set(9)},
		{map[string]any{"class_uid": json.Number("4001"), "kind": "net"}, event.DestMask(0).Set(0).Set(4)},
		{map[string]any{"class_uid": json.Number("4001")}, event.DestMask(0).Set(0)},
		{map[string]any{"class_uid": map[string]any{"x": 1}}, event.DestMask(0).Set(0)},
		{map[string]any{}, event.DestMask(0).Set(0)},
	}
	for i, c := range cases {
		if got := tb.Route(ev(c.f)); got != c.want {
			t.Errorf("case %d: got %b want %b", i, got, c.want)
		}
	}
	if id, ok := tb.ID("soar"); !ok || id != 9 || tb.Name(9) != "soar" {
		t.Fatal("ID/Name mismatch")
	}
	if _, err := New(ids, []config.Route{{To: []string{"zz"}}}); err == nil {
		t.Fatal("unknown destination accepted")
	}
}

func TestNoMatchIsEmpty(t *testing.T) {
	tb, err := New(map[string]event.DestID{"a": 0}, []config.Route{{Match: map[string]any{"x": true}, To: []string{"a"}}})
	if err != nil {
		t.Fatal(err)
	}
	if m := tb.Route(ev(map[string]any{"x": false})); m != 0 {
		t.Fatalf("mask %b", m)
	}
	if m := tb.Route(ev(map[string]any{"x": true})); m != 1 {
		t.Fatalf("mask %b", m)
	}
}

func TestOCSFFieldsShadowUnmapped(t *testing.T) {
	tb, err := New(map[string]event.DestID{"a": 0, "b": 1}, []config.Route{
		{Match: map[string]any{"class_uid": 3002}, To: []string{"a"}},
		{Match: map[string]any{"class": "net"}, To: []string{"b"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	auth := &event.LogEvent{Fields: map[string]any{"class_uid": 3002, "unmapped": map[string]any{"class_uid": 0}}}
	if m := tb.Route(auth); m != 1 {
		t.Fatalf("OCSF class_uid: %b", m)
	}
	base := &event.LogEvent{Fields: map[string]any{"class_uid": 0, "unmapped": map[string]any{"class": "net"}}}
	if m := tb.Route(base); m != 2 {
		t.Fatalf("unmapped fallback: %b", m)
	}
}

func TestMaxLevelRouting(t *testing.T) {
	tb, err := New(map[string]event.DestID{"archive": 0, "siem": 1, "soar": 2}, []config.Route{
		{To: []string{"archive", "siem", "soar"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	tb.SetMaxLevel(1, event.LevelImportant)
	tb.SetMaxLevel(2, event.LevelGeneral)
	all := event.DestMask(7)
	for _, c := range []struct {
		lvl  event.Level
		want event.DestMask
	}{
		{event.LevelUnset, all},
		{event.LevelGeneral, all},
		{event.LevelImportant, all.Clear(2)},
		{event.LevelCore, all.Clear(1).Clear(2)},
	} {
		e := ev(nil)
		e.DataLevel = c.lvl
		if got := tb.Route(e); got != c.want {
			t.Errorf("level %v: %b, want %b", c.lvl, got, c.want)
		}
	}
	if tb.LevelBlocked(0) != 0 || tb.LevelBlocked(1) != 1 || tb.LevelBlocked(2) != 2 {
		t.Fatalf("blocked %d %d %d", tb.LevelBlocked(0), tb.LevelBlocked(1), tb.LevelBlocked(2))
	}
	tb.SetMaxLevel(2, event.LevelUnset)
	e := ev(nil)
	e.DataLevel = event.LevelCore
	if got := tb.Route(e); got != all.Clear(1) {
		t.Fatalf("after removing limit: %b", got)
	}
}
