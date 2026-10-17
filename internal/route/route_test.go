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
