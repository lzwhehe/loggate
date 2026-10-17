package route

import (
	"testing"

	"github.com/lzwhehe/loggate/internal/config"
	"github.com/lzwhehe/loggate/internal/event"
)

func TestCatchAll(t *testing.T) {
	dests := []config.Destination{{Name: "a", Type: "file"}, {Name: "b", Type: "file"}, {Name: "c", Type: "file"}}
	tb, err := New(dests, []config.Route{{To: []string{"a"}}, {To: []string{"c"}}})
	if err != nil {
		t.Fatal(err)
	}
	m := tb.Route(&event.LogEvent{})
	if !m.Has(0) || m.Has(1) || !m.Has(2) {
		t.Fatalf("mask = %b", m)
	}
	if id, ok := tb.ID("c"); !ok || id != 2 || tb.Name(2) != "c" {
		t.Fatalf("ID/Name mismatch")
	}
	if _, err := New(dests, []config.Route{{To: []string{"zz"}}}); err == nil {
		t.Fatal("unknown destination accepted")
	}
	if _, err := New(dests, []config.Route{{Match: map[string]any{"x": 1}, To: []string{"a"}}}); err == nil {
		t.Fatal("match expression accepted")
	}
}
