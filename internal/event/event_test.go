package event

import (
	"encoding/json"
	"reflect"
	"testing"
)

func TestDestMask(t *testing.T) {
	var m DestMask
	m = m.Set(0).Set(5).Set(63)
	if !m.Has(0) || !m.Has(5) || !m.Has(63) || m.Has(1) {
		t.Fatalf("Has mismatch: %b", m)
	}
	if got := m.Count(); got != 3 {
		t.Fatalf("Count = %d, want 3", got)
	}
	if got, want := m.Bits(), []DestID{0, 5, 63}; !reflect.DeepEqual(got, want) {
		t.Fatalf("Bits = %v, want %v", got, want)
	}
	m = m.Clear(5)
	if m.Has(5) || m.Count() != 2 {
		t.Fatalf("Clear failed: %b", m)
	}
	if DestMask(0).Has(64) {
		t.Fatal("invalid DestID reported present")
	}
	if len(DestMask(0).Bits()) != 0 {
		t.Fatal("empty mask has bits")
	}
}

func TestIdemKeyer(t *testing.T) {
	key := []byte("0123456789abcdef")
	a, err := NewIdemKeyer(key, "gw-1", 42)
	if err != nil {
		t.Fatal(err)
	}
	if a.Key(1) != a.Key(1) {
		t.Fatal("key not deterministic")
	}
	if a.Key(1) == a.Key(2) {
		t.Fatal("different seqs share a key")
	}
	b, _ := NewIdemKeyer(key, "gw-1", 43)
	if a.Key(1) == b.Key(1) {
		t.Fatal("different boot epochs share a key")
	}
	c, _ := NewIdemKeyer(key, "gw-2", 42)
	if a.Key(1) == c.Key(1) {
		t.Fatal("different nodes share a key")
	}
	if _, err := NewIdemKeyer([]byte("short"), "gw", 1); err == nil {
		t.Fatal("short key accepted")
	}
}

func TestLogEventJSONRoundTrip(t *testing.T) {
	k, _ := NewIdemKeyer([]byte("0123456789abcdef"), "gw", 1)
	in := LogEvent{Seq: 7, IdemKey: k.Key(7), SourceID: "http:a", ReceivedAt: 5,
		Fields: map[string]any{"unmapped": map[string]any{"x": "y"}}, Raw: []byte(`{"x":"y"}`),
		Targets: DestMask(0).Set(3)}
	b, err := json.Marshal(in)
	if err != nil {
		t.Fatal(err)
	}
	var out LogEvent
	if err := json.Unmarshal(b, &out); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(in, out) {
		t.Fatalf("round trip mismatch:\n in=%+v\nout=%+v", in, out)
	}
}
