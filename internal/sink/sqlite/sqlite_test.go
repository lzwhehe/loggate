package sqlite

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/lzwhehe/loggate/internal/event"
	"github.com/lzwhehe/loggate/internal/sink"
)

func env(seq uint64) *sink.Envelope {
	var k event.IdemKey
	k[0], k[15] = byte(seq), byte(seq>>8)
	return &sink.Envelope{Seq: seq, Event: &event.LogEvent{Seq: seq, IdemKey: k, SourceID: "t", ReceivedAt: 1,
		Fields: map[string]any{"unmapped": map[string]any{"msg": "x"}}}}
}

func TestIdempotentInsert(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "siem.db")
	s := New()
	if err := s.Init(ctx, map[string]any{"path": path}); err != nil {
		t.Fatal(err)
	}
	if !s.Idempotent() {
		t.Fatal("sqlite sink must be idempotent")
	}
	batch := []*sink.Envelope{env(1), env(2), env(3)}
	for range 3 { // redelivery of the same events
		for i, err := range s.Write(ctx, batch) {
			if err != nil {
				t.Fatalf("write %d: %v", i, err)
			}
		}
	}
	for _, err := range s.Write(ctx, []*sink.Envelope{env(2), env(4)}) {
		if err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Health(ctx); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(ctx); err != nil {
		t.Fatal(err)
	}
	rows, err := ReadRows(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 4 {
		t.Fatalf("rows = %d, want 4", len(rows))
	}
	for i, r := range rows {
		if r.Seq != uint64(i+1) {
			t.Fatalf("row %d seq %d", i, r.Seq)
		}
	}
}
