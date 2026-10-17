package file

import (
	"bufio"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/lzwhehe/loggate/internal/event"
	"github.com/lzwhehe/loggate/internal/sink"
)

func TestWriteLines(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "sub", "out.jsonl")
	s := New()
	if err := s.Init(ctx, map[string]any{"path": path}); err != nil {
		t.Fatal(err)
	}
	if s.Idempotent() {
		t.Fatal("file sink must not claim idempotency")
	}
	var key event.IdemKey
	key[0] = 0xab
	batch := []*sink.Envelope{
		{Seq: 1, Event: &event.LogEvent{Seq: 1, IdemKey: key, SourceID: "http:a", ReceivedAt: 10, Fields: map[string]any{"unmapped": map[string]any{"k": "v"}}}},
		{Seq: 2, Event: &event.LogEvent{Seq: 2, SourceID: "http:b", ReceivedAt: 11}},
	}
	for i, err := range s.Write(ctx, batch) {
		if err != nil {
			t.Fatalf("envelope %d: %v", i, err)
		}
	}
	if err := s.Health(ctx); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(ctx); err != nil {
		t.Fatal(err)
	}

	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	var lines []map[string]any
	for sc.Scan() {
		var m map[string]any
		if err := json.Unmarshal(sc.Bytes(), &m); err != nil {
			t.Fatalf("bad line %q: %v", sc.Text(), err)
		}
		lines = append(lines, m)
	}
	if len(lines) != 2 {
		t.Fatalf("got %d lines", len(lines))
	}
	if lines[0]["seq"] != 1.0 || lines[0]["idem_key"] != "ab000000000000000000000000000000" || lines[0]["source_id"] != "http:a" {
		t.Fatalf("line 0 = %v", lines[0])
	}
	if lines[0]["fields"].(map[string]any)["unmapped"].(map[string]any)["k"] != "v" {
		t.Fatalf("fields = %v", lines[0]["fields"])
	}
	if errs := s.Write(ctx, batch[:1]); errs[0] == nil {
		t.Fatal("write after close succeeded")
	}
}

func TestInitRequiresPath(t *testing.T) {
	if err := New().Init(context.Background(), map[string]any{}); err == nil {
		t.Fatal("Init without path succeeded")
	}
}
