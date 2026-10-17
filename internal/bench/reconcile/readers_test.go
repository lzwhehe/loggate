package reconcile

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/lzwhehe/loggate/internal/dlq"
	"github.com/lzwhehe/loggate/internal/event"
	"github.com/lzwhehe/loggate/internal/sink"
	sqlitesink "github.com/lzwhehe/loggate/internal/sink/sqlite"
)

func benchEvent(seq, testSeq uint64) *event.LogEvent {
	var k event.IdemKey
	k[0], k[1] = byte(seq), byte(seq>>8)
	return &event.LogEvent{Seq: seq, IdemKey: k, SourceID: "http:loadgen",
		Fields: map[string]any{"unmapped": map[string]any{"run_id": "r1", "test_seq": testSeq}}}
}

// Rows are read back from the real sqlite sink, and redelivery with the
// same idem key does not add a row.
func TestSQLiteRowsAndTail(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "siem.db")
	s := sqlitesink.New()
	if err := s.Init(ctx, map[string]any{"path": path}); err != nil {
		t.Fatal(err)
	}
	tail, err := OpenSQLiteTail(path)
	if err != nil {
		t.Fatal(err)
	}
	defer tail.Close()
	write := func(envs ...*sink.Envelope) {
		for _, e := range s.Write(ctx, envs) {
			if e != nil {
				t.Fatal(e)
			}
		}
	}
	write(&sink.Envelope{Seq: 1, Event: benchEvent(1, 101)}, &sink.Envelope{Seq: 2, Event: benchEvent(2, 102)})
	if got, err := tail.Next(ctx); err != nil || len(got) != 2 {
		t.Fatalf("tail 1: %v %v", got, err)
	}
	write(&sink.Envelope{Seq: 2, Event: benchEvent(2, 102)}, &sink.Envelope{Seq: 3, Event: benchEvent(3, 103)})
	if got, err := tail.Next(ctx); err != nil || len(got) != 1 || got[0].TestSeq != 103 {
		t.Fatalf("tail 2: %v %v", got, err)
	}
	if err := s.Close(ctx); err != nil {
		t.Fatal(err)
	}
	rows, err := SQLiteRows(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 3 || rows[0].TestSeq != 101 || rows[0].RunID != "r1" {
		t.Fatalf("rows: %+v", rows)
	}
}

func TestReadDLQMapsTestSeq(t *testing.T) {
	dir := t.TempDir()
	l, err := dlq.Open(dir, 0)
	if err != nil {
		t.Fatal(err)
	}
	body := []byte(`{"seq":7,"fields":{"unmapped":{"run_id":"r1","test_seq":42}}}`)
	if err := l.Append([]dlq.Entry{
		{Seq: 7, DestName: "soar", Reason: "non_retryable", Body: body},
		{Seq: 8, DestName: "soar", Reason: "undecodable", Body: []byte(`"AAAA"`)},
	}); err != nil {
		t.Fatal(err)
	}
	l.Close()
	ents, err := ReadDLQ(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(ents) != 2 || ents[0].TestSeq != 42 || ents[0].Dest != "soar" || ents[1].TestSeq != 0 {
		t.Fatalf("%+v", ents)
	}
}
