package dlq

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func TestAppendReadRollAndTornTail(t *testing.T) {
	dir := t.TempDir()
	l, err := Open(dir, 512)
	if err != nil {
		t.Fatal(err)
	}
	for i := range 20 {
		e := Entry{Seq: uint64(i + 1), Dest: 2, Reason: "non_retryable", Attempts: 1, Body: json.RawMessage(`{"x":1}`)}
		if err := l.Append([]Entry{e}); err != nil {
			t.Fatal(err)
		}
	}
	if l.Total() != 20 || l.Count(2) != 20 {
		t.Fatalf("counts %d %d", l.Total(), l.Count(2))
	}
	l.Close()
	segs, _ := listSegments(dir)
	if len(segs) < 2 {
		t.Fatalf("expected rollover, got %v", segs)
	}
	// Torn tail on the last segment.
	last := filepath.Join(dir, segName(segs[len(segs)-1]))
	f, _ := os.OpenFile(last, os.O_APPEND|os.O_WRONLY, 0)
	f.Write([]byte{200, 0, 0, 0, 1, 2})
	f.Close()

	got, err := ReadAll(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 20 || got[0].Seq != 1 || got[19].Seq != 20 || string(got[3].Body) != `{"x":1}` {
		t.Fatalf("read %d entries: %+v", len(got), got[:1])
	}
	l, err = Open(dir, 512)
	if err != nil {
		t.Fatal(err)
	}
	if err := l.Append([]Entry{{Seq: 21, Body: json.RawMessage(`null`)}}); err != nil {
		t.Fatal(err)
	}
	l.Close()
	got, _ = ReadAll(dir)
	if len(got) != 21 || got[20].Seq != 21 {
		t.Fatalf("after reopen: %d", len(got))
	}
}
