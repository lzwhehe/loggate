package journal

import (
	"path/filepath"
	"strings"
	"testing"
)

func TestRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "j.csv")
	w, err := Create(path)
	if err != nil {
		t.Fatal(err)
	}
	in := []Record{
		{Dest: "soar", TestSeq: 1, IdemKey: "ab", RecvNS: 10, ScheduledNS: 5, Status: 200, GWSeq: 3, RunID: "r"},
		{Dest: "soar", TestSeq: 2, IdemKey: "cd", RecvNS: 11, ScheduledNS: 6, Status: 400, GWSeq: 4, RunID: "r"},
	}
	if err := w.Append(in); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	out, torn, err := ReadFile(path)
	if err != nil || torn != 0 || len(out) != 2 || out[0] != in[0] || out[1] != in[1] {
		t.Fatalf("%+v torn %d err %v", out, torn, err)
	}
	if out[1].Accepted() || !out[0].Accepted() {
		t.Fatal("Accepted")
	}
}

func TestTornLastLineTolerated(t *testing.T) {
	good := "soar,1,ab,10,5,200,3,r\n"
	for _, tail := range []string{"soar,2,cd,1", "soar,2,cd,11,6,200,4,r", "garbage\n"} {
		recs, torn, err := Read(strings.NewReader(Header + "\n" + good + tail))
		if err != nil || len(recs) != 1 || torn != 1 {
			t.Fatalf("tail %q: %d recs, torn %d, err %v", tail, len(recs), torn, err)
		}
	}
	if _, _, err := Read(strings.NewReader(good + "garbage\n" + good)); err == nil {
		t.Fatal("corrupt middle line accepted")
	}
}
