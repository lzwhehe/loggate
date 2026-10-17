package wal

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/lzwhehe/loggate/internal/clock"
	"github.com/lzwhehe/loggate/internal/event"
)

var errIO = errors.New("injected EIO")

func payloadFor(seq uint64) []byte { return []byte(fmt.Sprintf(`{"seq":%d,"msg":"hello"}`, seq)) }

func items(n int) []Item {
	out := make([]Item, n)
	for i := range out {
		out[i] = Item{Targets: event.DestMask(0).Set(0), Encode: func(seq uint64) ([]byte, error) { return payloadFor(seq), nil }}
	}
	return out
}

func open(t *testing.T, o Options) *WAL {
	t.Helper()
	if o.FsyncInterval == 0 {
		o.FsyncInterval = time.Millisecond
	}
	w, err := Open(o)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	return w
}

func mustAppend(t *testing.T, w *WAL, n int) Result {
	t.Helper()
	r, err := w.Append(context.Background(), items(n))
	if err != nil {
		t.Fatalf("Append(%d): %v", n, err)
	}
	if r.Count != n {
		t.Fatalf("Count = %d, want %d", r.Count, n)
	}
	return r
}

// readAll returns all committed records via a fresh iterator.
func readAll(t *testing.T, w *WAL) []Record {
	t.Helper()
	it := w.NewIterator(0)
	defer it.Close()
	var out []Record
	for {
		r, err := it.Next()
		if errors.Is(err, io.EOF) {
			return out
		}
		if err != nil {
			t.Fatalf("iterate: %v", err)
		}
		out = append(out, r)
	}
}

func seqsOf(recs []Record) []uint64 {
	out := make([]uint64, len(recs))
	for i, r := range recs {
		out[i] = r.Seq
	}
	return out
}

func span(from, to uint64) []uint64 {
	var out []uint64
	for s := from; s <= to; s++ {
		out = append(out, s)
	}
	return out
}

func checkPayloads(t *testing.T, recs []Record) {
	t.Helper()
	for _, r := range recs {
		if !bytes.Equal(r.Payload, payloadFor(r.Seq)) {
			t.Fatalf("seq %d payload = %q", r.Seq, r.Payload)
		}
	}
}

func TestRoundTrip(t *testing.T) {
	dir := t.TempDir()
	w := open(t, Options{Dir: dir})
	r1 := mustAppend(t, w, 3)
	r2 := mustAppend(t, w, 1)
	r3 := mustAppend(t, w, 5)
	if r1.FirstSeq != 1 || r2.FirstSeq != 4 || r3.FirstSeq != 5 {
		t.Fatalf("first seqs = %d,%d,%d", r1.FirstSeq, r2.FirstSeq, r3.FirstSeq)
	}
	recs := readAll(t, w)
	if got := seqsOf(recs); !slices.Equal(got, span(1, 9)) {
		t.Fatalf("seqs = %v", got)
	}
	checkPayloads(t, recs)
	for _, r := range recs {
		got, err := w.ReadAt(r.SegID, r.Offset, r.Len())
		if err != nil {
			t.Fatalf("ReadAt(seq %d): %v", r.Seq, err)
		}
		if got.Seq != r.Seq || !bytes.Equal(got.Payload, r.Payload) || got.Targets != r.Targets {
			t.Fatalf("ReadAt mismatch: %+v vs %+v", got, r)
		}
	}
	// Iterator from the middle.
	it := w.NewIterator(6)
	r, err := it.Next()
	it.Close()
	if err != nil || r.Seq != 6 {
		t.Fatalf("iterator from 6: seq %d err %v", r.Seq, err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := w.Append(context.Background(), items(1)); !errors.Is(err, ErrClosed) {
		t.Fatalf("append after close: %v", err)
	}

	// Reopen: data intact, seqs continue.
	w = open(t, Options{Dir: dir})
	defer w.Close()
	if got := seqsOf(readAll(t, w)); !slices.Equal(got, span(1, 9)) {
		t.Fatalf("after reopen seqs = %v", got)
	}
	if r := mustAppend(t, w, 1); r.FirstSeq != 10 {
		t.Fatalf("after reopen first seq = %d, want 10", r.FirstSeq)
	}
}

func TestCommittedStream(t *testing.T) {
	w := open(t, Options{Dir: t.TempDir(), EmitCommitted: true})
	var wg sync.WaitGroup
	var got []Record
	wg.Add(1)
	go func() {
		defer wg.Done()
		for b := range w.Committed() {
			got = append(got, b...)
		}
	}()
	for range 10 {
		mustAppend(t, w, 7)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	wg.Wait()
	if s := seqsOf(got); !slices.Equal(s, span(1, 70)) {
		t.Fatalf("committed seqs = %v", s)
	}
	checkPayloads(t, got)
}

func TestCRCCorruptionDetected(t *testing.T) {
	dir := t.TempDir()
	w := open(t, Options{Dir: dir})
	defer w.Close()
	mustAppend(t, w, 3)
	recs := readAll(t, w)
	victim := recs[1]

	// Flip one payload byte of the second record on disk.
	f, err := os.OpenFile(filepath.Join(dir, SegmentName(victim.SegID)), os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	b := []byte{0}
	off := victim.Offset + HeaderSize + 2
	if _, err := f.ReadAt(b, off); err != nil {
		t.Fatal(err)
	}
	b[0] ^= 0xff
	if _, err := f.WriteAt(b, off); err != nil {
		t.Fatal(err)
	}
	f.Close()

	if _, err := w.ReadAt(victim.SegID, victim.Offset, victim.Len()); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("ReadAt corrupt record: err = %v, want ErrCorrupt", err)
	}
	it := w.NewIterator(0)
	defer it.Close()
	if r, err := it.Next(); err != nil || r.Seq != 1 {
		t.Fatalf("first record: %d %v", r.Seq, err)
	}
	if _, err := it.Next(); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("iterator on corrupt record: err = %v, want ErrCorrupt", err)
	}
}

func TestReadAtBeyondCommitted(t *testing.T) {
	w := open(t, Options{Dir: t.TempDir()})
	defer w.Close()
	mustAppend(t, w, 1)
	r := readAll(t, w)[0]
	if _, err := w.ReadAt(r.SegID, r.Offset+1, r.Len()); !errors.Is(err, ErrNotCommitted) {
		t.Fatalf("err = %v, want ErrNotCommitted", err)
	}
	if _, err := w.ReadAt(999, 0, r.Len()); !errors.Is(err, ErrNotCommitted) {
		t.Fatalf("unknown segment err = %v", err)
	}
}

// A write torn mid-record followed by a crash: on reopen every acked record
// is still there, the torn tail is gone, and new seqs exceed all old ones.
func TestTornTailTruncatedOnReopen(t *testing.T) {
	dir := t.TempDir()
	ffs := NewFaultFS(nil)
	w := open(t, Options{Dir: dir, FS: ffs})
	acked := mustAppend(t, w, 5)
	lastAcked := acked.FirstSeq + uint64(acked.Count) - 1

	ffs.TearNextWrite(HeaderSize + 7) // first record complete header, partial payload
	if _, err := w.Append(context.Background(), items(3)); err == nil {
		t.Fatal("append during crash was acked")
	}
	_ = w.Close() // releases handles; errors expected after the crash

	seg := filepath.Join(dir, SegmentName(1))
	st, _ := os.Stat(seg)
	tornSize := st.Size()

	w2 := open(t, Options{Dir: dir})
	defer w2.Close()
	recs := readAll(t, w2)
	if got := seqsOf(recs); !slices.Equal(got, span(1, lastAcked)) {
		t.Fatalf("after reopen seqs = %v, want 1..%d", got, lastAcked)
	}
	checkPayloads(t, recs)
	st, _ = os.Stat(seg)
	if st.Size() >= tornSize {
		t.Fatalf("segment not truncated: %d >= %d", st.Size(), tornSize)
	}
	r := mustAppend(t, w2, 2)
	if r.FirstSeq <= lastAcked {
		t.Fatalf("new seq %d reuses acked seq <= %d", r.FirstSeq, lastAcked)
	}
	if got := seqsOf(readAll(t, w2)); len(got) != 7 || got[6] != r.FirstSeq+1 {
		t.Fatalf("seqs after new append = %v", got)
	}
}

// Garbage/partial bytes after the last record (e.g. kill -9 between write
// and fsync) are truncated; a corrupt final record is truncated too.
func TestRecoveryTruncatesGarbageTail(t *testing.T) {
	for name, tail := range map[string][]byte{
		"partial header": {1, 2, 3},
		"bad length":     bytes.Repeat([]byte{0xff}, HeaderSize+4),
		"bad crc":        appendRecord(nil, 100, 1, 0, []byte("x"))[:HeaderSize+1],
	} {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			w := open(t, Options{Dir: dir})
			mustAppend(t, w, 4)
			w.Close()
			if name == "bad crc" {
				tail[5] ^= 0xff
			}
			f, _ := os.OpenFile(filepath.Join(dir, SegmentName(1)), os.O_WRONLY|os.O_APPEND, 0)
			f.Write(tail)
			f.Close()

			w = open(t, Options{Dir: dir})
			defer w.Close()
			if got := seqsOf(readAll(t, w)); !slices.Equal(got, span(1, 4)) {
				t.Fatalf("seqs = %v", got)
			}
			if r := mustAppend(t, w, 1); r.FirstSeq != 5 {
				t.Fatalf("next seq = %d", r.FirstSeq)
			}
			if got := seqsOf(readAll(t, w)); !slices.Equal(got, span(1, 5)) {
				t.Fatalf("seqs after append = %v", got)
			}
		})
	}
}

func TestSeqFloor(t *testing.T) {
	dir := t.TempDir()
	w := open(t, Options{Dir: dir})
	mustAppend(t, w, 3)
	w.Close()
	w = open(t, Options{Dir: dir, SeqFloor: 1000})
	defer w.Close()
	if r := mustAppend(t, w, 1); r.FirstSeq != 1001 {
		t.Fatalf("first seq = %d, want 1001", r.FirstSeq)
	}
	// Floor below WAL max has no effect.
	if got := seqsOf(readAll(t, w)); !slices.Equal(got, []uint64{1, 2, 3, 1001}) {
		t.Fatalf("seqs = %v", got)
	}
}

func TestSegmentRollover(t *testing.T) {
	dir := t.TempDir()
	big := bytes.Repeat([]byte("x"), 1000)
	enc := func(seq uint64) ([]byte, error) { return append(fmt.Appendf(nil, "%d:", seq), big...), nil }
	w := open(t, Options{Dir: dir, SegmentBytes: 4096, FsyncInterval: -1})
	for range 20 {
		if _, err := w.Append(context.Background(), []Item{{Encode: enc}}); err != nil {
			t.Fatal(err)
		}
	}
	segs := w.Segments()
	if len(segs) < 5 {
		t.Fatalf("segments = %v, want >= 5", segs)
	}
	recs := readAll(t, w)
	if got := seqsOf(recs); !slices.Equal(got, span(1, 20)) {
		t.Fatalf("seqs = %v", got)
	}
	for _, r := range recs {
		if r.SegID > r.Seq {
			t.Fatalf("segment %d named after a seq greater than its record %d", r.SegID, r.Seq)
		}
	}
	for _, s := range segs {
		st, err := os.Stat(filepath.Join(dir, SegmentName(s)))
		if err != nil {
			t.Fatal(err)
		}
		if st.Size() > 4096 {
			t.Fatalf("segment %d size %d > 4096", s, st.Size())
		}
	}
	w.Close()
	w = open(t, Options{Dir: dir, SegmentBytes: 4096})
	defer w.Close()
	if got := seqsOf(readAll(t, w)); !slices.Equal(got, span(1, 20)) {
		t.Fatalf("after reopen seqs = %v", got)
	}
	if r := mustAppend(t, w, 1); r.FirstSeq != 21 {
		t.Fatalf("next seq = %d", r.FirstSeq)
	}
}

// fsync failure: batch is not acked, segment is poisoned and truncated to the
// last confirmed offset, the next batch succeeds on a new segment, and the
// failed batch's seqs are never reused.
func TestFsyncErrorPoisonsAndRolls(t *testing.T) {
	dir := t.TempDir()
	ffs := NewFaultFS(nil)
	w := open(t, Options{Dir: dir, FS: ffs})
	a := mustAppend(t, w, 2)
	segA := w.Segments()[0]
	st, _ := os.Stat(filepath.Join(dir, SegmentName(segA)))
	confirmed := st.Size()

	ffs.FailNextSync(errIO)
	if _, err := w.Append(context.Background(), items(2)); !errors.Is(err, errIO) {
		t.Fatalf("append with failing fsync: err = %v, want errIO", err)
	}
	if p := w.Poisoned(); !slices.Equal(p, []uint64{segA}) {
		t.Fatalf("poisoned = %v, want [%d]", p, segA)
	}
	st, _ = os.Stat(filepath.Join(dir, SegmentName(segA)))
	if st.Size() != confirmed {
		t.Fatalf("poisoned segment size %d, want truncated to %d", st.Size(), confirmed)
	}

	c := mustAppend(t, w, 2)
	if c.FirstSeq != a.FirstSeq+4 {
		t.Fatalf("seq after failed batch = %d, want %d (failed seqs burned)", c.FirstSeq, a.FirstSeq+4)
	}
	segs := w.Segments()
	if len(segs) != 2 || segs[1] != c.FirstSeq {
		t.Fatalf("segments = %v, want new segment %d", segs, c.FirstSeq)
	}
	want := []uint64{1, 2, 5, 6}
	if got := seqsOf(readAll(t, w)); !slices.Equal(got, want) {
		t.Fatalf("seqs = %v, want %v", got, want)
	}
	w.Close()
	w = open(t, Options{Dir: dir})
	defer w.Close()
	if got := seqsOf(readAll(t, w)); !slices.Equal(got, want) {
		t.Fatalf("after reopen seqs = %v, want %v", got, want)
	}
}

func TestENOSPCNotAcked(t *testing.T) {
	dir := t.TempDir()
	ffs := NewFaultFS(nil)
	w := open(t, Options{Dir: dir, FS: ffs})
	defer w.Close()
	mustAppend(t, w, 3)

	// Short write then ENOSPC: partial bytes must not survive.
	ffs.FailNextWritePartial(HeaderSize+3, ErrNoSpace)
	if _, err := w.Append(context.Background(), items(4)); !errors.Is(err, syscall.ENOSPC) {
		t.Fatalf("err = %v, want ENOSPC", err)
	}
	// Disk still full when the next segment is created.
	ffs.FailNextOpen(ErrNoSpace)
	if _, err := w.Append(context.Background(), items(1)); !errors.Is(err, syscall.ENOSPC) {
		t.Fatalf("err = %v, want ENOSPC on segment create", err)
	}
	// Space is back.
	r := mustAppend(t, w, 2)
	if r.FirstSeq != 9 {
		t.Fatalf("first seq = %d, want 9", r.FirstSeq)
	}
	if got := seqsOf(readAll(t, w)); !slices.Equal(got, []uint64{1, 2, 3, 9, 10}) {
		t.Fatalf("seqs = %v", got)
	}
}

// Every successful completion must be preceded by a successful fsync of the
// bytes it covers: when the callback runs, the most recent write/sync op on
// the filesystem is a successful sync.
func TestAckOnlyAfterFsync(t *testing.T) {
	ffs := NewFaultFS(nil)
	w := open(t, Options{Dir: t.TempDir(), FS: ffs, FsyncMaxEvents: 16})
	defer w.Close()

	var mu sync.Mutex
	var violations []string
	acks, fails := 0, 0
	check := func(r Result, err error) {
		mu.Lock()
		defer mu.Unlock()
		if err != nil {
			fails++
			return
		}
		acks++
		ops := ffs.Ops()
		for i := len(ops) - 1; i >= 0; i-- {
			switch ops[i].Kind {
			case OpSync:
				if ops[i].Err != nil {
					violations = append(violations, fmt.Sprintf("seq %d acked after failed sync", r.FirstSeq))
				}
				return
			case OpWrite:
				violations = append(violations, fmt.Sprintf("seq %d acked with unsynced write", r.FirstSeq))
				return
			}
		}
		violations = append(violations, fmt.Sprintf("seq %d acked with no sync at all", r.FirstSeq))
	}

	var wg sync.WaitGroup
	for g := range 20 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range 25 {
				if g == 7 && i == 10 {
					ffs.FailNextSync(errIO)
				}
				done := make(chan struct{})
				err := w.AppendAsync(context.Background(), items(1+i%3), func(r Result, err error) {
					check(r, err)
					close(done)
				})
				if err != nil {
					t.Error(err)
					return
				}
				<-done
			}
		}()
	}
	wg.Wait()
	if len(violations) > 0 {
		t.Fatalf("%d violations, first: %s", len(violations), violations[0])
	}
	if acks == 0 || fails == 0 {
		t.Fatalf("acks=%d fails=%d; want both > 0", acks, fails)
	}
}

// Group commit waits for FsyncInterval (driven by a fake clock) unless the
// batch reaches FsyncMaxEvents.
func TestGroupCommitIntervalAndMaxEvents(t *testing.T) {
	fc := clock.NewFake(time.Unix(0, 0))
	ffs := NewFaultFS(nil)
	w := open(t, Options{Dir: t.TempDir(), FS: ffs, Clock: fc, FsyncInterval: 10 * time.Millisecond, FsyncMaxEvents: 4})
	defer w.Close()

	acked := make(chan Result, 1)
	if err := w.AppendAsync(context.Background(), items(1), func(r Result, err error) {
		if err != nil {
			t.Error(err)
		}
		acked <- r
	}); err != nil {
		t.Fatal(err)
	}
	fc.BlockUntilTimers(1)
	select {
	case <-acked:
		t.Fatal("acked before fsync interval elapsed")
	case <-time.After(20 * time.Millisecond):
	}
	if n := ffs.SyncCount(); n != 0 {
		t.Fatalf("synced %d times before interval", n)
	}
	fc.Advance(10 * time.Millisecond)
	if r := <-acked; r.FirstSeq != 1 {
		t.Fatalf("first seq = %d", r.FirstSeq)
	}

	// A request that reaches FsyncMaxEvents commits without waiting.
	r, err := w.Append(context.Background(), items(4))
	if err != nil || r.FirstSeq != 2 {
		t.Fatalf("max-events batch: %+v %v", r, err)
	}
	if n := ffs.SyncCount(); n != 2 {
		t.Fatalf("sync count = %d, want 2", n)
	}
}

func TestEncodeErrorFailsOnlyThatRequest(t *testing.T) {
	w := open(t, Options{Dir: t.TempDir()})
	defer w.Close()
	bad := []Item{{Encode: func(uint64) ([]byte, error) { return nil, errIO }}}
	if _, err := w.Append(context.Background(), bad); !errors.Is(err, errIO) {
		t.Fatalf("err = %v", err)
	}
	if r := mustAppend(t, w, 1); r.FirstSeq != 1 {
		t.Fatalf("seq consumed by failed encode: first = %d", r.FirstSeq)
	}
}

// Simulated kill -9 after N acked appends: nothing is closed cleanly, yet a
// reopen from the same directory returns all N records.
func TestCrashAfterAckKeepsAllAcked(t *testing.T) {
	dir := t.TempDir()
	ffs := NewFaultFS(nil)
	w := open(t, Options{Dir: dir, FS: ffs, SegmentBytes: 8 << 10})
	var wg sync.WaitGroup
	var mu sync.Mutex
	var ackedSeqs []uint64
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range 25 {
				r, err := w.Append(context.Background(), items(1+i%4))
				if err != nil {
					t.Error(err)
					return
				}
				mu.Lock()
				ackedSeqs = append(ackedSeqs, span(r.FirstSeq, r.FirstSeq+uint64(r.Count)-1)...)
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	ffs.Crash()
	_ = w.Close()

	w2 := open(t, Options{Dir: dir})
	defer w2.Close()
	recs := readAll(t, w2)
	checkPayloads(t, recs)
	slices.Sort(ackedSeqs)
	if got := seqsOf(recs); !slices.Equal(got, ackedSeqs) {
		t.Fatalf("recovered %d records, acked %d", len(got), len(ackedSeqs))
	}
	if len(w2.Segments()) < 2 {
		t.Fatalf("expected several segments, got %v", w2.Segments())
	}
}
