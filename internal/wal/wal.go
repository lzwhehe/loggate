// Package wal implements LogGate's segmented, append-only write-ahead log.
//
// Guarantees:
//   - A single writer goroutine assigns sequence numbers and group-commits
//     batches with one fsync (after FsyncInterval or FsyncMaxEvents).
//   - Append completions fire only after fsync returned nil. A failed write
//     or fsync fails (never acks) the whole batch; the segment is marked
//     poisoned, truncated to the last fsynced offset, and the next batch goes
//     to a new segment (DESIGN §0.5 item 6).
//   - Only fsynced records are emitted on the Committed stream, so a record
//     that might be lost to a torn tail is never dispatched (§0.5 item 1).
//   - On Open, invalid tails are truncated at the last valid record and the
//     next sequence number is max(last valid seq, floor)+1.
package wal

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lzwhehe/loggate/internal/clock"
	"github.com/lzwhehe/loggate/internal/event"
)

// ErrClosed is returned by Append after Close.
var ErrClosed = errors.New("wal: closed")

// ErrEmpty is returned when appending zero items.
var ErrEmpty = errors.New("wal: empty append")

const segSuffix = ".seg"

// SegmentName returns the file name of the segment whose first record has
// sequence number base.
func SegmentName(base uint64) string { return fmt.Sprintf("%020d%s", base, segSuffix) }

func parseSegmentName(name string) (uint64, bool) {
	if !strings.HasSuffix(name, segSuffix) || len(name) != 20+len(segSuffix) {
		return 0, false
	}
	v, err := strconv.ParseUint(strings.TrimSuffix(name, segSuffix), 10, 64)
	return v, err == nil
}

// Options configures a WAL.
type Options struct {
	Dir            string
	SegmentBytes   int64         // roll threshold; default 64MiB
	FsyncInterval  time.Duration // max time to wait for more events before fsync; 0 = fsync as soon as the queue is drained
	FsyncMaxEvents int           // fsync once a batch holds this many events; default 512
	// SeqFloor is a lower bound for sequence numbers: the next assigned
	// seq is > SeqFloor. Later milestones pass max(ckpt, L1 max seq).
	SeqFloor uint64
	// ScanFrom lets recovery skip validating segments that only hold seqs
	// <= ScanFrom (everything at or below min(ckpt) is never replayed, and
	// L1-referenced records are CRC-checked on every ReadAt). The last
	// segment is always scanned.
	ScanFrom uint64
	// EmitCommitted enables the Committed stream. If enabled the consumer
	// must keep reading, or the writer (and thus ingest) blocks.
	EmitCommitted   bool
	CommittedBuffer int // channel capacity in batches; default 64

	FS     FS          // default OSFS{}
	Clock  clock.Clock // default clock.Real()
	Logger *slog.Logger
}

func (o *Options) withDefaults() {
	if o.SegmentBytes <= 0 {
		o.SegmentBytes = 64 << 20
	}
	if o.FsyncMaxEvents <= 0 {
		o.FsyncMaxEvents = 512
	}
	if o.CommittedBuffer <= 0 {
		o.CommittedBuffer = 64
	}
	if o.FS == nil {
		o.FS = OSFS{}
	}
	if o.Clock == nil {
		o.Clock = clock.Real()
	}
	if o.Logger == nil {
		o.Logger = slog.New(slog.DiscardHandler)
	}
}

// Item is one event to append. Encode is called on the writer goroutine
// with the assigned sequence number and must return the payload; it must
// be fast and must not block. If it fails, the whole Append fails and its
// sequence numbers are not consumed.
type Item struct {
	Targets event.DestMask
	Flags   uint8
	Encode  func(seq uint64) ([]byte, error)
}

// Result describes a successful append: seqs FirstSeq..FirstSeq+Count-1.
type Result struct {
	FirstSeq uint64
	Count    int
}

type request struct {
	items []Item
	done  func(Result, error)
	// filled during commit
	first uint64
}

// segMeta is the reader-visible view of a segment.
type segMeta struct {
	base      uint64
	committed atomic.Int64 // bytes covered by a successful fsync
	poisoned  atomic.Bool

	// guarded by WAL.segMu
	readers int  // open reader handles (ReadAt, Iterator)
	removed bool // RemoveSegment called; file deleted when readers == 0
}

// activeSeg is the writer-owned open segment.
type activeSeg struct {
	meta   *segMeta
	f      File
	size   int64 // bytes written
	synced int64 // bytes covered by the last successful fsync
}

// WAL is a segmented write-ahead log. Create it with Open.
type WAL struct {
	opts Options
	fs   FS
	log  *slog.Logger

	reqCh     chan *request
	closeCh   chan struct{}
	writerEnd chan struct{}
	committed chan []Record

	closeMu sync.RWMutex
	closed  bool

	segMu sync.RWMutex
	segs  []*segMeta // ascending by base

	nextSeq atomic.Uint64 // owned by the writer goroutine; atomic for readers of NextSeq

	// writer-goroutine state
	active   *activeSeg
	closeErr error
}

// Open recovers the WAL in opts.Dir and starts the writer goroutine.
func Open(opts Options) (*WAL, error) {
	opts.withDefaults()
	if opts.Dir == "" {
		return nil, errors.New("wal: empty dir")
	}
	w := &WAL{
		opts:      opts,
		fs:        opts.FS,
		log:       opts.Logger.With("component", "wal"),
		reqCh:     make(chan *request, 1024),
		closeCh:   make(chan struct{}),
		writerEnd: make(chan struct{}),
	}
	if opts.EmitCommitted {
		w.committed = make(chan []Record, opts.CommittedBuffer)
	}
	if err := w.recover(); err != nil {
		return nil, err
	}
	go w.run()
	return w, nil
}

// recover scans segments, truncates invalid tails, and computes nextSeq.
// Segments entirely at or below Options.ScanFrom are not scanned.
func (w *WAL) recover() error {
	if err := w.fs.MkdirAll(w.opts.Dir, 0o755); err != nil {
		return fmt.Errorf("wal: create dir: %w", err)
	}
	names, err := w.fs.ReadDir(w.opts.Dir)
	if err != nil {
		return fmt.Errorf("wal: list dir: %w", err)
	}
	var bases []uint64
	for _, n := range names {
		if b, ok := parseSegmentName(n); ok {
			bases = append(bases, b)
		}
	}
	slices.Sort(bases)

	var maxSeq uint64
	skipped := 0
	for i, base := range bases {
		last := i == len(bases)-1
		path := filepath.Join(w.opts.Dir, SegmentName(base))
		if !last && bases[i+1] <= w.opts.ScanFrom+1 {
			// Every seq in this segment is < bases[i+1] <= ScanFrom+1.
			size, err := w.fileSize(path)
			if err != nil {
				return fmt.Errorf("wal: stat segment %d: %w", base, err)
			}
			m := &segMeta{base: base}
			m.committed.Store(size)
			w.segs = append(w.segs, m)
			skipped++
			continue
		}
		f, err := w.fs.OpenFile(path, os.O_RDWR, 0)
		if err != nil {
			return fmt.Errorf("wal: open segment %d: %w", base, err)
		}
		validEnd, segMax, n, scanErr := scanForRecovery(f, base, maxSeq)
		if scanErr != nil {
			size, _ := f.Size()
			lvl := slog.LevelWarn
			if !last {
				// Only a poisoned segment (failed truncate after an fsync
				// error) or media corruption can leave a bad tail here.
				lvl = slog.LevelError
			}
			w.log.Log(context.Background(), lvl, "truncating invalid segment tail",
				"segment", base, "valid_end", validEnd, "size", size, "records", n, "cause", scanErr)
			if err := f.Truncate(validEnd); err != nil {
				f.Close()
				return fmt.Errorf("wal: truncate segment %d to %d: %w", base, validEnd, err)
			}
			if err := f.Sync(); err != nil {
				f.Close()
				return fmt.Errorf("wal: sync truncated segment %d: %w", base, err)
			}
		}
		if n > 0 {
			maxSeq = segMax
		}
		m := &segMeta{base: base}
		m.committed.Store(validEnd)
		w.segs = append(w.segs, m)
		if last {
			w.active = &activeSeg{meta: m, f: f, size: validEnd, synced: validEnd}
		} else if err := f.Close(); err != nil {
			return fmt.Errorf("wal: close segment %d: %w", base, err)
		}
	}

	next := max(maxSeq, w.opts.SeqFloor) + 1
	if w.active != nil && w.active.size == 0 && w.active.meta.base > next {
		// Empty last segment created for seqs that were never committed.
		next = w.active.meta.base
	}
	w.nextSeq.Store(next)
	w.log.Info("wal recovered", "segments", len(w.segs), "skipped_scan", skipped, "max_seq", maxSeq, "next_seq", next)
	return nil
}

func (w *WAL) fileSize(path string) (int64, error) {
	f, err := w.fs.OpenFile(path, os.O_RDONLY, 0)
	if err != nil {
		return 0, err
	}
	defer f.Close()
	return f.Size()
}

// scanForRecovery returns the end of the valid prefix of a segment, the max
// seq in it, the record count, and the error that stopped the scan (nil if
// the whole file is valid). Records must have strictly increasing seqs that
// are >= base and > prevMax.
func scanForRecovery(f File, base, prevMax uint64) (validEnd int64, segMax uint64, n int, err error) {
	size, err := f.Size()
	if err != nil {
		return 0, 0, 0, fmt.Errorf("stat: %w", err)
	}
	sc := newSegmentScanner(f, base, 0, size)
	last := prevMax
	for {
		r, err := sc.next()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return sc.off, last, n, nil
			}
			return sc.off, last, n, err
		}
		if r.Seq < base || (last != 0 && r.Seq <= last) {
			return r.Offset, last, n, fmt.Errorf("%w: seq %d out of order (base %d, prev %d)", ErrCorrupt, r.Seq, base, last)
		}
		last = r.Seq
		n++
	}
}

// NextSeq returns the sequence number the next appended event will get
// (approximate while appends are in flight).
func (w *WAL) NextSeq() uint64 { return w.nextSeq.Load() }

// Committed returns the stream of fsynced record batches in seq order, or
// nil if Options.EmitCommitted is false. It is closed by Close after the
// final batch.
func (w *WAL) Committed() <-chan []Record { return w.committed }

// Segments returns the bases of all known segments in ascending order.
func (w *WAL) Segments() []uint64 {
	w.segMu.RLock()
	defer w.segMu.RUnlock()
	out := make([]uint64, len(w.segs))
	for i, s := range w.segs {
		out[i] = s.base
	}
	return out
}

// Poisoned returns the bases of segments poisoned by write/fsync errors
// during this process lifetime.
func (w *WAL) Poisoned() []uint64 {
	w.segMu.RLock()
	defer w.segMu.RUnlock()
	var out []uint64
	for _, s := range w.segs {
		if s.poisoned.Load() {
			out = append(out, s.base)
		}
	}
	return out
}

// AppendAsync enqueues items. done is called exactly once, on the writer
// goroutine, after the batch was fsynced (err == nil) or failed. done must
// not block. AppendAsync itself may block when the queue is full.
func (w *WAL) AppendAsync(ctx context.Context, items []Item, done func(Result, error)) error {
	if len(items) == 0 {
		return ErrEmpty
	}
	w.closeMu.RLock()
	defer w.closeMu.RUnlock()
	if w.closed {
		return ErrClosed
	}
	select {
	case w.reqCh <- &request{items: items, done: done}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Append appends items and waits until they are durable. If ctx ends first
// it returns ctx.Err(); the items may still be committed later (they were
// not acknowledged, so that is safe).
func (w *WAL) Append(ctx context.Context, items []Item) (Result, error) {
	type res struct {
		r   Result
		err error
	}
	ch := make(chan res, 1)
	if err := w.AppendAsync(ctx, items, func(r Result, err error) { ch <- res{r, err} }); err != nil {
		return Result{}, err
	}
	select {
	case x := <-ch:
		return x.r, x.err
	case <-ctx.Done():
		return Result{}, ctx.Err()
	}
}

// Close stops accepting appends, commits everything already queued, closes
// the active segment, and closes the Committed stream.
func (w *WAL) Close() error {
	w.closeMu.Lock()
	if w.closed {
		w.closeMu.Unlock()
		<-w.writerEnd
		return nil
	}
	w.closed = true
	w.closeMu.Unlock()
	close(w.closeCh)
	<-w.writerEnd
	return w.closeErr
}

// run is the single writer goroutine.
func (w *WAL) run() {
	defer close(w.writerEnd)
	for {
		var first *request
		select {
		case first = <-w.reqCh:
		case <-w.closeCh:
			w.shutdown()
			return
		}
		batch := []*request{first}
		n := len(first.items)
		closing := false
		if w.opts.FsyncInterval > 0 && n < w.opts.FsyncMaxEvents {
			t := w.opts.Clock.NewTimer(w.opts.FsyncInterval)
		collect:
			for n < w.opts.FsyncMaxEvents {
				select {
				case r := <-w.reqCh:
					batch = append(batch, r)
					n += len(r.items)
				case <-t.C():
					break collect
				case <-w.closeCh:
					closing = true
					break collect
				}
			}
			t.Stop()
		} else {
			// No wait: take whatever is already queued.
		drain:
			for n < w.opts.FsyncMaxEvents {
				select {
				case r := <-w.reqCh:
					batch = append(batch, r)
					n += len(r.items)
				default:
					break drain
				}
			}
		}
		w.commit(batch)
		if closing {
			w.shutdown()
			return
		}
	}
}

// shutdown commits all still-queued requests and releases resources. Called
// on the writer goroutine after closeCh is closed; no new sends can happen.
func (w *WAL) shutdown() {
	for {
		var batch []*request
		n := 0
	drain:
		for n < w.opts.FsyncMaxEvents {
			select {
			case r := <-w.reqCh:
				batch = append(batch, r)
				n += len(r.items)
			default:
				break drain
			}
		}
		if len(batch) == 0 {
			break
		}
		w.commit(batch)
	}
	if w.active != nil {
		if err := w.active.f.Close(); err != nil {
			w.closeErr = fmt.Errorf("wal: close segment %d: %w", w.active.meta.base, err)
		}
		w.active = nil
	}
	if w.committed != nil {
		close(w.committed)
	}
}

type pendingRec struct {
	seq     uint64
	targets event.DestMask
	flags   uint8
	off     int // offset within the batch buffer
	end     int
}

// commit encodes, writes, and fsyncs one group-commit batch.
func (w *WAL) commit(batch []*request) {
	var (
		buf  []byte
		recs []pendingRec
		ok   []*request
		next = w.nextSeq.Load()
	)
	for _, r := range batch {
		mark, recMark := len(buf), len(recs)
		r.first = next
		var encErr error
		for i, it := range r.items {
			seq := next + uint64(i)
			p, err := it.Encode(seq)
			if err == nil && len(p) > MaxPayload {
				err = fmt.Errorf("payload %d bytes exceeds max %d", len(p), MaxPayload)
			}
			if err != nil {
				encErr = fmt.Errorf("wal: encode seq %d: %w", seq, err)
				break
			}
			off := len(buf)
			buf = appendRecord(buf, seq, it.Targets, it.Flags, p)
			recs = append(recs, pendingRec{seq: seq, targets: it.Targets, flags: it.Flags, off: off, end: len(buf)})
		}
		if encErr != nil {
			buf, recs = buf[:mark], recs[:recMark]
			r.done(Result{}, encErr)
			continue
		}
		next += uint64(len(r.items))
		ok = append(ok, r)
	}
	if len(ok) == 0 {
		return
	}
	// Seqs handed out in this batch are burned even if the batch fails, so a
	// seq that might exist in a poisoned segment is never reassigned.
	w.nextSeq.Store(next)

	fail := func(err error) {
		for _, r := range ok {
			r.done(Result{}, err)
		}
	}

	if w.active == nil || (w.active.size > 0 && w.active.size+int64(len(buf)) > w.opts.SegmentBytes) {
		if err := w.roll(ok[0].first); err != nil {
			fail(err)
			return
		}
	}
	seg := w.active
	base := seg.size
	if _, err := seg.f.WriteAt(buf, base); err != nil {
		err = fmt.Errorf("wal: write segment %d at %d: %w", seg.meta.base, base, err)
		w.poison(err)
		fail(err)
		return
	}
	seg.size += int64(len(buf))
	if err := seg.f.Sync(); err != nil {
		err = fmt.Errorf("wal: fsync segment %d: %w", seg.meta.base, err)
		w.poison(err)
		fail(err)
		return
	}
	seg.synced = seg.size
	seg.meta.committed.Store(seg.synced)

	// Durable: acknowledge, then hand records to the dispatcher.
	for _, r := range ok {
		r.done(Result{FirstSeq: r.first, Count: len(r.items)}, nil)
	}
	if w.committed != nil {
		out := make([]Record, len(recs))
		for i, pr := range recs {
			out[i] = Record{
				Seq: pr.seq, Targets: pr.targets, Flags: pr.flags,
				Payload: buf[pr.off+HeaderSize : pr.end],
				SegID:   seg.meta.base, Offset: base + int64(pr.off),
			}
		}
		w.committed <- out
	}
}

// roll closes the active segment (already fully fsynced) and creates a new
// one named after base, the first seq it will hold.
func (w *WAL) roll(base uint64) error {
	if w.active != nil {
		if err := w.active.f.Close(); err != nil {
			w.log.Warn("close rolled segment", "segment", w.active.meta.base, "err", err)
		}
		w.active = nil
	}
	path := filepath.Join(w.opts.Dir, SegmentName(base))
	f, err := w.fs.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return fmt.Errorf("wal: create segment %d: %w", base, err)
	}
	if err := w.fs.SyncDir(w.opts.Dir); err != nil {
		f.Close()
		return fmt.Errorf("wal: sync dir after creating segment %d: %w", base, err)
	}
	m := &segMeta{base: base}
	w.segMu.Lock()
	w.segs = append(w.segs, m)
	w.segMu.Unlock()
	w.active = &activeSeg{meta: m, f: f}
	return nil
}

// poison handles a write or fsync failure on the active segment: mark it,
// truncate to the last fsynced offset, close it, and force the next batch
// onto a new segment. After a failed fsync the page cache state is unknown
// (fsyncgate), so the segment is never written again.
func (w *WAL) poison(cause error) {
	seg := w.active
	w.active = nil
	seg.meta.poisoned.Store(true)
	w.log.Error("segment poisoned", "segment", seg.meta.base, "confirmed", seg.synced, "cause", cause)
	if err := seg.f.Truncate(seg.synced); err != nil {
		w.log.Error("truncate poisoned segment (recovery will truncate the tail)", "segment", seg.meta.base, "err", err)
	} else if err := seg.f.Sync(); err != nil {
		w.log.Error("sync poisoned segment", "segment", seg.meta.base, "err", err)
	}
	if err := seg.f.Close(); err != nil {
		w.log.Warn("close poisoned segment", "segment", seg.meta.base, "err", err)
	}
}
