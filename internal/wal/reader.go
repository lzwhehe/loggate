package wal

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
)

// ErrNotCommitted is returned when reading beyond the fsynced end of a
// segment, or from a segment that does not exist.
var ErrNotCommitted = errors.New("wal: record not committed")

// findSeg returns the index of base in w.segs, or -1. Caller holds segMu.
func (w *WAL) findSeg(base uint64) int {
	i := sort.Search(len(w.segs), func(i int) bool { return w.segs[i].base >= base })
	if i < len(w.segs) && w.segs[i].base == base {
		return i
	}
	return -1
}

// acquire pins segment base against deletion while a reader has it open.
func (w *WAL) acquire(base uint64) *segMeta {
	w.segMu.Lock()
	defer w.segMu.Unlock()
	i := w.findSeg(base)
	if i < 0 {
		return nil
	}
	m := w.segs[i]
	m.readers++
	return m
}

// acquireMeta pins a segment taken from a snapshot; nil if already removed.
func (w *WAL) acquireMeta(m *segMeta) bool {
	w.segMu.Lock()
	defer w.segMu.Unlock()
	if m.removed {
		return false
	}
	m.readers++
	return true
}

// release unpins a segment and performs a deferred removal.
func (w *WAL) release(m *segMeta) {
	w.segMu.Lock()
	m.readers--
	del := m.removed && m.readers == 0
	w.segMu.Unlock()
	if del {
		w.deleteFile(m.base)
	}
}

func (w *WAL) deleteFile(base uint64) {
	if err := w.fs.Remove(filepath.Join(w.opts.Dir, SegmentName(base))); err != nil {
		w.log.Warn("remove segment", "segment", base, "err", err)
	}
}

// ErrActiveSegment is returned when trying to remove the newest segment.
var ErrActiveSegment = errors.New("wal: cannot remove the active segment")

// RemoveSegment deletes segment base (GC). The newest segment is never
// removed. If readers still have the file open (Windows cannot delete open
// files), deletion is deferred until the last one releases it; the segment
// is unreadable for new readers immediately.
func (w *WAL) RemoveSegment(base uint64) error {
	w.segMu.Lock()
	i := w.findSeg(base)
	if i < 0 {
		w.segMu.Unlock()
		return fmt.Errorf("%w: unknown segment %d", ErrNotCommitted, base)
	}
	if i == len(w.segs)-1 {
		w.segMu.Unlock()
		return ErrActiveSegment
	}
	m := w.segs[i]
	w.segs = append(w.segs[:i:i], w.segs[i+1:]...)
	m.removed = true
	now := m.readers == 0
	w.segMu.Unlock()
	if now {
		if err := w.fs.Remove(filepath.Join(w.opts.Dir, SegmentName(base))); err != nil {
			return fmt.Errorf("wal: remove segment %d: %w", base, err)
		}
	}
	return nil
}

// ReadAt reads and validates the record of total length length at offset
// in segment segID. Only fsynced bytes are readable.
func (w *WAL) ReadAt(segID uint64, offset int64, length uint32) (Record, error) {
	m := w.acquire(segID)
	if m == nil {
		return Record{}, fmt.Errorf("%w: unknown segment %d", ErrNotCommitted, segID)
	}
	defer w.release(m)
	limit := m.committed.Load()
	if offset < 0 || offset+int64(length) > limit {
		return Record{}, fmt.Errorf("%w: segment %d range [%d,+%d) beyond committed %d", ErrNotCommitted, segID, offset, length, limit)
	}
	f, err := w.fs.OpenFile(filepath.Join(w.opts.Dir, SegmentName(segID)), os.O_RDONLY, 0)
	if err != nil {
		return Record{}, fmt.Errorf("wal: open segment %d: %w", segID, err)
	}
	defer f.Close()
	r, err := readRecordAt(f, offset, offset+int64(length))
	if err != nil {
		return Record{}, fmt.Errorf("wal: segment %d: %w", segID, err)
	}
	if r.Len() != length {
		return Record{}, fmt.Errorf("%w: segment %d offset %d: length %d, want %d", ErrCorrupt, segID, offset, r.Len(), length)
	}
	r.SegID = segID
	return r, nil
}

// Iterator reads committed records sequentially. It sees records committed
// up to the moment each segment is opened. Not safe for concurrent use.
type Iterator struct {
	w    *WAL
	from uint64
	segs []*segMeta
	idx  int
	f    File
	cur  *segMeta // pinned segment
	sc   *segmentScanner
	err  error
}

// NewIterator returns an iterator over committed records with seq >= from.
// Call Close when done.
func (w *WAL) NewIterator(from uint64) *Iterator {
	w.segMu.RLock()
	segs := append([]*segMeta(nil), w.segs...)
	w.segMu.RUnlock()
	// Start at the last segment whose base <= from.
	start := sort.Search(len(segs), func(i int) bool { return segs[i].base > from }) - 1
	return &Iterator{w: w, from: from, segs: segs, idx: max(start, 0)}
}

// Next returns the next record, or io.EOF when no more committed records
// exist. A corrupt record yields an ErrCorrupt-wrapped error.
func (it *Iterator) Next() (Record, error) {
	for it.err == nil {
		if it.sc == nil {
			if it.idx >= len(it.segs) {
				it.err = io.EOF
				break
			}
			m := it.segs[it.idx]
			if !it.w.acquireMeta(m) {
				it.err = fmt.Errorf("%w: segment %d removed during iteration", ErrNotCommitted, m.base)
				break
			}
			it.cur = m
			f, err := it.w.fs.OpenFile(filepath.Join(it.w.opts.Dir, SegmentName(m.base)), os.O_RDONLY, 0)
			if err != nil {
				it.closeSeg()
				it.err = fmt.Errorf("wal: open segment %d: %w", m.base, err)
				break
			}
			it.f = f
			it.sc = newSegmentScanner(f, m.base, 0, m.committed.Load())
		}
		r, err := it.sc.next()
		if errors.Is(err, io.EOF) {
			it.closeSeg()
			it.idx++
			continue
		}
		if err != nil {
			if errors.Is(err, io.ErrUnexpectedEOF) {
				err = fmt.Errorf("%w: truncated record in committed range: %v", ErrCorrupt, err)
			}
			it.err = err
			break
		}
		if r.Seq < it.from {
			continue
		}
		return r, nil
	}
	return Record{}, it.err
}

func (it *Iterator) closeSeg() {
	if it.f != nil {
		it.f.Close()
		it.f = nil
	}
	if it.cur != nil {
		it.w.release(it.cur)
		it.cur = nil
	}
	it.sc = nil
}

// Close releases the iterator's file handle.
func (it *Iterator) Close() error {
	it.closeSeg()
	if it.err == nil {
		it.err = io.EOF
	}
	return nil
}
