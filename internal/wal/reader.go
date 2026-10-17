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

func (w *WAL) segment(base uint64) *segMeta {
	w.segMu.RLock()
	defer w.segMu.RUnlock()
	i := sort.Search(len(w.segs), func(i int) bool { return w.segs[i].base >= base })
	if i < len(w.segs) && w.segs[i].base == base {
		return w.segs[i]
	}
	return nil
}

// ReadAt reads and validates the record of total length length at offset
// in segment segID. Only fsynced bytes are readable.
func (w *WAL) ReadAt(segID uint64, offset int64, length uint32) (Record, error) {
	m := w.segment(segID)
	if m == nil {
		return Record{}, fmt.Errorf("%w: unknown segment %d", ErrNotCommitted, segID)
	}
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
			f, err := it.w.fs.OpenFile(filepath.Join(it.w.opts.Dir, SegmentName(m.base)), os.O_RDONLY, 0)
			if err != nil {
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
