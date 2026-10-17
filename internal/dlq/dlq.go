// Package dlq is LogGate's dead-letter queue: an append-only, segmented log
// of events that will not be delivered (non-retryable error, retry budget
// exhausted, orphan TTL, L1 quota overflow).
//
// Each entry carries a copy of the event body, so the DLQ never pins WAL
// segments (DESIGN §4.8). Append returns only after fsync; callers must not
// commit the corresponding L1 Dead or tracker Resolve before that (DESIGN
// §0.5 item 4).
//
// Record layout (little-endian): | len u32 | crc32c u32 | JSON entry |.
package dlq

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/lzwhehe/loggate/internal/event"
)

const (
	segSuffix  = ".dlq"
	headerSize = 8
	maxEntry   = 128 << 20
)

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

// Entry is one dead letter.
type Entry struct {
	Seq       uint64          `json:"seq"`
	Dest      event.DestID    `json:"dest"`
	DestName  string          `json:"dest_name"`
	Reason    string          `json:"reason"`
	Attempts  uint16          `json:"attempts"`
	LastError string          `json:"last_error,omitempty"`
	IdemKey   event.IdemKey   `json:"idem_key"`
	At        int64           `json:"at"`   // unix nanos
	Body      json.RawMessage `json:"body"` // the event as stored in the WAL
}

// Log is the DLQ writer. Safe for concurrent use.
type Log struct {
	dir      string
	segBytes int64

	mu   sync.Mutex
	f    *os.File
	base uint64
	size int64

	total  atomic.Uint64
	byDest [event.MaxDests]atomic.Uint64
}

func segName(n uint64) string { return fmt.Sprintf("%020d%s", n, segSuffix) }

func listSegments(dir string) ([]uint64, error) {
	ents, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var out []uint64
	for _, e := range ents {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, segSuffix) {
			continue
		}
		if n, err := strconv.ParseUint(strings.TrimSuffix(name, segSuffix), 10, 64); err == nil {
			out = append(out, n)
		}
	}
	slices.Sort(out)
	return out, nil
}

// Open opens the DLQ in dir, truncating a torn tail of the last segment
// (an unsynced append was never reported as durable).
func Open(dir string, segBytes int64) (*Log, error) {
	if segBytes <= 0 {
		segBytes = 64 << 20
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("dlq: create dir: %w", err)
	}
	segs, err := listSegments(dir)
	if err != nil {
		return nil, fmt.Errorf("dlq: list: %w", err)
	}
	l := &Log{dir: dir, segBytes: segBytes}
	if len(segs) == 0 {
		return l, l.openSegment(1)
	}
	last := segs[len(segs)-1]
	f, err := os.OpenFile(filepath.Join(dir, segName(last)), os.O_RDWR, 0o644)
	if err != nil {
		return nil, fmt.Errorf("dlq: open segment: %w", err)
	}
	valid, _, err := scan(f, nil)
	if err != nil {
		if terr := f.Truncate(valid); terr != nil {
			f.Close()
			return nil, fmt.Errorf("dlq: truncate torn tail: %w", terr)
		}
		if serr := f.Sync(); serr != nil {
			f.Close()
			return nil, fmt.Errorf("dlq: sync: %w", serr)
		}
	}
	l.f, l.base, l.size = f, last, valid
	return l, nil
}

func (l *Log) openSegment(n uint64) error {
	f, err := os.OpenFile(filepath.Join(l.dir, segName(n)), os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return fmt.Errorf("dlq: create segment: %w", err)
	}
	l.f, l.base, l.size = f, n, 0
	return nil
}

// Append writes entries and fsyncs once. On error nothing may be assumed
// durable; the caller must keep responsibility for the events.
func (l *Log) Append(entries []Entry) error {
	if len(entries) == 0 {
		return nil
	}
	var buf []byte
	for i := range entries {
		b, err := json.Marshal(&entries[i])
		if err != nil {
			return fmt.Errorf("dlq: marshal seq %d: %w", entries[i].Seq, err)
		}
		buf = binary.LittleEndian.AppendUint32(buf, uint32(len(b)))
		buf = binary.LittleEndian.AppendUint32(buf, crc32.Checksum(b, castagnoli))
		buf = append(buf, b...)
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.f == nil {
		return errors.New("dlq: closed")
	}
	if l.size > 0 && l.size+int64(len(buf)) > l.segBytes {
		if err := l.f.Close(); err != nil {
			return fmt.Errorf("dlq: close segment: %w", err)
		}
		l.f = nil
		if err := l.openSegment(l.base + 1); err != nil {
			return err
		}
	}
	if _, err := l.f.WriteAt(buf, l.size); err != nil {
		// Drop the partial write so a later append does not land after it.
		_ = l.f.Truncate(l.size)
		return fmt.Errorf("dlq: write: %w", err)
	}
	if err := l.f.Sync(); err != nil {
		_ = l.f.Truncate(l.size)
		return fmt.Errorf("dlq: fsync: %w", err)
	}
	l.size += int64(len(buf))
	l.total.Add(uint64(len(entries)))
	for i := range entries {
		l.byDest[entries[i].Dest&63].Add(1)
	}
	return nil
}

// Total returns the number of entries appended by this process.
func (l *Log) Total() uint64 { return l.total.Load() }

// Count returns entries appended for d by this process.
func (l *Log) Count(d event.DestID) uint64 { return l.byDest[d&63].Load() }

// Close closes the active segment.
func (l *Log) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.f == nil {
		return nil
	}
	err := l.f.Close()
	l.f = nil
	if err != nil {
		return fmt.Errorf("dlq: close: %w", err)
	}
	return nil
}

// scan reads valid entries from f, calling fn for each (if non-nil). It
// returns the end offset of the valid prefix and the error that stopped it
// (nil at a clean end).
func scan(f *os.File, fn func(Entry) error) (int64, int, error) {
	br := bufio.NewReader(f)
	var off int64
	n := 0
	for {
		var h [headerSize]byte
		if _, err := io.ReadFull(br, h[:]); err != nil {
			if errors.Is(err, io.EOF) {
				return off, n, nil
			}
			return off, n, fmt.Errorf("dlq: torn header at %d", off)
		}
		ln, crc := binary.LittleEndian.Uint32(h[:4]), binary.LittleEndian.Uint32(h[4:])
		if ln > maxEntry {
			return off, n, fmt.Errorf("dlq: bad length at %d", off)
		}
		b := make([]byte, ln)
		if _, err := io.ReadFull(br, b); err != nil {
			return off, n, fmt.Errorf("dlq: torn entry at %d", off)
		}
		if crc32.Checksum(b, castagnoli) != crc {
			return off, n, fmt.Errorf("dlq: crc mismatch at %d", off)
		}
		if fn != nil {
			var e Entry
			if err := json.Unmarshal(b, &e); err != nil {
				return off, n, fmt.Errorf("dlq: decode at %d: %w", off, err)
			}
			if err := fn(e); err != nil {
				return off, n, err
			}
		}
		off += headerSize + int64(ln)
		n++
	}
}

// ReadAll returns every valid entry in dir, oldest first (a torn tail of
// the last segment is ignored).
func ReadAll(dir string) ([]Entry, error) {
	segs, err := listSegments(dir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, fmt.Errorf("dlq: list: %w", err)
	}
	var out []Entry
	for i, s := range segs {
		f, err := os.Open(filepath.Join(dir, segName(s)))
		if err != nil {
			return nil, fmt.Errorf("dlq: open: %w", err)
		}
		_, _, serr := scan(f, func(e Entry) error { out = append(out, e); return nil })
		f.Close()
		if serr != nil && i != len(segs)-1 {
			return out, serr
		}
	}
	return out, nil
}
