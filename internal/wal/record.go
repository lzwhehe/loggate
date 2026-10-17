package wal

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"

	"github.com/lzwhehe/loggate/internal/event"
)

// On-disk record layout (little-endian):
//
//	| len u32 | crc u32 | seq u64 | targets u64 | flags u8 | payload |
//
// len is the payload length. crc is CRC32C over everything after the crc
// field (seq, targets, flags, payload). len itself is not covered, but a
// corrupted len makes the CRC check fail (or exceed the segment) anyway.
const (
	HeaderSize = 4 + 4 + 8 + 8 + 1
	// MaxPayload bounds a single record; larger lengths are treated as
	// corruption during recovery.
	MaxPayload = 64 << 20
)

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

// ErrCorrupt reports a record that fails validation (bad CRC, bad length,
// or sequence numbers going backwards).
var ErrCorrupt = errors.New("wal: corrupt record")

// Record is one committed WAL record. SegID/Offset locate it on disk so
// later milestones (L1) can store a pointer instead of a copy.
type Record struct {
	Seq     uint64
	Targets event.DestMask
	Flags   uint8
	Payload []byte

	SegID  uint64
	Offset int64
}

// Len is the total on-disk length of the record.
func (r Record) Len() uint32 { return uint32(HeaderSize + len(r.Payload)) }

func appendRecord(dst []byte, seq uint64, targets event.DestMask, flags uint8, payload []byte) []byte {
	start := len(dst)
	dst = binary.LittleEndian.AppendUint32(dst, uint32(len(payload)))
	dst = binary.LittleEndian.AppendUint32(dst, 0) // crc placeholder
	dst = binary.LittleEndian.AppendUint64(dst, seq)
	dst = binary.LittleEndian.AppendUint64(dst, uint64(targets))
	dst = append(dst, flags)
	dst = append(dst, payload...)
	crc := crc32.Checksum(dst[start+8:], castagnoli)
	binary.LittleEndian.PutUint32(dst[start+4:], crc)
	return dst
}

// parseHeader returns payload length and stored crc.
func parseHeader(h []byte) (plen uint32, crc uint32) {
	return binary.LittleEndian.Uint32(h[0:4]), binary.LittleEndian.Uint32(h[4:8])
}

// decodeBody validates crc over body (= header[8:] + payload) and fills r.
func decodeBody(header, payload []byte, wantCRC uint32, r *Record) error {
	c := crc32.Update(0, castagnoli, header[8:HeaderSize])
	c = crc32.Update(c, castagnoli, payload)
	if c != wantCRC {
		return fmt.Errorf("%w: crc mismatch", ErrCorrupt)
	}
	r.Seq = binary.LittleEndian.Uint64(header[8:16])
	r.Targets = event.DestMask(binary.LittleEndian.Uint64(header[16:24]))
	r.Flags = header[24]
	r.Payload = payload
	return nil
}

// readRecordAt reads and validates one record at off. limit is the number of
// readable bytes in the segment; a record extending past it is reported as
// io.ErrUnexpectedEOF (torn).
func readRecordAt(f io.ReaderAt, off, limit int64) (Record, error) {
	var h [HeaderSize]byte
	if off+HeaderSize > limit {
		return Record{}, io.ErrUnexpectedEOF
	}
	if _, err := f.ReadAt(h[:], off); err != nil {
		return Record{}, fmt.Errorf("read header at %d: %w", off, err)
	}
	plen, crc := parseHeader(h[:])
	if plen > MaxPayload {
		return Record{}, fmt.Errorf("%w: payload length %d", ErrCorrupt, plen)
	}
	if off+HeaderSize+int64(plen) > limit {
		return Record{}, io.ErrUnexpectedEOF
	}
	payload := make([]byte, plen)
	if _, err := f.ReadAt(payload, off+HeaderSize); err != nil {
		return Record{}, fmt.Errorf("read payload at %d: %w", off, err)
	}
	r := Record{Offset: off}
	if err := decodeBody(h[:], payload, crc, &r); err != nil {
		return Record{}, fmt.Errorf("record at offset %d: %w", off, err)
	}
	return r, nil
}

// segmentScanner reads records sequentially through a buffered reader.
type segmentScanner struct {
	br    *bufio.Reader
	off   int64
	limit int64
	segID uint64
	err   error
}

func newSegmentScanner(f io.ReaderAt, segID uint64, start, limit int64) *segmentScanner {
	return &segmentScanner{
		br:    bufio.NewReaderSize(io.NewSectionReader(f, start, limit-start), 256<<10),
		off:   start,
		limit: limit,
		segID: segID,
	}
}

// next returns the next record, io.EOF at a clean end, io.ErrUnexpectedEOF
// for a truncated record, or an ErrCorrupt-wrapped error.
func (s *segmentScanner) next() (Record, error) {
	if s.err != nil {
		return Record{}, s.err
	}
	if s.off == s.limit {
		s.err = io.EOF
		return Record{}, io.EOF
	}
	var h [HeaderSize]byte
	if _, err := io.ReadFull(s.br, h[:]); err != nil {
		s.err = io.ErrUnexpectedEOF
		return Record{}, s.err
	}
	plen, crc := parseHeader(h[:])
	if plen > MaxPayload {
		s.err = fmt.Errorf("%w: payload length %d at offset %d", ErrCorrupt, plen, s.off)
		return Record{}, s.err
	}
	if s.off+HeaderSize+int64(plen) > s.limit {
		s.err = io.ErrUnexpectedEOF
		return Record{}, s.err
	}
	payload := make([]byte, plen)
	if _, err := io.ReadFull(s.br, payload); err != nil {
		s.err = io.ErrUnexpectedEOF
		return Record{}, s.err
	}
	r := Record{SegID: s.segID, Offset: s.off}
	if err := decodeBody(h[:], payload, crc, &r); err != nil {
		s.err = fmt.Errorf("segment %d offset %d: %w", s.segID, s.off, err)
		return Record{}, s.err
	}
	s.off += HeaderSize + int64(plen)
	return r, nil
}
