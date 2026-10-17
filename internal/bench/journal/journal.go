// Package journal is the mocksink receive journal: one CSV line per event
// seen by a mock destination,
//
//	dest,test_seq,idem_key,recv_unix_ns,scheduled_send_ns,status,gw_seq,run_id
//
// status is the HTTP status the mock answered for the request carrying the
// event (2xx = accepted, anything else = rejected by fault injection).
// recv_unix_ns is taken when the mock decides the outcome, i.e. after any
// injected delay: that is when the receiver "has" the event.
package journal

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"sync"
)

// Header is the CSV header line.
const Header = "dest,test_seq,idem_key,recv_unix_ns,scheduled_send_ns,status,gw_seq,run_id"

// Record is one journal line.
type Record struct {
	Dest        string
	TestSeq     uint64
	IdemKey     string
	RecvNS      int64
	ScheduledNS int64
	Status      int
	GWSeq       uint64
	RunID       string
}

// Accepted reports whether the mock accepted the event (2xx).
func (r Record) Accepted() bool { return r.Status >= 200 && r.Status < 300 }

// Writer appends records; safe for concurrent use.
type Writer struct {
	mu sync.Mutex
	f  *os.File
	w  *bufio.Writer
}

// Create creates (truncates) a journal file and writes the header.
func Create(path string) (*Writer, error) {
	f, err := os.Create(path)
	if err != nil {
		return nil, err
	}
	w := &Writer{f: f, w: bufio.NewWriterSize(f, 1<<20)}
	if _, err := w.w.WriteString(Header + "\n"); err != nil {
		f.Close()
		return nil, err
	}
	return w, nil
}

// Append writes records (buffered).
func (w *Writer) Append(recs []Record) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.f == nil {
		return errors.New("journal: closed")
	}
	var b []byte
	for _, r := range recs {
		b = b[:0]
		b = append(b, r.Dest...)
		b = append(b, ',')
		b = strconv.AppendUint(b, r.TestSeq, 10)
		b = append(b, ',')
		b = append(b, r.IdemKey...)
		b = append(b, ',')
		b = strconv.AppendInt(b, r.RecvNS, 10)
		b = append(b, ',')
		b = strconv.AppendInt(b, r.ScheduledNS, 10)
		b = append(b, ',')
		b = strconv.AppendInt(b, int64(r.Status), 10)
		b = append(b, ',')
		b = strconv.AppendUint(b, r.GWSeq, 10)
		b = append(b, ',')
		b = append(b, r.RunID...)
		b = append(b, '\n')
		if _, err := w.w.Write(b); err != nil {
			return err
		}
	}
	return nil
}

// Flush writes buffered records to the OS.
func (w *Writer) Flush() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.f == nil {
		return nil
	}
	return w.w.Flush()
}

// Close flushes and closes.
func (w *Writer) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.f == nil {
		return nil
	}
	ferr := w.w.Flush()
	cerr := w.f.Close()
	w.f = nil
	return errors.Join(ferr, cerr)
}

// ParseLine parses one CSV line (without newline).
func ParseLine(line string) (Record, error) {
	p := strings.Split(line, ",")
	if len(p) != 8 {
		return Record{}, fmt.Errorf("journal: want 8 fields, got %d", len(p))
	}
	var r Record
	var err error
	r.Dest, r.IdemKey, r.RunID = p[0], p[2], p[7]
	if r.TestSeq, err = strconv.ParseUint(p[1], 10, 64); err != nil {
		return r, fmt.Errorf("journal: test_seq: %w", err)
	}
	if r.RecvNS, err = strconv.ParseInt(p[3], 10, 64); err != nil {
		return r, fmt.Errorf("journal: recv_unix_ns: %w", err)
	}
	if r.ScheduledNS, err = strconv.ParseInt(p[4], 10, 64); err != nil {
		return r, fmt.Errorf("journal: scheduled_send_ns: %w", err)
	}
	if r.Status, err = strconv.Atoi(p[5]); err != nil {
		return r, fmt.Errorf("journal: status: %w", err)
	}
	if r.GWSeq, err = strconv.ParseUint(p[6], 10, 64); err != nil {
		return r, fmt.Errorf("journal: gw_seq: %w", err)
	}
	return r, nil
}

// Read parses a journal. A malformed or unterminated last line (the
// writer was killed mid-write) is skipped and counted in torn; a malformed
// line elsewhere is an error.
func Read(r io.Reader) (recs []Record, torn int, err error) {
	br := bufio.NewReaderSize(r, 1<<20)
	first := true
	var pending error
	for {
		line, rerr := br.ReadString('\n')
		if rerr != nil && rerr != io.EOF {
			return recs, torn, rerr
		}
		complete := strings.HasSuffix(line, "\n")
		line = strings.TrimRight(line, "\r\n")
		if pending != nil && line != "" {
			return recs, torn, pending // a bad line that was not the last one
		}
		if line != "" {
			if first && line == Header {
				first = false
			} else {
				first = false
				rec, perr := ParseLine(line)
				switch {
				case perr == nil && complete:
					recs = append(recs, rec)
				case perr == nil && !complete:
					torn++ // unterminated: may be truncated mid-number
				default:
					pending = perr
					torn++
				}
			}
		}
		if rerr == io.EOF {
			return recs, torn, nil
		}
	}
}

// ReadFile reads a journal file.
func ReadFile(path string) ([]Record, int, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, 0, err
	}
	defer f.Close()
	return Read(f)
}
