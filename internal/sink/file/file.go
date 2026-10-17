// Package file implements a JSON-lines file sink. It is deliberately not
// idempotent: it exposes the raw duplicate rate of the delivery pipeline.
package file

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/lzwhehe/loggate/internal/event"
	"github.com/lzwhehe/loggate/internal/sink"
)

// Type is the destination type name used in configuration.
const Type = "file"

// Line is the JSON written for each event.
type Line struct {
	Seq        uint64         `json:"seq"`
	IdemKey    event.IdemKey  `json:"idem_key"`
	SourceID   string         `json:"source_id"`
	ReceivedAt int64          `json:"received_at"`
	Fields     map[string]any `json:"fields,omitempty"`
}

// Sink appends one JSON line per event and flushes once per batch. Flush
// hands data to the OS; it does not fsync.
type Sink struct {
	mu   sync.Mutex
	path string
	f    *os.File
	w    *bufio.Writer
}

var _ sink.Sink = (*Sink)(nil)

// New returns an uninitialised file sink.
func New() sink.Sink { return &Sink{} }

// Init opens (creating if needed) the file named by cfg["path"].
func (s *Sink) Init(_ context.Context, cfg map[string]any) error {
	p, _ := cfg["path"].(string)
	if p == "" {
		return errors.New("config.path is required")
	}
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		return fmt.Errorf("create dir for %s: %w", p, err)
	}
	f, err := os.OpenFile(p, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0o644)
	if err != nil {
		return fmt.Errorf("open %s: %w", p, err)
	}
	if err := terminateTornLine(f); err != nil {
		f.Close()
		return fmt.Errorf("%s: %w", p, err)
	}
	s.path, s.f, s.w = p, f, bufio.NewWriterSize(f, 256<<10)
	return nil
}

// terminateTornLine appends a newline if the file does not end with one.
// A process killed in the middle of a write leaves a partial last line;
// without this, the first record after a restart would be glued to that
// fragment and unreadable by line-oriented consumers.
func terminateTornLine(f *os.File) error {
	st, err := f.Stat()
	if err != nil {
		return err
	}
	if st.Size() == 0 {
		return nil
	}
	var last [1]byte
	if _, err := f.ReadAt(last[:], st.Size()-1); err != nil {
		return fmt.Errorf("read last byte: %w", err)
	}
	if last[0] == '\n' {
		return nil
	}
	_, err = f.Write([]byte{'\n'})
	return err
}

// Write appends the batch. A marshal failure affects only that envelope; a
// write/flush failure fails every envelope that was not yet confirmed.
func (s *Sink) Write(ctx context.Context, batch []*sink.Envelope) []error {
	errs := make([]error, len(batch))
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.f == nil {
		return sink.FillErrors(len(batch), errors.New("file sink: not open"))
	}
	if err := ctx.Err(); err != nil {
		return sink.FillErrors(len(batch), err)
	}
	var buf []byte
	for i, env := range batch {
		ev := env.Event
		b, err := json.Marshal(Line{Seq: ev.Seq, IdemKey: ev.IdemKey, SourceID: ev.SourceID, ReceivedAt: ev.ReceivedAt, Fields: ev.Fields})
		if err != nil {
			errs[i] = fmt.Errorf("file sink: marshal seq %d: %w", ev.Seq, err)
			continue
		}
		buf = append(append(buf[:0], b...), '\n')
		if _, err := s.w.Write(buf); err != nil {
			return s.failAll(errs, err)
		}
	}
	if err := s.w.Flush(); err != nil {
		return s.failAll(errs, err)
	}
	return errs
}

// failAll marks every not-yet-failed envelope with err and resets the
// buffered writer (bufio.Writer is sticky after an error). Lines that already
// reached the file will be duplicated on retry; this sink is not idempotent.
func (s *Sink) failAll(errs []error, cause error) []error {
	err := fmt.Errorf("file sink %s: %w", s.path, cause)
	for i := range errs {
		if errs[i] == nil {
			errs[i] = err
		}
	}
	s.w.Reset(s.f)
	return errs
}

// Health checks that the file is still accessible.
func (s *Sink) Health(context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.f == nil {
		return errors.New("file sink: not open")
	}
	if _, err := s.f.Stat(); err != nil {
		return fmt.Errorf("file sink %s: %w", s.path, err)
	}
	return nil
}

// Close flushes and closes the file.
func (s *Sink) Close(context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.f == nil {
		return nil
	}
	ferr := s.w.Flush()
	cerr := s.f.Close()
	s.f = nil
	if err := errors.Join(ferr, cerr); err != nil {
		return fmt.Errorf("file sink %s: close: %w", s.path, err)
	}
	return nil
}

// Idempotent is false: duplicates are written as-is.
func (*Sink) Idempotent() bool { return false }
