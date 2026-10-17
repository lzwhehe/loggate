// Package sqlite implements an idempotent SQLite sink (pure Go driver
// modernc.org/sqlite). Rows are keyed by idem_key and inserted with
// ON CONFLICT DO NOTHING, so redelivery and crash replay never create
// duplicate rows (DESIGN §4.5).
package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	_ "modernc.org/sqlite" // registers the "sqlite" driver

	"github.com/lzwhehe/loggate/internal/delivery"
	"github.com/lzwhehe/loggate/internal/sink"
)

// Type is the destination type name used in configuration.
const Type = "sqlite"

const schema = `CREATE TABLE IF NOT EXISTS events (
	idem_key    BLOB PRIMARY KEY,
	seq         INTEGER NOT NULL,
	source_id   TEXT NOT NULL,
	received_at INTEGER NOT NULL,
	body        TEXT NOT NULL
)`

// Sink writes events into an SQLite database.
type Sink struct {
	mu   sync.Mutex
	path string
	db   *sql.DB
}

var _ sink.Sink = (*Sink)(nil)

// New returns an uninitialised sink.
func New() sink.Sink { return &Sink{} }

// Init opens cfg["path"] (or cfg["dsn"]) and creates the table.
func (s *Sink) Init(ctx context.Context, cfg map[string]any) error {
	p, _ := cfg["path"].(string)
	if p == "" {
		p, _ = cfg["dsn"].(string)
	}
	if p == "" {
		return errors.New("config.path is required")
	}
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		return fmt.Errorf("create dir for %s: %w", p, err)
	}
	// WAL journal + synchronous=FULL: a committed Write survives a crash,
	// which is what makes "sink success" a durable resolution.
	dsn := "file:" + filepath.ToSlash(p) + "?_pragma=journal_mode(WAL)&_pragma=synchronous(FULL)&_pragma=busy_timeout(5000)"
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return fmt.Errorf("open %s: %w", p, err)
	}
	db.SetMaxOpenConns(1)
	if _, err := db.ExecContext(ctx, schema); err != nil {
		db.Close()
		return fmt.Errorf("create schema in %s: %w", p, err)
	}
	s.path, s.db = p, db
	return nil
}

// Write inserts the batch in one transaction. Rows whose idem_key already
// exists are skipped (that is a success). A marshal failure is
// NonRetryable for that envelope; a database failure fails the batch
// (Retryable).
func (s *Sink) Write(ctx context.Context, batch []*sink.Envelope) []error {
	errs := make([]error, len(batch))
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.db == nil {
		return sink.FillErrors(len(batch), errors.New("sqlite sink: not open"))
	}
	fail := func(err error) []error {
		err = fmt.Errorf("sqlite sink: %w", err)
		for i := range errs {
			if errs[i] == nil {
				errs[i] = err
			}
		}
		return errs
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fail(err)
	}
	stmt, err := tx.PrepareContext(ctx, `INSERT INTO events (idem_key, seq, source_id, received_at, body)
		VALUES (?, ?, ?, ?, ?) ON CONFLICT(idem_key) DO NOTHING`)
	if err != nil {
		_ = tx.Rollback()
		return fail(err)
	}
	defer stmt.Close()
	for i, env := range batch {
		ev := env.Event
		body, err := json.Marshal(ev)
		if err != nil {
			errs[i] = delivery.NewError(delivery.NonRetryable, fmt.Errorf("sqlite sink: marshal seq %d: %w", ev.Seq, err))
			continue
		}
		if _, err := stmt.ExecContext(ctx, ev.IdemKey[:], int64(ev.Seq), ev.SourceID, ev.ReceivedAt, string(body)); err != nil {
			_ = tx.Rollback()
			return fail(err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fail(err)
	}
	return errs
}

// Health pings the database.
func (s *Sink) Health(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.db == nil {
		return errors.New("sqlite sink: not open")
	}
	return s.db.PingContext(ctx)
}

// Close closes the database.
func (s *Sink) Close(context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.db == nil {
		return nil
	}
	err := s.db.Close()
	s.db = nil
	if err != nil {
		return fmt.Errorf("sqlite sink %s: close: %w", s.path, err)
	}
	return nil
}

// Idempotent is true: duplicates are absorbed by the primary key.
func (*Sink) Idempotent() bool { return true }

// Row is one stored event (for tests and reconciliation tools).
type Row struct {
	IdemKey []byte
	Seq     uint64
}

// ReadRows returns all rows of the database at path.
func ReadRows(ctx context.Context, path string) ([]Row, error) {
	db, err := sql.Open("sqlite", "file:"+filepath.ToSlash(path)+"?mode=ro")
	if err != nil {
		return nil, err
	}
	defer db.Close()
	rows, err := db.QueryContext(ctx, `SELECT idem_key, seq FROM events ORDER BY seq`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Row
	for rows.Next() {
		var r Row
		var seq int64
		if err := rows.Scan(&r.IdemKey, &seq); err != nil {
			return nil, err
		}
		r.Seq = uint64(seq)
		out = append(out, r)
	}
	return out, rows.Err()
}
