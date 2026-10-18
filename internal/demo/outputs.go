package demo

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"

	_ "modernc.org/sqlite" // read-only observer of the sqlite destination
)

// fileObserver incrementally reads the file destination's JSON lines and
// counts copies per gateway seq. A line torn by a crash followed by a
// complete record on the same line is recovered from the last record
// prefix (like internal/bench/reconcile).
type fileObserver struct {
	path   string
	off    int64
	rest   []byte
	copies map[uint64]int
	torn   int
}

var linePrefix = []byte(`{"seq":`)

func newFileObserver(path string) *fileObserver {
	return &fileObserver{path: path, copies: map[uint64]int{}}
}

func (o *fileObserver) poll() error {
	f, err := os.Open(o.path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	defer f.Close()
	if _, err := f.Seek(o.off, io.SeekStart); err != nil {
		return err
	}
	b, err := io.ReadAll(f)
	if err != nil {
		return err
	}
	o.off += int64(len(b))
	buf := append(o.rest, b...)
	end := bytes.LastIndexByte(buf, '\n')
	if end < 0 {
		o.rest = buf
		return nil
	}
	for line := range bytes.SplitSeq(buf[:end], []byte{'\n'}) {
		line = bytes.TrimRight(line, "\r")
		if len(line) == 0 {
			continue
		}
		var ev struct {
			Seq uint64 `json:"seq"`
		}
		if json.Unmarshal(line, &ev) != nil {
			i := bytes.LastIndex(line, linePrefix)
			if i <= 0 || json.Unmarshal(line[i:], &ev) != nil {
				o.torn++
				continue
			}
			o.torn++
		}
		if ev.Seq > 0 {
			o.copies[ev.Seq]++
		}
	}
	o.rest = append([]byte(nil), buf[end+1:]...)
	return nil
}

// sqliteObserver reads new rows of the sqlite destination (read-only).
type sqliteObserver struct {
	path   string
	db     *sql.DB
	last   int64
	copies map[uint64]int
}

func newSQLiteObserver(path string) *sqliteObserver {
	return &sqliteObserver{path: path, copies: map[uint64]int{}}
}

func (o *sqliteObserver) poll(ctx context.Context) error {
	if o.db == nil {
		if _, err := os.Stat(o.path); err != nil {
			return nil // not created yet
		}
		db, err := sql.Open("sqlite", "file:"+filepath.ToSlash(o.path)+"?_pragma=busy_timeout(2000)&_pragma=query_only(1)")
		if err != nil {
			return err
		}
		db.SetMaxOpenConns(1)
		o.db = db
	}
	rows, err := o.db.QueryContext(ctx, `SELECT rowid, seq FROM events WHERE rowid > ? ORDER BY rowid`, o.last)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var id int64
		var seq uint64
		if err := rows.Scan(&id, &seq); err != nil {
			return err
		}
		o.last = max(o.last, id)
		o.copies[seq]++
	}
	return rows.Err()
}

func (o *sqliteObserver) close() {
	if o.db != nil {
		_ = o.db.Close()
	}
}
