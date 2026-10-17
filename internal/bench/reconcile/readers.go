package reconcile

import (
	"bufio"
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	_ "modernc.org/sqlite" // read the siem sink database

	"github.com/lzwhehe/loggate/internal/bench/journal"
	"github.com/lzwhehe/loggate/internal/dlq"
)

// Receipt is one copy of an event seen at a destination.
type Receipt struct {
	TestSeq uint64
	IdemKey string
	RunID   string
	RecvNS  int64 // 0 = unknown
	Status  int   // HTTP status for mock sinks; 200 for stored rows
}

// Accepted reports whether the destination kept this copy.
func (r Receipt) Accepted() bool { return r.Status >= 200 && r.Status < 300 }

// eventFields is the part of a gateway LogEvent the oracle reads: the
// loadgen fields survive unchanged under fields.unmapped.
type eventFields struct {
	Seq     uint64 `json:"seq"`
	IdemKey string `json:"idem_key"`
	Fields  struct {
		Unmapped struct {
			RunID   string `json:"run_id"`
			TestSeq uint64 `json:"test_seq"`
		} `json:"unmapped"`
	} `json:"fields"`
}

// FileStats describes problems found while reading a file sink.
type FileStats struct {
	TornFragments int `json:"torn_fragments"` // partial lines left by a killed writer
}

// fileLinePrefix starts every file sink line (internal/sink/file.Line).
var fileLinePrefix = []byte(`{"seq":`)

// ReadFileSink reads a file sink's JSON-lines output. A writer killed
// mid-line leaves a fragment; if the restarted writer appended to the same
// line, the fragment is followed by a complete record that is recovered by
// re-parsing from the last record prefix. Fragments are counted, not
// fatal.
func ReadFileSink(r io.Reader) ([]Receipt, FileStats, error) {
	var out []Receipt
	var st FileStats
	br := bufio.NewReaderSize(r, 1<<20)
	for {
		line, rerr := br.ReadBytes('\n')
		if rerr != nil && rerr != io.EOF {
			return out, st, rerr
		}
		complete := bytes.HasSuffix(line, []byte("\n"))
		line = bytes.TrimRight(line, "\r\n")
		if len(line) > 0 {
			rec, ok := parseFileLine(line, complete)
			switch {
			case !ok:
				st.TornFragments++
			case bytes.LastIndex(line, fileLinePrefix) > 0:
				st.TornFragments++ // fragment glued to a recovered record
				out = append(out, rec)
			default:
				out = append(out, rec)
			}
		}
		if rerr == io.EOF {
			return out, st, nil
		}
	}
}

func parseFileLine(line []byte, complete bool) (Receipt, bool) {
	if !complete {
		return Receipt{}, false // unterminated: may be cut mid-value
	}
	var ev eventFields
	if err := json.Unmarshal(line, &ev); err != nil {
		i := bytes.LastIndex(line, fileLinePrefix)
		if i <= 0 || json.Unmarshal(line[i:], &ev) != nil {
			return Receipt{}, false
		}
	}
	if ev.Fields.Unmapped.TestSeq == 0 {
		return Receipt{}, false
	}
	return Receipt{TestSeq: ev.Fields.Unmapped.TestSeq, IdemKey: ev.IdemKey, RunID: ev.Fields.Unmapped.RunID, Status: 200}, true
}

// ReadFileSinkPath reads a file sink output file (missing = empty).
func ReadFileSinkPath(path string) ([]Receipt, FileStats, error) {
	f, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, FileStats{}, nil
	}
	if err != nil {
		return nil, FileStats{}, err
	}
	defer f.Close()
	return ReadFileSink(f)
}

// SQLiteRows returns every stored row of the sqlite sink at path (one
// Receipt per row; rows are unique by idem_key by construction).
func SQLiteRows(ctx context.Context, path string) ([]Receipt, error) {
	if _, err := os.Stat(path); errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	db, err := sql.Open("sqlite", "file:"+filepath.ToSlash(path)+"?_pragma=busy_timeout(5000)&_pragma=query_only(1)")
	if err != nil {
		return nil, err
	}
	defer db.Close()
	return sqliteRowsSince(ctx, db, 0, nil)
}

// sqliteRowsSince reads rows with rowid > after; lastRowID receives the
// highest rowid seen.
func sqliteRowsSince(ctx context.Context, db *sql.DB, after int64, lastRowID *int64) ([]Receipt, error) {
	rows, err := db.QueryContext(ctx, `SELECT rowid, body FROM events WHERE rowid > ? ORDER BY rowid`, after)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Receipt
	for rows.Next() {
		var id int64
		var body string
		if err := rows.Scan(&id, &body); err != nil {
			return nil, err
		}
		if lastRowID != nil {
			*lastRowID = max(*lastRowID, id)
		}
		var ev eventFields
		if err := json.Unmarshal([]byte(body), &ev); err != nil {
			return nil, fmt.Errorf("sqlite row %d: %w", id, err)
		}
		out = append(out, Receipt{TestSeq: ev.Fields.Unmapped.TestSeq, IdemKey: ev.IdemKey, RunID: ev.Fields.Unmapped.RunID, Status: 200})
	}
	return out, rows.Err()
}

// SQLiteTail incrementally reads new rows of a live sqlite sink (used by
// the faultbench observer to timestamp arrivals).
type SQLiteTail struct {
	db   *sql.DB
	last int64
}

// OpenSQLiteTail opens path for incremental reading.
func OpenSQLiteTail(path string) (*SQLiteTail, error) {
	db, err := sql.Open("sqlite", "file:"+filepath.ToSlash(path)+"?_pragma=busy_timeout(5000)&_pragma=query_only(1)")
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	return &SQLiteTail{db: db}, nil
}

// Next returns rows added since the previous call.
func (t *SQLiteTail) Next(ctx context.Context) ([]Receipt, error) {
	return sqliteRowsSince(ctx, t.db, t.last, &t.last)
}

// Close closes the database handle.
func (t *SQLiteTail) Close() error { return t.db.Close() }

// FileTail incrementally reads complete lines appended to a file sink.
type FileTail struct {
	path string
	off  int64
	rest []byte
}

// NewFileTail returns a tail reader for path.
func NewFileTail(path string) *FileTail { return &FileTail{path: path} }

// Next returns receipts for lines completed since the previous call.
func (t *FileTail) Next() ([]Receipt, error) {
	f, err := os.Open(t.path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	defer f.Close()
	if _, err := f.Seek(t.off, io.SeekStart); err != nil {
		return nil, err
	}
	b, err := io.ReadAll(f)
	if err != nil {
		return nil, err
	}
	t.off += int64(len(b))
	buf := append(t.rest, b...)
	end := bytes.LastIndexByte(buf, '\n')
	if end < 0 {
		t.rest = buf
		return nil, nil
	}
	recs, _, _ := ReadFileSink(bytes.NewReader(buf[:end+1]))
	t.rest = append([]byte(nil), buf[end+1:]...)
	return recs, nil
}

// ReadJournal reads a mocksink journal as receipts.
func ReadJournal(path string) ([]Receipt, int, error) {
	recs, torn, err := journal.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, 0, nil
	}
	if err != nil {
		return nil, torn, err
	}
	out := make([]Receipt, len(recs))
	for i, r := range recs {
		out[i] = Receipt{TestSeq: r.TestSeq, IdemKey: r.IdemKey, RunID: r.RunID, RecvNS: r.RecvNS, Status: r.Status}
	}
	return out, torn, nil
}

// DLQEntry is a dead letter mapped back to the load.
type DLQEntry struct {
	Dest    string
	TestSeq uint64 // 0 = body undecodable
	RunID   string
	Reason  string
}

// ReadDLQ reads the gateway's DLQ directory.
func ReadDLQ(dir string) ([]DLQEntry, error) {
	ents, err := dlq.ReadAll(dir)
	if err != nil {
		return nil, err
	}
	out := make([]DLQEntry, 0, len(ents))
	for _, e := range ents {
		d := DLQEntry{Dest: e.DestName, Reason: e.Reason}
		var ev eventFields
		if json.Unmarshal(e.Body, &ev) == nil {
			d.TestSeq, d.RunID = ev.Fields.Unmapped.TestSeq, ev.Fields.Unmapped.RunID
		}
		out = append(out, d)
	}
	return out, nil
}

// ObservedHeader is the header of the faultbench observer's first-seen file.
const ObservedHeader = "test_seq,seen_ns"

// ReadObserved reads an observer file: test_seq -> first time seen.
func ReadObserved(path string) (map[uint64]int64, error) {
	f, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	defer f.Close()
	out := map[uint64]int64{}
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		a, b, ok := strings.Cut(sc.Text(), ",")
		if !ok || a == "test_seq" {
			continue
		}
		s, err1 := strconv.ParseUint(a, 10, 64)
		ns, err2 := strconv.ParseInt(b, 10, 64)
		if err1 != nil || err2 != nil {
			continue // torn last line
		}
		if old, ok := out[s]; !ok || ns < old {
			out[s] = ns
		}
	}
	return out, sc.Err()
}
