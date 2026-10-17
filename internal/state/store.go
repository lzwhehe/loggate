// Package state is LogGate's bbolt-backed delivery state: the L1 parking
// area (per-event destination bitmaps plus per-destination due indexes),
// per-destination checkpoints, and the destination registry.
//
// Buckets:
//
//	l1             be64(seq)            -> Record
//	due/<destID>   be64(nextAt)|be64(seq) -> {}   (redelivery index)
//	ckpt           destID               -> be64(watermark)
//	registry       name                 -> JSON RegEntry
//	meta           reserved
//
// All writes go through a single writer goroutine that groups queued
// operations into one db.Update transaction (not db.Batch: Batch may run a
// function twice, and appending a slot is not idempotent; DESIGN §0.5 item 8).
// Completion callbacks run on the writer goroutine strictly after the
// transaction committed. bbolt is copy-on-write: every commit rewrites the
// touched B+tree pages, so an "in-place" bitmap update still costs at least a
// page write plus fsync; grouping amortises that.
package state

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	bolt "go.etcd.io/bbolt"

	"github.com/lzwhehe/loggate/internal/clock"
	"github.com/lzwhehe/loggate/internal/event"
)

// ErrClosed is reported to callbacks of operations submitted after Close.
var ErrClosed = errors.New("state: closed")

var (
	bucketL1       = []byte("l1")
	bucketDue      = []byte("due")
	bucketCkpt     = []byte("ckpt")
	bucketRegistry = []byte("registry")
	bucketMeta     = []byte("meta")
)

// Options configures a Store.
type Options struct {
	Path string
	// MaxOpsPerTx bounds how many queued operations share one transaction.
	MaxOpsPerTx int // default 1000
	Clock       clock.Clock
	Logger      *slog.Logger
}

// Store is the bbolt state store. Create it with Open.
type Store struct {
	db   *bolt.DB
	opts Options
	log  *slog.Logger

	ops      chan *op
	closeMu  sync.RWMutex
	closed   bool
	quit     chan struct{}
	done     chan struct{}
	aborted  atomic.Bool
	closeErr error

	mu       sync.Mutex
	segRef   map[uint64]int // WAL segment -> live L1 records pointing into it
	ckpt     map[event.DestID]uint64
	registry map[string]RegEntry
	maxSeq   uint64 // highest seq ever present in l1 since open

	pending [event.MaxDests]atomic.Int64 // records whose Pending contains d
}

// Open opens (creating if needed) the database, rebuilds in-memory
// indexes from l1 (segRef, per-destination pending counts, max seq), loads
// ckpt and registry, and starts the writer.
func Open(opts Options) (*Store, error) {
	if opts.MaxOpsPerTx <= 0 {
		opts.MaxOpsPerTx = 1000
	}
	if opts.Clock == nil {
		opts.Clock = clock.Real()
	}
	if opts.Logger == nil {
		opts.Logger = slog.New(slog.DiscardHandler)
	}
	db, err := bolt.Open(opts.Path, 0o600, &bolt.Options{Timeout: 2 * time.Second})
	if err != nil {
		return nil, fmt.Errorf("state: open %s: %w", opts.Path, err)
	}
	s := &Store{
		db: db, opts: opts, log: opts.Logger.With("component", "state"),
		ops: make(chan *op, 4096), quit: make(chan struct{}), done: make(chan struct{}),
		segRef: map[uint64]int{}, ckpt: map[event.DestID]uint64{}, registry: map[string]RegEntry{},
	}
	if err := s.load(); err != nil {
		db.Close()
		return nil, err
	}
	go s.run()
	return s, nil
}

func (s *Store) load() error {
	err := s.db.Update(func(tx *bolt.Tx) error {
		for _, b := range [][]byte{bucketL1, bucketDue, bucketCkpt, bucketRegistry, bucketMeta} {
			if _, err := tx.CreateBucketIfNotExists(b); err != nil {
				return fmt.Errorf("create bucket %s: %w", b, err)
			}
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("state: init buckets: %w", err)
	}
	return s.db.View(func(tx *bolt.Tx) error {
		n := 0
		err := tx.Bucket(bucketL1).ForEach(func(k, v []byte) error {
			r, err := decodeRecord(v)
			if err != nil {
				return fmt.Errorf("state: l1 key %x: %w", k, err)
			}
			if r.Pending == 0 {
				return nil // cannot happen (deleted on empty), tolerated
			}
			s.segRef[r.Ref.SegID]++
			for _, d := range r.Pending.Bits() {
				s.pending[d].Add(1)
			}
			s.maxSeq = max(s.maxSeq, r.Seq)
			n++
			return nil
		})
		if err != nil {
			return err
		}
		err = tx.Bucket(bucketCkpt).ForEach(func(k, v []byte) error {
			if len(k) != 1 || len(v) != 8 {
				return fmt.Errorf("state: bad ckpt entry %x", k)
			}
			s.ckpt[event.DestID(k[0])] = binary.BigEndian.Uint64(v)
			return nil
		})
		if err != nil {
			return err
		}
		err = tx.Bucket(bucketRegistry).ForEach(func(k, v []byte) error {
			var e RegEntry
			if err := json.Unmarshal(v, &e); err != nil {
				return fmt.Errorf("state: registry %q: %w", k, err)
			}
			e.Name = string(k)
			s.registry[e.Name] = e
			return nil
		})
		if err != nil {
			return err
		}
		s.log.Info("state loaded", "l1_records", n, "segments_referenced", len(s.segRef), "destinations", len(s.registry))
		return nil
	})
}

// MaxL1Seq returns the highest seq seen in L1 at open (or parked since).
func (s *Store) MaxL1Seq() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.maxSeq
}

// Pending returns how many L1 records are pending for d.
func (s *Store) Pending(d event.DestID) int64 { return s.pending[d&63].Load() }

// SegRef returns how many L1 records point into WAL segment seg.
func (s *Store) SegRef(seg uint64) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.segRef[seg]
}

// SegRefs returns a copy of the segment reference counts.
func (s *Store) SegRefs() map[uint64]int {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[uint64]int, len(s.segRef))
	for k, v := range s.segRef {
		out[k] = v
	}
	return out
}

// Checkpoint returns d's persisted watermark (0 if none).
func (s *Store) Checkpoint(d event.DestID) uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.ckpt[d]
}

// Checkpoints returns a copy of all persisted watermarks.
func (s *Store) Checkpoints() map[event.DestID]uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[event.DestID]uint64, len(s.ckpt))
	for k, v := range s.ckpt {
		out[k] = v
	}
	return out
}

// Close processes every already-submitted operation, stops the writer and
// closes the database.
func (s *Store) Close() error { return s.shutdown(false) }

// Abort simulates a crash: queued operations are discarded without being
// committed and their callbacks never run. For tests.
func (s *Store) Abort() error { return s.shutdown(true) }

func (s *Store) shutdown(abort bool) error {
	if abort {
		s.aborted.Store(true)
	}
	s.closeMu.Lock()
	if s.closed {
		s.closeMu.Unlock()
		<-s.done
		return s.closeErr
	}
	s.closed = true
	s.closeMu.Unlock()
	close(s.quit)
	<-s.done
	if err := s.db.Close(); err != nil {
		s.closeErr = fmt.Errorf("state: close db: %w", err)
	}
	return s.closeErr
}

// View runs a read-only transaction (for scans and tests).
func (s *Store) View(fn func(tx *bolt.Tx) error) error { return s.db.View(fn) }

// Get returns the L1 record for seq, or nil.
func (s *Store) Get(seq uint64) (*Record, error) {
	var r *Record
	err := s.db.View(func(tx *bolt.Tx) error {
		v := tx.Bucket(bucketL1).Get(seqKey(seq))
		if v == nil {
			return nil
		}
		var err error
		r, err = decodeRecord(v)
		return err
	})
	return r, err
}

// Count returns the number of L1 records.
func (s *Store) Count() int {
	n := 0
	_ = s.db.View(func(tx *bolt.Tx) error {
		n = tx.Bucket(bucketL1).Stats().KeyN
		return nil
	})
	return n
}

// Flush waits until every operation submitted before the call has been
// committed and its callback has run.
func (s *Store) Flush(ctx context.Context) error {
	ch := make(chan error, 1)
	s.submit(&op{kind: opBarrier, done: func(_ bool, err error) { ch <- err }})
	select {
	case err := <-ch:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}
