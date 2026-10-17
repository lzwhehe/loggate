package wal

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"syscall"
)

// ErrCrashed is returned by every FaultFS operation after a simulated crash.
var ErrCrashed = errors.New("faultfs: simulated crash")

// ErrNoSpace is the error FaultFS users inject to simulate a full disk.
var ErrNoSpace error = syscall.ENOSPC

// OpKind names a filesystem operation recorded by FaultFS.
type OpKind string

const (
	OpOpen     OpKind = "open"
	OpWrite    OpKind = "write"
	OpSync     OpKind = "sync"
	OpTruncate OpKind = "truncate"
	OpSyncDir  OpKind = "syncdir"
)

// Op is one recorded filesystem operation.
type Op struct {
	Kind OpKind
	Name string // base name of the file
	Off  int64  // write offset or truncate size
	N    int    // bytes actually written
	Err  error
}

type writeFault struct {
	keep  int   // bytes to actually write before failing
	err   error // error to return
	crash bool  // enter crashed state afterwards
}

// FaultFS wraps an FS and injects deterministic faults. It is exported so
// that tests in other packages (ingest, gateway) can make the WAL fail.
// All methods are safe for concurrent use.
type FaultFS struct {
	inner FS

	mu         sync.Mutex
	writeFault []writeFault
	syncFault  []error
	openFault  []error
	crashed    bool
	ops        []Op
}

// NewFaultFS wraps inner (OSFS{} if nil).
func NewFaultFS(inner FS) *FaultFS {
	if inner == nil {
		inner = OSFS{}
	}
	return &FaultFS{inner: inner}
}

// FailNextWrite makes the next WriteAt write nothing and return err.
func (f *FaultFS) FailNextWrite(err error) { f.FailNextWritePartial(0, err) }

// FailNextWritePartial makes the next WriteAt persist only the first keep
// bytes and then return err (e.g. a short write followed by ENOSPC).
func (f *FaultFS) FailNextWritePartial(keep int, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.writeFault = append(f.writeFault, writeFault{keep: keep, err: err})
}

// TearNextWrite makes the next WriteAt persist only keep bytes and then
// "crash": every later operation on the FS fails with ErrCrashed. Combined
// with reopening the directory through a fresh FS this simulates a torn
// write at power loss / kill -9.
func (f *FaultFS) TearNextWrite(keep int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.writeFault = append(f.writeFault, writeFault{keep: keep, err: ErrCrashed, crash: true})
}

// FailNextSync makes the next Sync return err without syncing.
func (f *FaultFS) FailNextSync(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.syncFault = append(f.syncFault, err)
}

// FailNextOpen makes the next OpenFile return err.
func (f *FaultFS) FailNextOpen(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.openFault = append(f.openFault, err)
}

// Crash puts the FS in the crashed state.
func (f *FaultFS) Crash() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.crashed = true
}

// Ops returns a copy of the recorded operations.
func (f *FaultFS) Ops() []Op {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]Op(nil), f.ops...)
}

// SyncCount returns the number of successful Sync calls.
func (f *FaultFS) SyncCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := 0
	for _, op := range f.ops {
		if op.Kind == OpSync && op.Err == nil {
			n++
		}
	}
	return n
}

func (f *FaultFS) record(op Op) {
	f.ops = append(f.ops, op)
}

func (f *FaultFS) OpenFile(name string, flag int, perm os.FileMode) (File, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.crashed {
		return nil, ErrCrashed
	}
	if len(f.openFault) > 0 {
		err := f.openFault[0]
		f.openFault = f.openFault[1:]
		f.record(Op{Kind: OpOpen, Name: filepath.Base(name), Err: err})
		return nil, err
	}
	inner, err := f.inner.OpenFile(name, flag, perm)
	f.record(Op{Kind: OpOpen, Name: filepath.Base(name), Err: err})
	if err != nil {
		return nil, err
	}
	return &faultFile{fs: f, inner: inner, name: filepath.Base(name)}, nil
}

func (f *FaultFS) ReadDir(dir string) ([]string, error) {
	if f.isCrashed() {
		return nil, ErrCrashed
	}
	return f.inner.ReadDir(dir)
}

func (f *FaultFS) MkdirAll(dir string, perm os.FileMode) error {
	if f.isCrashed() {
		return ErrCrashed
	}
	return f.inner.MkdirAll(dir, perm)
}

func (f *FaultFS) Remove(name string) error {
	if f.isCrashed() {
		return ErrCrashed
	}
	return f.inner.Remove(name)
}

func (f *FaultFS) SyncDir(dir string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.crashed {
		return ErrCrashed
	}
	err := f.inner.SyncDir(dir)
	f.record(Op{Kind: OpSyncDir, Name: dir, Err: err})
	return err
}

func (f *FaultFS) isCrashed() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.crashed
}

type faultFile struct {
	fs    *FaultFS
	inner File
	name  string
}

func (ff *faultFile) ReadAt(p []byte, off int64) (int, error) {
	if ff.fs.isCrashed() {
		return 0, ErrCrashed
	}
	return ff.inner.ReadAt(p, off)
}

func (ff *faultFile) WriteAt(p []byte, off int64) (int, error) {
	f := ff.fs
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.crashed {
		return 0, ErrCrashed
	}
	if len(f.writeFault) > 0 {
		wf := f.writeFault[0]
		f.writeFault = f.writeFault[1:]
		keep := min(max(wf.keep, 0), len(p))
		n := 0
		if keep > 0 {
			var err error
			if n, err = ff.inner.WriteAt(p[:keep], off); err != nil {
				return n, fmt.Errorf("faultfs partial write: %w", err)
			}
		}
		f.record(Op{Kind: OpWrite, Name: ff.name, Off: off, N: n, Err: wf.err})
		if wf.crash {
			f.crashed = true
		}
		return n, wf.err
	}
	n, err := ff.inner.WriteAt(p, off)
	f.record(Op{Kind: OpWrite, Name: ff.name, Off: off, N: n, Err: err})
	return n, err
}

func (ff *faultFile) Sync() error {
	f := ff.fs
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.crashed {
		return ErrCrashed
	}
	var err error
	if len(f.syncFault) > 0 {
		err = f.syncFault[0]
		f.syncFault = f.syncFault[1:]
	} else {
		err = ff.inner.Sync()
	}
	f.record(Op{Kind: OpSync, Name: ff.name, Err: err})
	return err
}

func (ff *faultFile) Truncate(size int64) error {
	f := ff.fs
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.crashed {
		return ErrCrashed
	}
	err := ff.inner.Truncate(size)
	f.record(Op{Kind: OpTruncate, Name: ff.name, Off: size, Err: err})
	return err
}

func (ff *faultFile) Size() (int64, error) {
	if ff.fs.isCrashed() {
		return 0, ErrCrashed
	}
	return ff.inner.Size()
}

// Close always closes the underlying handle (even after a crash) so tests can
// reopen the directory and clean up temp dirs on Windows.
func (ff *faultFile) Close() error {
	err := ff.inner.Close()
	if ff.fs.isCrashed() {
		return ErrCrashed
	}
	return err
}
