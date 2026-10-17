package wal

import (
	"fmt"
	"io"
	"os"
	"runtime"
)

// File is the subset of *os.File the WAL needs. Writes are positional so the
// writer controls offsets explicitly (required for truncate-and-continue).
type File interface {
	io.ReaderAt
	io.WriterAt
	Sync() error
	Truncate(size int64) error
	Size() (int64, error)
	Close() error
}

// FS abstracts the filesystem so tests can inject faults (see FaultFS).
type FS interface {
	OpenFile(name string, flag int, perm os.FileMode) (File, error)
	// ReadDir returns the names (not paths) of the entries in dir.
	ReadDir(dir string) ([]string, error)
	MkdirAll(dir string, perm os.FileMode) error
	Remove(name string) error
	// SyncDir makes directory entry changes (file creation) durable.
	SyncDir(dir string) error
}

// OSFS is the real filesystem.
type OSFS struct{}

var _ FS = OSFS{}

func (OSFS) OpenFile(name string, flag int, perm os.FileMode) (File, error) {
	f, err := os.OpenFile(name, flag, perm)
	if err != nil {
		return nil, err
	}
	return osFile{f}, nil
}

func (OSFS) ReadDir(dir string) ([]string, error) {
	ents, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	names := make([]string, 0, len(ents))
	for _, e := range ents {
		if !e.IsDir() {
			names = append(names, e.Name())
		}
	}
	return names, nil
}

func (OSFS) MkdirAll(dir string, perm os.FileMode) error { return os.MkdirAll(dir, perm) }
func (OSFS) Remove(name string) error                    { return os.Remove(name) }

// SyncDir fsyncs a directory. Windows does not support fsync on directory
// handles (NTFS journals metadata), so it is a no-op there.
func (OSFS) SyncDir(dir string) error {
	if runtime.GOOS == "windows" {
		return nil
	}
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	serr := d.Sync()
	cerr := d.Close()
	if serr != nil {
		return fmt.Errorf("sync dir %s: %w", dir, serr)
	}
	return cerr
}

type osFile struct{ *os.File }

func (f osFile) Size() (int64, error) {
	st, err := f.Stat()
	if err != nil {
		return 0, err
	}
	return st.Size(), nil
}
