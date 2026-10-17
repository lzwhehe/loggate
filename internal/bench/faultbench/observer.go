package faultbench

import (
	"bufio"
	"context"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/lzwhehe/loggate/internal/bench/reconcile"
)

// observer timestamps first arrival of each test_seq at the file and sqlite
// sinks, which have no receive clock of their own. It polls the sink
// outputs (resolution = poll interval) independently of the gateway and
// writes <dest>.observed.csv (test_seq,seen_ns).
type observer struct {
	interval time.Duration
	log      *slog.Logger
	targets  []*obsTarget
	stop     chan struct{}
	done     chan struct{}
}

type obsTarget struct {
	dest, typ, path string
	seen            map[uint64]bool
	out             *os.File
	w               *bufio.Writer
	file            *reconcile.FileTail
	sql             *reconcile.SQLiteTail
}

func newObserver(interval time.Duration, log *slog.Logger) *observer {
	return &observer{interval: interval, log: log, stop: make(chan struct{}), done: make(chan struct{})}
}

func (o *observer) add(dest, typ, sinkPath, outPath string) error {
	f, err := os.Create(outPath)
	if err != nil {
		return err
	}
	t := &obsTarget{dest: dest, typ: typ, path: sinkPath, seen: map[uint64]bool{}, out: f, w: bufio.NewWriter(f)}
	fmt.Fprintln(t.w, reconcile.ObservedHeader)
	if typ == "file" {
		t.file = reconcile.NewFileTail(sinkPath)
	}
	o.targets = append(o.targets, t)
	return nil
}

func (o *observer) start() {
	go func() {
		defer close(o.done)
		tk := time.NewTicker(o.interval)
		defer tk.Stop()
		for {
			select {
			case <-o.stop:
				o.poll() // final pass
				return
			case <-tk.C:
				o.poll()
			}
		}
	}()
}

func (o *observer) poll() {
	now := time.Now().UnixNano()
	for _, t := range o.targets {
		var recs []reconcile.Receipt
		var err error
		switch t.typ {
		case "file":
			recs, err = t.file.Next()
		case "sqlite":
			if t.sql == nil {
				if _, serr := os.Stat(t.path); serr != nil {
					continue
				}
				if t.sql, err = reconcile.OpenSQLiteTail(t.path); err != nil {
					break
				}
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			recs, err = t.sql.Next(ctx)
			cancel()
		}
		if err != nil {
			o.log.Debug("observer poll", "dest", t.dest, "err", err)
			continue
		}
		for _, r := range recs {
			if !t.seen[r.TestSeq] {
				t.seen[r.TestSeq] = true
				fmt.Fprintf(t.w, "%d,%d\n", r.TestSeq, now)
			}
		}
	}
}

// close stops polling after a final pass; call once.
func (o *observer) close() {
	close(o.stop)
	<-o.done
	for _, t := range o.targets {
		_ = t.w.Flush()
		_ = t.out.Close()
		if t.sql != nil {
			_ = t.sql.Close()
		}
	}
}
