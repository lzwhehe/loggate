package demo

import (
	"context"
	"encoding/json"
	"sync"
	"time"

	"github.com/lzwhehe/loggate/internal/dlq"
	"github.com/lzwhehe/loggate/internal/event"
	"github.com/lzwhehe/loggate/internal/ui"
)

// gatewayView is what reconciliation needs from the gateway, read BEFORE
// the receivers are polled: per destination the resolved watermark and
// the L1 backlog.
type gatewayView struct {
	up        bool
	watermark map[string]uint64
	l1        map[string]int64
}

// reconciler compares the load generator's committed set with what the
// receivers (mock webhook, file contents, sqlite rows) and the DLQ hold.
//
//	expected  acked events the oracle routes to d
//	received  distinct seqs observed at d
//	dup       extra copies observed at d (raw)
//	dlq       expected events found in the DLQ for d
//	outstanding  expected − received − dlq (in flight, parked or lost)
//	lost      outstanding events at or below the gateway's watermark for d,
//	          judged only while d has no L1 backlog (otherwise "settling");
//	          the watermark is read before polling, so anything at or below
//	          it was confirmed by the sink before the poll started
//	misrouted received at d although the oracle does not route it there
type reconciler struct {
	file   *fileObserver
	sqlite *sqliteObserver
	soar   *Receiver
	dlqDir string

	mu   sync.Mutex
	rows []ui.ReconRow
	at   time.Time
}

func (r *reconciler) update(ctx context.Context, gv gatewayView, expected map[uint64]uint8) {
	_ = r.file.poll()
	_ = r.sqlite.poll(ctx)
	soarCopies, soarKeys := r.soar.Copies()
	dlqSet := map[string]map[uint64]bool{}
	if ents, err := dlq.ReadAll(r.dlqDir); err == nil {
		for _, e := range ents {
			if dlqSet[e.DestName] == nil {
				dlqSet[e.DestName] = map[uint64]bool{}
			}
			dlqSet[e.DestName][e.Seq] = true
		}
	}
	copies := map[string]map[uint64]int{"archive": r.file.copies, "siem": r.sqlite.copies, "soar": soarCopies}
	rows := make([]ui.ReconRow, 0, len(destBits))
	for _, d := range destBits {
		got := copies[d.name]
		row := ui.ReconRow{Dest: d.name}
		wm, haveWM := gv.watermark[d.name]
		settled := gv.up && haveWM && gv.l1[d.name] == 0
		var lost uint64
		for seq, t := range expected {
			if t&d.bit == 0 {
				continue
			}
			row.Expected++
			switch {
			case got[seq] > 0:
				row.Received++
			case dlqSet[d.name][seq]:
				row.DLQ++
			default:
				row.Outstanding++
				if settled && seq <= wm {
					lost++
				}
			}
		}
		for seq, n := range got {
			if n > 1 {
				row.Dup += uint64(n - 1)
			}
			if t, ok := expected[seq]; ok && t&d.bit == 0 {
				row.Misrouted++
			}
		}
		// Duplicates a deduplicating consumer would still store: the file
		// keeps every copy; sqlite rows are unique by idem_key, so a
		// second row for a seq would be a stored duplicate; for the
		// webhook, copies of a seq under more than one idem key.
		row.StoredDup = int64(row.Dup)
		if d.name == "soar" {
			row.StoredDup = int64(soarKeys) - int64(len(got))
		}
		if settled {
			l := lost
			row.Lost = &l
		}
		row.Settled = settled
		rows = append(rows, row)
	}
	r.mu.Lock()
	r.rows, r.at = rows, time.Now()
	r.mu.Unlock()
}

func (r *reconciler) snapshot() []ui.ReconRow {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]ui.ReconRow(nil), r.rows...)
}

// maskRing keeps the last N acknowledged events AFTER masking. The
// pre-mask input is never stored or shown (DESIGN §0.5 item 11).
type maskRing struct {
	mu   sync.Mutex
	buf  []ui.MaskedEvent
	next int
	full bool
}

const maskRingSize = 20

func (m *maskRing) add(evs []*event.LogEvent, hits []int) {
	now := time.Now().UnixMilli()
	out := make([]ui.MaskedEvent, 0, len(evs))
	for i, ev := range evs {
		b, err := json.Marshal(ev.Fields)
		if err != nil {
			continue
		}
		if len(b) > 2048 {
			b = append(b[:2048:2048], []byte("…")...)
		}
		me := ui.MaskedEvent{
			Seq: ev.Seq, At: now, ClassUID: ev.ClassUID, DataClass: ev.DataClass,
			Level: ev.DataLevel.String(), Masked: string(b),
		}
		if i < len(hits) {
			me.Hits = hits[i]
		}
		out = append(out, me)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.buf == nil {
		m.buf = make([]ui.MaskedEvent, maskRingSize)
	}
	for _, me := range out {
		m.buf[m.next] = me
		m.next = (m.next + 1) % maskRingSize
		if m.next == 0 {
			m.full = true
		}
	}
}

// latest returns newest first.
func (m *maskRing) latest() []ui.MaskedEvent {
	m.mu.Lock()
	defer m.mu.Unlock()
	n := m.next
	if m.full {
		n = maskRingSize
	}
	out := make([]ui.MaskedEvent, 0, n)
	for i := 1; i <= n; i++ {
		out = append(out, m.buf[(m.next-i+maskRingSize)%maskRingSize])
	}
	return out
}
