package worker

import (
	"github.com/lzwhehe/loggate/internal/delivery"
	"github.com/lzwhehe/loggate/internal/sink"
)

func (s *Slot) redeliverLoop() {
	defer close(s.redeliverDone)
	for {
		t := s.clk.NewTimer(s.cfg.RedeliverInterval)
		select {
		case <-t.C():
		case <-s.redeliverStop:
			t.Stop()
			return
		}
		s.RedeliverOnce()
	}
}

// redeliverRoom returns how many envelopes L2 can take from L1 right now.
func (s *Slot) redeliverRoom() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.state != Active {
		return 0
	}
	return s.cfg.MaxEvents - s.qLen()
}

// RedeliverOnce runs one redeliver round (DESIGN §4.4) and returns how many
// envelopes it enqueued: it is skipped unless the slot is Active and the
// breaker is not open; due entries of due/<d> whose record is still pending
// for d are claimed, read back from the WAL and enqueued as
// OriginRedeliver until L2 is full (up to its whole capacity, see Enqueue
// for the share policy). Stale index entries are cleaned up. Only Pending
// bits are redelivered, so destinations that already acked never see the
// event again.
func (s *Slot) RedeliverOnce() int {
	if s.br.State() == delivery.BreakerOpen {
		return 0
	}
	room := s.redeliverRoom()
	if room <= 0 {
		return 0
	}
	id := s.cfg.ID
	items, stale, err := s.d.L1.Due(id, s.clk.Now(), room, room+s.cfg.MaxEvents,
		func(seq uint64) bool { return s.d.Claims.TryClaim(seq, id) })
	if err != nil {
		s.log.Error("scan due index", "err", err)
	}
	n := 0
	for i, it := range items {
		env, err := s.d.Loader.Load(it.Ref)
		if err != nil || env.Seq != it.Seq {
			s.errs.Add(1)
			s.log.Error("read parked event from wal; postponing (the sweeper DLQs it at max_age)", "seq", it.Seq, "segment", it.Ref.SegID, "err", err)
			seq := it.Seq
			s.ops.Add(1)
			s.d.L1.Fail(seq, id, it.Attempts, 0, s.clk.Now().Add(s.d.Backoff.Max).UnixNano(), func(bool, error) {
				defer s.ops.Done()
				s.d.Claims.Release(seq, id)
			})
			continue
		}
		env.Dest, env.Origin, env.Attempt = id, sink.OriginRedeliver, it.Attempts
		if !s.enqueueRedeliver(env) {
			for _, rest := range items[i:] {
				s.d.Claims.Release(rest.Seq, id)
			}
			break
		}
		n++
	}
	if len(stale) > 0 {
		s.d.L1.DeleteDue(id, stale, nil)
	}
	return n
}
