package state

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"slices"
	"time"

	bolt "go.etcd.io/bbolt"

	"github.com/lzwhehe/loggate/internal/event"
)

// RegState is the persisted registry state of a destination name.
type RegState string

const (
	// RegConfigured: the destination has a sink configured.
	RegConfigured RegState = "configured"
	// RegAbsent: referenced by a route but not configured (DESIGN §5:
	// reserve the ID, park its events with reason NoDest).
	RegAbsent RegState = "absent"
	// RegTombstone: neither configured nor routed any more. The ID is not
	// reused until the tombstone expires and nothing in L1 is pending for it.
	RegTombstone RegState = "tombstone"
)

// RegEntry is one registry row (name -> DestID).
type RegEntry struct {
	Name      string       `json:"-"`
	ID        event.DestID `json:"id"`
	State     RegState     `json:"state"`
	RemovedAt int64        `json:"removed_at,omitempty"` // unix nanos, tombstones only
}

// Registry returns a copy of the registry.
func (s *Store) Registry() map[string]RegEntry {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[string]RegEntry, len(s.registry))
	for k, v := range s.registry {
		out[k] = v
	}
	return out
}

// ReconcileResult reports what Reconcile changed.
type ReconcileResult struct {
	Entries map[string]RegEntry
	New     []event.DestID // IDs allocated by this call
	Expired []string       // tombstones removed
}

// Reconcile makes the registry match the configuration: configured names
// become RegConfigured, route-only names RegAbsent, everything else a
// tombstone. New names get the lowest free ID; IDs stay stable across
// restarts and config reordering. For every newly allocated ID the
// checkpoint is initialised to lastSeq, so older WAL records (which can
// only carry that bit for a long-expired previous owner) are never replayed
// to it. Tombstones older than tombstoneTTL with nothing pending are
// removed, freeing their ID.
func (s *Store) Reconcile(configured, referenced []string, lastSeq uint64, tombstoneTTL time.Duration) (ReconcileResult, error) {
	var res ReconcileResult
	now := s.opts.Clock.Now().UnixNano()
	err := s.execSync(func(tx *bolt.Tx, d *delta) error {
		reg := tx.Bucket(bucketRegistry)
		ckpt := tx.Bucket(bucketCkpt)
		cur := s.Registry()
		want := map[string]RegState{}
		for _, n := range referenced {
			want[n] = RegAbsent
		}
		for _, n := range configured {
			want[n] = RegConfigured
		}
		used := map[event.DestID]bool{}
		for _, e := range cur {
			used[e.ID] = true
		}
		put := func(e RegEntry) error {
			b, err := json.Marshal(e)
			if err != nil {
				return err
			}
			if err := reg.Put([]byte(e.Name), b); err != nil {
				return err
			}
			cp := e
			d.registry[e.Name] = &cp
			return nil
		}
		// Existing entries.
		names := make([]string, 0, len(cur))
		for n := range cur {
			names = append(names, n)
		}
		slices.Sort(names)
		for _, n := range names {
			e := cur[n]
			if st, ok := want[n]; ok {
				if e.State != st {
					e.State, e.RemovedAt = st, 0
					if err := put(e); err != nil {
						return err
					}
				}
				continue
			}
			if e.State != RegTombstone {
				e.State, e.RemovedAt = RegTombstone, now
				if err := put(e); err != nil {
					return err
				}
				continue
			}
			if now-e.RemovedAt >= int64(tombstoneTTL) && s.Pending(e.ID) == 0 {
				if err := reg.Delete([]byte(n)); err != nil {
					return err
				}
				if err := ckpt.Delete([]byte{byte(e.ID)}); err != nil {
					return err
				}
				d.registry[n] = nil
				delete(used, e.ID)
				res.Expired = append(res.Expired, n)
			}
		}
		// New entries in configuration order (configured, then route-only).
		var fresh []string
		seen := map[string]bool{}
		for _, n := range append(append([]string(nil), configured...), referenced...) {
			if _, ok := cur[n]; !ok && !seen[n] {
				seen[n] = true
				fresh = append(fresh, n)
			}
		}
		for _, n := range fresh {
			id, ok := lowestFree(used)
			if !ok {
				return fmt.Errorf("no free destination id for %q (max %d)", n, event.MaxDests)
			}
			used[id] = true
			if err := put(RegEntry{Name: n, ID: id, State: want[n]}); err != nil {
				return err
			}
			if err := ckpt.Put([]byte{byte(id)}, binary.BigEndian.AppendUint64(nil, lastSeq)); err != nil {
				return err
			}
			d.ckpt[id] = lastSeq
			res.New = append(res.New, id)
		}
		return nil
	})
	if err != nil {
		return res, fmt.Errorf("state: reconcile registry: %w", err)
	}
	res.Entries = s.Registry()
	return res, nil
}

// SetState changes the persisted state of an existing registry entry
// (runtime register/unregister). Leaving the tombstone state clears
// RemovedAt; entering it sets it.
func (s *Store) SetState(name string, st RegState) error {
	now := s.opts.Clock.Now().UnixNano()
	return s.execSync(func(tx *bolt.Tx, d *delta) error {
		e, ok := s.Registry()[name]
		if !ok {
			return fmt.Errorf("state: unknown destination %q", name)
		}
		e.State, e.RemovedAt = st, 0
		if st == RegTombstone {
			e.RemovedAt = now
		}
		b, err := json.Marshal(e)
		if err != nil {
			return err
		}
		d.registry[name] = &e
		return tx.Bucket(bucketRegistry).Put([]byte(name), b)
	})
}

func lowestFree(used map[event.DestID]bool) (event.DestID, bool) {
	for i := range event.MaxDests {
		if !used[event.DestID(i)] {
			return event.DestID(i), true
		}
	}
	return 0, false
}
