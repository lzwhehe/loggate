package event

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"hash"
	"sync"
)

// MinIdemKeyLen is the minimum HMAC key length accepted by NewIdemKeyer.
const MinIdemKeyLen = 16

// IdemKeyer derives gateway-assigned idempotency keys:
//
//	IdemKey = HMAC-SHA256(key, node_id | 0x00 | boot_epoch(be64) | seq(be64))[:16]
//
// boot_epoch distinguishes process lifetimes, so a sequence number that was
// assigned but never committed before a crash (and is therefore reused after
// torn-tail truncation) still gets a different key. The key is computed once
// at ingest, written into the WAL, and never recomputed on replay.
type IdemKeyer struct {
	prefix []byte // node_id | 0x00 | boot_epoch
	pool   sync.Pool
}

// NewIdemKeyer returns a keyer for one process lifetime.
func NewIdemKeyer(key []byte, nodeID string, bootEpoch uint64) (*IdemKeyer, error) {
	if len(key) < MinIdemKeyLen {
		return nil, fmt.Errorf("idem keyer: hmac key must be at least %d bytes, got %d", MinIdemKeyLen, len(key))
	}
	if nodeID == "" {
		return nil, errors.New("idem keyer: empty node id")
	}
	k := append([]byte(nil), key...)
	prefix := make([]byte, 0, len(nodeID)+9)
	prefix = append(prefix, nodeID...)
	prefix = append(prefix, 0)
	prefix = binary.BigEndian.AppendUint64(prefix, bootEpoch)
	ik := &IdemKeyer{prefix: prefix}
	ik.pool.New = func() any { return hmac.New(sha256.New, k) }
	return ik, nil
}

// Key returns the idempotency key for seq. Safe for concurrent use.
func (k *IdemKeyer) Key(seq uint64) IdemKey {
	h := k.pool.Get().(hash.Hash)
	defer k.pool.Put(h)
	h.Reset()
	h.Write(k.prefix)
	var s [8]byte
	binary.BigEndian.PutUint64(s[:], seq)
	h.Write(s[:])
	var sum [sha256.Size]byte
	var out IdemKey
	copy(out[:], h.Sum(sum[:0]))
	return out
}
