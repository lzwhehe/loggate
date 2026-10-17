// Package event defines the in-memory log event model shared by every stage
// of the pipeline (ingest, WAL, dispatch, sinks).
package event

import (
	"encoding/hex"
	"fmt"
	"math/bits"
)

// Level is a data-classification level loosely modelled on GB/T 43697-2024.
// It is a configurable label, not a legal determination.
type Level uint8

const (
	LevelUnset     Level = iota // no classification applied
	LevelGeneral                // 一般
	LevelImportant              // 重要
	LevelCore                   // 核心
)

func (l Level) String() string {
	switch l {
	case LevelUnset:
		return "unset"
	case LevelGeneral:
		return "general"
	case LevelImportant:
		return "important"
	case LevelCore:
		return "core"
	default:
		return fmt.Sprintf("level(%d)", uint8(l))
	}
}

// MaxDests is the number of destinations a DestMask can address.
const MaxDests = 64

// DestID identifies a destination slot (0..63). IDs are assigned at
// registration and (from M2 on) persisted in the registry.
type DestID uint8

// Valid reports whether d fits in a DestMask.
func (d DestID) Valid() bool { return d < MaxDests }

// DestMask is a bitmap of destinations.
type DestMask uint64

// Set returns m with d added. d must be Valid.
func (m DestMask) Set(d DestID) DestMask { return m | 1<<(d&63) }

// Clear returns m with d removed. d must be Valid.
func (m DestMask) Clear(d DestID) DestMask { return m &^ (1 << (d & 63)) }

// Has reports whether d is in m.
func (m DestMask) Has(d DestID) bool { return d.Valid() && m&(1<<d) != 0 }

// Count returns the number of destinations in m.
func (m DestMask) Count() int { return bits.OnesCount64(uint64(m)) }

// Bits returns the destinations in m in ascending order.
func (m DestMask) Bits() []DestID {
	out := make([]DestID, 0, m.Count())
	for v := uint64(m); v != 0; v &= v - 1 {
		out = append(out, DestID(bits.TrailingZeros64(v)))
	}
	return out
}

// IdemKey is the 16-byte idempotency key carried end to end to the sinks.
// It is rendered as lowercase hex in JSON.
type IdemKey [16]byte

func (k IdemKey) String() string { return hex.EncodeToString(k[:]) }

// IsZero reports whether the key has not been assigned.
func (k IdemKey) IsZero() bool { return k == IdemKey{} }

// MarshalText implements encoding.TextMarshaler.
func (k IdemKey) MarshalText() ([]byte, error) {
	out := make([]byte, hex.EncodedLen(len(k)))
	hex.Encode(out, k[:])
	return out, nil
}

// UnmarshalText implements encoding.TextUnmarshaler.
func (k *IdemKey) UnmarshalText(b []byte) error {
	if hex.DecodedLen(len(b)) != len(k) {
		return fmt.Errorf("idem key: want %d hex chars, got %d", 2*len(k), len(b))
	}
	if _, err := hex.Decode(k[:], b); err != nil {
		return fmt.Errorf("idem key: %w", err)
	}
	return nil
}

// LogEvent is a single log record after ingest. Only fields needed by the
// current milestone are populated; OCSF and masking fields are filled in M4.
type LogEvent struct {
	Seq       uint64  `json:"seq"`
	IdemKey   IdemKey `json:"idem_key"`
	SourceID  string  `json:"source_id"`
	SourceSeq uint64  `json:"source_seq,omitempty"`

	ReceivedAt int64 `json:"received_at"` // unix nanoseconds
	Time       int64 `json:"time"`        // unix nanoseconds (event time)

	ClassUID   int32 `json:"class_uid,omitempty"`
	ActivityID int32 `json:"activity_id,omitempty"`
	StatusID   int32 `json:"status_id,omitempty"`
	SeverityID int32 `json:"severity_id,omitempty"`

	// Fields holds OCSF fields; anything not mapped lives under "unmapped".
	Fields map[string]any `json:"fields,omitempty"`
	// Raw is the (masked, from M4 on) original payload, optionally kept.
	Raw []byte `json:"raw,omitempty"`

	DataClass []string `json:"data_class,omitempty"`
	DataLevel Level    `json:"data_level,omitempty"`

	Targets DestMask `json:"targets"`
}
