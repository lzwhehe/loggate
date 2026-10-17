// Package sink defines the output plugin contract (DESIGN §3) shared by all
// destinations and the delivery machinery.
package sink

import (
	"context"
	"fmt"
	"sort"

	"github.com/lzwhehe/loggate/internal/event"
)

// Origin tells a sink (and metrics) why an envelope is being delivered.
type Origin uint8

const (
	OriginLive      Origin = iota // first delivery after WAL commit
	OriginReplay                  // re-dispatched from the WAL during crash recovery (M2)
	OriginRedeliver               // re-delivered from L1 after a failure (M2)
)

func (o Origin) String() string {
	switch o {
	case OriginLive:
		return "live"
	case OriginReplay:
		return "replay"
	case OriginRedeliver:
		return "redeliver"
	default:
		return fmt.Sprintf("origin(%d)", uint8(o))
	}
}

// Envelope is one event bound for one destination.
type Envelope struct {
	Seq    uint64
	Dest   event.DestID
	Event  *event.LogEvent
	Origin Origin
	// WAL location, so L1 (M2) can point at the record instead of copying it.
	SegID  uint64
	Offset int64
	Length uint32
	// Attempt counts real send attempts for this destination (M2).
	Attempt uint16
}

// Sink is an output plugin. Write returns one error per envelope (nil on
// success); the slice must have len(batch) entries.
type Sink interface {
	Init(ctx context.Context, cfg map[string]any) error
	Write(ctx context.Context, batch []*Envelope) []error
	Health(ctx context.Context) error
	Close(ctx context.Context) error
	// Idempotent reports whether the sink deduplicates by IdemKey, which
	// makes redelivery effectively-once.
	Idempotent() bool
}

// Factory creates an uninitialised sink.
type Factory func() Sink

// Registry maps destination types to factories. Plugins are registered at
// compile time by the binary's wiring code, not via package-level state.
type Registry struct {
	factories map[string]Factory
}

// NewRegistry returns an empty registry.
func NewRegistry() *Registry { return &Registry{factories: map[string]Factory{}} }

// Register adds a factory. It returns an error on duplicates.
func (r *Registry) Register(typ string, f Factory) error {
	if _, dup := r.factories[typ]; dup {
		return fmt.Errorf("sink: type %q already registered", typ)
	}
	r.factories[typ] = f
	return nil
}

// New creates and initialises a sink of the given type.
func (r *Registry) New(ctx context.Context, typ string, cfg map[string]any) (Sink, error) {
	f, ok := r.factories[typ]
	if !ok {
		return nil, fmt.Errorf("sink: unknown type %q (known: %v)", typ, r.Types())
	}
	s := f()
	if err := s.Init(ctx, cfg); err != nil {
		return nil, fmt.Errorf("sink %s: init: %w", typ, err)
	}
	return s, nil
}

// Types lists registered types, sorted.
func (r *Registry) Types() []string {
	out := make([]string, 0, len(r.factories))
	for t := range r.factories {
		out = append(out, t)
	}
	sort.Strings(out)
	return out
}

// FillErrors returns a slice of n copies of err (helper for whole-batch
// failures).
func FillErrors(n int, err error) []error {
	out := make([]error, n)
	for i := range out {
		out[i] = err
	}
	return out
}
