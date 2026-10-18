package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lzwhehe/loggate/internal/clock"
	"github.com/lzwhehe/loggate/internal/delivery"
	"github.com/lzwhehe/loggate/internal/dlq"
	"github.com/lzwhehe/loggate/internal/event"
	"github.com/lzwhehe/loggate/internal/sink"
	"github.com/lzwhehe/loggate/internal/state"
)

// State is a slot's lifecycle state (DESIGN §5, without Replacing).
type State int32

const (
	Registered   State = iota // ID assigned, not delivering yet
	Active                    // accepting and delivering
	Draining                  // finishing the in-flight batch; new events park (NotActive)
	Unregistered              // stopped; parked events wait for re-registration or orphan TTL
)

func (s State) String() string {
	switch s {
	case Registered:
		return "registered"
	case Active:
		return "active"
	case Draining:
		return "draining"
	case Unregistered:
		return "unregistered"
	default:
		return fmt.Sprintf("state(%d)", int32(s))
	}
}

// EnqueueResult is the outcome of a non-blocking enqueue.
type EnqueueResult uint8

const (
	Enqueued EnqueueResult = iota
	QueueFull
	NotActive
	BreakerOpen
)

// Reason maps a failed enqueue to the L1 park reason.
func (r EnqueueResult) Reason() state.Reason {
	switch r {
	case QueueFull:
		return state.ReasonQueueFull
	case BreakerOpen:
		return state.ReasonBreakerOpen
	default:
		return state.ReasonNotActive
	}
}

// Config configures one slot.
type Config struct {
	ID   event.DestID
	Name string
	// L2 bounds.
	MaxEvents int
	MaxBytes  int64
	// Batching: up to MaxBatch envelopes, waiting at most BatchWait for the
	// batch to fill once the first envelope is available.
	MaxBatch  int
	BatchWait time.Duration
	// RedeliverShare is the fraction of L2 (events and bytes) reserved for
	// redelivery while this destination has an L1 backlog. See Enqueue.
	RedeliverShare    float64
	RedeliverInterval time.Duration
	BreakerFailures   int
	BreakerOpenFor    time.Duration
}

func (c *Config) defaults() {
	if c.MaxEvents <= 0 {
		c.MaxEvents = 10000
	}
	if c.MaxBytes <= 0 {
		c.MaxBytes = 64 << 20
	}
	if c.MaxBatch <= 0 {
		c.MaxBatch = 256
	}
	if c.RedeliverInterval <= 0 {
		c.RedeliverInterval = 200 * time.Millisecond
	}
	if c.RedeliverShare <= 0 {
		c.RedeliverShare = 0.5
	}
}

// Stats is a point-in-time view of a slot.
type Stats struct {
	ID        event.DestID `json:"id"`
	Name      string       `json:"name"`
	State     string       `json:"state"`
	Breaker   string       `json:"breaker"`
	L2Depth   int          `json:"l2_depth"`
	L2Bytes   int64        `json:"l2_bytes"`
	InFlight  int64        `json:"in_flight"`
	L1Pending int64        `json:"l1_pending"`
	Delivered uint64       `json:"delivered"`
	Retried   uint64       `json:"retried"`
	Errors    uint64       `json:"errors"`
	Dead      uint64       `json:"dead"`
	Parked    uint64       `json:"parked"`
	Redeliver uint64       `json:"redelivered"`
}

// Slot is the delivery slot of one destination. The L2 queue belongs to
// the slot, not to a sink instance (DESIGN §3).
type Slot struct {
	cfg  Config
	sink sink.Sink
	d    *Deps
	br   *delivery.Breaker
	clk  clock.Clock
	log  *slog.Logger

	mu        sync.Mutex
	state     State
	q         []*sink.Envelope
	qHead     int
	qBytes    int64
	nLive     int
	liveBytes int64
	nL1       int

	notify      chan struct{}
	ctx         context.Context
	cancel      context.CancelFunc
	drainReason state.Reason
	aborted     atomic.Bool
	ops         sync.WaitGroup // outstanding async L1 operations

	consumerDone  chan struct{}
	redeliverStop chan struct{}
	redeliverDone chan struct{}
	redeliverOn   atomic.Bool
	stopOnce      sync.Once

	inflight  atomic.Int64
	delivered atomic.Uint64
	retried   atomic.Uint64
	errs      atomic.Uint64
	dead      atomic.Uint64
	parked    atomic.Uint64
	redeliv   atomic.Uint64
}

// New creates a Registered slot.
func New(cfg Config, s sink.Sink, d *Deps) *Slot {
	cfg.defaults()
	ctx, cancel := context.WithCancel(context.Background())
	return &Slot{
		cfg: cfg, sink: s, d: d,
		br:  delivery.NewBreaker(cfg.BreakerFailures, cfg.BreakerOpenFor, d.Clock),
		clk: d.Clock, log: d.Logger.With("component", "worker", "dest", cfg.Name),
		notify: make(chan struct{}, 1), ctx: ctx, cancel: cancel,
		consumerDone: make(chan struct{}), redeliverStop: make(chan struct{}), redeliverDone: make(chan struct{}),
	}
}

// ID returns the destination ID.
func (s *Slot) ID() event.DestID { return s.cfg.ID }

// Name returns the destination name.
func (s *Slot) Name() string { return s.cfg.Name }

// Sink returns the slot's sink.
func (s *Slot) Sink() sink.Sink { return s.sink }

// State returns the lifecycle state.
func (s *Slot) State() State {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.state
}

// Breaker exposes the circuit breaker (stats, tests).
func (s *Slot) Breaker() *delivery.Breaker { return s.br }

// Start moves Registered → Active and starts the consumer. If redeliver
// is true the redeliver loop is started too (recovery starts it later via
// StartRedeliver, after replay).
func (s *Slot) Start(redeliver bool) error {
	s.mu.Lock()
	if s.state != Registered {
		st := s.state
		s.mu.Unlock()
		return fmt.Errorf("worker %s: start from state %s", s.cfg.Name, st)
	}
	s.state = Active
	s.mu.Unlock()
	go s.consume()
	if redeliver {
		s.StartRedeliver()
	}
	return nil
}

// StartRedeliver starts the redeliver loop (at most once).
func (s *Slot) StartRedeliver() {
	if s.redeliverOn.CompareAndSwap(false, true) {
		go s.redeliverLoop()
	}
}

func (s *Slot) signal() {
	select {
	case s.notify <- struct{}{}:
	default:
	}
}

func (s *Slot) qLen() int { return len(s.q) - s.qHead }

func (s *Slot) push(env *sink.Envelope) {
	s.q = append(s.q, env)
	s.qBytes += int64(env.Length)
	if env.Origin == sink.OriginRedeliver {
		s.nL1++
	} else {
		s.nLive++
		s.liveBytes += int64(env.Length)
	}
}

func (s *Slot) popN(n int) []*sink.Envelope {
	out := make([]*sink.Envelope, n)
	copy(out, s.q[s.qHead:s.qHead+n])
	for i := s.qHead; i < s.qHead+n; i++ {
		s.q[i] = nil
	}
	s.qHead += n
	for _, env := range out {
		s.qBytes -= int64(env.Length)
		if env.Origin == sink.OriginRedeliver {
			s.nL1--
		} else {
			s.nLive--
			s.liveBytes -= int64(env.Length)
		}
	}
	if s.qHead == len(s.q) {
		s.q, s.qHead = s.q[:0], 0
	} else if s.qHead > 1024 && s.qHead > len(s.q)/2 {
		n := copy(s.q, s.q[s.qHead:])
		clear(s.q[n:])
		s.q, s.qHead = s.q[:n], 0
	}
	return out
}

func (s *Slot) fits(size int64) bool {
	n := s.qLen()
	if n >= s.cfg.MaxEvents {
		return false
	}
	return n == 0 || s.qBytes+size <= s.cfg.MaxBytes
}

// Enqueue offers a live or replayed envelope without blocking.
//
// Redelivery share policy: while the destination has an L1 backlog (L1
// records pending for it, or redelivered envelopes queued), live/replay
// envelopes may occupy at most ⌊(1−share)·MaxEvents⌋ entries and
// (1−share)·MaxBytes bytes of L2; the rest is reserved for the redeliver
// loop, which may fill L2 up to its full capacity. Without a backlog live
// traffic may use the whole queue. This guarantees redelivery at least
// `share` of L2 (≥ 20% by config validation) without starving live traffic.
func (s *Slot) Enqueue(env *sink.Envelope) EnqueueResult {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.state != Active {
		return NotActive
	}
	if s.br.State() == delivery.BreakerOpen {
		return BreakerOpen
	}
	size := int64(env.Length)
	if !s.fits(size) {
		return QueueFull
	}
	if s.nL1 > 0 || s.d.L1.Pending(s.cfg.ID) > 0 {
		liveCap := int(float64(s.cfg.MaxEvents) * (1 - s.cfg.RedeliverShare))
		liveBytesCap := int64(float64(s.cfg.MaxBytes) * (1 - s.cfg.RedeliverShare))
		if s.nLive >= liveCap || (s.nLive > 0 && s.liveBytes+size > liveBytesCap) {
			return QueueFull
		}
	}
	s.push(env)
	s.signal()
	return Enqueued
}

// enqueueRedeliver offers an envelope read back from L1.
func (s *Slot) enqueueRedeliver(env *sink.Envelope) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.state != Active || !s.fits(int64(env.Length)) {
		return false
	}
	s.push(env)
	s.signal()
	return true
}

// Depth returns the L2 length.
func (s *Slot) Depth() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.qLen()
}

// Stats returns a snapshot.
func (s *Slot) Stats() Stats {
	s.mu.Lock()
	st, depth, bytes := s.state, s.qLen(), s.qBytes
	s.mu.Unlock()
	return Stats{
		ID: s.cfg.ID, Name: s.cfg.Name, State: st.String(), Breaker: s.br.State().String(),
		L2Depth: depth, L2Bytes: bytes, InFlight: s.inflight.Load(), L1Pending: s.d.L1.Pending(s.cfg.ID),
		Delivered: s.delivered.Load(), Retried: s.retried.Load(), Errors: s.errs.Load(),
		Dead: s.dead.Load(), Parked: s.parked.Load(), Redeliver: s.redeliv.Load(),
	}
}

// Delivered returns envelopes confirmed by the sink.
func (s *Slot) Delivered() uint64 { return s.delivered.Load() }

// take waits for the next batch. stop is true once the slot is no longer
// Active (nothing is popped in that case).
func (s *Slot) take() (batch []*sink.Envelope, stop bool) {
	var timer clock.Timer
	defer func() {
		if timer != nil {
			timer.Stop()
		}
	}()
	fired := false
	for {
		s.mu.Lock()
		if s.state != Active || s.aborted.Load() {
			s.mu.Unlock()
			return nil, true
		}
		n := s.qLen()
		if n >= s.cfg.MaxBatch || (n > 0 && (s.cfg.BatchWait <= 0 || fired)) {
			b := s.popN(min(n, s.cfg.MaxBatch))
			s.inflight.Store(int64(len(b)))
			s.mu.Unlock()
			return b, false
		}
		s.mu.Unlock()
		if n > 0 && timer == nil {
			timer = s.clk.NewTimer(s.cfg.BatchWait)
		}
		var tc <-chan time.Time
		if timer != nil {
			tc = timer.C()
		}
		select {
		case <-s.notify:
		case <-tc:
			fired = true
		case <-s.ctx.Done():
			// Only Drain (deadline) and Abort cancel ctx; both change state
			// first, so the next iteration returns.
		}
	}
}

func (s *Slot) consume() {
	defer close(s.consumerDone)
	for {
		batch, stop := s.take()
		if len(batch) > 0 {
			s.process(batch)
			s.inflight.Store(0)
		}
		if stop {
			if !s.aborted.Load() {
				s.parkRemaining()
			}
			return
		}
	}
}

// process sends one batch and handles every result (DESIGN §4.1).
func (s *Slot) process(batch []*sink.Envelope) {
	if !s.br.Allow() {
		until := s.br.OpenUntil()
		for _, env := range batch {
			s.postpone(env, state.ReasonBreakerOpen, until)
		}
		return
	}
	for _, env := range batch {
		env.Attempt++
	}
	errs := s.sink.Write(s.ctx, batch)
	if len(errs) != len(batch) {
		errs = sink.FillErrors(len(batch), fmt.Errorf("sink %s returned %d results for %d envelopes", s.cfg.Name, len(errs), len(batch)))
	}
	if s.aborted.Load() {
		return // simulated crash: results are lost, recovery handles them
	}

	outcome := delivery.OutcomeNeutral
	for _, err := range errs {
		if err == nil {
			outcome = delivery.OutcomeSuccess
			break
		}
		if k, _ := delivery.Classify(err); k == delivery.Retryable {
			outcome = delivery.OutcomeFailure
		}
	}
	shuttingDown := s.ctx.Err() != nil
	if !shuttingDown {
		s.br.Record(outcome)
	}

	now := s.clk.Now()
	var dead []*sink.Envelope
	var deadEntries []dlq.Entry
	for i, env := range batch {
		err := errs[i]
		if err == nil {
			if s.d.Observer != nil {
				s.d.Observer.Delivered(env, now)
			}
			s.succeeded(env)
			continue
		}
		s.errs.Add(1)
		if shuttingDown {
			// Interrupted by the drain deadline: not a real failure.
			env.Attempt--
			s.postpone(env, s.drainReason, now)
			continue
		}
		kind, retryAfter := delivery.Classify(err)
		if s.d.Observer != nil {
			s.d.Observer.DeliveryError(env.Dest, kind)
		}
		why := ""
		if kind == delivery.NonRetryable {
			why = "non_retryable"
		} else if ex, w := s.d.Budget.Exhausted(int(env.Attempt), env.Event.ReceivedAt, now); ex {
			why = w
		}
		if why != "" {
			dead = append(dead, env)
			deadEntries = append(deadEntries, s.d.Entry(env, why, err))
			continue
		}
		s.retry(env, kind, now.Add(s.d.Backoff.Delay(int(env.Attempt), kind, retryAfter)))
	}
	s.bury(dead, deadEntries, now)
}

func (s *Slot) succeeded(env *sink.Envelope) {
	s.delivered.Add(1)
	if env.Origin != sink.OriginRedeliver {
		s.d.Resolver.Resolve(env.Dest, env.Seq)
		return
	}
	s.redeliv.Add(1)
	s.ops.Add(1)
	s.d.L1.Ack(env.Seq, env.Dest, func(_ bool, err error) {
		defer s.ops.Done()
		s.d.Claims.Release(env.Seq, env.Dest)
		if err != nil {
			s.log.Error("l1 ack failed; event will be redelivered", "seq", env.Seq, "err", err)
		}
	})
}

// retry records a real failed attempt.
func (s *Slot) retry(env *sink.Envelope, kind delivery.Kind, nextAt time.Time) {
	s.retried.Add(1)
	if env.Origin != sink.OriginRedeliver {
		s.parked.Add(1)
		s.d.ParkLive(env, state.ReasonSendFailed, kind, nextAt, &s.ops)
		return
	}
	s.ops.Add(1)
	s.d.L1.Fail(env.Seq, env.Dest, env.Attempt, uint16(kind), nextAt.UnixNano(), s.releaseDone(env))
}

// postpone parks or reschedules without counting an attempt (breaker
// open, draining, shutdown).
func (s *Slot) postpone(env *sink.Envelope, reason state.Reason, nextAt time.Time) {
	if nextAt.IsZero() {
		nextAt = s.clk.Now()
	}
	if env.Origin != sink.OriginRedeliver {
		s.parked.Add(1)
		s.d.ParkLive(env, reason, 0, nextAt, &s.ops)
		return
	}
	s.ops.Add(1)
	s.d.L1.Fail(env.Seq, env.Dest, env.Attempt, 0, nextAt.UnixNano(), s.releaseDone(env))
}

func (s *Slot) releaseDone(env *sink.Envelope) state.Done {
	return func(_ bool, err error) {
		defer s.ops.Done()
		s.d.Claims.Release(env.Seq, env.Dest)
		if err != nil {
			s.log.Error("l1 update failed", "seq", env.Seq, "err", err)
		}
	}
}

// bury writes dead envelopes to the DLQ (one fsync) and only then marks
// them dead in L1 / resolves them. If the DLQ write fails they are kept
// for retry instead.
func (s *Slot) bury(envs []*sink.Envelope, entries []dlq.Entry, now time.Time) {
	if len(envs) == 0 {
		return
	}
	if err := s.d.DLQ.Append(entries); err != nil {
		s.log.Error("dlq append failed; keeping events for retry", "count", len(envs), "err", err)
		for _, env := range envs {
			s.retry(env, delivery.Retryable, now.Add(s.d.Backoff.Delay(int(env.Attempt), delivery.Retryable, 0)))
		}
		return
	}
	s.dead.Add(uint64(len(envs)))
	for _, env := range envs {
		if env.Origin != sink.OriginRedeliver {
			s.d.Resolver.Resolve(env.Dest, env.Seq)
			continue
		}
		s.ops.Add(1)
		s.d.L1.Dead(env.Seq, env.Dest, s.releaseDone(env))
	}
}

// parkRemaining parks everything left in L2 after the consumer stopped.
func (s *Slot) parkRemaining() {
	s.mu.Lock()
	rest := s.popN(s.qLen())
	reason := s.drainReason
	s.mu.Unlock()
	now := s.clk.Now()
	for _, env := range rest {
		s.postpone(env, reason, now)
	}
}

// ErrNotActive is returned by Drain on a slot that is not Active.
var ErrNotActive = errors.New("worker: not active")

// Drain stops the slot (Active → Draining → Unregistered): new envelopes
// are refused (the dispatcher parks them as NotActive), the redeliver loop
// stops, the in-flight batch completes through the normal result path, and
// everything still in L2 is parked with reason. If ctx expires first, the
// in-flight Sink.Write is cancelled; its envelopes are postponed without
// counting an attempt. Drain returns after every resulting L1 operation
// committed.
func (s *Slot) Drain(ctx context.Context, reason state.Reason) error {
	s.mu.Lock()
	switch s.state {
	case Registered:
		s.state = Unregistered
		s.mu.Unlock()
		return nil
	case Active:
	default:
		st := s.state
		s.mu.Unlock()
		return fmt.Errorf("%w: %s is %s", ErrNotActive, s.cfg.Name, st)
	}
	s.state = Draining
	s.drainReason = reason
	s.mu.Unlock()
	s.signal()
	s.stopRedeliver()

	select {
	case <-s.consumerDone:
	case <-ctx.Done():
		s.cancel()
		<-s.consumerDone
	}
	s.ops.Wait()
	s.cancel()
	s.mu.Lock()
	s.state = Unregistered
	s.mu.Unlock()
	return nil
}

// Abort stops the slot as if the process crashed: results of the
// in-flight batch are dropped and L2 is discarded (it is in the WAL).
func (s *Slot) Abort() {
	s.aborted.Store(true)
	s.mu.Lock()
	prev := s.state
	s.state = Unregistered
	s.mu.Unlock()
	s.cancel()
	s.signal()
	s.stopRedeliver()
	if prev == Active || prev == Draining {
		<-s.consumerDone
	}
}

// stopRedeliver stops the redeliver loop and waits for it to exit.
func (s *Slot) stopRedeliver() {
	s.stopOnce.Do(func() {
		close(s.redeliverStop)
		if !s.redeliverOn.CompareAndSwap(false, true) { // already started
			<-s.redeliverDone
		}
	})
}
