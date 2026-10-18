package demo

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lzwhehe/loggate/internal/piibench"
)

// Destination bits of the demo topology (the oracle's routing table).
const (
	bitArchive uint8 = 1 << iota
	bitSIEM
	bitSOAR
)

var destBits = []struct {
	name string
	bit  uint8
}{{"archive", bitArchive}, {"siem", bitSIEM}, {"soar", bitSOAR}}

// synthEvent is one generated request body with what the oracle expects.
type synthEvent struct {
	body    []byte
	text    bool  // text/plain (sshd line) vs JSON
	targets uint8 // destinations the demo routing table should pick
	kind    string
}

// generator builds SYNTHETIC events only: sshd auth lines and JSON app /
// auth events whose PII values come from piibench (algorithmic, seeded).
// The routing oracle is computed here from what was generated, independent
// of the gateway's router: auth events (sshd lines, JSON event.type=auth)
// go to archive+siem+soar, everything else to archive+siem; soar has
// max_level general, so an event carrying both an ID card and a bank card
// (level important) is kept away from it.
type generator struct {
	s *piibench.Synth
}

func (g *generator) next() synthEvent {
	s := g.s
	switch r := s.IntN(100); {
	case r < 40: // sshd text line -> OCSF 3002
		user := s.Name()
		var line string
		switch s.IntN(5) {
		case 0:
			line = fmt.Sprintf("Accepted password for %s from %s port %d ssh2", user, s.IP(), 1024+s.IntN(60000))
		case 1:
			line = fmt.Sprintf("Accepted publickey for deploy from %s port %d ssh2", s.IP(), 1024+s.IntN(60000))
		case 2: // credential stuffing with a phone number as user name (PII)
			line = fmt.Sprintf("Failed password for %s from %s port %d ssh2", s.Phone(), s.IP(), 1024+s.IntN(60000))
		default:
			line = fmt.Sprintf("Failed password for invalid user %s from %s port %d ssh2",
				[]string{"admin", "test", "oracle", "ubnt", "pi"}[s.IntN(5)], s.IP(), 1024+s.IntN(60000))
		}
		line = fmt.Sprintf("Oct 18 %02d:%02d:%02d bastion-%02d sshd[%d]: %s",
			s.IntN(24), s.IntN(60), s.IntN(60), 1+s.IntN(9), 1000+s.IntN(60000), line)
		return synthEvent{body: []byte(line + "\n"), text: true, targets: bitArchive | bitSIEM | bitSOAR, kind: "sshd"}
	case r < 65: // JSON auth event -> OCSF 3002
		ev := map[string]any{
			"event":  map[string]any{"type": "auth", "outcome": []string{"success", "failure", "failure"}[s.IntN(3)], "action": "login"},
			"user":   s.Name(),
			"src_ip": s.IP(),
			"app":    []string{"vpn", "oa", "crm"}[s.IntN(3)],
			"mfa":    map[string]any{"phone": s.Phone()},
		}
		t := bitArchive | bitSIEM | bitSOAR
		if s.IntN(5) == 0 { // real-name verification payload: ID card + bank card -> important
			ev["kyc"] = map[string]any{"id_card": s.IDCard(), "bank_card": s.BankCard()}
			t = bitArchive | bitSIEM
		}
		return synthEvent{body: mustJSON(ev), targets: t, kind: "auth_json"}
	default: // JSON application event -> Base Event 0
		var ev map[string]any
		switch s.IntN(3) {
		case 0:
			ev = map[string]any{"service": "payment", "msg": "refund issued to card " + s.BankCard(), "order_id": "ORD-" + s.Hex(10), "operator": s.Email()}
		case 1:
			ev = map[string]any{"service": "crm", "msg": "用户 " + s.Phone() + " 修改了收货地址", "ticket": "T-" + s.Hex(8)}
		default:
			ev = map[string]any{"service": "hr", "msg": "onboarding record updated", "employee": map[string]any{"id_card": s.IDCard(), "email": s.Email()}}
		}
		return synthEvent{body: mustJSON(ev), targets: bitArchive | bitSIEM, kind: "app_json"}
	}
}

func mustJSON(v any) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err) // maps of strings only
	}
	return b
}

// loadGen sends one synthetic event per request at a fixed rate and keeps
// the oracle: acked seq -> expected destinations. Only HTTP 200 responses
// (fsynced in the WAL) enter the committed set; refused or failed
// requests are counted and not retried.
type loadGen struct {
	url    string
	eps    float64
	gen    *generator
	client *http.Client

	mu       sync.Mutex
	expected map[uint64]uint8
	maxSeq   uint64
	lastErr  string

	sent, acked, refused, failed atomic.Uint64
	paused                       atomic.Bool
}

func newLoadGen(url string, eps float64, seed uint64) *loadGen {
	return &loadGen{
		url: url, eps: eps, gen: &generator{s: piibench.NewSynth(seed)},
		client:   &http.Client{Timeout: 3 * time.Second},
		expected: map[uint64]uint8{},
	}
}

func (l *loadGen) run(ctx context.Context) {
	period := time.Duration(float64(time.Second) / l.eps)
	t := time.NewTicker(period)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if !l.paused.Load() {
				l.sendOne(ctx)
			}
		}
	}
}

func (l *loadGen) sendOne(ctx context.Context) {
	ev := l.gen.next()
	l.sent.Add(1)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, l.url, bytes.NewReader(ev.body))
	if err != nil {
		l.fail(err.Error())
		return
	}
	if ev.text {
		req.Header.Set("Content-Type", "text/plain")
	} else {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := l.client.Do(req)
	if err != nil {
		if ctx.Err() == nil {
			l.fail(shortErr(err))
		}
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		l.refused.Add(1)
		l.setErr(fmt.Sprintf("HTTP %d", resp.StatusCode))
		return
	}
	var ar struct {
		Accepted int    `json:"accepted"`
		FirstSeq uint64 `json:"first_seq"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&ar); err != nil || ar.Accepted != 1 {
		l.fail("bad ack response")
		return
	}
	l.acked.Add(1)
	l.mu.Lock()
	l.expected[ar.FirstSeq] = ev.targets
	l.maxSeq = max(l.maxSeq, ar.FirstSeq)
	l.mu.Unlock()
}

func (l *loadGen) fail(msg string) {
	l.failed.Add(1)
	l.setErr(msg)
}

func (l *loadGen) setErr(msg string) {
	l.mu.Lock()
	l.lastErr = msg
	l.mu.Unlock()
}

// shortErr drops the URL from transport errors.
func shortErr(err error) string {
	s := err.Error()
	if i := strings.LastIndex(s, ": "); i >= 0 {
		return s[i+2:]
	}
	return s
}

// expectedCopy returns a copy of the oracle.
func (l *loadGen) expectedCopy() map[uint64]uint8 {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := make(map[uint64]uint8, len(l.expected))
	for k, v := range l.expected {
		out[k] = v
	}
	return out
}
