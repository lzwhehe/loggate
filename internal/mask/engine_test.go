package mask

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/lzwhehe/loggate/internal/event"
)

var testKey = []byte("0123456789abcdef0123456789abcdef")

func newTestEngine(t testing.TB, c Config) *Engine {
	t.Helper()
	c.Detector.Today = testToday
	if c.HMACKey == nil {
		c.HMACKey = testKey
	}
	e, err := New(c)
	if err != nil {
		t.Fatal(err)
	}
	return e
}

func decode(t testing.TB, s string) map[string]any {
	t.Helper()
	dec := json.NewDecoder(strings.NewReader(s))
	dec.UseNumber()
	var m map[string]any
	if err := dec.Decode(&m); err != nil {
		t.Fatal(err)
	}
	return m
}

func encode(t testing.TB, m map[string]any) string {
	t.Helper()
	b, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func TestMaskFormats(t *testing.T) {
	e := newTestEngine(t, Config{})
	cases := []struct{ in, want string }{
		{"id 11010519491231002X ok", "id 110105********002X ok"},
		{"card 4111 1111 1111 1111", "card 4111 **** **** 1111"},
		{"card 4111111111111111", "card 4111********1111"},
		{"tel 13812345678", "tel 138****5678"},
		{"tel +8613812345678", "tel +86138****5678"},
		{"mail zhang.wei@example.com", "mail z***@example.com"},
		{"nothing here 1760688000000", "nothing here 1760688000000"},
	}
	for _, c := range cases {
		got, _ := e.MaskString(c.in)
		if got != c.want {
			t.Errorf("MaskString(%q) = %q, want %q", c.in, got, c.want)
		}
	}
}

func TestMaskRescanConverges(t *testing.T) {
	// Two adjacent card windows: after the first is masked its last group
	// is kept, which could leave a second valid window; the rescan loop
	// must mask that one too.
	e := newTestEngine(t, Config{})
	d := e.Detector()
	var card1 string
	for i := 100000; ; i++ {
		c := fmt.Sprintf("622202%06d4000", i)
		if Luhn([]byte(c)) {
			card1 = c
			break
		}
	}
	card2 := mkCard("400012345678901")
	in := "x " + group(card1, ' ') + " " + group(card2[4:], ' ')
	if sp := d.Detect(in, nil); len(sp) != 1 {
		t.Fatalf("setup: first pass should see one card, got %v", sp)
	}
	out, res := e.MaskString(in)
	if res.Errors != 0 {
		t.Fatalf("errors: %+v", res)
	}
	for _, sp := range d.Detect(out, nil) {
		if sp.Kind == BankCard || sp.Kind == IDCard {
			t.Fatalf("residual %s in %q", sp.Kind, out)
		}
	}
}

func TestFieldPolicies(t *testing.T) {
	e := newTestEngine(t, Config{Fields: map[string]Policy{
		"unmapped.test_seq":  PolicyKeep,
		"unmapped.secret":    PolicyDrop,
		"user.name":          PolicyHash,
		"unmapped.card_hint": PolicyMask,
		"unmapped.kept":      PolicyKeep,
		"unmapped.kept.deep": PolicyScan,
		"unmapped.*.token":   PolicyDrop,
	}})
	m := decode(t, `{
		"class_uid": 0, "metadata": {"version": "1.3.0"},
		"user": {"name": "13812345678"},
		"raw_data": "login from 13812345678",
		"unmapped": {
			"test_seq": 11010519491231002, "secret": "s3cr3t",
			"card_hint": "ab-12", "phone": 13812345678,
			"kept": {"x": "13812345678", "deep": "13812345678"},
			"a": {"token": "t", "other": "o"},
			"list": ["13812345678", {"m": "zhang.wei@example.com"}]
		}}`)
	res := e.MaskEvent(m)
	got := encode(t, m)
	h := e.hashValue("13812345678", &accum{}).(string)
	want := `{"class_uid":0,"metadata":{"version":"1.3.0"},"raw_data":"login from 138****5678",` +
		`"unmapped":{"a":{"other":"o"},"card_hint":"**-**","kept":{"deep":"138****5678","x":"13812345678"},` +
		`"list":["138****5678",{"m":"z***@example.com"}],"phone":"138****5678","test_seq":11010519491231002},` +
		`"user":{"name":"` + h + `"}}`
	if got != want {
		t.Fatalf("got\n%s\nwant\n%s", got, want)
	}
	if len(h) != 32 {
		t.Fatalf("hash length %d", len(h))
	}
	if !res.Has(Phone) || !res.Has(Email) || res.Has(IDCard) || res.Level != event.LevelGeneral {
		t.Fatalf("result %+v", res)
	}
	if !reflect.DeepEqual(res.Classes(), []string{"pii.phone", "pii.email"}) {
		t.Fatalf("classes %v", res.Classes())
	}
}

func TestDefaultDropIsFailClosedForUnknownFields(t *testing.T) {
	e := newTestEngine(t, Config{Default: PolicyDrop, Fields: map[string]Policy{"unmapped.ok": PolicyScan}})
	m := decode(t, `{"class_uid":0,"unmapped":{"ok":"tel 13812345678","other":"x"},"raw_data":"y"}`)
	e.MaskEvent(m)
	if got := encode(t, m); got != `{"class_uid":0,"unmapped":{"ok":"tel 138****5678"}}` {
		t.Fatalf("got %s", got)
	}
}

func TestFailClosedOnPanic(t *testing.T) {
	e := newTestEngine(t, Config{})
	e.testHook = func(s string) {
		if strings.Contains(s, "boom") {
			panic("injected")
		}
	}
	m := decode(t, `{"unmapped":{"a":"boom 13812345678","b":"tel 13812345678"}}`)
	res := e.MaskEvent(m)
	if got := encode(t, m); got != `{"unmapped":{"a":"[REDACTED:error]","b":"tel 138****5678"}}` {
		t.Fatalf("got %s", got)
	}
	if res.Errors != 1 || e.Stats().Errors != 1 {
		t.Fatalf("errors %d / %d", res.Errors, e.Stats().Errors)
	}
}

func TestLevelMapping(t *testing.T) {
	id, card := "11010519491231002X", "4111111111111111"
	e := newTestEngine(t, Config{})
	lvl := func(e *Engine, s string) event.Level {
		return e.MaskEvent(map[string]any{"raw_data": s}).Level
	}
	if l := lvl(e, "nothing"); l != event.LevelUnset {
		t.Fatalf("no pii: %v", l)
	}
	if l := lvl(e, "id "+id); l != event.LevelGeneral {
		t.Fatalf("id: %v", l)
	}
	if l := lvl(e, "card "+card); l != event.LevelGeneral {
		t.Fatalf("card: %v", l)
	}
	if l := lvl(e, "id "+id+" card "+card); l != event.LevelImportant {
		t.Fatalf("combo: %v", l)
	}
	// Combo across two fields of one event.
	r := e.MaskEvent(map[string]any{"a": "id " + id, "b": map[string]any{"c": card}})
	if r.Level != event.LevelImportant {
		t.Fatalf("cross-field combo: %v", r.Level)
	}
	core := newTestEngine(t, Config{Levels: map[string]event.Level{"pii.phone": event.LevelCore, "pii.email": event.LevelUnset}})
	if l := lvl(core, "13812345678"); l != event.LevelCore {
		t.Fatalf("explicit core: %v", l)
	}
	if l := lvl(core, "a@example.com"); l != event.LevelUnset {
		t.Fatalf("removed email level: %v", l)
	}
}

func TestConfigErrors(t *testing.T) {
	if _, err := New(Config{Fields: map[string]Policy{"a": PolicyHash}}); err == nil {
		t.Fatal("hash without key accepted")
	}
	if _, err := New(Config{Fields: map[string]Policy{"a..b": PolicyKeep}}); err == nil {
		t.Fatal("bad path accepted")
	}
	if _, err := New(Config{Levels: map[string]event.Level{"pii.name": event.LevelGeneral}}); err == nil {
		t.Fatal("bad level key accepted")
	}
	if _, err := New(Config{Detector: DetectorConfig{BINs: []string{"62-6"}}}); err == nil {
		t.Fatal("bad BIN accepted")
	}
}

func TestMaskEventNoAllocWithoutPII(t *testing.T) {
	e := newTestEngine(t, Config{})
	m := decode(t, `{"class_uid":0,"time":1,"unmapped":{"msg":"user login ok from host-7 ts 1760688000000","n":42,"tags":["a","b"]},"raw_data":"GET /x?trace=4bf92f3577b34da6"}`)
	if n := testing.AllocsPerRun(100, func() { e.MaskEvent(m) }); n != 0 {
		t.Fatalf("allocs = %v", n)
	}
}
