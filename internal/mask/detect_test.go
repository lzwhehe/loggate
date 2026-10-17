package mask

import (
	"fmt"
	"testing"
	"time"
)

var testToday = time.Date(2026, 10, 17, 0, 0, 0, 0, time.UTC)

func newTestDetector(t testing.TB) *Detector {
	t.Helper()
	d, err := NewDetector(DetectorConfig{Today: testToday})
	if err != nil {
		t.Fatal(err)
	}
	return d
}

// mkID completes a 17-digit prefix with its MOD 11-2 check character.
func mkID(p17 string) string { return p17 + string(IDChecksum(p17)) }

// mkCard completes a payload with its Luhn digit.
func mkCard(payload string) string { return payload + string(LuhnDigit([]byte(payload))) }

func TestIDChecksumKnownVectors(t *testing.T) {
	// Examples from GB 11643-1999 and common references.
	for _, v := range []string{"11010519491231002X", "440524188001010014", "110105194912310021"} {
		want := v[17]
		got := IDChecksum(v)
		if v == "110105194912310021" {
			if got == want {
				t.Fatalf("%s: wrong check digit accepted", v)
			}
			continue
		}
		if got != want {
			t.Fatalf("%s: checksum %c, want %c", v, got, want)
		}
	}
	d := newTestDetector(t)
	cases := []struct {
		v    string
		want bool
	}{
		{"11010519491231002X", true},
		{"11010519491231002x", true}, // lowercase x accepted
		{"110105194912310021", false},
		{"440524188001010014", false}, // born 1880: before 1900
		{mkID("11010520000229123"), true},
		{mkID("11010519000229123"), false}, // 1900 is not a leap year
		{mkID("11010520040229123"), true},
		{mkID("11010520230229123"), false},
		{mkID("11010519900230123"), false}, // Feb 30
		{mkID("11010519901301123"), false}, // month 13
		{mkID("11010519900100123"), false}, // day 0
		{mkID("11010520261017123"), true},  // today
		{mkID("11010520261018123"), false}, // tomorrow
		{mkID("99010519900101123"), false}, // province 99
		{mkID("10010519900101123"), false}, // province 10
		{mkID("71010519900101123"), true},  // 71
		{mkID("91010519900101123"), true},  // 91
	}
	for _, c := range cases {
		if got := d.ValidIDCard(c.v); got != c.want {
			t.Errorf("ValidIDCard(%s) = %v, want %v", c.v, got, c.want)
		}
	}
}

func TestLuhnVectors(t *testing.T) {
	for _, c := range []struct {
		v    string
		want bool
	}{
		{"79927398713", true}, {"79927398710", false},
		{"4111111111111111", true}, {"4111111111111112", false},
		{"6011111111111117", true}, {"0", true},
	} {
		if Luhn([]byte(c.v)) != c.want {
			t.Errorf("Luhn(%s) != %v", c.v, c.want)
		}
	}
	if LuhnDigit([]byte("7992739871")) != '3' {
		t.Fatal("LuhnDigit")
	}
	d := newTestDetector(t)
	if !d.ValidBankCard([]byte("4111111111111111")) {
		t.Fatal("visa test card rejected")
	}
	if d.ValidBankCard([]byte("6011111111111117")) {
		t.Fatal("6011 is not in the default BIN allowlist")
	}
	if d.ValidBankCard([]byte(mkCard("123456789012345678"))) {
		t.Fatal("Luhn-passing 19-digit number without BIN accepted")
	}
	for _, p := range []string{"622202123456789", "222100123456789", "272099123456789", "510000123456789", "550000123456789", "340000123456789", "370000123456789", "350000123456789"} {
		if !d.ValidBankCard([]byte(mkCard(p))) {
			t.Errorf("BIN %s rejected", p[:4])
		}
	}
	for _, p := range []string{"222000123456789", "272100123456789", "560000123456789", "360000123456789"} {
		if d.ValidBankCard([]byte(mkCard(p))) {
			t.Errorf("BIN %s accepted", p[:4])
		}
	}
}

func TestPhone(t *testing.T) {
	for _, c := range []struct {
		v    string
		want bool
	}{
		{"13812345678", true}, {"19912345678", true}, {"16612345678", true},
		{"15412345678", false}, {"12012345678", false}, {"10012345678", false},
		{"17412345678", false}, {"1381234567", false},
	} {
		if ValidPhone(c.v) != c.want {
			t.Errorf("ValidPhone(%s) != %v", c.v, c.want)
		}
	}
}

func spansOf(d *Detector, s string) string {
	out := ""
	for _, sp := range d.Detect(s, nil) {
		out += fmt.Sprintf("%s:%s;", sp.Kind, s[sp.Start:sp.End])
	}
	return out
}

func TestDetectBoundariesAndFormats(t *testing.T) {
	d := newTestDetector(t)
	id := mkID("11010519900307123")
	card16 := mkCard("622202123456789")
	card19 := mkCard("622202123456789012")
	visa := "4111111111111111"
	cases := []struct{ in, want string }{
		{"id=" + id + ";", "id_card:" + id + ";"},
		{"身份证" + id + "。", "id_card:" + id + ";"},
		{"x" + id, ""}, // adjacent letter
		{id + "7", ""}, // 19 digits: no ID, no BIN 11
		{"1" + id, ""}, // adjacent digit
		{id + "a", ""}, // adjacent letter after
		{"card " + card16 + " ok", "bank_card:" + card16 + ";"},
		{"card " + card19, "bank_card:" + card19 + ";"},
		{"6222 0212 3456 7890", ""}, // Luhn fails (unless lucky)
		{"c=" + group(visa, ' '), "bank_card:" + group(visa, ' ') + ";"},
		{"c=" + group(visa, '-'), "bank_card:" + group(visa, '-') + ";"},
		{"c=" + group(card19, ' ') + " end", "bank_card:" + group(card19, ' ') + ";"},
		{"c=4111 1111-1111 1111", ""}, // mixed separators
		{"at 2026-10-17 " + group(visa, ' ') + " 2024", "bank_card:" + group(visa, ' ') + ";"},
		{"tel:13812345678", "phone:13812345678;"},
		{"tel:+8613812345678", "phone:13812345678;"},
		{"tel:+86 13812345678", "phone:13812345678;"},
		{"tel:138123456789", ""},                         // 12 digits
		{"tel:12012345678", ""},                          // 120 prefix
		{"ts=1760688000000", ""},                         // 13-digit timestamp
		{"a13812345678", ""},                             // adjacent letter
		{"q?id%3D" + id + "%26x", "id_card:" + id + ";"}, // URL-encoded '='
		{"q?id%20" + id + "%20x", "id_card:" + id + ";"}, // %20 before digits
		{"mail zhang.wei@example.com.", "email:zhang.wei@example.com;"},
		{"a%40example.com", ""}, // URL-encoded '@': known limitation
		{"１３８１２３４５６７８", ""},     // full-width digits: known limitation
		{"v1.13812345678.2", "phone:13812345678;"},
		{"ORD13812345678", ""},
	}
	for _, c := range cases {
		if got := spansOf(d, c.in); got != c.want {
			t.Errorf("Detect(%q) = %q, want %q", c.in, got, c.want)
		}
	}
}

func group(digits string, sep byte) string {
	out := ""
	for i := 0; i < len(digits); i += 4 {
		if i > 0 {
			out += string(sep)
		}
		out += digits[i:min(i+4, len(digits))]
	}
	return out
}

func TestOverlapResolution(t *testing.T) {
	sp := []Span{
		{Start: 0, End: 11, Kind: Phone},
		{Start: 0, End: 20, Kind: Email},
		{Start: 5, End: 23, Kind: IDCard},
		{Start: 30, End: 46, Kind: BankCard},
		{Start: 28, End: 47, Kind: BankCard}, // longer wins within kind
		{Start: 50, End: 61, Kind: Phone},
	}
	got := resolve(sp)
	want := []Span{{5, 23, IDCard}, {28, 47, BankCard}, {50, 61, Phone}}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("resolve = %v, want %v", got, want)
	}
	// Engine-level: a phone used as an e-mail local part is a phone.
	d := newTestDetector(t)
	if got := spansOf(d, "13812345678@example.com"); got != "phone:13812345678;" {
		t.Fatalf("phone vs email: %q", got)
	}
}

func TestDetectNoAllocWithoutPII(t *testing.T) {
	d := newTestDetector(t)
	s := "GET /api/v1/orders/20261017000123?ts=1760688000000&trace=4bf92f3577b34da6a3ce929d0e0e4736 HTTP/1.1 200 512"
	var buf [16]Span
	if n := testing.AllocsPerRun(100, func() { _ = d.Detect(s, buf[:0]) }); n != 0 {
		t.Fatalf("allocs = %v", n)
	}
}
