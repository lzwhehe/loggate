package mask

import (
	"regexp"
	"strings"
	"testing"
	"time"
)

// The oracle below re-implements the token rules independently of the
// scanner (regexp-based tokenisation, time.Date-based date check, separate
// Luhn/BIN code) so the fuzz property does not trust the code under test.

var (
	oracleRun   = regexp.MustCompile(`[0-9]+`)
	oracleBINs  = [][2]string{{"62", "62"}, {"4", "4"}, {"51", "55"}, {"2221", "2720"}, {"34", "34"}, {"37", "37"}, {"35", "35"}}
	oracleProvs = " 11 12 13 14 15 21 22 23 31 32 33 34 35 36 37 41 42 43 44 45 46 50 51 52 53 54 61 62 63 64 65 71 81 82 91 "
)

func oWord(c byte) bool {
	return ('0' <= c && c <= '9') || ('a' <= c && c <= 'z') || ('A' <= c && c <= 'Z')
}

func oHex(c byte) bool { return strings.IndexByte("0123456789abcdefABCDEF", c) >= 0 }

func oLeft(s string, i int) bool {
	return i == 0 || !oWord(s[i-1]) || (i >= 3 && s[i-3] == '%' && oHex(s[i-2]) && oHex(s[i-1]))
}

func oRight(s string, j int) bool { return j == len(s) || !oWord(s[j]) }

func oIDValid(v string, today time.Time) bool {
	if len(v) != 18 || !strings.Contains(oracleProvs, " "+v[:2]+" ") {
		return false
	}
	w := []int{7, 9, 10, 5, 8, 4, 2, 1, 6, 3, 7, 9, 10, 5, 8, 4, 2}
	sum := 0
	for i := 0; i < 17; i++ {
		sum += int(v[i]-'0') * w[i]
	}
	if strings.ToUpper(v[17:]) != string("10X98765432"[sum%11]) {
		return false
	}
	y, m, d := atoiN(v[6:10]), atoiN(v[10:12]), atoiN(v[12:14])
	t := time.Date(y, time.Month(m), d, 0, 0, 0, 0, time.UTC)
	return y >= 1900 && t.Year() == y && int(t.Month()) == m && t.Day() == d && !t.After(today)
}

func oCardValid(d string) bool {
	if len(d) < 16 || len(d) > 19 {
		return false
	}
	bin := false
	for _, r := range oracleBINs {
		p := d[:len(r[0])]
		if p >= r[0] && p <= r[1] {
			bin = true
		}
	}
	sum := 0
	for i := 0; i < len(d); i++ {
		v := int(d[len(d)-1-i] - '0')
		if i%2 == 1 {
			v = v * 2
			if v > 9 {
				v -= 9
			}
		}
		sum += v
	}
	return bin && sum%10 == 0
}

// oracleLeaks returns every boundary-delimited token of s that validates
// as an ID card or a bank card.
func oracleLeaks(s string, today time.Time) []string {
	var out []string
	runs := oracleRun.FindAllStringIndex(s, -1)
	for _, r := range runs {
		a, b := r[0], r[1]
		if a > 0 && s[a-1] == '%' && b-a >= 2 {
			a += 2
		}
		if !oLeft(s, a) {
			continue
		}
		if b-a == 17 && b < len(s) && (s[b] == 'X' || s[b] == 'x') && oRight(s, b+1) && oIDValid(s[a:b+1], today) {
			out = append(out, s[a:b+1])
		}
		if oRight(s, b) && (b-a == 18 && oIDValid(s[a:b], today) || oCardValid(s[a:b])) {
			out = append(out, s[a:b])
		}
	}
	// Separated cards: windows of 4 or 5 consecutive runs joined by the
	// same single separator, all of 4 digits except a last of 1-4.
	for i := range runs {
		a := runs[i][0]
		if a > 0 && s[a-1] == '%' && runs[i][1]-a >= 2 {
			a += 2
		}
		if runs[i][1]-a != 4 || !oLeft(s, a) {
			continue
		}
		digits := s[a:runs[i][1]]
		var sep byte
		for j := i + 1; j < len(runs) && j <= i+4; j++ {
			gap := s[runs[j-1][1]:runs[j][0]]
			if len(gap) != 1 || (gap[0] != ' ' && gap[0] != '-') || (sep != 0 && gap[0] != sep) {
				break
			}
			sep = gap[0]
			g := s[runs[j][0]:runs[j][1]]
			if len(g) > 4 {
				break
			}
			digits += g
			if j-i >= 3 && oRight(s, runs[j][1]) && oCardValid(digits) {
				out = append(out, s[a:runs[j][1]])
			}
			if len(g) < 4 {
				break
			}
		}
	}
	return out
}

func TestOracleAgreesOnPositives(t *testing.T) {
	for _, s := range []string{"x 11010519491231002X", "4111 1111 1111 1111", "c=4111111111111111;", "%3D11010519491231002x"} {
		if len(oracleLeaks(s, testToday)) == 0 {
			t.Fatalf("oracle misses %q", s)
		}
	}
}

func FuzzMaskString(f *testing.F) {
	for _, s := range []string{
		"", "plain text", "id 11010519491231002X", "4111 1111 1111 1111 4000",
		"tel +8613812345678 mail a.b@example.com", "%2011010519491231002X%26",
		"6222-0212-3456-7890-123", "1760688000000 1234567890123456789",
		"a@b.co 13812345678@example.com 4111111111111111",
	} {
		f.Add(s)
	}
	e, err := New(Config{Detector: DetectorConfig{Today: testToday}})
	if err != nil {
		f.Fatal(err)
	}
	f.Fuzz(func(t *testing.T, s string) {
		out, res := e.MaskString(s)
		if out == RedactedError && res.Errors > 0 {
			return // fail-closed is acceptable; nothing leaks
		}
		if leaks := oracleLeaks(out, testToday); len(leaks) > 0 {
			t.Fatalf("input %q\noutput %q\nstill contains %q", s, out, leaks)
		}
		m := map[string]any{"raw_data": s, "unmapped": map[string]any{"k": []any{s}}}
		e.MaskEvent(m)
		for _, v := range []string{m["raw_data"].(string), m["unmapped"].(map[string]any)["k"].([]any)[0].(string)} {
			if v != RedactedError && len(oracleLeaks(v, testToday)) > 0 {
				t.Fatalf("MaskEvent leaked in %q", v)
			}
		}
	})
}

// fuzzToken builds a valid ID card or bank card from n, formatted by sel.
func fuzzToken(n uint64, sel uint8) string {
	provs := strings.Fields(oracleProvs)
	if sel%2 == 0 {
		day := time.Date(1950, 1, 1, 0, 0, 0, 0, time.UTC).AddDate(0, 0, int(n%25000))
		p := provs[n%uint64(len(provs))] + "0105" + day.Format("20060102") + strings.Repeat("0", 3)
		seq := []byte(p)
		seq[14], seq[15], seq[16] = byte('0'+n/7%10), byte('0'+n/70%10), byte('0'+n/700%10)
		id := mkID(string(seq))
		if sel&4 != 0 {
			id = strings.ToLower(id)
		}
		return id
	}
	bins := []string{"622202", "4", "5100", "2221", "2720", "34", "37", "35"}
	length := 16 + int(n%4)
	payload := []byte(bins[n/4%uint64(len(bins))])
	for x := n; len(payload) < length-1; x = x*6364136223846793005 + 1442695040888963407 {
		payload = append(payload, byte('0'+x>>59%10))
	}
	card := mkCard(string(payload))
	switch sel % 6 {
	case 3:
		return group(card, ' ')
	case 5:
		return group(card, '-')
	}
	return card
}

func FuzzMaskEmbedded(f *testing.F) {
	f.Add("id ", " and ", ".", uint64(1), uint8(0), uint8(1))
	f.Add("%3D", "%26", "", uint64(77), uint8(3), uint8(5))
	f.Add("4111 ", " ", " 2024", uint64(9), uint8(5), uint8(3))
	f.Add("x", "", "X", uint64(5), uint8(4), uint8(0))
	e, err := New(Config{Detector: DetectorConfig{Today: testToday}})
	if err != nil {
		f.Fatal(err)
	}
	f.Fuzz(func(t *testing.T, pre, mid, post string, n uint64, sel1, sel2 uint8) {
		s := pre + fuzzToken(n, sel1) + mid + fuzzToken(n^0x9e3779b97f4a7c15, sel2) + post
		out, res := e.MaskString(s)
		if out == RedactedError && res.Errors > 0 {
			return
		}
		if leaks := oracleLeaks(out, testToday); len(leaks) > 0 {
			t.Fatalf("input %q\noutput %q\nstill contains %q", s, out, leaks)
		}
	})
}
