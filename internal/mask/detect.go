package mask

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// Kind is a PII type. Lower values win overlap resolution
// (id_card > bank_card > phone > email).
type Kind uint8

// PII kinds.
const (
	IDCard Kind = iota
	BankCard
	Phone
	Email
	NumKinds
)

var kindNames = [NumKinds]string{"id_card", "bank_card", "phone", "email"}

func (k Kind) String() string {
	if k < NumKinds {
		return kindNames[k]
	}
	return "kind(" + strconv.Itoa(int(k)) + ")"
}

// Class returns the DataClass tag of k ("pii.id_card", ...).
func (k Kind) Class() string { return "pii." + k.String() }

// ParseKind parses a detector name.
func ParseKind(s string) (Kind, error) {
	for i, n := range kindNames {
		if n == s {
			return Kind(i), nil
		}
	}
	return 0, fmt.Errorf("unknown detector %q (want id_card|bank_card|phone|email)", s)
}

// Span is a detected PII occurrence: s[Start:End].
type Span struct {
	Start, End int
	Kind       Kind
}

// binRange matches a card whose first Width digits, read as an integer,
// lie in [Lo, Hi].
type binRange struct {
	lo, hi, width int
}

// DefaultBINs is the default issuer prefix allowlist: UnionPay 62, Visa 4,
// Mastercard 51-55 and 2221-2720, Amex 34/37, JCB 35.
var DefaultBINs = []string{"62", "4", "51-55", "2221-2720", "34", "37", "35"}

func parseBINs(specs []string) ([]binRange, error) {
	out := make([]binRange, 0, len(specs))
	for _, sp := range specs {
		lo, hi, ok := strings.Cut(sp, "-")
		if !ok {
			hi = lo
		}
		if lo == "" || len(lo) != len(hi) || len(lo) > 6 || !allDigits(lo) || !allDigits(hi) {
			return nil, fmt.Errorf("mask: invalid BIN prefix %q (want digits or lo-hi of equal width, <= 6 digits)", sp)
		}
		l, _ := strconv.Atoi(lo)
		h, _ := strconv.Atoi(hi)
		if l > h {
			return nil, fmt.Errorf("mask: invalid BIN range %q", sp)
		}
		out = append(out, binRange{lo: l, hi: h, width: len(lo)})
	}
	return out, nil
}

func allDigits(s string) bool {
	for i := 0; i < len(s); i++ {
		if !isDigit(s[i]) {
			return false
		}
	}
	return true
}

// provinceOK[p] reports whether p is a valid two-digit province-level code
// (GB/T 2260 first level, incl. 71 Taiwan, 81/82 HK/Macau, 91 overseas).
var provinceOK = func() (t [100]bool) {
	for _, p := range []int{11, 12, 13, 14, 15, 21, 22, 23, 31, 32, 33, 34, 35, 36, 37,
		41, 42, 43, 44, 45, 46, 50, 51, 52, 53, 54, 61, 62, 63, 64, 65, 71, 81, 82, 91} {
		t[p] = true
	}
	return
}()

// mobileOK[xy] reports whether 1xy is an allocated mainland mobile prefix
// (13x, 145-149, 15x except 154, 162/165-167, 170-173/175-178, 18x,
// 190-193/195-199).
var mobileOK = func() (t [100]bool) {
	for i := 30; i <= 39; i++ {
		t[i] = true
	}
	for _, i := range []int{45, 46, 47, 48, 49, 50, 51, 52, 53, 55, 56, 57, 58, 59,
		62, 65, 66, 67, 70, 71, 72, 73, 75, 76, 77, 78, 90, 91, 92, 93, 95, 96, 97, 98, 99} {
		t[i] = true
	}
	for i := 80; i <= 89; i++ {
		t[i] = true
	}
	return
}()

var idWeights = [17]int{7, 9, 10, 5, 8, 4, 2, 1, 6, 3, 7, 9, 10, 5, 8, 4, 2}

const idCheckChars = "10X98765432"

// IDChecksum returns the ISO 7064 MOD 11-2 check character for the first
// 17 digits of a resident ID number. d must hold at least 17 ASCII digits.
func IDChecksum(d string) byte {
	sum := 0
	for i, w := range idWeights {
		sum += int(d[i]-'0') * w
	}
	return idCheckChars[sum%11]
}

// Luhn reports whether the ASCII digit string d passes the Luhn check.
func Luhn(d []byte) bool {
	sum, dbl := 0, false
	for i := len(d) - 1; i >= 0; i-- {
		v := int(d[i] - '0')
		if dbl {
			v *= 2
			if v > 9 {
				v -= 9
			}
		}
		sum += v
		dbl = !dbl
	}
	return sum%10 == 0
}

// LuhnDigit returns the check digit to append to payload.
func LuhnDigit(payload []byte) byte {
	sum, dbl := 0, true
	for i := len(payload) - 1; i >= 0; i-- {
		v := int(payload[i] - '0')
		if dbl {
			v *= 2
			if v > 9 {
				v -= 9
			}
		}
		sum += v
		dbl = !dbl
	}
	return byte('0' + (10-sum%10)%10)
}

func daysIn(y, m int) int {
	switch m {
	case 2:
		if y%4 == 0 && (y%100 != 0 || y%400 == 0) {
			return 29
		}
		return 28
	case 4, 6, 9, 11:
		return 30
	}
	return 31
}

func atoiN(s string) int {
	n := 0
	for i := 0; i < len(s); i++ {
		n = n*10 + int(s[i]-'0')
	}
	return n
}

// Detector finds PII spans in strings. It is immutable and safe for
// concurrent use.
type Detector struct {
	enabled  [NumKinds]bool
	numeric  bool
	bins     []binRange
	maxBirth int // yyyymmdd, inclusive
	emailRE  *regexp.Regexp
}

// DetectorConfig configures a Detector.
type DetectorConfig struct {
	// Kinds lists enabled detectors; nil enables all.
	Kinds []Kind
	// BINs is the issuer prefix allowlist; nil means DefaultBINs. A card
	// must match a BIN prefix AND pass Luhn (REVIEW_01 item 11).
	BINs []string
	// Today bounds valid birth dates; zero means time.Now().
	Today time.Time
}

// emailPattern: local part, '@', dot-separated labels, TLD of >= 2 letters.
const emailPattern = `[A-Za-z0-9._%+\-]+@[A-Za-z0-9](?:[A-Za-z0-9\-]*[A-Za-z0-9])?(?:\.[A-Za-z0-9](?:[A-Za-z0-9\-]*[A-Za-z0-9])?)*\.[A-Za-z]{2,}`

// NewDetector builds a detector.
func NewDetector(c DetectorConfig) (*Detector, error) {
	d := &Detector{emailRE: regexp.MustCompile(emailPattern)}
	if c.Kinds == nil {
		c.Kinds = []Kind{IDCard, BankCard, Phone, Email}
	}
	for _, k := range c.Kinds {
		if k >= NumKinds {
			return nil, fmt.Errorf("mask: unknown detector %d", k)
		}
		d.enabled[k] = true
	}
	d.numeric = d.enabled[IDCard] || d.enabled[BankCard] || d.enabled[Phone]
	if c.BINs == nil {
		c.BINs = DefaultBINs
	}
	var err error
	if d.bins, err = parseBINs(c.BINs); err != nil {
		return nil, err
	}
	if len(d.bins) == 0 && d.enabled[BankCard] {
		return nil, fmt.Errorf("mask: bank_card detector needs at least one BIN prefix")
	}
	today := c.Today
	if today.IsZero() {
		today = time.Now()
	}
	d.maxBirth = today.Year()*10000 + int(today.Month())*100 + today.Day()
	return d, nil
}

func isDigit(c byte) bool { return c-'0' < 10 }

func isWord(c byte) bool { return c-'0' < 10 || (c|0x20)-'a' < 26 }

func isHex(c byte) bool { return c-'0' < 10 || (c|0x20)-'a' < 6 }

// leftOK reports whether a token may start at i: start of string, a
// non-alphanumeric byte before it, or the end of a %XX URL escape.
func leftOK(s string, i int) bool {
	if i == 0 || !isWord(s[i-1]) {
		return true
	}
	return i >= 3 && s[i-3] == '%' && isHex(s[i-2]) && isHex(s[i-1])
}

// rightOK reports whether a token may end at j (exclusive).
func rightOK(s string, j int) bool { return j >= len(s) || !isWord(s[j]) }

// ValidIDCard reports whether the 18-byte string v is a valid resident ID
// number: province code, birth date in [1900-01-01, today], MOD 11-2.
func (d *Detector) ValidIDCard(v string) bool {
	if len(v) != 18 || !provinceOK[atoiN(v[:2])] {
		return false
	}
	for i := 0; i < 17; i++ {
		if !isDigit(v[i]) {
			return false
		}
	}
	y, m, day := atoiN(v[6:10]), atoiN(v[10:12]), atoiN(v[12:14])
	if y < 1900 || m < 1 || m > 12 || day < 1 || day > daysIn(y, m) || y*10000+m*100+day > d.maxBirth {
		return false
	}
	c := v[17]
	if c == 'x' {
		c = 'X'
	}
	return c == IDChecksum(v)
}

func (d *Detector) binOK(digits []byte) bool {
	for _, b := range d.bins {
		if b.width > len(digits) {
			continue
		}
		p := 0
		for i := 0; i < b.width; i++ {
			p = p*10 + int(digits[i]-'0')
		}
		if p >= b.lo && p <= b.hi {
			return true
		}
	}
	return false
}

// ValidBankCard reports whether the ASCII digit string (16-19 digits, no
// separators) has an allowlisted BIN prefix and passes Luhn.
func (d *Detector) ValidBankCard(digits []byte) bool {
	return len(digits) >= 16 && len(digits) <= 19 && d.binOK(digits) && Luhn(digits)
}

// ValidPhone reports whether the 11-digit string is a mainland mobile
// number with an allocated prefix (no checksum exists).
func ValidPhone(v string) bool {
	return len(v) == 11 && v[0] == '1' && isDigit(v[1]) && isDigit(v[2]) && mobileOK[atoiN(v[1:3])]
}

// Detect appends the resolved PII spans of s to dst, sorted by Start.
// It does not allocate unless s contains '@' (the e-mail regexp runs only
// then) or dst has to grow.
func (d *Detector) Detect(s string, dst []Span) []Span {
	base := len(dst)
	if d.numeric && countDigits(s) >= 11 {
		dst = d.scanDigits(s, dst)
	}
	if d.enabled[Email] && strings.IndexByte(s, '@') >= 0 {
		for _, m := range d.emailRE.FindAllStringIndex(s, -1) {
			dst = append(dst, Span{Start: m[0], End: m[1], Kind: Email})
		}
	}
	if len(dst)-base > 1 {
		dst = append(dst[:base], resolve(dst[base:])...)
	}
	return dst
}

// countDigits is the prefilter: no numeric detector matches fewer than 11
// digits.
func countDigits(s string) int {
	n := 0
	for i := 0; i < len(s); i++ {
		if isDigit(s[i]) {
			n++
		}
	}
	return n
}

// scanDigits walks maximal digit runs ("groups"). A 4-digit group may start
// a separated card number (groups of 4 joined by one ' ' or '-', the same
// separator throughout, last group 1-4 digits, 16-19 digits in total);
// otherwise each group is checked on its own as a pure digit run.
func (d *Detector) scanDigits(s string, dst []Span) []Span {
	n := len(s)
	for i := 0; i < n; {
		if !isDigit(s[i]) {
			i++
			continue
		}
		a, b := i, i
		for b < n && isDigit(s[b]) {
			b++
		}
		// A run right after '%' starts with the two hex digits of a URL
		// escape (e.g. "%20"); the token proper starts after them.
		if a > 0 && s[a-1] == '%' && b-a >= 2 {
			a += 2
		}
		if d.enabled[BankCard] && b-a == 4 && leftOK(s, a) {
			if end, ok := d.groupedCard(s, a, b); ok {
				dst = append(dst, Span{Start: a, End: end, Kind: BankCard})
				i = end
				continue
			}
		}
		dst = d.pureRun(s, a, b, dst)
		i = b
	}
	return dst
}

// groupedCard tries the separated card formats starting at the 4-digit
// group s[a:b]; it returns the end of the longest valid match.
func (d *Detector) groupedCard(s string, a, b int) (int, bool) {
	var digits [20]byte
	copy(digits[:], s[a:b])
	nd := 4
	var ends [6]int // ends[k] = end of group k (k groups so far)
	var tot [6]int
	ends[1], tot[1] = b, 4
	k, pos := 1, b
	var sep byte
	for k < 5 {
		if pos+1 >= len(s) || (s[pos] != ' ' && s[pos] != '-') || !isDigit(s[pos+1]) {
			break
		}
		if sep == 0 {
			sep = s[pos]
		} else if s[pos] != sep {
			break
		}
		ga, gb := pos+1, pos+1
		for gb < len(s) && isDigit(s[gb]) {
			gb++
		}
		gl := gb - ga
		if gl > 4 {
			break
		}
		copy(digits[nd:], s[ga:gb])
		nd += gl
		k++
		ends[k], tot[k] = gb, nd
		if gl < 4 {
			break
		}
		pos = gb
	}
	for c := min(k, 5); c >= 4; c-- {
		t := tot[c]
		if t < 16 || t > 19 || !rightOK(s, ends[c]) {
			continue
		}
		if d.ValidBankCard(digits[:t]) {
			return ends[c], true
		}
	}
	return 0, false
}

// pureRun checks the digit run s[a:b] (plus an X/x check character after
// 17 digits) against the ID card, bank card and phone formats.
func (d *Detector) pureRun(s string, a, b int, dst []Span) []Span {
	if !leftOK(s, a) {
		return dst
	}
	l := b - a
	if l == 17 && b < len(s) && (s[b] == 'X' || s[b] == 'x') {
		if d.enabled[IDCard] && rightOK(s, b+1) && d.ValidIDCard(s[a:b+1]) {
			dst = append(dst, Span{Start: a, End: b + 1, Kind: IDCard})
		}
		return dst
	}
	if !rightOK(s, b) {
		return dst
	}
	switch {
	case l == 18 && d.enabled[IDCard] && d.ValidIDCard(s[a:b]):
		return append(dst, Span{Start: a, End: b, Kind: IDCard})
	case l >= 16 && l <= 19:
		if d.enabled[BankCard] {
			var digits [19]byte
			copy(digits[:], s[a:b])
			if d.ValidBankCard(digits[:l]) {
				return append(dst, Span{Start: a, End: b, Kind: BankCard})
			}
		}
	case l == 11:
		if d.enabled[Phone] && ValidPhone(s[a:b]) {
			return append(dst, Span{Start: a, End: b, Kind: Phone})
		}
	case l == 13 && s[a] == '8' && s[a+1] == '6':
		// 86 country code without '+'; the span is the national number.
		if d.enabled[Phone] && ValidPhone(s[a+2:b]) {
			return append(dst, Span{Start: a + 2, End: b, Kind: Phone})
		}
	}
	return dst
}

// resolve keeps a non-overlapping subset of sp: higher priority kind first,
// longer span first within a kind; the result is sorted by Start. It works
// in place (insertion sorts: n is tiny) and returns a prefix of sp.
func resolve(sp []Span) []Span {
	better := func(x, y Span) bool {
		if x.Kind != y.Kind {
			return x.Kind < y.Kind
		}
		if lx, ly := x.End-x.Start, y.End-y.Start; lx != ly {
			return lx > ly
		}
		return x.Start < y.Start
	}
	for i := 1; i < len(sp); i++ {
		for j := i; j > 0 && better(sp[j], sp[j-1]); j-- {
			sp[j], sp[j-1] = sp[j-1], sp[j]
		}
	}
	kept := 0
	for i := range sp {
		ok := true
		for j := 0; j < kept; j++ {
			if sp[i].Start < sp[j].End && sp[j].Start < sp[i].End {
				ok = false
				break
			}
		}
		if ok {
			sp[kept] = sp[i]
			kept++
		}
	}
	sp = sp[:kept]
	for i := 1; i < len(sp); i++ {
		for j := i; j > 0 && sp[j].Start < sp[j-1].Start; j-- {
			sp[j], sp[j-1] = sp[j-1], sp[j]
		}
	}
	return sp
}
