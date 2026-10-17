// Package piibench generates a synthetic, span-labelled PII dataset and
// evaluates detectors on it (DESIGN §7.5). Every value is generated
// algorithmically from a seed; no real personal data is used. Synthetic
// distributions are not real-world distributions.
package piibench

import (
	"fmt"
	"math/rand/v2"
	"strconv"
	"strings"

	"github.com/lzwhehe/loggate/internal/mask"
)

// PII types (span labels).
const (
	TypeIDCard   = "id_card"
	TypeBankCard = "bank_card"
	TypePhone    = "phone"
	TypeEmail    = "email"
)

// Types lists the labels in report order.
var Types = []string{TypeIDCard, TypeBankCard, TypePhone, TypeEmail}

// Known-limitation tags: spans that are real PII but that the engine is
// not designed to find. They are reported separately and excluded from
// P/R/F1 (a prediction on them is neither TP nor FP).
const (
	LimFullWidth    = "fullwidth_digits"
	LimURLEncodedAt = "urlencoded_at"
	LimAdjacent     = "adjacent_alnum"
)

// Span is a labelled PII occurrence: Text[Start:End] (byte offsets).
type Span struct {
	Start      int    `json:"start"`
	End        int    `json:"end"`
	Type       string `json:"type"`
	Variant    string `json:"variant,omitempty"`
	Limitation string `json:"limitation,omitempty"`
}

// Neg is a recorded hard negative (a decoy that must not be reported).
type Neg struct {
	Start int    `json:"start"`
	End   int    `json:"end"`
	Kind  string `json:"kind"`
}

// Record is one dataset line.
type Record struct {
	ID        int    `json:"id"`
	Template  string `json:"template"`
	Text      string `json:"text"`
	Spans     []Span `json:"spans"`
	Negatives []Neg  `json:"negatives"`
}

type builder struct {
	b     strings.Builder
	spans []Span
	negs  []Neg
}

func (w *builder) lit(s string) { w.b.WriteString(s) }

func (w *builder) pii(typ, variant, lim, v string) {
	start := w.b.Len()
	w.b.WriteString(v)
	w.spans = append(w.spans, Span{Start: start, End: w.b.Len(), Type: typ, Variant: variant, Limitation: lim})
}

func (w *builder) neg(kind, v string) {
	start := w.b.Len()
	w.b.WriteString(v)
	w.negs = append(w.negs, Neg{Start: start, End: w.b.Len(), Kind: kind})
}

// gen holds the generator state.
type gen struct {
	r *rand.Rand
}

func (g *gen) digits(n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte('0' + g.r.IntN(10))
	}
	return string(b)
}

func (g *gen) pick(xs ...string) string { return xs[g.r.IntN(len(xs))] }

func (g *gen) chance(p float64) bool { return g.r.Float64() < p }

var provinces = []int{11, 12, 13, 14, 15, 21, 22, 23, 31, 32, 33, 34, 35, 36, 37,
	41, 42, 43, 44, 45, 46, 50, 51, 52, 53, 54, 61, 62, 63, 64, 65, 71, 81, 82, 91}

var badProvinces = []int{0, 10, 16, 19, 20, 24, 28, 30, 38, 40, 47, 49, 55, 60, 66, 70, 72, 80, 83, 90, 92, 99}

func (g *gen) birth() string {
	y := 1950 + g.r.IntN(59) // 1950..2008
	m := 1 + g.r.IntN(12)
	d := 1 + g.r.IntN(daysIn(y, m))
	return fmt.Sprintf("%04d%02d%02d", y, m, d)
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

func (g *gen) idPrefix(prov int, birth string) string {
	return fmt.Sprintf("%02d%02d%02d", prov, 1+g.r.IntN(20), 1+g.r.IntN(30)) + birth + g.digits(3)
}

// idCard returns a valid resident ID number.
func (g *gen) idCard() string {
	p := g.idPrefix(provinces[g.r.IntN(len(provinces))], g.birth())
	id := p + string(mask.IDChecksum(p))
	if id[17] == 'X' && g.chance(0.2) {
		id = id[:17] + "x"
	}
	return id
}

func (g *gen) idBadChecksum() string {
	p := g.idPrefix(provinces[g.r.IntN(len(provinces))], g.birth())
	good := mask.IDChecksum(p)
	for {
		c := "0123456789X"[g.r.IntN(11)]
		if c != good {
			return p + string(c)
		}
	}
}

func (g *gen) idBadDate() string {
	bad := g.pick("19900230", "19870431", "19000229", "20230229", "19951301", "19880000", "19921232", "18991231", "20300101", "19960631")
	p := g.idPrefix(provinces[g.r.IntN(len(provinces))], bad)
	return p + string(mask.IDChecksum(p))
}

func (g *gen) idBadProvince() string {
	p := g.idPrefix(badProvinces[g.r.IntN(len(badProvinces))], g.birth())
	return p + string(mask.IDChecksum(p))
}

func (g *gen) binPrefix() string {
	switch g.r.IntN(7) {
	case 0, 1, 2:
		return "62" + g.digits(4) // UnionPay
	case 3:
		return "4" + g.digits(5)
	case 4:
		return strconv.Itoa(51+g.r.IntN(5)) + g.digits(4)
	case 5:
		return strconv.Itoa(2221+g.r.IntN(500)) + g.digits(2)
	default:
		return strconv.Itoa(3528+g.r.IntN(62)) + g.digits(2) // JCB
	}
}

// bankCard returns a valid card number (allowlisted BIN + Luhn).
func (g *gen) bankCard() string {
	n := 16
	if g.chance(0.3) {
		n = 19
	}
	p := g.binPrefix()
	p += g.digits(n - 1 - len(p))
	return p + string(mask.LuhnDigit([]byte(p)))
}

func (g *gen) luhnFail() string {
	c := []byte(g.bankCard())
	last := c[len(c)-1]
	c[len(c)-1] = byte('0' + (int(last-'0')+1+g.r.IntN(9))%10)
	return string(c)
}

// snowflake returns a 19-digit Luhn-passing ID with a leading digit outside
// every default BIN (1, 7, 8, 9).
func (g *gen) snowflake() string {
	p := g.pick("1", "1", "1", "7", "8", "9") + g.digits(17)
	return p + string(mask.LuhnDigit([]byte(p)))
}

// randomNumericID is a uniformly random 16-19 digit ID (no leading zero):
// a realistic source of unavoidable false positives (some pass BIN+Luhn).
func (g *gen) randomNumericID() string {
	return string(byte('1'+g.r.IntN(9))) + g.digits(15+g.r.IntN(4))
}

var phonePrefixes = func() []string {
	var out []string
	for i := 0; i < 100; i++ {
		v := fmt.Sprintf("1%02d", i)
		if mask.ValidPhone(v + "00000000") {
			out = append(out, v)
		}
	}
	return out
}()

func (g *gen) phone() string { return phonePrefixes[g.r.IntN(len(phonePrefixes))] + g.digits(8) }

func (g *gen) phoneBadPrefix() string {
	return g.pick("100", "101", "110", "112", "120", "122", "154", "160", "163", "168", "174", "179", "194") + g.digits(8)
}

var (
	given   = []string{"wei", "fang", "na", "min", "jing", "li", "qiang", "lei", "jun", "yang", "yan", "jie", "tao", "ming", "chao", "xiu"}
	family  = []string{"zhang", "wang", "li", "zhao", "liu", "chen", "yang", "huang", "zhou", "wu", "xu", "sun", "ma", "zhu", "hu", "guo"}
	domains = []string{"example.com", "example.org", "example.net", "mail.example.com", "corp.example", "dev.corp.example"}
)

func (g *gen) email() string {
	local := g.pick(family...) + g.pick(".", "_", "") + g.pick(given...)
	if g.chance(0.4) {
		local += g.digits(1 + g.r.IntN(4))
	}
	return local + "@" + g.pick(domains...)
}

func (g *gen) msTimestamp() string { return "17" + g.digits(11) }

func (g *gen) hex(n int) string {
	const h = "0123456789abcdef"
	b := make([]byte, n)
	for i := range b {
		b[i] = h[g.r.IntN(16)]
	}
	return string(b)
}

func (g *gen) orderID() string {
	switch g.r.IntN(3) {
	case 0:
		return "2026" + fmt.Sprintf("%02d%02d", 1+g.r.IntN(12), 1+g.r.IntN(28)) + g.digits(10+g.r.IntN(4))
	case 1:
		return "ORD" + g.digits(16)
	default:
		return "20" + g.digits(18)
	}
}

func (g *gen) version() string {
	return fmt.Sprintf("%d.%d.%d", g.r.IntN(5), g.r.IntN(30), g.r.IntN(200))
}

func (g *gen) ip() string {
	return fmt.Sprintf("%d.%d.%d.%d", g.pick3(10, 172, 192, 198, 203), g.r.IntN(256), g.r.IntN(256), 1+g.r.IntN(254))
}

func (g *gen) pick3(xs ...int) int { return xs[g.r.IntN(len(xs))] }

// fullWidth converts ASCII digits to U+FF10..U+FF19.
func fullWidth(s string) string {
	var b strings.Builder
	for _, c := range s {
		if c >= '0' && c <= '9' {
			b.WriteRune('０' + (c - '0'))
		} else {
			b.WriteRune(c)
		}
	}
	return b.String()
}

func group4(d string, sep string) string {
	var parts []string
	for i := 0; i < len(d); i += 4 {
		parts = append(parts, d[i:min(i+4, len(d))])
	}
	return strings.Join(parts, sep)
}

// positive writes one PII value of typ with a random surface variant.
func (g *gen) positive(w *builder, typ string) {
	switch typ {
	case TypeIDCard:
		v := g.idCard()
		if g.chance(0.03) {
			w.pii(typ, "fullwidth", LimFullWidth, fullWidth(v))
			return
		}
		w.pii(typ, "plain", "", v)
	case TypeBankCard:
		v := g.bankCard()
		switch {
		case g.chance(0.25):
			w.pii(typ, "grouped_space", "", group4(v, " "))
		case g.chance(0.1):
			w.pii(typ, "grouped_hyphen", "", group4(v, "-"))
		default:
			w.pii(typ, "plain", "", v)
		}
	case TypePhone:
		v := g.phone()
		switch {
		case g.chance(0.03):
			w.pii(typ, "fullwidth", LimFullWidth, fullWidth(v))
		case g.chance(0.1):
			w.lit("+86")
			w.pii(typ, "plus86", "", v)
		case g.chance(0.05):
			w.lit("+86 ")
			w.pii(typ, "plus86_space", "", v)
		default:
			w.pii(typ, "plain", "", v)
		}
	case TypeEmail:
		w.pii(typ, "plain", "", g.email())
	}
}

// negative writes one hard negative chosen at random.
func (g *gen) negative(w *builder) {
	switch g.r.IntN(11) {
	case 0:
		w.neg("id_bad_checksum", g.idBadChecksum())
	case 1:
		w.neg("id_bad_date", g.idBadDate())
	case 2:
		w.neg("id_bad_province", g.idBadProvince())
	case 3:
		w.neg("card_luhn_fail", g.luhnFail())
	case 4:
		w.neg("snowflake_luhn_pass_no_bin", g.snowflake())
	case 5:
		w.neg("ms_timestamp", g.msTimestamp())
	case 6:
		w.neg("order_id", g.orderID())
	case 7:
		w.neg("trace_id", g.hex(32))
	case 8:
		w.neg("phone_bad_prefix", g.phoneBadPrefix())
	case 9:
		w.neg("version", "v"+g.version())
	default:
		w.neg("random_numeric_id", g.randomNumericID())
	}
}

// Generate returns n records from seed.
func Generate(seed uint64, n int) []Record {
	g := &gen{r: rand.New(rand.NewPCG(seed, seed^0x9e3779b97f4a7c15))}
	out := make([]Record, n)
	for i := range out {
		w := &builder{}
		tmpl := []string{"sshd", "nginx", "app_json", "ticket"}[g.r.IntN(4)]
		switch tmpl {
		case "sshd":
			g.sshd(w)
		case "nginx":
			g.nginx(w)
		case "app_json":
			g.appJSON(w)
		default:
			g.ticket(w)
		}
		out[i] = Record{ID: i + 1, Template: tmpl, Text: w.b.String(), Spans: w.spans, Negatives: w.negs}
		if out[i].Spans == nil {
			out[i].Spans = []Span{}
		}
		if out[i].Negatives == nil {
			out[i].Negatives = []Neg{}
		}
	}
	return out
}

func (g *gen) sshd(w *builder) {
	w.lit(fmt.Sprintf("Oct 17 %02d:%02d:%02d bastion-%02d sshd[%d]: ", g.r.IntN(24), g.r.IntN(60), g.r.IntN(60), g.r.IntN(20), 1000+g.r.IntN(60000)))
	switch g.r.IntN(4) {
	case 0:
		w.lit(g.pick("Failed", "Accepted") + " password for ")
		switch g.r.IntN(3) {
		case 0:
			g.positive(w, TypePhone)
		case 1:
			g.positive(w, TypeEmail)
		default:
			w.lit(g.pick("root", "admin", "deploy", "oracle", "git"))
		}
		w.lit(" from " + g.ip() + fmt.Sprintf(" port %d ssh2", 1024+g.r.IntN(60000)))
	case 1:
		w.lit("Failed password for invalid user ")
		if g.chance(0.5) {
			g.negative(w)
		} else {
			w.lit(g.pick("test", "user1", "ubnt", "pi"))
		}
		w.lit(" from " + g.ip() + fmt.Sprintf(" port %d ssh2", 1024+g.r.IntN(60000)))
	case 2:
		w.lit("Accepted publickey for deploy from " + g.ip() + fmt.Sprintf(" port %d ssh2: RSA SHA256:", 1024+g.r.IntN(60000)))
		w.neg("key_fingerprint", g.hex(43))
	default:
		w.lit("Connection closed by authenticating user ")
		g.positive(w, TypePhone)
		w.lit(" " + g.ip() + fmt.Sprintf(" port %d [preauth]", 1024+g.r.IntN(60000)))
	}
}

func (g *gen) nginx(w *builder) {
	w.lit(g.ip() + fmt.Sprintf(` - - [17/Oct/2026:%02d:%02d:%02d +0800] "GET `, g.r.IntN(24), g.r.IntN(60), g.r.IntN(60)))
	switch g.r.IntN(3) {
	case 0:
		w.lit("/api/v1/users?mobile=")
		g.positive(w, TypePhone)
		w.lit("&ts=")
		w.neg("ms_timestamp", g.msTimestamp())
	case 1:
		// URL-encoded query: "%3A" / "%20" right before the value.
		w.lit("/search?q=" + g.pick("%E8%BA%AB%E4%BB%BD%E8%AF%81%3A", "id%20", "id%3D"))
		if g.chance(0.7) {
			v := g.idCard()
			w.pii(TypeIDCard, "url_escaped", "", v)
		} else {
			g.negative(w)
		}
		w.lit("&card=")
		g.positive(w, TypeBankCard)
		w.lit("&email=")
		if g.chance(0.5) {
			e := g.email()
			w.pii(TypeEmail, "urlencoded_at", LimURLEncodedAt, strings.Replace(e, "@", "%40", 1))
		} else {
			g.positive(w, TypeEmail)
		}
	default:
		w.lit("/api/orders/")
		w.neg("order_id", g.orderID())
		w.lit("?trace=")
		w.neg("trace_id", g.hex(32))
		w.lit("&uid=")
		g.negative(w)
	}
	w.lit(fmt.Sprintf(` HTTP/1.1" %d %d "-" "Mozilla/5.0 (Windows NT 10.0; Win64; x64)" rt=0.%03d req_id=`, g.pick3(200, 200, 302, 404, 500), 200+g.r.IntN(20000), g.r.IntN(1000)))
	w.neg("trace_id", g.hex(32))
}

func (g *gen) appJSON(w *builder) {
	w.lit(`{"ts":`)
	w.neg("ms_timestamp", g.msTimestamp())
	w.lit(`,"level":"` + g.pick("INFO", "WARN", "ERROR") + `","svc":"` + g.pick("payment", "account", "kyc", "order") + `","trace_id":"`)
	w.neg("trace_id", g.hex(32))
	w.lit(`","order_id":"`)
	w.neg("order_id", g.orderID())
	w.lit(`","snowflake":`)
	w.neg("snowflake_luhn_pass_no_bin", g.snowflake())
	w.lit(`,"user":{"mobile":"`)
	g.positive(w, TypePhone)
	if g.chance(0.6) {
		w.lit(`","email":"`)
		g.positive(w, TypeEmail)
	}
	if g.chance(0.5) {
		w.lit(`","id_no":"`)
		if g.chance(0.8) {
			g.positive(w, TypeIDCard)
		} else {
			g.negative(w)
		}
	}
	w.lit(`"},"card":"`)
	if g.chance(0.6) {
		g.positive(w, TypeBankCard)
	} else {
		g.negative(w)
	}
	w.lit(`","ref":"`)
	g.negative(w)
	w.lit(`","msg":"` + g.pick("verify ok", "kyc check passed", "payment authorised", "risk score computed") + `","ver":"`)
	w.neg("version", g.version())
	w.lit(`"}`)
}

func (g *gen) ticket(w *builder) {
	w.lit("工单#")
	w.neg("order_id", g.orderID())
	w.lit(g.pick("：客户反馈无法登录，", ": user cannot log in; ", "【投诉】"))
	if g.chance(0.6) {
		w.lit(g.pick("身份证号", "ID no. ", "证件号码："))
		g.positive(w, TypeIDCard)
		w.lit(g.pick("，", ", ", "；"))
	}
	if g.chance(0.5) {
		w.lit(g.pick("银行卡（", "card [", "卡号:"))
		g.positive(w, TypeBankCard)
		w.lit(g.pick("），", "], ", "。"))
	}
	w.lit(g.pick("联系电话：", "tel ", "手机"))
	if g.chance(0.05) {
		// Glued to ASCII letters: real PII the strict boundary rule skips.
		w.lit("tel")
		w.pii(TypePhone, "adjacent_alnum", LimAdjacent, g.phone())
	} else {
		g.positive(w, TypePhone)
	}
	if g.chance(0.5) {
		w.lit(g.pick("，邮箱 ", ", mail <", " email="))
		g.positive(w, TypeEmail)
		w.lit(g.pick("。", ">", " "))
	}
	w.lit(" App 版本 v")
	w.neg("version", g.version())
	w.lit(g.pick("，设备号 ", ", device ", " 设备ID="))
	g.negative(w)
	w.lit(g.pick("。", ".", ""))
}
