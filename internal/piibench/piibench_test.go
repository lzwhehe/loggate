package piibench

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/lzwhehe/loggate/internal/mask"
)

func TestGenerateDeterministicAndConsistent(t *testing.T) {
	a, b := Generate(7, 400), Generate(7, 400)
	if !reflect.DeepEqual(a, b) {
		t.Fatal("same seed, different output")
	}
	if reflect.DeepEqual(a, Generate(8, 400)) {
		t.Fatal("different seeds, same output")
	}
	det, err := mask.NewDetector(mask.DetectorConfig{Today: time.Date(2026, 10, 17, 0, 0, 0, 0, time.UTC)})
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range a {
		if r.Template == "app_json" && !json.Valid([]byte(r.Text)) {
			t.Fatalf("record %d: invalid JSON %s", r.ID, r.Text)
		}
		for _, s := range r.Spans {
			v := r.Text[s.Start:s.End]
			if s.Limitation != "" {
				continue
			}
			digits := strings.NewReplacer(" ", "", "-", "").Replace(v)
			ok := true
			switch s.Type {
			case TypeIDCard:
				ok = det.ValidIDCard(v)
			case TypeBankCard:
				ok = det.ValidBankCard([]byte(digits))
			case TypePhone:
				ok = mask.ValidPhone(v)
			case TypeEmail:
				ok = strings.Contains(v, "@")
			}
			if !ok {
				t.Fatalf("record %d: invalid positive %s %q", r.ID, s.Type, v)
			}
		}
		for _, n := range r.Negatives {
			v := r.Text[n.Start:n.End]
			switch n.Kind {
			case "id_bad_checksum", "id_bad_date", "id_bad_province":
				if det.ValidIDCard(v) {
					t.Fatalf("record %d: negative %s validates: %q", r.ID, n.Kind, v)
				}
			case "card_luhn_fail", "snowflake_luhn_pass_no_bin":
				if det.ValidBankCard([]byte(v)) {
					t.Fatalf("record %d: negative %s validates: %q", r.ID, n.Kind, v)
				}
				if n.Kind == "snowflake_luhn_pass_no_bin" && !mask.Luhn([]byte(v)) {
					t.Fatalf("snowflake %q should pass Luhn", v)
				}
			case "phone_bad_prefix":
				if mask.ValidPhone(v) {
					t.Fatalf("record %d: bad phone validates: %q", r.ID, v)
				}
			}
		}
	}
}

func TestEvaluateCounting(t *testing.T) {
	recs := []Record{{ID: 1, Text: "a 13812345678 b 1234", Spans: []Span{{Start: 2, End: 13, Type: TypePhone}},
		Negatives: []Neg{{Start: 16, End: 20, Kind: "x"}}}}
	sys := System{Name: "t", Detect: func(string) []Pred {
		return []Pred{{2, 13, TypePhone}, {16, 20, TypeBankCard}, {0, 1, TypeEmail}}
	}}
	r := Evaluate(sys, recs)
	if c := r.PerType[TypePhone]; c.TP != 1 || c.FP != 0 || c.FN != 0 || c.F1 != 1 {
		t.Fatalf("phone %+v", c)
	}
	if r.FPCauses[TypeBankCard]["x"] != 1 || r.FPCauses[TypeEmail]["unlabelled_text"] != 1 || r.Micro.FP != 2 {
		t.Fatalf("fp causes %+v micro %+v", r.FPCauses, r.Micro)
	}
	miss := Evaluate(System{Name: "none", Detect: func(string) []Pred { return nil }}, recs)
	if miss.PerType[TypePhone].FN != 1 || miss.FNVariants[TypePhone]["plain"] != 1 {
		t.Fatalf("fn %+v", miss.PerType[TypePhone])
	}
}
