package gateway

import (
	"bytes"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// Synthetic values only: the GB 11643 example ID, a Visa test card number,
// and a phone number with an allocated prefix.
const (
	gwTestID    = "11010519491231002X"
	gwTestCard  = "4111111111111111"
	gwTestPhone = "13812345678"
)

func TestMaskingLevelsAndMaxLevelRouting(t *testing.T) {
	dir := t.TempDir()
	fakes := map[string]*fakeSink{"soar": newFake()}
	cfg := testCfg(t, dir, `
destinations:
  - { name: archive, type: file, config: { path: "$OUT/archive.jsonl" } }
  - { name: soar, type: file, max_level: general, config: { path: "fake:soar" } }
routes:
  - { match: { class_uid: 3002 }, to: [archive, soar] }
  - { match: {}, to: [archive, soar] }
`)
	g := start(t, cfg, fakes)
	acked := post(t, g.Addr(), 3, func(i int) string {
		switch i {
		case 0:
			return `{"msg":"nothing sensitive"}`
		case 1:
			return `{"msg":"call ` + gwTestPhone + `"}`
		}
		return `{"msg":"id ` + gwTestID + ` card ` + gwTestCard + `"}` // important
	})
	resp, err := http.Post("http://"+g.Addr()+"/v1/events", "text/plain",
		strings.NewReader("sshd[9]: Failed password for invalid user "+gwTestPhone+" from 192.0.2.7 port 4242 ssh2\n"))
	if err != nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("text post: %v %v", resp, err)
	}
	resp.Body.Close()
	eventually(t, "deliveries", func() bool { return g.Delivered() == 4+3 })
	st := g.Stats()
	shutdown(t, g)

	soar := fakes["soar"].snapshot()
	if len(soar) != 3 || soar[acked[2]] != 0 {
		t.Fatalf("soar got %v; the important event (seq %d) must be blocked", soar, acked[2])
	}
	var blocked uint64
	for _, d := range st.Destinations {
		if d.Name == "soar" {
			blocked = d.RouteLevelBlocked
		}
	}
	if blocked != 1 || st.Masking == nil || st.Masking.Hits["id_card"] != 1 || st.Masking.Hits["phone"] != 3 {
		t.Fatalf("blocked %d masking %+v", blocked, st.Masking)
	}

	archive, err := os.ReadFile(filepath.Join(dir, "out", "archive.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"110105********002X", "4111********1111", "138****5678", `"class_uid":3002`} {
		if !bytes.Contains(archive, []byte(want)) {
			t.Errorf("archive lacks %q", want)
		}
	}
	// Nothing under data_dir (WAL, state.db, DLQ) or the sink holds raw PII.
	_ = filepath.Walk(dir, func(p string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() {
			return err
		}
		b, err := os.ReadFile(p)
		if err != nil {
			return err
		}
		for _, pii := range []string{gwTestID, gwTestCard, gwTestPhone} {
			if bytes.Contains(b, []byte(pii)) {
				t.Errorf("%s contains raw PII %q", p, pii)
			}
		}
		return nil
	})
}
