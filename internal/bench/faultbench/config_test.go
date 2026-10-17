package faultbench

import (
	"path/filepath"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"

	"github.com/lzwhehe/loggate/internal/bench/scenario"
	"github.com/lzwhehe/loggate/internal/config"
)

// Every shipped scenario generates a gateway config that loggate accepts.
func TestGeneratedGatewayConfigsParse(t *testing.T) {
	files, _ := filepath.Glob("../../../bench/scenarios/S*.yaml")
	if len(files) == 0 {
		t.Fatal("no scenarios")
	}
	for _, f := range files {
		sc, err := scenario.Load(f)
		if err != nil {
			t.Fatal(err)
		}
		r, _, err := sc.Resolve("quick")
		if err != nil {
			t.Fatal(err)
		}
		m, err := BuildGatewayConfig(r, "data", "out", "127.0.0.1:1", map[string]string{"soar": "127.0.0.1:2", "probe": "127.0.0.1:3"})
		if err != nil {
			t.Fatal(err)
		}
		b, err := yaml.Marshal(m)
		if err != nil {
			t.Fatal(err)
		}
		c, err := config.Parse(b)
		if err != nil {
			t.Fatalf("%s: %v\n%s", sc.Name, err, b)
		}
		if len(c.Destinations) != 4 || len(c.Routes) != 3 || !c.Admin.Enabled() || len(c.Ingest.HTTP.Tokens) != 1 {
			t.Fatalf("%s: %+v", sc.Name, c)
		}
		for _, d := range c.Destinations {
			if u, _ := d.Config["url"].(string); d.Type == "webhook" && !strings.HasPrefix(u, "http://127.0.0.1:") {
				t.Fatalf("%s: url %q", sc.Name, u)
			}
		}
		if sc.Name == "S4b" && c.L1.OrphanTTL.String() != "30s" {
			t.Fatalf("S4b orphan_ttl = %v", c.L1.OrphanTTL)
		}
		if sc.Name == "S5" && (c.Disk.MaxBytes != 64<<20 || c.WAL.SegmentBytes != 4<<20) {
			t.Fatalf("S5 disk/segment: %+v %+v", c.Disk, c.WAL)
		}
	}
}

func TestDrainedPredicate(t *testing.T) {
	var s gwStats
	s.NextSeq, s.Cursor = 11, 10
	if !s.drained() {
		t.Fatal("empty gateway not drained")
	}
	s.Cursor = 9
	if s.drained() {
		t.Fatal("undispatched record ignored")
	}
	s.Cursor = 10
	s.Destinations = make([]struct {
		Name       string `json:"name"`
		State      string `json:"state"`
		L2Depth    int    `json:"l2_depth"`
		InFlight   int64  `json:"in_flight"`
		L1Pending  int64  `json:"l1_pending"`
		Unresolved int    `json:"unresolved"`
	}, 1)
	s.Destinations[0].L1Pending = 1
	if s.drained() {
		t.Fatal("L1 pending ignored")
	}
}
