package scenario

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestShippedScenariosLoad(t *testing.T) {
	files, err := filepath.Glob("../../../bench/scenarios/S*.yaml")
	if err != nil || len(files) < 8 {
		t.Fatalf("scenario files: %v %v", files, err)
	}
	for _, f := range files {
		sc, err := Load(f)
		if err != nil {
			t.Fatal(err)
		}
		if len(sc.Destinations) != 4 || len(sc.Checks) == 0 {
			t.Fatalf("%s: base not merged: %+v", f, sc)
		}
		for _, p := range []string{"quick", "full"} {
			r, prof, err := sc.Resolve(p)
			if err != nil {
				t.Fatal(err)
			}
			for _, a := range r.Timeline {
				if a.At > r.Duration {
					t.Fatalf("%s/%s: action at %v after load end %v", sc.Name, p, a.At, r.Duration)
				}
			}
			if p == "full" && (prof.Rate != 2000 || r.Duration != 5*time.Minute || prof.Reps != 5) {
				t.Fatalf("%s full profile: %+v %v", sc.Name, prof, r.Duration)
			}
		}
	}
}

func TestResolveScales(t *testing.T) {
	sc, err := Load("../../../bench/scenarios/S1_sink_down.yaml")
	if err != nil {
		t.Fatal(err)
	}
	r, p, err := sc.Resolve("quick")
	if err != nil {
		t.Fatal(err)
	}
	if p.Rate != 200 || r.Duration != time.Minute || r.Timeline[0].At != 6*time.Second || r.Timeline[1].At != 30*time.Second ||
		r.FaultWindow.Start != 6*time.Second || r.FaultWindow.End != 30*time.Second {
		t.Fatalf("%+v %+v", r.Timeline, r.FaultWindow)
	}
	if sc.Timeline[0].At != 30*time.Second {
		t.Fatal("Resolve modified the original")
	}
	if _, _, err := sc.Resolve("nope"); err == nil {
		t.Fatal("unknown profile accepted")
	}
}

func TestOracleFirstMatch(t *testing.T) {
	sc, err := Load("../../../bench/scenarios/S0_baseline.yaml")
	if err != nil {
		t.Fatal(err)
	}
	r := sc.Router()
	if got := r.RouteClass("auth"); len(got) != 4 {
		t.Fatalf("auth -> %v", got)
	}
	if got := r.RouteClass("net"); len(got) != 3 || got[2] != "probe" {
		t.Fatalf("net -> %v", got)
	}
	if got := r.RouteClass("other"); len(got) != 1 || got[0] != "archive" {
		t.Fatalf("other -> %v", got)
	}
}

func TestValidateRejects(t *testing.T) {
	const head = "name: x\nduration: 1s\nclasses: [{name: a, weight: 1}]\n"
	bad := []string{
		head + "destinations: [{name: d, type: webhook}]\nroutes: [{match: {}, to: [d]}]\n",
		head + "destinations: [{name: d, type: file}]\nroutes: [{match: {}, to: [nope]}]\n",
		head + "destinations: [{name: d, type: file}]\nroutes: [{match: {host: x}, to: [d]}]\n",
		head + "destinations: [{name: d, type: file}]\nroutes: [{match: {}, to: [d]}]\ntimeline: [{at: 1s, action: fault, dest: d, mode: down}]\n",
		head + "destinations: [{name: d, type: file}]\nroutes: [{match: {}, to: [d]}]\nbogus: 1\n",
	}
	for i, b := range bad {
		if _, err := Parse([]byte(b)); err == nil {
			t.Fatalf("case %d accepted", i)
		}
	}
}

func TestDeepMergeAndBase(t *testing.T) {
	dir := t.TempDir()
	write := func(n, s string) {
		if err := os.WriteFile(filepath.Join(dir, n), []byte(s), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	write("b.yaml", "duration: 10s\nclasses: [{name: a, weight: 1}]\ndestinations: [{name: d, type: file}]\nroutes: [{match: {}, to: [d]}]\ngateway: {l1: {checkpoint_interval: 1s, sweep_interval: 1s}}\n")
	write("s.yaml", "base: b.yaml\nname: s\ngateway: {l1: {orphan_ttl: 30s}}\n")
	sc, err := Load(filepath.Join(dir, "s.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	l1 := sc.Gateway["l1"].(map[string]any)
	if l1["checkpoint_interval"] != "1s" || l1["orphan_ttl"] != "30s" || sc.Duration != 10*time.Second {
		t.Fatalf("%+v", sc.Gateway)
	}
}
