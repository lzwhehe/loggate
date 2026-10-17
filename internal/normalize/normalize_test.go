package normalize

import (
	"bytes"
	"encoding/json"
	"flag"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/santhosh-tekuri/jsonschema/v6"
)

var update = flag.Bool("update", false, "rewrite golden files")

var recv = time.Date(2026, 10, 17, 2, 30, 0, 0, time.UTC)

func loadSchema(t *testing.T) *jsonschema.Schema {
	t.Helper()
	c := jsonschema.NewCompiler()
	sch, err := c.Compile(filepath.Join("testdata", "ocsf_subset.schema.json"))
	if err != nil {
		t.Fatal(err)
	}
	return sch
}

func decodeObj(t *testing.T, b []byte) map[string]any {
	t.Helper()
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.UseNumber()
	var m map[string]any
	if err := dec.Decode(&m); err != nil {
		t.Fatal(err)
	}
	return m
}

func normalizeFile(t *testing.T, path string) Event {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if strings.HasSuffix(path, ".json") {
		return JSON(decodeObj(t, b), recv)
	}
	return Text(string(b), recv)
}

func TestGolden(t *testing.T) {
	sch := loadSchema(t)
	inputs, _ := filepath.Glob(filepath.Join("testdata", "golden", "*.txt"))
	jsons, _ := filepath.Glob(filepath.Join("testdata", "golden", "*.json"))
	for _, j := range jsons {
		if !strings.HasSuffix(j, ".out.json") {
			inputs = append(inputs, j)
		}
	}
	if len(inputs) < 8 {
		t.Fatalf("only %d golden inputs", len(inputs))
	}
	for _, in := range inputs {
		t.Run(filepath.Base(in), func(t *testing.T) {
			ev := normalizeFile(t, in)
			got, err := json.MarshalIndent(ev.Fields, "", "  ")
			if err != nil {
				t.Fatal(err)
			}
			got = append(got, '\n')
			golden := strings.TrimSuffix(strings.TrimSuffix(in, ".txt"), ".json") + ".out.json"
			if *update {
				if err := os.WriteFile(golden, got, 0o644); err != nil {
					t.Fatal(err)
				}
			}
			want, err := os.ReadFile(golden)
			if err != nil {
				t.Fatalf("%v (run with -update)", err)
			}
			if !bytes.Equal(bytes.ReplaceAll(want, []byte("\r\n"), []byte("\n")), got) {
				t.Fatalf("mismatch\n--- got\n%s\n--- want\n%s", got, want)
			}
			// Schema validation on the JSON round trip, as a consumer sees it.
			inst, err := jsonschema.UnmarshalJSON(bytes.NewReader(got))
			if err != nil {
				t.Fatal(err)
			}
			if err := sch.Validate(inst); err != nil {
				t.Fatalf("schema: %v", err)
			}
			// type_uid = class_uid*100 + activity_id (not expressible in the schema).
			f := ev.Fields
			if f["type_uid"].(int) != f["class_uid"].(int)*100+f["activity_id"].(int) {
				t.Fatalf("type_uid %v", f["type_uid"])
			}
			if int(ev.ClassUID) != f["class_uid"].(int) || int(ev.ActivityID) != f["activity_id"].(int) {
				t.Fatal("struct fields disagree with map")
			}
		})
	}
}

func TestSSHDMapping(t *testing.T) {
	cases := []struct {
		line             string
		class, act, stat int
		user, ip         string
		port             int
	}{
		{"sshd[1]: Accepted password for alice from 192.0.2.1 port 22 ssh2", ClassAuth, 1, 1, "alice", "192.0.2.1", 22},
		{"sshd[1]: Failed password for invalid user bob from 192.0.2.2 port 23 ssh2", ClassAuth, 1, 2, "bob", "192.0.2.2", 23},
		{"sshd[1]: Failed password for root from 2001:db8::1 port 24 ssh2", ClassAuth, 1, 2, "root", "2001:db8::1", 24},
		{"sshd[1]: Connection closed by 192.0.2.3 port 25 [preauth]", ClassAuth, 1, 2, "", "192.0.2.3", 25},
		{"sshd[1]: Server listening on 0.0.0.0 port 22.", ClassBase, 0, 0, "", "", 0},
		{"random text", ClassBase, 0, 0, "", "", 0},
	}
	for _, c := range cases {
		ev := Text(c.line, recv)
		if int(ev.ClassUID) != c.class || int(ev.ActivityID) != c.act || int(ev.StatusID) != c.stat {
			t.Errorf("%q: class %d act %d status %d", c.line, ev.ClassUID, ev.ActivityID, ev.StatusID)
			continue
		}
		if c.class != ClassAuth {
			if ev.Fields["raw_data"] != c.line {
				t.Errorf("%q: raw_data %v", c.line, ev.Fields["raw_data"])
			}
			continue
		}
		u, _ := ev.Fields["user"].(map[string]any)
		if c.user != "" && (u == nil || u["name"] != c.user) {
			t.Errorf("%q: user %v", c.line, u)
		}
		ep := ev.Fields["src_endpoint"].(map[string]any)
		if ep["ip"] != c.ip || ep["port"] != c.port {
			t.Errorf("%q: endpoint %v", c.line, ep)
		}
	}
}

func TestJSONBaseKeepsObject(t *testing.T) {
	obj := decodeObj(t, []byte(`{"test_seq":7,"class":"auth","event":{"type":"net"}}`))
	ev := JSON(obj, recv)
	if ev.ClassUID != ClassBase {
		t.Fatalf("class %d", ev.ClassUID)
	}
	um := ev.Fields["unmapped"].(map[string]any)
	if um["test_seq"] != json.Number("7") || um["class"] != "auth" {
		t.Fatalf("unmapped %v", um)
	}
	if _, ok := ev.Fields["raw_data"]; ok {
		t.Fatal("raw_data should not duplicate JSON input")
	}
}

func TestSchemaRejectsBadEvents(t *testing.T) {
	sch := loadSchema(t)
	for _, doc := range []string{
		`{"class_uid":3002,"class_name":"Authentication","category_uid":0,"activity_id":1,"type_uid":300201,"severity_id":1,"time":1,"metadata":{"version":"1.3.0","product":{"name":"x"}},"status_id":1,"auth_protocol_id":0}`,
		`{"class_uid":0,"class_name":"Base Event","category_uid":0,"activity_id":0,"type_uid":0,"severity_id":0,"time":1,"metadata":{"version":"1.3.0","product":{"name":"x"}}}`,
		`{"class_uid":4001}`,
	} {
		inst, err := jsonschema.UnmarshalJSON(strings.NewReader(doc))
		if err != nil {
			t.Fatal(err)
		}
		if sch.Validate(inst) == nil {
			t.Errorf("schema accepted %s", doc)
		}
	}
}
