package gateway

import (
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"
)

const testAdminToken = "admin-token-0123456789"

func adminPost(t *testing.T, addr, path, token, body string) int {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, "http://"+addr+path, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	return resp.StatusCode
}

func TestAdminUnregisterRegisterShutdown(t *testing.T) {
	dir := t.TempDir()
	fakes := map[string]*fakeSink{"a": newFake(), "b": newFake()}
	cfg := testCfg(t, dir, `
admin: { token: "`+testAdminToken+`" }
destinations:
  - { name: a, type: file, config: { path: "fake:a" } }
  - { name: b, type: file, config: { path: "fake:b" } }
routes:
  - { match: {}, to: [a, b] }
`)
	g := start(t, cfg, fakes)
	addr := g.Addr()

	if c := adminPost(t, addr, "/v1/admin/unregister", "", `{"name":"b"}`); c != http.StatusUnauthorized {
		t.Fatalf("no token: status %d, want 401", c)
	}
	if c := adminPost(t, addr, "/v1/admin/unregister", "wrong-token-0123456789", `{"name":"b"}`); c != http.StatusUnauthorized {
		t.Fatalf("bad token: status %d, want 401", c)
	}
	if c := adminPost(t, addr, "/v1/admin/unregister", testAdminToken, `{"name":"b"}`); c != http.StatusOK {
		t.Fatalf("unregister: status %d", c)
	}
	if c := adminPost(t, addr, "/v1/admin/unregister", testAdminToken, `{"name":"b"}`); c != http.StatusConflict {
		t.Fatalf("second unregister: status %d, want 409", c)
	}
	acked := post(t, addr, 20, plain)
	eventually(t, "a delivered", func() bool { return fakes["a"].count() == 20 })
	if n := fakes["b"].count(); n != 0 {
		t.Fatalf("unregistered destination got %d events", n)
	}
	if c := adminPost(t, addr, "/v1/admin/register", testAdminToken, `{"name":"b"}`); c != http.StatusOK {
		t.Fatalf("register: status %d", c)
	}
	eventually(t, "b delivered after re-register", func() bool { return fakes["b"].count() == 20 })

	select {
	case <-g.ShutdownRequested():
		t.Fatal("shutdown requested before the call")
	default:
	}
	if c := adminPost(t, addr, "/v1/admin/shutdown", testAdminToken, ``); c != http.StatusAccepted {
		t.Fatalf("shutdown: status %d", c)
	}
	select {
	case <-g.ShutdownRequested():
	case <-time.After(5 * time.Second):
		t.Fatal("shutdown not signalled")
	}
	shutdown(t, g)
	checkExactlyOnce(t, "a", fakes["a"].snapshot(), acked)
	checkExactlyOnce(t, "b", fakes["b"].snapshot(), acked)
}

func TestAdminDisabledWithoutToken(t *testing.T) {
	dir := t.TempDir()
	fakes := map[string]*fakeSink{"a": newFake()}
	cfg := testCfg(t, dir, `
destinations:
  - { name: a, type: file, config: { path: "fake:a" } }
routes:
  - { match: {}, to: [a] }
`)
	g := start(t, cfg, fakes)
	if c := adminPost(t, g.Addr(), "/v1/admin/shutdown", testAdminToken, ``); c == http.StatusAccepted {
		t.Fatal("admin API reachable without admin config")
	}
	shutdown(t, g)
}

// Disk usage over disk.max_bytes refuses new events with 429; acked events
// are still delivered.
func TestDiskQuota429(t *testing.T) {
	dir := t.TempDir()
	fakes := map[string]*fakeSink{"a": newFake()}
	cfg := testCfg(t, dir, `
disk: { max_bytes: 200KiB, check_interval: 5ms }
destinations:
  - { name: a, type: file, config: { path: "fake:a" } }
routes:
  - { match: {}, to: [a] }
`)
	g := start(t, cfg, fakes)
	var acked []uint64
	big := strings.Repeat("x", 2000)
	mk := func(i int) string { return `{"i":` + strconv.Itoa(i) + `,"pad":"` + big + `"}` }
	deadline := time.Now().Add(20 * time.Second)
	for {
		s, err := postErr(g.Addr(), 50, mk)
		if err != nil {
			if !strings.Contains(err.Error(), "status 429") {
				t.Fatalf("want 429, got %v", err)
			}
			break
		}
		acked = append(acked, s...)
		if time.Now().After(deadline) {
			t.Fatal("disk quota never triggered")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if st := g.Stats(); !st.DiskOver || st.DiskBytes <= 200<<10 {
		t.Fatalf("stats: over=%v bytes=%d", st.DiskOver, st.DiskBytes)
	}
	eventually(t, "acked delivered", func() bool { return fakes["a"].count() == len(acked) })
	shutdown(t, g)
	checkExactlyOnce(t, "a", fakes["a"].snapshot(), acked)
}
