package faultbench

import (
	"os"
	"os/exec"
	"regexp"
	"runtime"
	"strings"
	"time"
)

// Environment is recorded with every result (DESIGN §11).
type Environment struct {
	Profile   string   `json:"profile"`
	OS        string   `json:"os"`
	Arch      string   `json:"arch"`
	OSVersion string   `json:"os_version"`
	CPU       string   `json:"cpu"`
	NumCPU    int      `json:"num_cpu"`
	GoVersion string   `json:"go_version"`
	GitCommit string   `json:"git_commit"`
	GitDirty  bool     `json:"git_dirty"`
	StartedAt string   `json:"started_at"`
	Notes     []string `json:"notes,omitempty"`
}

var winVersion = regexp.MustCompile(`\d+\.\d+\.\d+(\.\d+)?`)

func run(name string, args ...string) string {
	out, err := exec.Command(name, args...).Output()
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(out))
}

// CollectEnvironment gathers host and build facts.
func CollectEnvironment(profile string) *Environment {
	e := &Environment{
		Profile: profile, OS: runtime.GOOS, Arch: runtime.GOARCH, NumCPU: runtime.NumCPU(),
		GoVersion: runtime.Version(), StartedAt: time.Now().Format(time.RFC3339),
	}
	e.GitCommit = run("git", "rev-parse", "--short=12", "HEAD")
	if e.GitCommit == "" {
		e.GitCommit = "unknown"
	}
	e.GitDirty = run("git", "status", "--porcelain", "--untracked-files=no") != ""
	switch runtime.GOOS {
	case "windows":
		// "ver" is localised; keep only the version number.
		if v := winVersion.FindString(run("cmd", "/c", "ver")); v != "" {
			e.OSVersion = "Windows " + v
		}
		e.CPU = strings.TrimSpace(os.Getenv("PROCESSOR_IDENTIFIER"))
		if out := run("reg", "query", `HKLM\HARDWARE\DESCRIPTION\System\CentralProcessor\0`, "/v", "ProcessorNameString"); out != "" {
			if i := strings.Index(out, "REG_SZ"); i >= 0 {
				e.CPU = strings.TrimSpace(out[i+len("REG_SZ"):])
			}
		}
		e.Notes = append(e.Notes,
			"Windows run: smoke test only. Final numbers come from Linux (DESIGN §0.5 item 12).",
			"kill = TerminateProcess (os.Process.Kill); graceful stop = POST /v1/admin/shutdown.")
	case "linux":
		e.OSVersion = run("uname", "-sr")
		if b, err := os.ReadFile("/proc/cpuinfo"); err == nil {
			for _, l := range strings.Split(string(b), "\n") {
				if strings.HasPrefix(l, "model name") {
					if _, v, ok := strings.Cut(l, ":"); ok {
						e.CPU = strings.TrimSpace(v)
					}
					break
				}
			}
		}
		e.Notes = append(e.Notes, "kill = SIGKILL; graceful stop = SIGTERM. kill -9 is not power loss (page cache survives).")
	default:
		e.OSVersion = run("uname", "-sr")
	}
	return e
}
