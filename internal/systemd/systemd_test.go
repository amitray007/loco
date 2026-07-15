//go:build linux

package systemd

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/amitray007/loco/internal/paths"
)

func testPaths(t *testing.T) paths.Paths {
	t.Helper()
	root := t.TempDir()
	unitDir := filepath.Join(root, "units")
	// Install calls EnsureRuntimeDirs, which MkdirAll's every runtime dir — so all of
	// them must be set. Leaving RunDir/RunsDir/EventsDir empty makes it mkdir "" and
	// fail with a bare "no such file or directory".
	return paths.Paths{
		ConfigDir:    root,
		RunDir:       filepath.Join(root, "run"),
		LogDir:       filepath.Join(root, "logs"),
		RunsDir:      filepath.Join(root, "runs"),
		EventsDir:    filepath.Join(root, "events"),
		ServiceUnit:  filepath.Join(unitDir, paths.Label+".service"),
		WatchdogUnit: filepath.Join(unitDir, paths.WatchdogLabel+".service"),
	}
}

func TestDaemonUnitHasRestartAlways(t *testing.T) {
	p := testPaths(t)
	unit := string(RenderDaemonUnit("/usr/local/bin/loco", p))
	for _, want := range []string{"Restart=always", "ExecStart=/usr/local/bin/loco --config-dir", "daemon run", "WantedBy=default.target"} {
		if !strings.Contains(unit, want) {
			t.Fatalf("daemon unit missing %q:\n%s", want, unit)
		}
	}
}

func TestExecStartQuotesPathsWithSpaces(t *testing.T) {
	p := testPaths(t)
	unit := string(RenderDaemonUnit("/opt/my apps/loco", p))
	if !strings.Contains(unit, `ExecStart="/opt/my apps/loco"`) {
		t.Fatalf("path with spaces not quoted:\n%s", unit)
	}
}

func TestWatchdogTimerFiresPeriodically(t *testing.T) {
	timer := string(RenderWatchdogTimer())
	for _, want := range []string{"[Timer]", "OnUnitActiveSec=60", "WantedBy=timers.target"} {
		if !strings.Contains(timer, want) {
			t.Fatalf("timer missing %q:\n%s", want, timer)
		}
	}
}

// fakeRunner records systemctl invocations instead of running them.
type fakeRunner struct{ calls [][]string }

func (f *fakeRunner) Run(_ context.Context, name string, args ...string) ([]byte, error) {
	f.calls = append(f.calls, append([]string{name}, args...))
	return nil, nil
}

func TestInstallWritesUnitsAndEnables(t *testing.T) {
	p := testPaths(t)
	runner := &fakeRunner{}
	action, err := Install(context.Background(), runner, p, "/usr/local/bin/loco")
	if err != nil {
		t.Fatal(err)
	}
	if action != "installed" {
		t.Fatalf("action = %q", action)
	}
	// Unit files must exist on disk.
	for _, path := range []string{p.ServiceUnit, p.WatchdogUnit, timerPathFor(p.WatchdogUnit)} {
		if _, err := os.Stat(path); err != nil {
			t.Fatalf("unit not written: %s: %v", path, err)
		}
	}
	// daemon-reload + two enables must have been issued.
	var reloaded, enabled int
	for _, c := range runner.calls {
		joined := strings.Join(c, " ")
		if strings.Contains(joined, "daemon-reload") {
			reloaded++
		}
		if strings.Contains(joined, "enable --now") {
			enabled++
		}
	}
	if reloaded < 1 || enabled < 2 {
		t.Fatalf("expected reload + 2 enables, got reload=%d enable=%d", reloaded, enabled)
	}
}

// Install refuses a path containing a control character (unit-directive injection guard).
func TestInstallRejectsControlCharInPath(t *testing.T) {
	p := testPaths(t)
	runner := &fakeRunner{}
	_, err := Install(context.Background(), runner, p, "/usr/local/bin/lo\nco")
	if err == nil || !strings.Contains(err.Error(), "control character") {
		t.Fatalf("expected control-char rejection, got: %v", err)
	}
}
