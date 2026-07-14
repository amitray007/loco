//go:build darwin

package launchd

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/amitray007/loco/internal/paths"
)

type call struct {
	name string
	args []string
}

type fakeRunner struct {
	loaded bool
	calls  []call
}

func (f *fakeRunner) Run(_ context.Context, name string, args ...string) ([]byte, error) {
	f.calls = append(f.calls, call{name: name, args: append([]string(nil), args...)})
	if name == "launchctl" && len(args) > 0 && args[0] == "print" && !f.loaded {
		return []byte("not found"), errors.New("exit status 113")
	}
	return nil, nil
}

func TestRenderPlistContainsKeepAliveAndEscapesPaths(t *testing.T) {
	p := paths.Paths{
		ConfigDir: "/tmp/loco & friends",
		LogDir:    "/tmp/loco & friends/logs",
	}
	plist := string(RenderPlist("/tmp/a < b/loco", p))
	for _, want := range []string{
		"<key>KeepAlive</key>",
		"<true/>",
		"/tmp/a &lt; b/loco",
		"/tmp/loco &amp; friends",
		"<string>daemon</string>",
		"<string>run</string>",
	} {
		if !strings.Contains(plist, want) {
			t.Errorf("plist missing %q", want)
		}
	}
}
func TestRenderWatchdogPlistIsIndependentPeriodicJob(t *testing.T) {
	p := paths.Paths{ConfigDir: "/tmp/config", LogDir: "/tmp/logs"}
	plist := string(RenderWatchdogPlist("/tmp/loco", p))
	for _, want := range []string{paths.WatchdogLaunchdLabel, "<key>StartInterval</key><integer>60</integer>", "<string>watchdog</string>"} {
		if !strings.Contains(plist, want) {
			t.Errorf("missing %q", want)
		}
	}
	if strings.Contains(plist, "<key>KeepAlive</key>") {
		t.Fatal("watchdog must not be KeepAlive")
	}
}

func TestInstallBootstrapsAndIsIdempotent(t *testing.T) {
	p := launchdTestPaths(t)
	runner := &fakeRunner{}
	result, err := Install(context.Background(), runner, p, "/usr/local/bin/loco")
	if err != nil {
		t.Fatal(err)
	}
	if result.Action != "installed" {
		t.Fatalf("Action = %q", result.Action)
	}
	assertCommands(t, runner.calls, "print", "bootstrap", "kickstart", "print", "bootstrap", "kickstart")

	runner = &fakeRunner{loaded: true}
	result, err = Install(context.Background(), runner, p, "/usr/local/bin/loco")
	if err != nil {
		t.Fatal(err)
	}
	if result.Action != "already installed" {
		t.Fatalf("Action = %q", result.Action)
	}
	assertCommands(t, runner.calls, "print", "kickstart", "print", "kickstart")
}

func TestInstallReloadsChangedPlist(t *testing.T) {
	p := launchdTestPaths(t)
	if err := os.MkdirAll(filepath.Dir(p.Plist()), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p.Plist(), []byte("old"), 0o644); err != nil {
		t.Fatal(err)
	}
	runner := &fakeRunner{loaded: true}
	if _, err := Install(context.Background(), runner, p, "/new/loco"); err != nil {
		t.Fatal(err)
	}
	assertCommands(t, runner.calls, "print", "bootout", "bootstrap", "kickstart", "print", "bootout", "bootstrap", "kickstart")
}

func TestUninstallRemovesBothJobs(t *testing.T) {
	p := launchdTestPaths(t)
	if err := os.MkdirAll(filepath.Dir(p.Plist()), 0o755); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{p.Plist(), p.WatchdogPlist()} {
		if err := os.WriteFile(path, []byte("x"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	runner := &fakeRunner{loaded: true}
	removed, err := Uninstall(context.Background(), runner, p)
	if err != nil {
		t.Fatal(err)
	}
	if !removed {
		t.Fatal("removed=false")
	}
	assertCommands(t, runner.calls, "print", "bootout", "print", "bootout")
	for _, path := range []string{p.Plist(), p.WatchdogPlist()} {
		if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("%s remains", path)
		}
	}
}

func launchdTestPaths(t *testing.T) paths.Paths {
	t.Helper()
	root := t.TempDir()
	return paths.Paths{
		ConfigDir:     filepath.Join(root, "config"),
		RunDir:        filepath.Join(root, "config", "run"),
		LogDir:        filepath.Join(root, "config", "logs"),
		RunsDir:       filepath.Join(root, "config", "runs"),
		EventsDir:     filepath.Join(root, "config", "events"),
		Alerts:        filepath.Join(root, "config", "alerts.jsonl"),
		NotifyState:   filepath.Join(root, "config", "notify.json"),
		WatchdogState: filepath.Join(root, "config", "watchdog.json"),
		Socket:        filepath.Join(root, "config", "run", "daemon.sock"),
		Lock:          filepath.Join(root, "config", "run", "daemon.lock"),
		State:         filepath.Join(root, "config", "run", "daemon.json"),
		Registry:      filepath.Join(root, "config", "registry.json"),
		ServiceUnit:   filepath.Join(root, "LaunchAgents", paths.LaunchdLabel+".plist"),
		WatchdogUnit:  filepath.Join(root, "LaunchAgents", paths.WatchdogLaunchdLabel+".plist"),
	}
}

func assertCommands(t *testing.T, calls []call, want ...string) {
	t.Helper()
	if len(calls) != len(want) {
		t.Fatalf("got %d calls (%v), want %d (%v)", len(calls), calls, len(want), want)
	}
	for i, command := range want {
		if calls[i].name != "launchctl" || len(calls[i].args) == 0 || calls[i].args[0] != command {
			t.Errorf("call %d = %#v, want launchctl %s", i, calls[i], command)
		}
	}
}
