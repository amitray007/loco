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
	assertCommands(t, runner.calls, "print", "bootstrap", "kickstart")

	runner = &fakeRunner{loaded: true}
	result, err = Install(context.Background(), runner, p, "/usr/local/bin/loco")
	if err != nil {
		t.Fatal(err)
	}
	if result.Action != "already installed" {
		t.Fatalf("Action = %q", result.Action)
	}
	assertCommands(t, runner.calls, "print", "kickstart")
}

func TestInstallReloadsChangedPlist(t *testing.T) {
	p := launchdTestPaths(t)
	if err := os.MkdirAll(filepath.Dir(p.Plist), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p.Plist, []byte("old"), 0o644); err != nil {
		t.Fatal(err)
	}
	runner := &fakeRunner{loaded: true}
	if _, err := Install(context.Background(), runner, p, "/new/loco"); err != nil {
		t.Fatal(err)
	}
	assertCommands(t, runner.calls, "print", "bootout", "bootstrap", "kickstart")
}

func launchdTestPaths(t *testing.T) paths.Paths {
	t.Helper()
	root := t.TempDir()
	return paths.Paths{
		ConfigDir: filepath.Join(root, "config"),
		RunDir:    filepath.Join(root, "config", "run"),
		LogDir:    filepath.Join(root, "config", "logs"),
		Socket:    filepath.Join(root, "config", "run", "daemon.sock"),
		Lock:      filepath.Join(root, "config", "run", "daemon.lock"),
		State:     filepath.Join(root, "config", "run", "daemon.json"),
		Registry:  filepath.Join(root, "config", "registry.json"),
		Plist:     filepath.Join(root, "LaunchAgents", paths.LaunchdLabel+".plist"),
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
