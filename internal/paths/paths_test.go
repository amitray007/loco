package paths

import (
	"os"
	"path/filepath"
	"testing"
)

func TestResolveUsesExplicitConfigDirectory(t *testing.T) {
	t.Setenv("LOCO_LAUNCH_AGENTS_DIR", filepath.Join(t.TempDir(), "agents"))
	config := filepath.Join(t.TempDir(), "config")
	p, err := Resolve(config)
	if err != nil {
		t.Fatal(err)
	}
	if p.ConfigDir != config {
		t.Fatalf("ConfigDir = %q, want %q", p.ConfigDir, config)
	}
	if p.Socket != filepath.Join(config, "run", "daemon.sock") {
		t.Fatalf("Socket = %q", p.Socket)
	}
	if filepath.Base(p.Plist) != LaunchdLabel+".plist" {
		t.Fatalf("Plist = %q", p.Plist)
	}
}

// F-22: a leading ~ in --config-dir expands to the home directory instead of creating a
// stray literal "~" directory.
func TestConfigDirTildeExpands(t *testing.T) {
	home, err := os.UserHomeDir()
	if err != nil {
		t.Fatal(err)
	}
	p, err := Resolve("~/loco-tilde-test")
	if err != nil {
		t.Fatal(err)
	}
	want := filepath.Join(home, "loco-tilde-test")
	if p.ConfigDir != want {
		t.Fatalf("tilde not expanded: got %s want %s", p.ConfigDir, want)
	}
	bare, err := Resolve("~")
	if err != nil {
		t.Fatal(err)
	}
	if bare.ConfigDir != home {
		t.Fatalf("bare ~ not expanded: got %s want %s", bare.ConfigDir, home)
	}
}
