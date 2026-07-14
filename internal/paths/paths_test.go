package paths

import (
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
