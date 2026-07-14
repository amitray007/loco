package paths

import (
	"os"
	"path/filepath"
	"testing"
)

func TestResolveUsesExplicitConfigDirectory(t *testing.T) {
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
	// The service unit is the OS-appropriate file (.plist on darwin, .service on linux).
	if filepath.Base(p.ServiceUnit) != serviceUnitName(Label) {
		t.Fatalf("ServiceUnit = %q, want basename %q", p.ServiceUnit, serviceUnitName(Label))
	}
}

// B3: LOCO_CONFIG_DIR selects the config dir when no --config-dir is given.
func TestResolveHonorsLocoConfigDirEnv(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "envcfg")
	t.Setenv("LOCO_CONFIG_DIR", dir)
	p, err := Resolve("")
	if err != nil {
		t.Fatal(err)
	}
	if p.ConfigDir != dir {
		t.Fatalf("ConfigDir = %q, want %q from LOCO_CONFIG_DIR", p.ConfigDir, dir)
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
