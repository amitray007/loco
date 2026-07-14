//go:build linux

package paths

import (
	"os"
	"path/filepath"
)

// serviceUnitDir is where systemd user units live. LOCO_SYSTEMD_UNIT_DIR overrides it
// so tests never touch the real ~/.config/systemd/user.
func serviceUnitDir(home string) (string, error) {
	if dir := os.Getenv("LOCO_SYSTEMD_UNIT_DIR"); dir != "" {
		return dir, nil
	}
	if xdg := os.Getenv("XDG_CONFIG_HOME"); xdg != "" {
		return filepath.Join(xdg, "systemd", "user"), nil
	}
	return filepath.Join(home, ".config", "systemd", "user"), nil
}

// serviceUnitName maps a reverse-DNS label to its systemd unit filename. systemd unit
// names conventionally avoid the reverse-DNS dotted form, so we keep the label as the
// basename with a .service suffix (dots are legal in unit names).
func serviceUnitName(label string) string { return label + ".service" }
