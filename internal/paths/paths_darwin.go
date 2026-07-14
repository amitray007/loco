//go:build darwin

package paths

import (
	"os"
	"path/filepath"
)

// serviceUnitDir is where launchd LaunchAgent plists live. LOCO_LAUNCH_AGENTS_DIR
// overrides it so tests never touch the real ~/Library/LaunchAgents.
func serviceUnitDir(home string) (string, error) {
	if dir := os.Getenv("LOCO_LAUNCH_AGENTS_DIR"); dir != "" {
		return dir, nil
	}
	return filepath.Join(home, "Library", "LaunchAgents"), nil
}

// serviceUnitName maps a reverse-DNS label to its launchd plist filename.
func serviceUnitName(label string) string { return label + ".plist" }
