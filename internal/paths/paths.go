package paths

import (
	"fmt"
	"os"
	"path/filepath"
)

const LaunchdLabel = "com.amitray007.loco"

// Paths contains every filesystem location needed by the daemon milestone.
// ConfigDir can be overridden by --config-dir so tests and alternate installs
// never have to touch the user's real state.
type Paths struct {
	ConfigDir string
	RunDir    string
	LogDir    string
	Socket    string
	Lock      string
	State     string
	Plist     string
}

func Resolve(configOverride string) (Paths, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return Paths{}, fmt.Errorf("find home directory: %w", err)
	}

	configDir := configOverride
	if configDir == "" {
		if xdg := os.Getenv("XDG_CONFIG_HOME"); xdg != "" {
			configDir = filepath.Join(xdg, "loco")
		} else {
			configDir = filepath.Join(home, ".config", "loco")
		}
	}
	configDir, err = filepath.Abs(configDir)
	if err != nil {
		return Paths{}, fmt.Errorf("make config path absolute: %w", err)
	}

	launchAgentsDir := os.Getenv("LOCO_LAUNCH_AGENTS_DIR")
	if launchAgentsDir == "" {
		launchAgentsDir = filepath.Join(home, "Library", "LaunchAgents")
	}
	runDir := filepath.Join(configDir, "run")
	return Paths{
		ConfigDir: configDir,
		RunDir:    runDir,
		LogDir:    filepath.Join(configDir, "logs"),
		Socket:    filepath.Join(runDir, "daemon.sock"),
		Lock:      filepath.Join(runDir, "daemon.lock"),
		State:     filepath.Join(runDir, "daemon.json"),
		Plist:     filepath.Join(launchAgentsDir, LaunchdLabel+".plist"),
	}, nil
}

func (p Paths) EnsureRuntimeDirs() error {
	for _, dir := range []string{p.ConfigDir, p.RunDir, p.LogDir} {
		if err := os.MkdirAll(dir, 0o700); err != nil {
			return fmt.Errorf("create %s: %w", dir, err)
		}
	}
	return nil
}
