package paths

import (
	"fmt"
	"os"
	"path/filepath"
)

const LaunchdLabel = "com.amitray007.loco"
const WatchdogLaunchdLabel = "com.amitray007.loco.watchdog"

// Paths contains every filesystem location needed by the daemon milestone.
// ConfigDir can be overridden by --config-dir so tests and alternate installs
// never have to touch the user's real state.
type Paths struct {
	ConfigDir     string
	RunDir        string
	LogDir        string
	RunsDir       string
	EventsDir     string
	Alerts        string
	NotifyState   string
	WatchdogState string
	Socket        string
	Lock          string
	State         string
	Registry      string
	Plist         string
	WatchdogPlist string
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
		ConfigDir:     configDir,
		RunDir:        runDir,
		LogDir:        filepath.Join(configDir, "logs"),
		RunsDir:       filepath.Join(configDir, "runs"),
		EventsDir:     filepath.Join(configDir, "events"),
		Alerts:        filepath.Join(configDir, "alerts.jsonl"),
		NotifyState:   filepath.Join(configDir, "notify.json"),
		WatchdogState: filepath.Join(configDir, "watchdog.json"),
		Socket:        filepath.Join(runDir, "daemon.sock"),
		Lock:          filepath.Join(runDir, "daemon.lock"),
		State:         filepath.Join(runDir, "daemon.json"),
		Registry:      filepath.Join(configDir, "registry.json"),
		Plist:         filepath.Join(launchAgentsDir, LaunchdLabel+".plist"),
		WatchdogPlist: filepath.Join(launchAgentsDir, WatchdogLaunchdLabel+".plist"),
	}, nil
}

func (p Paths) EnsureRuntimeDirs() error {
	for _, dir := range []string{p.ConfigDir, p.RunDir, p.LogDir, p.RunsDir, p.EventsDir} {
		if err := os.MkdirAll(dir, 0o700); err != nil {
			return fmt.Errorf("create %s: %w", dir, err)
		}
	}
	return nil
}
