package paths

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// Label is the reverse-DNS identifier used for the OS service unit (launchd label on
// macOS, systemd unit basename on Linux).
const Label = "dev.loco.daemon"
const WatchdogLabel = "dev.loco.watchdog"

// LaunchdLabel/WatchdogLaunchdLabel are retained as aliases for the macOS launchd
// backend, which references them directly.
const LaunchdLabel = Label
const WatchdogLaunchdLabel = WatchdogLabel

// Paths contains every filesystem location needed by the daemon. ConfigDir can be
// overridden by --config-dir (or LOCO_CONFIG_DIR) so tests and alternate installs never
// have to touch the user's real state. ServiceUnit/WatchdogUnit are the OS-appropriate
// service definition files (launchd .plist on macOS, systemd .service on Linux).
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
	ServiceUnit   string
	WatchdogUnit  string
}

// Plist and WatchdogPlist are backward-compatible accessors for the macOS launchd
// backend, which speaks in terms of plists.
func (p Paths) Plist() string         { return p.ServiceUnit }
func (p Paths) WatchdogPlist() string { return p.WatchdogUnit }

func Resolve(configOverride string) (Paths, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return Paths{}, fmt.Errorf("find home directory: %w", err)
	}

	configDir := configOverride
	if configDir == "" {
		// Explicit LOCO_CONFIG_DIR wins over XDG so callers (and wok's pr-watch, which
		// sets it) can pin the config dir without a flag.
		if env := os.Getenv("LOCO_CONFIG_DIR"); env != "" {
			configDir = env
		} else if xdg := os.Getenv("XDG_CONFIG_HOME"); xdg != "" {
			configDir = filepath.Join(xdg, "loco")
		} else {
			configDir = filepath.Join(home, ".config", "loco")
		}
	}
	if configDir == "~" {
		configDir = home
	} else if strings.HasPrefix(configDir, "~/") {
		// Expand a leading ~ so a quoted or unexpanded tilde does not create a stray
		// directory literally named "~" under the current directory.
		configDir = filepath.Join(home, configDir[2:])
	}
	// Reject control characters up front so every command fails fast and consistently,
	// rather than materializing dirs / a socket path with an embedded newline.
	if strings.ContainsAny(configDir, "\n\r\x00") {
		return Paths{}, fmt.Errorf("config dir contains a control character: %q", configDir)
	}
	configDir, err = filepath.Abs(configDir)
	if err != nil {
		return Paths{}, fmt.Errorf("make config path absolute: %w", err)
	}

	unitDir, err := serviceUnitDir(home)
	if err != nil {
		return Paths{}, err
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
		ServiceUnit:   filepath.Join(unitDir, serviceUnitName(Label)),
		WatchdogUnit:  filepath.Join(unitDir, serviceUnitName(WatchdogLabel)),
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
