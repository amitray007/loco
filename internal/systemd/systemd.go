//go:build linux

// Package systemd installs loco's daemon and watchdog as systemd user units on Linux,
// the counterpart to the launchd backend on macOS. The daemon unit uses Restart=always
// (the equivalent of launchd KeepAlive); the watchdog runs on a timer.
package systemd

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/amitray007/loco/internal/paths"
)

type Runner interface {
	Run(ctx context.Context, name string, args ...string) ([]byte, error)
}

type OSRunner struct{}

func (OSRunner) Run(ctx context.Context, name string, args ...string) ([]byte, error) {
	return exec.CommandContext(ctx, name, args...).CombinedOutput()
}

// Install writes the daemon service, the watchdog service, and the watchdog timer, then
// enables and starts them via systemctl --user. Idempotent.
func Install(ctx context.Context, runner Runner, p paths.Paths, executable string) (string, error) {
	if err := p.EnsureRuntimeDirs(); err != nil {
		return "", err
	}
	unitDir := filepath.Dir(p.ServiceUnit)
	if err := os.MkdirAll(unitDir, 0o755); err != nil {
		return "", fmt.Errorf("create systemd user unit directory: %w", err)
	}
	executable, err := filepath.Abs(executable)
	if err != nil {
		return "", fmt.Errorf("make executable path absolute: %w", err)
	}

	timerPath := timerPathFor(p.WatchdogUnit)
	units := []struct {
		path    string
		content []byte
	}{
		{p.ServiceUnit, RenderDaemonUnit(executable, p)},
		{p.WatchdogUnit, RenderWatchdogUnit(executable, p)},
		{timerPath, RenderWatchdogTimer()},
	}
	changed := false
	for _, u := range units {
		c, err := writeUnit(u.path, u.content)
		if err != nil {
			return "", err
		}
		changed = changed || c
	}

	// Reload so systemd picks up new/changed unit files before we enable them.
	if out, err := runner.Run(ctx, "systemctl", "--user", "daemon-reload"); err != nil {
		return "", commandError("systemctl daemon-reload", out, err)
	}
	// Enable+start the daemon and the watchdog timer. --now enables and starts in one step.
	if out, err := runner.Run(ctx, "systemctl", "--user", "enable", "--now", filepath.Base(p.ServiceUnit)); err != nil {
		return "", commandError("enable loco daemon", out, err)
	}
	if out, err := runner.Run(ctx, "systemctl", "--user", "enable", "--now", filepath.Base(timerPath)); err != nil {
		return "", commandError("enable loco watchdog timer", out, err)
	}

	if changed {
		return "installed", nil
	}
	return "already installed", nil
}

// Uninstall stops and disables the units and removes their files.
func Uninstall(ctx context.Context, runner Runner, p paths.Paths) (bool, error) {
	timerPath := timerPathFor(p.WatchdogUnit)
	removedAny := false
	// Best-effort disable+stop; ignore "not loaded" errors from systemctl.
	for _, base := range []string{filepath.Base(timerPath), filepath.Base(p.WatchdogUnit), filepath.Base(p.ServiceUnit)} {
		_, _ = runner.Run(ctx, "systemctl", "--user", "disable", "--now", base)
	}
	for _, path := range []string{timerPath, p.WatchdogUnit, p.ServiceUnit} {
		if err := os.Remove(path); err == nil {
			removedAny = true
		} else if !errors.Is(err, os.ErrNotExist) {
			return false, err
		}
	}
	if out, err := runner.Run(ctx, "systemctl", "--user", "daemon-reload"); err != nil {
		return removedAny, commandError("systemctl daemon-reload", out, err)
	}
	return removedAny, nil
}

func writeUnit(path string, content []byte) (bool, error) {
	current, readErr := os.ReadFile(path)
	if readErr != nil && !errors.Is(readErr, os.ErrNotExist) {
		return false, fmt.Errorf("read existing unit: %w", readErr)
	}
	if readErr == nil && bytes.Equal(current, content) {
		return false, nil
	}
	if err := writeAtomic(path, content, 0o644); err != nil {
		return false, fmt.Errorf("write unit %s: %w", path, err)
	}
	return true, nil
}

// RenderDaemonUnit renders the systemd user service for the resident daemon. Restart=always
// is the systemd equivalent of launchd's KeepAlive.
func RenderDaemonUnit(executable string, p paths.Paths) []byte {
	return []byte(fmt.Sprintf(`[Unit]
Description=loco local operator daemon
After=default.target

[Service]
Type=simple
ExecStart=%s --config-dir %s daemon run
Restart=always
RestartSec=2
StandardOutput=append:%s
StandardError=append:%s

[Install]
WantedBy=default.target
`,
		systemdQuote(executable), systemdQuote(p.ConfigDir),
		filepath.Join(p.LogDir, "daemon.stdout.log"),
		filepath.Join(p.LogDir, "daemon.stderr.log")))
}

// RenderWatchdogUnit renders the oneshot service the timer invokes.
func RenderWatchdogUnit(executable string, p paths.Paths) []byte {
	return []byte(fmt.Sprintf(`[Unit]
Description=loco daemon liveness watchdog

[Service]
Type=oneshot
ExecStart=%s --config-dir %s daemon watchdog --threshold 3
StandardOutput=append:%s
StandardError=append:%s
`,
		systemdQuote(executable), systemdQuote(p.ConfigDir),
		filepath.Join(p.LogDir, "watchdog.stdout.log"),
		filepath.Join(p.LogDir, "watchdog.stderr.log")))
}

// RenderWatchdogTimer runs the watchdog every 60s, matching the launchd StartInterval.
func RenderWatchdogTimer() []byte {
	return []byte(`[Unit]
Description=loco daemon watchdog timer

[Timer]
OnBootSec=60
OnUnitActiveSec=60
AccuracySec=10

[Install]
WantedBy=timers.target
`)
}

// timerPathFor derives the .timer path that pairs with a watchdog .service unit.
func timerPathFor(watchdogUnit string) string {
	return strings.TrimSuffix(watchdogUnit, ".service") + ".timer"
}

// systemdQuote escapes a path for use in an ExecStart line. systemd splits on
// whitespace, so wrap paths containing spaces in double quotes.
func systemdQuote(value string) string {
	if strings.ContainsAny(value, " \t") {
		return `"` + strings.ReplaceAll(value, `"`, `\"`) + `"`
	}
	return value
}

func commandError(action string, output []byte, err error) error {
	detail := strings.TrimSpace(string(output))
	if detail == "" {
		return fmt.Errorf("%s: %w", action, err)
	}
	return fmt.Errorf("%s: %w: %s", action, err, detail)
}

func writeAtomic(path string, data []byte, mode os.FileMode) error {
	tmp, err := os.CreateTemp(filepath.Dir(path), ".loco-unit-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	if err := tmp.Chmod(mode); err != nil {
		_ = tmp.Close()
		return err
	}
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmpName, path)
}
