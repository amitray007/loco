//go:build darwin

package launchd

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
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

type InstallResult struct {
	Action string
}

func Install(ctx context.Context, runner Runner, p paths.Paths, executable string) (InstallResult, error) {
	if err := p.EnsureRuntimeDirs(); err != nil {
		return InstallResult{}, err
	}
	if err := os.MkdirAll(filepath.Dir(p.Plist()), 0o755); err != nil {
		return InstallResult{}, fmt.Errorf("create LaunchAgents directory: %w", err)
	}
	executable, err := filepath.Abs(executable)
	if err != nil {
		return InstallResult{}, fmt.Errorf("make executable path absolute: %w", err)
	}

	domain := "gui/" + strconv.Itoa(os.Getuid())
	changed, err := installOne(ctx, runner, domain, paths.LaunchdLabel, p.Plist(), RenderPlist(executable, p))
	if err != nil {
		return InstallResult{}, err
	}
	watchdogChanged, err := installOne(ctx, runner, domain, paths.WatchdogLaunchdLabel, p.WatchdogPlist(), RenderWatchdogPlist(executable, p))
	if err != nil {
		return InstallResult{}, err
	}
	action := "already installed"
	if changed || watchdogChanged {
		action = "installed"
	}
	return InstallResult{Action: action}, nil
}

func Uninstall(ctx context.Context, runner Runner, p paths.Paths) (bool, error) {
	domain := "gui/" + strconv.Itoa(os.Getuid())
	removed := false
	for _, item := range []struct{ label, path string }{{paths.WatchdogLaunchdLabel, p.WatchdogPlist()}, {paths.LaunchdLabel, p.Plist()}} {
		exists, err := uninstallOne(ctx, runner, domain, item.label, item.path)
		if err != nil {
			return false, err
		}
		removed = removed || exists
	}
	return removed, nil
}

func installOne(ctx context.Context, runner Runner, domain, label, plistPath string, desired []byte) (bool, error) {
	current, readErr := os.ReadFile(plistPath)
	changed := readErr != nil || !bytes.Equal(current, desired)
	if readErr != nil && !errors.Is(readErr, os.ErrNotExist) {
		return false, fmt.Errorf("read existing plist: %w", readErr)
	}
	if changed {
		if err := writeAtomic(plistPath, desired, 0o644); err != nil {
			return false, fmt.Errorf("write plist: %w", err)
		}
	}
	service := domain + "/" + label
	loaded := commandOK(ctx, runner, "launchctl", "print", service)
	wasLoaded := loaded
	if loaded && changed {
		if out, err := runner.Run(ctx, "launchctl", "bootout", service); err != nil {
			return false, commandError("bootout existing service", out, err)
		}
		loaded = false
	}
	if !loaded {
		if out, err := runner.Run(ctx, "launchctl", "bootstrap", domain, plistPath); err != nil {
			return false, commandError("bootstrap service", out, err)
		}
	}
	if out, err := runner.Run(ctx, "launchctl", "kickstart", service); err != nil {
		return false, commandError("start service", out, err)
	}
	return changed || !wasLoaded, nil
}
func uninstallOne(ctx context.Context, runner Runner, domain, label, plistPath string) (bool, error) {
	_, err := os.Stat(plistPath)
	exists := err == nil
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return false, err
	}
	service := domain + "/" + label
	loaded := commandOK(ctx, runner, "launchctl", "print", service)
	if loaded {
		if out, err := runner.Run(ctx, "launchctl", "bootout", service); err != nil {
			return false, commandError("bootout service", out, err)
		}
	}
	if err := os.Remove(plistPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		return false, err
	}
	return exists || loaded, nil
}

func RenderPlist(executable string, p paths.Paths) []byte {
	values := map[string]string{
		"executable": executable,
		"config":     p.ConfigDir,
		"stdout":     filepath.Join(p.LogDir, "daemon.stdout.log"),
		"stderr":     filepath.Join(p.LogDir, "daemon.stderr.log"),
	}
	escape := func(value string) string {
		var b strings.Builder
		for _, r := range value {
			switch r {
			case '&':
				b.WriteString("&amp;")
			case '<':
				b.WriteString("&lt;")
			case '>':
				b.WriteString("&gt;")
			default:
				b.WriteRune(r)
			}
		}
		return b.String()
	}
	return []byte(fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key>
  <string>%s</string>
  <key>ProgramArguments</key>
  <array>
    <string>%s</string>
    <string>--config-dir</string>
    <string>%s</string>
    <string>daemon</string>
    <string>run</string>
  </array>
  <key>KeepAlive</key>
  <true/>
  <key>RunAtLoad</key>
  <true/>
  <key>ProcessType</key>
  <string>Background</string>
  <key>StandardOutPath</key>
  <string>%s</string>
  <key>StandardErrorPath</key>
  <string>%s</string>
</dict>
</plist>
`, paths.LaunchdLabel, escape(values["executable"]), escape(values["config"]), escape(values["stdout"]), escape(values["stderr"])))
}

func RenderWatchdogPlist(executable string, p paths.Paths) []byte {
	escape := func(value string) string {
		var b strings.Builder
		for _, r := range value {
			switch r {
			case '&':
				b.WriteString("&amp;")
			case '<':
				b.WriteString("&lt;")
			case '>':
				b.WriteString("&gt;")
			default:
				b.WriteRune(r)
			}
		}
		return b.String()
	}
	return []byte(fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key><string>%s</string>
  <key>ProgramArguments</key>
  <array><string>%s</string><string>--config-dir</string><string>%s</string><string>daemon</string><string>watchdog</string><string>--threshold</string><string>3</string></array>
  <key>StartInterval</key><integer>60</integer>
  <key>RunAtLoad</key><true/>
  <key>ProcessType</key><string>Background</string>
  <key>StandardOutPath</key><string>%s</string>
  <key>StandardErrorPath</key><string>%s</string>
</dict>
</plist>
`, paths.WatchdogLaunchdLabel, escape(executable), escape(p.ConfigDir), escape(filepath.Join(p.LogDir, "watchdog.stdout.log")), escape(filepath.Join(p.LogDir, "watchdog.stderr.log"))))
}

func commandOK(ctx context.Context, runner Runner, name string, args ...string) bool {
	_, err := runner.Run(ctx, name, args...)
	return err == nil
}

func commandError(action string, output []byte, err error) error {
	detail := strings.TrimSpace(string(output))
	if detail == "" {
		return fmt.Errorf("%s: %w", action, err)
	}
	return fmt.Errorf("%s: %w: %s", action, err, detail)
}

func writeAtomic(path string, data []byte, mode os.FileMode) error {
	tmp, err := os.CreateTemp(filepath.Dir(path), ".loco-plist-*")
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
