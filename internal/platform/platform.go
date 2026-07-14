// Package platform is the OS-abstraction seam for loco's host integration: installing
// the daemon as an OS-managed service and delivering desktop notifications. The rest of
// loco depends only on these interfaces; the concrete backends (launchd/osascript on
// macOS, systemd/notify-send on Linux) are selected by build tags.
package platform

import (
	"context"

	"github.com/amitray007/loco/internal/paths"
)

// ServiceManager installs and removes loco's daemon + watchdog as OS-managed services
// so they survive logout/reboot and are restarted on crash.
type ServiceManager interface {
	// Install writes the service unit(s) for the given executable and starts them.
	// It is idempotent. Returns a short human action word ("installed" / "already
	// installed").
	Install(ctx context.Context, p paths.Paths, executable string) (string, error)
	// Uninstall stops and removes the service unit(s). Returns whether anything was
	// present to remove.
	Uninstall(ctx context.Context, p paths.Paths) (bool, error)
	// Name identifies the backend (e.g. "launchd", "systemd") for diagnostics.
	Name() string
}

// Desktop notification is handled by the notification package (which owns its own
// OS-specific senders); platform is concerned only with service management.

// Supported reports whether this OS has a real daemon backend. On unsupported systems
// the CLI still runs (events/doctor/etc. over an already-running daemon) but
// install/run are refused with a clear message.
func Supported() bool { return supported }
