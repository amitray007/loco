//go:build linux

package platform

import (
	"context"

	"github.com/amitray007/loco/internal/paths"
	"github.com/amitray007/loco/internal/systemd"
)

const supported = true

type systemdManager struct{}

func (systemdManager) Name() string { return "systemd" }

func (systemdManager) Install(ctx context.Context, p paths.Paths, executable string) (string, error) {
	return systemd.Install(ctx, systemd.OSRunner{}, p, executable)
}

func (systemdManager) Uninstall(ctx context.Context, p paths.Paths) (bool, error) {
	return systemd.Uninstall(ctx, systemd.OSRunner{}, p)
}

// Manager returns the Linux systemd (user units) service manager.
func Manager() ServiceManager { return systemdManager{} }

// RestartHint returns the command that restarts the daemon on this OS. `loco daemon`
// has no restart verb — the service is systemd-managed — so a reader told only to
// "restart the daemon" has to go find this out.
func RestartHint() string {
	return "systemctl --user restart " + paths.Label + ".service"
}
