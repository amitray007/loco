//go:build darwin

package platform

import (
	"context"

	"github.com/amitray007/loco/internal/launchd"
	"github.com/amitray007/loco/internal/paths"
)

const supported = true

type launchdManager struct{}

func (launchdManager) Name() string { return "launchd" }

func (launchdManager) Install(ctx context.Context, p paths.Paths, executable string) (string, error) {
	result, err := launchd.Install(ctx, launchd.OSRunner{}, p, executable)
	if err != nil {
		return "", err
	}
	return result.Action, nil
}

func (launchdManager) Uninstall(ctx context.Context, p paths.Paths) (bool, error) {
	return launchd.Uninstall(ctx, launchd.OSRunner{}, p)
}

// Manager returns the macOS launchd service manager.
func Manager() ServiceManager { return launchdManager{} }
