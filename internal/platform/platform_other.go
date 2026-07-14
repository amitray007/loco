//go:build !darwin && !linux

package platform

import (
	"context"
	"fmt"
	"runtime"

	"github.com/amitray007/loco/internal/paths"
)

const supported = false

type unsupportedManager struct{}

func (unsupportedManager) Name() string { return runtime.GOOS + " (unsupported)" }

func (unsupportedManager) Install(context.Context, paths.Paths, string) (string, error) {
	return "", fmt.Errorf("loco daemon install is not supported on %s (macOS and Linux only)", runtime.GOOS)
}

func (unsupportedManager) Uninstall(context.Context, paths.Paths) (bool, error) {
	return false, fmt.Errorf("loco daemon uninstall is not supported on %s", runtime.GOOS)
}

func Manager() ServiceManager { return unsupportedManager{} }
