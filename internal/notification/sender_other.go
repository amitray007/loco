//go:build !darwin && !linux

package notification

import (
	"context"
	"fmt"
	"runtime"
)

// unsupportedSender is used on platforms without a desktop notifier. The file channel
// still works; only the desktop channel errors.
type unsupportedSender struct{}

func (unsupportedSender) Send(context.Context, string, string) error {
	return fmt.Errorf("desktop notifications are not supported on %s; use --channel file", runtime.GOOS)
}

// DesktopSender returns a sender that reports desktop notifications are unavailable.
func DesktopSender() Sender { return unsupportedSender{} }
