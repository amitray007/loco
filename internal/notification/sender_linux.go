//go:build linux

package notification

import (
	"context"
	"fmt"
	"os/exec"
	"strings"
)

// NotifySendSender delivers desktop notifications via notify-send (libnotify) on Linux.
type NotifySendSender struct{}

func (NotifySendSender) Send(ctx context.Context, title, body string) error {
	output, err := exec.CommandContext(ctx, "notify-send", title, body).CombinedOutput()
	if err != nil {
		return fmt.Errorf("notify-send: %w: %s", err, strings.TrimSpace(string(output)))
	}
	return nil
}

// DesktopSender returns the Linux desktop notifier.
func DesktopSender() Sender { return NotifySendSender{} }
