//go:build darwin

package notification

import (
	"context"
	"fmt"
	"os/exec"
	"strings"
)

// MacSender delivers desktop notifications via osascript on macOS.
type MacSender struct{}

func (MacSender) Send(ctx context.Context, title, body string) error {
	script := appleScript(title, body)
	output, err := exec.CommandContext(ctx, "osascript", "-e", script).CombinedOutput()
	if err != nil {
		return fmt.Errorf("osascript: %w: %s", err, strings.TrimSpace(string(output)))
	}
	return nil
}

// DesktopSender returns the macOS desktop notifier.
func DesktopSender() Sender { return MacSender{} }

func appleScript(title, body string) string {
	return fmt.Sprintf(`display notification "%s" with title "%s"`, escapeAppleScript(body), escapeAppleScript(title))
}

func escapeAppleScript(value string) string {
	value = strings.ReplaceAll(value, "\\", "\\\\")
	value = strings.ReplaceAll(value, "\"", "\\\"")
	value = strings.ReplaceAll(value, "\r", " ")
	value = strings.ReplaceAll(value, "\n", " ")
	return value
}
