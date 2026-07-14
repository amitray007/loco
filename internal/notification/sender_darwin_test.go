//go:build darwin

package notification

import (
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestAppleScriptEscaping(t *testing.T) {
	got := escapeAppleScript("a \\\"quoted\\\"\nline")
	if strings.Contains(got, "\n") || !strings.Contains(got, `\\\"quoted\\\"`) {
		t.Fatalf("escaped=%q", got)
	}
}

func TestNotificationAppleScriptCompiles(t *testing.T) {
	if _, err := exec.LookPath("osacompile"); err != nil {
		t.Skip("osacompile unavailable")
	}
	output := filepath.Join(t.TempDir(), "notification.scpt")
	if data, err := exec.Command("osacompile", "-o", output, "-e", appleScript(`Title "quoted"`, `Body \\ text`)).CombinedOutput(); err != nil {
		t.Fatalf("compile: %v: %s", err, data)
	}
}

// MacSender is the desktop sender on darwin.
func TestDesktopSenderIsMacSender(t *testing.T) {
	if _, ok := DesktopSender().(MacSender); !ok {
		t.Fatalf("darwin DesktopSender = %T, want MacSender", DesktopSender())
	}
}
