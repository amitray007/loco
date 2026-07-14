package notification

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

type fakeSender struct {
	calls int
	err   error
}

func (f *fakeSender) Send(context.Context, string, string) error { f.calls++; return f.err }

func TestGroupedNotificationIsDurablySuppressed(t *testing.T) {
	root := t.TempDir()
	state := filepath.Join(root, "notify.json")
	alerts := filepath.Join(root, "alerts.jsonl")
	sender := &fakeSender{}
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	manager := New(state, alerts, sender)
	manager.now = func() time.Time { return now }
	manager.id = func() string { return "first" }
	first, err := manager.Send(context.Background(), Input{Title: "failure", Body: "worker exited", Group: "worker", Channel: "macos"})
	if err != nil {
		t.Fatal(err)
	}
	if !first.Delivered || sender.calls != 1 {
		t.Fatalf("first=%+v calls=%d", first, sender.calls)
	}
	reopened := New(state, alerts, sender)
	reopened.now = func() time.Time { return now.Add(30 * time.Minute) }
	second, err := reopened.Send(context.Background(), Input{Title: "failure", Body: "again", Group: "worker", Channel: "macos"})
	if err != nil {
		t.Fatal(err)
	}
	if !second.Suppressed || sender.calls != 1 {
		t.Fatalf("second=%+v calls=%d", second, sender.calls)
	}
	reopened.now = func() time.Time { return now.Add(61 * time.Minute) }
	reopened.id = func() string { return "third" }
	third, err := reopened.Send(context.Background(), Input{Title: "failure", Body: "later", Group: "worker", Channel: "macos"})
	if err != nil {
		t.Fatal(err)
	}
	if !third.Delivered || sender.calls != 2 {
		t.Fatalf("third=%+v calls=%d", third, sender.calls)
	}
	data, err := os.ReadFile(alerts)
	if err != nil {
		t.Fatal(err)
	}
	if lines := strings.Count(string(data), "\n"); lines != 2 {
		t.Fatalf("alert lines=%d", lines)
	}
}
func TestFileChannelNeedsNoSender(t *testing.T) {
	root := t.TempDir()
	manager := New(filepath.Join(root, "state"), filepath.Join(root, "alerts"), nil)
	result, err := manager.Send(context.Background(), Input{Title: "hello", Body: "world", Channel: "file"})
	if err != nil {
		t.Fatal(err)
	}
	if !result.Delivered {
		t.Fatalf("result=%+v", result)
	}
}
func TestFailedDeliveryDoesNotConsumeGroup(t *testing.T) {
	root := t.TempDir()
	sender := &fakeSender{err: errors.New("nope")}
	manager := New(filepath.Join(root, "state"), filepath.Join(root, "alerts"), sender)
	if _, err := manager.Send(context.Background(), Input{Title: "x", Body: "y", Group: "g"}); err == nil {
		t.Fatal("send succeeded")
	}
	sender.err = nil
	if result, err := manager.Send(context.Background(), Input{Title: "x", Body: "y", Group: "g"}); err != nil || !result.Delivered {
		t.Fatalf("result=%+v err=%v", result, err)
	}
}

// F-05: SenderForChannel("file") writes to the alerts log instead of shelling out to a
// desktop notifier, so the watchdog can run headless without posting real banners.
func TestFileSenderWritesToAlertsNotDesktop(t *testing.T) {
	alerts := filepath.Join(t.TempDir(), "alerts.jsonl")
	sender := SenderForChannel("file", alerts)
	if _, ok := sender.(FileSender); !ok {
		t.Fatalf("file channel returned %T, want FileSender", sender)
	}
	if err := sender.Send(context.Background(), "title", "body"); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(alerts)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(data), `"title":"title"`) || !strings.Contains(string(data), `"channel":"file"`) {
		t.Fatalf("alert not written to file channel: %s", data)
	}
	// The default channel must NOT be the file sender (it's the OS desktop sender).
	if _, ok := SenderForChannel("desktop", alerts).(FileSender); ok {
		t.Fatal("desktop channel should not be the file sender")
	}
}
