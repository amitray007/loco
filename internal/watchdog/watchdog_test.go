package watchdog

import (
	"context"
	"errors"
	"github.com/amitray007/loco/internal/notification"
	"os"
	"path/filepath"
	"testing"
)

type sender struct{ calls int }

func (s *sender) Send(context.Context, string, string) error { s.calls++; return nil }
func TestAlertsOnceAfterConsecutiveFailuresAndResetsOnHealth(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	send := &sender{}
	failure := func(context.Context) error { return errors.New("down") }
	for i := 1; i <= 4; i++ {
		result, err := Check(context.Background(), path, 3, failure, send)
		if err != nil {
			t.Fatal(err)
		}
		if result.Failures != i {
			t.Fatalf("failures=%d", result.Failures)
		}
		if (i == 3) != result.AlertSent {
			t.Fatalf("iteration %d result=%+v", i, result)
		}
	}
	if send.calls != 1 {
		t.Fatalf("calls=%d", send.calls)
	}
	healthy := func(context.Context) error { return nil }
	result, err := Check(context.Background(), path, 3, healthy, send)
	if err != nil {
		t.Fatal(err)
	}
	if !result.Healthy || result.Failures != 0 || result.Alerted {
		t.Fatalf("healthy=%+v", result)
	}
	for i := 0; i < 3; i++ {
		if _, err := Check(context.Background(), path, 3, failure, send); err != nil {
			t.Fatal(err)
		}
	}
	if send.calls != 2 {
		t.Fatalf("calls after reset=%d", send.calls)
	}
}
func TestThresholdAndSenderErrors(t *testing.T) {
	checker := func(context.Context) error { return errors.New("down") }
	if _, err := Check(context.Background(), filepath.Join(t.TempDir(), "x"), 0, checker, &sender{}); err == nil {
		t.Fatal("zero threshold accepted")
	}
	if _, err := Check(context.Background(), filepath.Join(t.TempDir(), "y"), 1, checker, nil); err == nil {
		t.Fatal("missing sender accepted")
	}
}

var _ notification.Sender = (*sender)(nil)

// F-08: a corrupt or schema-mismatched state file must self-heal, not permanently
// disable the watchdog.
func TestCorruptStateSelfHeals(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	if err := os.WriteFile(path, []byte("{not valid json"), 0o600); err != nil {
		t.Fatal(err)
	}
	send := &sender{}
	healthy := func(context.Context) error { return nil }
	result, err := Check(context.Background(), path, 3, healthy, send)
	if err != nil {
		t.Fatalf("corrupt state should self-heal, got error: %v", err)
	}
	if !result.Healthy {
		t.Fatalf("expected healthy after self-heal, got %+v", result)
	}

	// Schema mismatch (future version) must also reset, not error.
	if err := os.WriteFile(path, []byte(`{"schema_version":999,"failures":5}`), 0o600); err != nil {
		t.Fatal(err)
	}
	failure := func(context.Context) error { return errors.New("down") }
	result, err = Check(context.Background(), path, 3, failure, send)
	if err != nil {
		t.Fatalf("schema mismatch should self-heal, got error: %v", err)
	}
	// Counter must have reset to 1, not resumed at the bogus 5.
	if result.Failures != 1 {
		t.Fatalf("expected failures reset to 1 after schema mismatch, got %d", result.Failures)
	}
}
