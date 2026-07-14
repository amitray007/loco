package daemon

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/amitray007/loco/internal/paths"
	"github.com/amitray007/loco/internal/registry"
)

func TestRunServesHealthAndCleansUp(t *testing.T) {
	p := testPaths(t)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- Run(ctx, p, "test-version") }()

	status := waitForStatus(t, p.Socket)
	if !status.Healthy {
		t.Fatal("daemon was not healthy")
	}
	if status.PID != os.Getpid() {
		t.Fatalf("PID = %d, want %d", status.PID, os.Getpid())
	}
	if status.Version != "test-version" {
		t.Fatalf("Version = %q", status.Version)
	}
	if status.StartedAt == "" {
		t.Fatal("StartedAt is empty")
	}
	if _, err := os.Stat(p.State); err != nil {
		t.Fatalf("state file: %v", err)
	}

	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Run returned %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("daemon did not stop")
	}
	if _, err := os.Stat(p.Socket); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("socket remains after shutdown: %v", err)
	}
	if _, err := os.Stat(p.State); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("state remains after shutdown: %v", err)
	}
}

func TestRunRejectsSecondDaemon(t *testing.T) {
	p := testPaths(t)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- Run(ctx, p, "first") }()
	waitForStatus(t, p.Socket)

	err := Run(context.Background(), p, "second")
	if !errors.Is(err, ErrAlreadyRunning) {
		t.Fatalf("second Run error = %v, want ErrAlreadyRunning", err)
	}
	cancel()
	<-done
}

func TestRegistrySurvivesDaemonRestart(t *testing.T) {
	p := testPaths(t)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- Run(ctx, p, "first") }()
	waitForStatus(t, p.Socket)

	put, err := RegistryPut(context.Background(), p.Socket, time.Second, registry.PutInput{
		Name: "restart.demo",
		Kind: "cron",
		Spec: json.RawMessage(`{"schedule":"*/5 * * * *"}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	if !put.Changed || put.Revision != 1 {
		t.Fatalf("put = %+v", put)
	}
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("first daemon returned %v", err)
	}

	secondCtx, secondCancel := context.WithCancel(context.Background())
	secondDone := make(chan error, 1)
	go func() { secondDone <- Run(secondCtx, p, "second") }()
	waitForStatus(t, p.Socket)
	document, err := RegistryList(context.Background(), p.Socket, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	job, ok := document.Jobs["restart.demo"]
	if !ok || job.Kind != "cron" || document.Revision != 1 {
		t.Fatalf("reloaded registry = %+v", document)
	}
	secondCancel()
	if err := <-secondDone; !errors.Is(err, context.Canceled) {
		t.Fatalf("second daemon returned %v", err)
	}
}

func testPaths(t *testing.T) paths.Paths {
	t.Helper()
	root, err := os.MkdirTemp("/tmp", "loco-daemon-test-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(root) })
	return paths.Paths{
		ConfigDir:   root,
		RunDir:      filepath.Join(root, "run"),
		LogDir:      filepath.Join(root, "logs"),
		RunsDir:     filepath.Join(root, "runs"),
		EventsDir:   filepath.Join(root, "events"),
		Alerts:      filepath.Join(root, "alerts.jsonl"),
		NotifyState: filepath.Join(root, "notify.json"),
		Socket:      filepath.Join(root, "run", "daemon.sock"),
		Lock:        filepath.Join(root, "run", "daemon.lock"),
		State:       filepath.Join(root, "run", "daemon.json"),
		Registry:    filepath.Join(root, "registry.json"),
		Plist:       filepath.Join(root, "LaunchAgents", paths.LaunchdLabel+".plist"),
	}
}

func waitForStatus(t *testing.T, socket string) Health {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		status, err := Status(context.Background(), socket, 100*time.Millisecond)
		if err == nil {
			return status
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("daemon did not become healthy at %s", socket)
	return Health{}
}
