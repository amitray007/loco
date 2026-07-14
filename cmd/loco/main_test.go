package main

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/amitray007/loco/internal/daemon"
	"github.com/amitray007/loco/internal/paths"
)

func TestStatusJSON(t *testing.T) {
	root, err := os.MkdirTemp("/tmp", "loco-cli-test-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(root) })
	p := paths.Paths{
		ConfigDir: root,
		RunDir:    filepath.Join(root, "run"),
		LogDir:    filepath.Join(root, "logs"),
		Socket:    filepath.Join(root, "run", "daemon.sock"),
		Lock:      filepath.Join(root, "run", "daemon.lock"),
		State:     filepath.Join(root, "run", "daemon.json"),
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- daemon.Run(ctx, p, "cli-test") }()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := daemon.Status(context.Background(), p.Socket, 50*time.Millisecond); err == nil {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	var stdout, stderr bytes.Buffer
	code := run([]string{"--config-dir", root, "daemon", "status", "--json"}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("code = %d, stderr = %s", code, stderr.String())
	}
	var status daemon.Health
	if err := json.Unmarshal(stdout.Bytes(), &status); err != nil {
		t.Fatalf("invalid JSON %q: %v", stdout.String(), err)
	}
	if !status.Healthy || status.Version != "cli-test" {
		t.Fatalf("status = %+v", status)
	}
	cancel()
	<-done
}

func TestUnknownCommandIsCallerError(t *testing.T) {
	var stdout, stderr bytes.Buffer
	if code := run([]string{"wat"}, &stdout, &stderr); code != 1 {
		t.Fatalf("code = %d, want 1", code)
	}
}

func TestUnavailableStatusIsStructuredSystemError(t *testing.T) {
	root, err := os.MkdirTemp("/tmp", "loco-cli-unavailable-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(root)

	var stdout, stderr bytes.Buffer
	code := run([]string{"--config-dir", root, "daemon", "status", "--json"}, &stdout, &stderr)
	if code != 2 {
		t.Fatalf("code = %d, want 2", code)
	}
	var status daemon.Health
	if err := json.Unmarshal(stdout.Bytes(), &status); err != nil {
		t.Fatalf("invalid JSON %q: %v", stdout.String(), err)
	}
	if status.Healthy || status.SchemaVersion != daemon.SchemaVersion || status.Error == "" {
		t.Fatalf("status = %+v", status)
	}
}
