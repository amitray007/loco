package main

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/amitray007/loco/internal/cronjob"
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
		RunsDir:   filepath.Join(root, "runs"),
		Socket:    filepath.Join(root, "run", "daemon.sock"),
		Lock:      filepath.Join(root, "run", "daemon.lock"),
		State:     filepath.Join(root, "run", "daemon.json"),
		Registry:  filepath.Join(root, "registry.json"),
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

func TestRegistryPutAndListJSON(t *testing.T) {
	root, err := os.MkdirTemp("/tmp", "loco-cli-registry-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(root)
	p, err := paths.Resolve(root)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- daemon.Run(ctx, p, "registry-test") }()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := daemon.Status(context.Background(), p.Socket, 50*time.Millisecond); err == nil {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	var stdout, stderr bytes.Buffer
	code := run([]string{"--config-dir", root, "registry", "put", "demo", "--kind", "svc", "--data", `{"command":["sleep","1"]}`, "--json"}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("put code = %d, stderr = %s", code, stderr.String())
	}
	stdout.Reset()
	stderr.Reset()
	code = run([]string{"--config-dir", root, "registry", "list", "--json"}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("list code = %d, stderr = %s", code, stderr.String())
	}
	var document struct {
		SchemaVersion int                    `json:"schema_version"`
		Revision      uint64                 `json:"revision"`
		Jobs          map[string]interface{} `json:"jobs"`
	}
	if err := json.Unmarshal(stdout.Bytes(), &document); err != nil {
		t.Fatal(err)
	}
	if document.SchemaVersion != 1 || document.Revision != 1 || document.Jobs["demo"] == nil {
		t.Fatalf("document = %+v", document)
	}
	cancel()
	<-done
}

func TestCronAddListAndRunNow(t *testing.T) {
	root, err := os.MkdirTemp("/tmp", "loco-cli-cron-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(root)
	p, err := paths.Resolve(root)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- daemon.Run(ctx, p, "cron-test") }()
	waitForDaemon(t, p.Socket)

	var stdout, stderr bytes.Buffer
	code := run([]string{"--config-dir", root, "cron", "add", "echo.job", "--schedule", "*/5 * * * *", "--cmd", "printf cli-ok", "--json"}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("add code = %d, stderr = %s", code, stderr.String())
	}
	stdout.Reset()
	stderr.Reset()
	code = run([]string{"--config-dir", root, "cron", "list", "--json"}, &stdout, &stderr)
	if code != 0 || !bytes.Contains(stdout.Bytes(), []byte(`"echo.job"`)) {
		t.Fatalf("list code = %d, stdout = %s, stderr = %s", code, stdout.String(), stderr.String())
	}
	stdout.Reset()
	stderr.Reset()
	code = run([]string{"--config-dir", root, "cron", "run-now", "echo.job", "--json"}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("run-now code = %d, stderr = %s", code, stderr.String())
	}
	var trigger cronjob.TriggerResult
	if err := json.Unmarshal(stdout.Bytes(), &trigger); err != nil {
		t.Fatal(err)
	}
	recordPath := filepath.Join(p.RunsDir, "echo.job", trigger.RunID+".json")
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(recordPath); err == nil {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	data, err := os.ReadFile(recordPath)
	if err != nil {
		t.Fatal(err)
	}
	var record cronjob.RunRecord
	if err := json.Unmarshal(data, &record); err != nil {
		t.Fatal(err)
	}
	if record.Status != "success" || record.ExitCode != 0 {
		t.Fatalf("record = %+v", record)
	}
	output, err := os.ReadFile(record.StdoutPath)
	if err != nil || string(output) != "cli-ok" {
		t.Fatalf("stdout = %q, err = %v", output, err)
	}
	cancel()
	<-done
}

func TestCronAddRejectsNonFiveFieldSchedule(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := run([]string{"--config-dir", t.TempDir(), "cron", "add", "bad", "--schedule", "* * * * * *", "--cmd", "true"}, &stdout, &stderr)
	if code != 1 || !bytes.Contains(stderr.Bytes(), []byte("invalid 5-field schedule")) {
		t.Fatalf("code = %d, stderr = %s", code, stderr.String())
	}
}

func waitForDaemon(t *testing.T, socket string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := daemon.Status(context.Background(), socket, 50*time.Millisecond); err == nil {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("daemon did not become healthy")
}
