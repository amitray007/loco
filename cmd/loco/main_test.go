package main

import (
	"bytes"
	"context"
	"encoding/json"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/amitray007/loco/internal/cronjob"
	"github.com/amitray007/loco/internal/daemon"
	"github.com/amitray007/loco/internal/events"
	"github.com/amitray007/loco/internal/notification"
	"github.com/amitray007/loco/internal/paths"
	"github.com/amitray007/loco/internal/registry"
	"github.com/amitray007/loco/internal/service"
	watchdogpkg "github.com/amitray007/loco/internal/watchdog"
)

func TestMain(m *testing.M) { _ = os.Setenv("LOCO_NOTIFY_CHANNEL", "file"); os.Exit(m.Run()) }

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
		EventsDir: filepath.Join(root, "events"),
		Alerts:    filepath.Join(root, "alerts.jsonl"),
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

func TestDoctorReportsHealthySpineAndLaunchdWarnings(t *testing.T) {
	root, err := os.MkdirTemp("/tmp", "loco-doctor-test-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(root) })
	// --config-dir does not redirect the service-unit paths: those derive from $HOME, so
	// without this the test reads the developer's real ~/Library/LaunchAgents and asserts
	// "warn" (plist not installed) against it. That passes only on a machine where loco
	// is not installed — it fails for anyone who actually runs what they are developing.
	// Point the unit dir at an empty dir so "not installed" is a fact about the fixture.
	unitDir := t.TempDir()
	t.Setenv("LOCO_LAUNCH_AGENTS_DIR", unitDir) // macOS
	t.Setenv("LOCO_SYSTEMD_UNIT_DIR", unitDir)  // Linux
	p, err := paths.Resolve(root)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	// Run the daemon at the CLI's own version: this test asserts the healthy spine, and
	// a version mismatch is now a (correct) warning of its own, covered separately.
	go func() { done <- daemon.Run(ctx, p, version) }()
	waitForDaemon(t, p.Socket)
	t.Cleanup(func() {
		cancel()
		<-done
	})

	var stdout, stderr bytes.Buffer
	code := run([]string{"--config-dir", root, "doctor", "--json"}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("code = %d, stderr = %s", code, stderr.String())
	}
	var report doctorReport
	if err := json.Unmarshal(stdout.Bytes(), &report); err != nil {
		t.Fatalf("invalid JSON %q: %v", stdout.String(), err)
	}
	if !report.Healthy || len(report.Checks) != 8 {
		t.Fatalf("report = %+v", report)
	}
	want := map[string]string{
		"runtime-paths": "pass", "daemon-rpc": "pass", "daemon-version": "pass", "registry": "pass",
		"durable-stores": "pass", "disk-usage": "pass", "service-unit": "warn", "watchdog-unit": "warn",
	}
	for _, check := range report.Checks {
		if check.Status != want[check.Name] {
			t.Errorf("check %s status = %q, want %q", check.Name, check.Status, want[check.Name])
		}
	}
}

// #19: `loco events ack` is served by the long-lived daemon, not by the CLI binary, so
// upgrading loco leaves the OLD code running until launchd restarts it. That is how #19's
// reporter saw v0.1.2's strict ack silently not fire: their daemon predated the upgrade.
// The skew is invisible — every command keeps working, just with the old logic — so the
// one place that answers "is my install sane?" has to say it out loud.
func TestDoctorFlagsDaemonBinarySkew(t *testing.T) {
	root, err := os.MkdirTemp("/tmp", "loco-skew-test-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(root) })
	unitDir := t.TempDir()
	t.Setenv("LOCO_LAUNCH_AGENTS_DIR", unitDir)
	t.Setenv("LOCO_SYSTEMD_UNIT_DIR", unitDir)
	p, err := paths.Resolve(root)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	// The daemon runs a version the CLI does not: exactly the post-upgrade state.
	go func() { done <- daemon.Run(ctx, p, "0.0.1-stale") }()
	waitForDaemon(t, p.Socket)
	t.Cleanup(func() {
		cancel()
		<-done
	})

	var stdout, stderr bytes.Buffer
	// Skew warns rather than fails: `loco doctor` treats warn as non-fatal (an
	// uninstalled plist warns too), and running a dev binary against an installed
	// daemon is a normal thing to do. The check has to be visible, not fatal.
	if code := run([]string{"--config-dir", root, "doctor", "--json"}, &stdout, &stderr); code != 0 {
		t.Errorf("doctor exit = %d on a skewed daemon, want 0 (skew warns, it does not fail)", code)
	}
	var report doctorReport
	if err := json.Unmarshal(stdout.Bytes(), &report); err != nil {
		t.Fatalf("invalid JSON %q: %v", stdout.String(), err)
	}
	var check *doctorCheck
	for i := range report.Checks {
		if report.Checks[i].Name == "daemon-version" {
			check = &report.Checks[i]
		}
	}
	if check == nil {
		t.Fatalf("no daemon-version check in report: %+v", report)
	}
	if check.Status != "warn" {
		t.Errorf("daemon-version status = %q, want %q", check.Status, "warn")
	}
	// The detail has to name both versions and say what to do; "skew detected" alone
	// leaves the reader exactly where #19's reporter was.
	for _, want := range []string{"0.0.1-stale", version, "restart"} {
		if !strings.Contains(check.Detail, want) {
			t.Errorf("detail %q omits %q", check.Detail, want)
		}
	}
}

// The skew check must stay quiet when the daemon matches, or it becomes noise that
// trains the reader to ignore the one case that matters.
func TestDoctorPassesWhenDaemonVersionMatches(t *testing.T) {
	root, err := os.MkdirTemp("/tmp", "loco-noskew-test-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(root) })
	unitDir := t.TempDir()
	t.Setenv("LOCO_LAUNCH_AGENTS_DIR", unitDir)
	t.Setenv("LOCO_SYSTEMD_UNIT_DIR", unitDir)
	p, err := paths.Resolve(root)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- daemon.Run(ctx, p, version) }()
	waitForDaemon(t, p.Socket)
	t.Cleanup(func() {
		cancel()
		<-done
	})

	var stdout, stderr bytes.Buffer
	run([]string{"--config-dir", root, "doctor", "--json"}, &stdout, &stderr)
	var report doctorReport
	if err := json.Unmarshal(stdout.Bytes(), &report); err != nil {
		t.Fatalf("invalid JSON %q: %v", stdout.String(), err)
	}
	for _, check := range report.Checks {
		if check.Name == "daemon-version" && check.Status != "pass" {
			t.Errorf("matched versions reported %q: %s", check.Status, check.Detail)
		}
	}
}

func TestDoctorReportsUnavailableDaemonAsFailure(t *testing.T) {
	root := t.TempDir()

	var stdout, stderr bytes.Buffer
	code := run([]string{"--config-dir", root, "doctor", "--json"}, &stdout, &stderr)
	if code != 2 {
		t.Fatalf("code = %d, want 2; stderr = %s", code, stderr.String())
	}
	var report doctorReport
	if err := json.Unmarshal(stdout.Bytes(), &report); err != nil {
		t.Fatalf("invalid JSON %q: %v", stdout.String(), err)
	}
	if report.Healthy {
		t.Fatalf("report = %+v", report)
	}
	statuses := make(map[string]string)
	for _, check := range report.Checks {
		statuses[check.Name] = check.Status
	}
	if statuses["runtime-paths"] != "fail" || statuses["daemon-rpc"] != "fail" ||
		statuses["registry"] != "fail" || statuses["durable-stores"] != "fail" {
		t.Fatalf("statuses = %+v", statuses)
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

func TestServiceCLIControlsAndSupervisesProcess(t *testing.T) {
	root, err := os.MkdirTemp("/tmp", "loco-cli-service-")
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
	go func() { done <- daemon.Run(ctx, p, "service-test") }()
	waitForDaemon(t, p.Socket)
	defer func() { cancel(); <-done }()

	var stdout, stderr bytes.Buffer
	code := run([]string{"--config-dir", root, "svc", "add", "worker", "--cmd", "sleep 60", "--backoff-initial", "10ms", "--backoff-max", "20ms", "--json"}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("add code=%d stderr=%s", code, stderr.String())
	}
	first := waitForServiceCLI(t, root, func(s service.Status) bool { return s.State == "running" && s.PID > 0 })
	proc, _ := os.FindProcess(first.PID)
	if err := proc.Kill(); err != nil {
		t.Fatal(err)
	}
	second := waitForServiceCLI(t, root, func(s service.Status) bool { return s.State == "running" && s.PID != first.PID && s.Restarts >= 1 })

	stdout.Reset()
	stderr.Reset()
	code = run([]string{"--config-dir", root, "svc", "restart", "worker", "--json"}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("restart code=%d stderr=%s", code, stderr.String())
	}
	third := waitForServiceCLI(t, root, func(s service.Status) bool { return s.State == "running" && s.PID != second.PID })
	if third.PID == second.PID {
		t.Fatal("restart did not replace process")
	}
	stdout.Reset()
	stderr.Reset()
	code = run([]string{"--config-dir", root, "svc", "stop", "worker", "--json"}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("stop code=%d stderr=%s", code, stderr.String())
	}
	waitForServiceCLI(t, root, func(s service.Status) bool { return s.State == "stopped" && !s.Enabled })
	stdout.Reset()
	stderr.Reset()
	code = run([]string{"--config-dir", root, "svc", "start", "worker", "--json"}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("start code=%d stderr=%s", code, stderr.String())
	}
	waitForServiceCLI(t, root, func(s service.Status) bool { return s.State == "running" && s.Enabled })
}

func waitForServiceCLI(t *testing.T, root string, ready func(service.Status) bool) service.Status {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		var stdout, stderr bytes.Buffer
		if run([]string{"--config-dir", root, "svc", "list", "--json"}, &stdout, &stderr) == 0 {
			var result service.ListResult
			if json.Unmarshal(stdout.Bytes(), &result) == nil {
				for _, status := range result.Services {
					if ready(status) {
						return status
					}
				}
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("service did not reach expected CLI state")
	return service.Status{}
}

func TestEventsPersistAcrossDaemonRestartAndRequireExplicitAck(t *testing.T) {
	root, err := os.MkdirTemp("/tmp", "loco-cli-events-")
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
	go func() { done <- daemon.Run(ctx, p, "events-1") }()
	waitForDaemon(t, p.Socket)
	var stdout, stderr bytes.Buffer
	code := run([]string{"--config-dir", root, "events", "emit", "demo.topic", "--id", "persisted", "--data", `{"value":1}`, "--json"}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("emit code=%d stderr=%s", code, stderr.String())
	}
	cancel()
	<-done
	ctx, cancel = context.WithCancel(context.Background())
	done = make(chan error, 1)
	go func() { done <- daemon.Run(ctx, p, "events-2") }()
	waitForDaemon(t, p.Socket)
	defer func() { cancel(); <-done }()

	read := func(cursor string) events.ReadResult {
		stdout.Reset()
		stderr.Reset()
		code = run([]string{"--config-dir", root, "events", "read", "demo.topic", "--cursor", cursor, "--json"}, &stdout, &stderr)
		if code != 0 {
			t.Fatalf("read code=%d stderr=%s", code, stderr.String())
		}
		var result events.ReadResult
		if err := json.Unmarshal(stdout.Bytes(), &result); err != nil {
			t.Fatal(err)
		}
		return result
	}
	first := read("c1")
	if len(first.Events) != 1 || first.Events[0].ID != "persisted" {
		t.Fatalf("first=%+v", first)
	}
	again := read("c1")
	if len(again.Events) != 1 {
		t.Fatal("read advanced without ack")
	}
	stdout.Reset()
	stderr.Reset()
	code = run([]string{"--config-dir", root, "events", "ack", "demo.topic", "--cursor", "c1", "--through", "persisted", "--json"}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("ack code=%d stderr=%s", code, stderr.String())
	}
	if after := read("c1"); len(after.Events) != 0 {
		t.Fatalf("after ack=%+v", after)
	}
	if other := read("c2"); len(other.Events) != 1 {
		t.Fatalf("independent cursor=%+v", other)
	}
	stdout.Reset()
	stderr.Reset()
	code = run([]string{"--config-dir", root, "events", "emit", "demo.topic", "--id", "persisted", "--data", `{"value":999}`, "--json"}, &stdout, &stderr)
	if code != 0 {
		t.Fatal(stderr.String())
	}
	var duplicate events.EmitResult
	if err := json.Unmarshal(stdout.Bytes(), &duplicate); err != nil {
		t.Fatal(err)
	}
	if duplicate.Emitted || duplicate.Event.Seq != 1 {
		t.Fatalf("duplicate=%+v", duplicate)
	}
}

// A job whose spec cannot be scheduled must say so in both views. Reload skips such a
// job silently and forever; `cron list` discarded its decode error and printed the
// zero-value spec (blank columns), and `status` hardcoded every cron job's state to
// "scheduled". So the two places you would look both reported a job that could never
// run as if it were healthy.
//
// Prevention (rejecting the spec at Put) cannot reach a job already on disk — this is
// what does.
func TestInvalidCronSpecIsReportedByListAndStatus(t *testing.T) {
	root, err := os.MkdirTemp("/tmp", "loco-cli-invalid-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(root) })
	p, err := paths.Resolve(root)
	if err != nil {
		t.Fatal(err)
	}
	// Write the bad job straight to the registry: Put now rejects it, which is the
	// point — this is the job that predates the check, or arrived some other way.
	document := registry.Document{SchemaVersion: registry.SchemaVersion, Revision: 1, Jobs: map[string]registry.Job{
		"badjob":  {Name: "badjob", Kind: "cron", Spec: json.RawMessage(`{"schema_version":1,"schedule":"@every 5m","command":["echo","hi"]}`)},
		"healthy": {Name: "healthy", Kind: "cron", Spec: json.RawMessage(`{"schema_version":1,"schedule":"*/5 * * * *","command":["echo","ok"]}`)},
	}}
	data, err := json.MarshalIndent(document, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p.Registry, data, 0o600); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- daemon.Run(ctx, p, "invalid-test") }()
	waitForDaemon(t, p.Socket)
	t.Cleanup(func() {
		cancel()
		<-done
	})

	// cron list must name the reason, not render blank columns.
	var stdout, stderr bytes.Buffer
	if code := run([]string{"--config-dir", root, "cron", "list"}, &stdout, &stderr); code != 0 {
		t.Fatalf("cron list code = %d, stderr = %s", code, stderr.String())
	}
	listing := stdout.String()
	if !strings.Contains(listing, "invalid") || !strings.Contains(listing, "descriptors") {
		t.Errorf("cron list does not report the bad job's reason:\n%s", listing)
	}
	if !strings.Contains(listing, "healthy") || !strings.Contains(listing, "*/5 * * * *") {
		t.Errorf("cron list lost the healthy job:\n%s", listing)
	}

	// status must not call an unschedulable job "scheduled".
	stdout.Reset()
	stderr.Reset()
	if code := run([]string{"--config-dir", root, "status", "--json"}, &stdout, &stderr); code != 0 {
		t.Fatalf("status code = %d, stderr = %s", code, stderr.String())
	}
	var overview daemon.Overview
	if err := json.Unmarshal(stdout.Bytes(), &overview); err != nil {
		t.Fatalf("invalid JSON %q: %v", stdout.String(), err)
	}
	states := make(map[string]daemon.JobOverview)
	for _, job := range overview.Jobs {
		states[job.Name] = job
	}
	if got := states["badjob"].State; got != "invalid" {
		t.Errorf("badjob state = %q, want invalid", got)
	}
	if states["badjob"].Error == "" {
		t.Error("badjob reports no reason; the parse error is the actionable part")
	}
	if got := states["healthy"].State; got != "scheduled" {
		t.Errorf("healthy state = %q, want scheduled", got)
	}
}

func TestTopLevelStatusReflectsJobsRunsAndUnreadEvents(t *testing.T) {
	root, err := os.MkdirTemp("/tmp", "loco-cli-status-")
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
	go func() { done <- daemon.Run(ctx, p, "status-test") }()
	waitForDaemon(t, p.Socket)
	defer func() { cancel(); <-done }()
	call := func(args ...string) bytes.Buffer {
		var stdout, stderr bytes.Buffer
		full := append([]string{"--config-dir", root}, args...)
		if code := run(full, &stdout, &stderr); code != 0 {
			t.Fatalf("%v code=%d stderr=%s", args, code, stderr.String())
		}
		return stdout
	}
	call("cron", "add", "quiet.job", "--schedule", "0 0 * * *", "--cmd", "printf done")
	triggerJSON := call("cron", "run-now", "quiet.job", "--json")
	var trigger cronjob.TriggerResult
	if err := json.Unmarshal(triggerJSON.Bytes(), &trigger); err != nil {
		t.Fatal(err)
	}
	recordPath := filepath.Join(p.RunsDir, "quiet.job", trigger.RunID+".json")
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(recordPath); err == nil {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	call("svc", "add", "resident", "--cmd", "sleep 60", "--backoff-initial", "10ms", "--backoff-max", "20ms")
	waitForServiceCLI(t, root, func(s service.Status) bool { return s.Name == "resident" && s.State == "running" })
	call("events", "emit", "dispatch.topic", "--id", "waiting", "--data", `{"work":true}`)
	call("events", "read", "dispatch.topic", "--cursor", "agent", "--json")
	statusJSON := call("status", "--json")
	var status daemon.Overview
	if err := json.Unmarshal(statusJSON.Bytes(), &status); err != nil {
		t.Fatal(err)
	}
	if !status.Daemon.Healthy || len(status.Jobs) != 2 || len(status.Topics) != 1 {
		t.Fatalf("status=%+v", status)
	}
	if status.Topics[0].Cursors[0].Unread != 1 {
		t.Fatalf("topic status=%+v", status.Topics)
	}
	var foundRun bool
	for _, job := range status.Jobs {
		if job.Name == "quiet.job" && job.LastRun != nil && job.LastRun.Status == "success" {
			foundRun = true
		}
	}
	if !foundRun {
		t.Fatalf("last run missing: %+v", status.Jobs)
	}
	if statusJSON.Len() > 2000 {
		t.Fatalf("status heartbeat too large: %d bytes", statusJSON.Len())
	}
	call("svc", "stop", "resident")
}

func TestTopLevelStatusUnavailableJSON(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := run([]string{"--config-dir", t.TempDir(), "status", "--json"}, &stdout, &stderr)
	if code != 2 {
		t.Fatalf("code=%d", code)
	}
	var status daemon.Overview
	if err := json.Unmarshal(stdout.Bytes(), &status); err != nil {
		t.Fatal(err)
	}
	if status.Daemon.Healthy || status.Jobs == nil || status.Topics == nil || status.Alerts == nil {
		t.Fatalf("status=%+v", status)
	}
}

func TestNotifyCLIUsesDurableGroupDedupe(t *testing.T) {
	root, err := os.MkdirTemp("/tmp", "loco-cli-notify-")
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
	go func() { done <- daemon.Run(ctx, p, "notify-test") }()
	waitForDaemon(t, p.Socket)
	defer func() { cancel(); <-done }()
	call := func() notification.Result {
		var stdout, stderr bytes.Buffer
		code := run([]string{"--config-dir", root, "notify", "Failure", "worker crashed", "--channel", "file", "--group", "worker", "--json"}, &stdout, &stderr)
		if code != 0 {
			t.Fatalf("code=%d stderr=%s", code, stderr.String())
		}
		var result notification.Result
		if err := json.Unmarshal(stdout.Bytes(), &result); err != nil {
			t.Fatal(err)
		}
		return result
	}
	first, second := call(), call()
	if !first.Delivered || !second.Suppressed {
		t.Fatalf("first=%+v second=%+v", first, second)
	}
	data, err := os.ReadFile(p.Alerts)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Count(string(data), "\n") != 1 {
		t.Fatalf("alerts=%s", data)
	}
}

func TestCrashLoopProducesOneGroupedAlert(t *testing.T) {
	root, err := os.MkdirTemp("/tmp", "loco-cli-crash-alert-")
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
	go func() { done <- daemon.Run(ctx, p, "alert-test") }()
	waitForDaemon(t, p.Socket)
	defer func() { cancel(); <-done }()
	var stdout, stderr bytes.Buffer
	if code := run([]string{"--config-dir", root, "svc", "add", "crasher", "--cmd", "exit 7", "--backoff-initial", "10ms", "--backoff-max", "20ms"}, &stdout, &stderr); code != 0 {
		t.Fatalf("code=%d stderr=%s", code, stderr.String())
	}
	deadline := time.Now().Add(3 * time.Second)
	records := []string{}
	for time.Now().Before(deadline) {
		records, _ = filepath.Glob(filepath.Join(p.RunsDir, "crasher", "*.json"))
		if len(records) >= 3 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if len(records) < 3 {
		t.Fatalf("records=%d", len(records))
	}
	time.Sleep(50 * time.Millisecond)
	data, err := os.ReadFile(p.Alerts)
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	if len(lines) != 1 {
		t.Fatalf("got %d alerts for crash loop: %s", len(lines), data)
	}
	var alert notification.Alert
	if err := json.Unmarshal([]byte(lines[0]), &alert); err != nil {
		t.Fatal(err)
	}
	if alert.Group != "job-failure:crasher" {
		t.Fatalf("alert=%+v", alert)
	}
	stdout.Reset()
	stderr.Reset()
	if code := run([]string{"--config-dir", root, "svc", "stop", "crasher"}, &stdout, &stderr); code != 0 {
		t.Fatalf("stop code=%d stderr=%s", code, stderr.String())
	}
}

func TestWatchdogCLITracksFailuresAndResetsWhenDaemonRecovers(t *testing.T) {
	root, err := os.MkdirTemp("/tmp", "loco-watchdog-cli-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(root)
	call := func() int {
		var stdout, stderr bytes.Buffer
		code := run([]string{"--config-dir", root, "daemon", "watchdog", "--threshold", "3"}, &stdout, &stderr)
		if code != 0 {
			t.Fatalf("code=%d stderr=%s", code, stderr.String())
		}
		return code
	}
	call()
	call()
	data, err := os.ReadFile(filepath.Join(root, "watchdog.json"))
	if err != nil {
		t.Fatal(err)
	}
	var state watchdogpkg.State
	if err := json.Unmarshal(data, &state); err != nil {
		t.Fatal(err)
	}
	if state.Failures != 2 || state.Alerted {
		t.Fatalf("state=%+v", state)
	}
	p, err := paths.Resolve(root)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- daemon.Run(ctx, p, "watchdog-recovery") }()
	waitForDaemon(t, p.Socket)
	call()
	cancel()
	<-done
	data, err = os.ReadFile(p.WatchdogState)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(data, &state); err != nil {
		t.Fatal(err)
	}
	if state.Failures != 0 || state.Alerted {
		t.Fatalf("recovered state=%+v", state)
	}
}

// F-15: `notify --channel file TITLE BODY` (flag before positionals) must parse, not
// silently fail. Verified through the parser without a daemon by checking it reaches the
// dial stage (exit 2, connection error) rather than the arg-usage stage (exit 1).
func TestNotifyAcceptsFlagBeforePositionals(t *testing.T) {
	root := t.TempDir()
	var stdout, stderr bytes.Buffer
	code := run([]string{"--config-dir", root, "notify", "--channel", "file", "Title", "Body"}, &stdout, &stderr)
	// No daemon is running, so this must fail at the RPC dial (exit 2) — proving the
	// args parsed. The old behavior exited 1 with an empty message.
	if code != 2 {
		t.Fatalf("flag-first notify did not parse: code=%d stderr=%q", code, stderr.String())
	}
	if !strings.Contains(stderr.String(), "send notification") {
		t.Fatalf("expected a dial error, got: %q", stderr.String())
	}
}

// F-19: the reserved job-failure: group prefix is rejected from user notify.
func TestNotifyRejectsReservedGroup(t *testing.T) {
	root := t.TempDir()
	var stdout, stderr bytes.Buffer
	code := run([]string{"--config-dir", root, "notify", "T", "B", "--group", "job-failure:worker"}, &stdout, &stderr)
	if code != 1 || !strings.Contains(stderr.String(), "reserved") {
		t.Fatalf("reserved group not rejected: code=%d stderr=%q", code, stderr.String())
	}
}

// A subcommand group's usage line must list exactly the verbs its switch
// dispatches. The usage lines used to be hand-written beside the switch and had
// drifted in four of five groups: `cron` omitted remove/pause/resume, `svc`
// omitted remove, `events` omitted peek/list, and `daemon` omitted watchdog. A
// wrong usage line is worse than a missing one — it asserts a verb does not
// exist, so nobody tries it.
//
// The switch is the ground truth, so this reads the case clauses out of the
// source with go/ast and compares them to the declared verb list. Driving the
// CLI instead would not work: a verb missing from the list is simply never
// exercised, so the drift that matters would pass unnoticed.
func TestGroupUsageListsEveryDispatchableVerb(t *testing.T) {
	groups := map[string]struct {
		fn    string
		verbs []string
	}{
		"daemon":   {"runDaemon", daemonVerbs},
		"registry": {"runRegistry", registryVerbs},
		"cron":     {"runCron", cronVerbs},
		"svc":      {"runService", svcVerbs},
		"events":   {"runEvents", eventsVerbs},
	}

	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "main.go", nil, 0)
	if err != nil {
		t.Fatalf("parse main.go: %v", err)
	}

	for group, want := range groups {
		cases := switchCaseVerbs(t, file, want.fn)
		if len(cases) == 0 {
			t.Errorf("%s: found no switch cases in %s (test is looking in the wrong place)",
				group, want.fn)
			continue
		}
		declared := map[string]bool{}
		for _, v := range want.verbs {
			declared[v] = true
		}
		for _, verb := range cases {
			if !declared[verb] {
				t.Errorf("loco %s %s: dispatched by %s but missing from its usage line; add it to %sVerbs",
					group, verb, want.fn, group)
			}
			delete(declared, verb)
		}
		for verb := range declared {
			t.Errorf("loco %s %s: listed in usage but no case in %s handles it",
				group, verb, want.fn)
		}
	}
}

// #11 recurred inside the fix for #11 (#19): `events ack` grew `--allow-skip` and
// one of its two usage strings was updated. The arg-count guard printed the stale
// one, so the flag was undiscoverable from the CLI's own help. The verb guard above
// only checks group usage lines, so a flag missing from a subcommand usage line
// sailed through. This asserts the other half: every flag a subcommand registers
// appears in every usage string that subcommand prints.
func TestSubcommandUsageStringsListEveryFlag(t *testing.T) {
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "main.go", nil, 0)
	if err != nil {
		t.Fatalf("parse main.go: %v", err)
	}
	// --json is registered on nearly every subcommand and is genuinely optional
	// noise in a usage line; the class of bug being guarded is a *behavioural*
	// flag going undocumented, which is what --allow-skip was.
	exempt := map[string]bool{"json": true}

	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || !strings.HasPrefix(fn.Name.Name, "run") {
			continue
		}
		ast.Inspect(fn, func(n ast.Node) bool {
			clause, ok := n.(*ast.CaseClause)
			if !ok {
				return true
			}
			verb := ""
			for _, expr := range clause.List {
				if lit, ok := expr.(*ast.BasicLit); ok && lit.Kind == token.STRING {
					if v, err := strconv.Unquote(lit.Value); err == nil {
						verb = v
					}
				}
			}
			flags, usages := flagsAndUsagesIn(clause)
			for _, flagName := range flags {
				if exempt[flagName] {
					continue
				}
				for _, usage := range usages {
					if !strings.Contains(usage, "--"+flagName) {
						t.Errorf("%s (case %q): registers --%s but usage string %q omits it;\n"+
							"  a flag absent from the usage the CLI actually prints is undiscoverable (#11/#19)",
							fn.Name.Name, verb, flagName, usage)
					}
				}
			}
			return true
		})
	}
}

// flagsAndUsagesIn returns the flag names registered via flags.String/Bool/Int/
// Duration inside a case clause, and every "usage: ..." string literal it prints.
func flagsAndUsagesIn(clause *ast.CaseClause) (flags []string, usages []string) {
	ast.Inspect(clause, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if ok {
			if sel, ok := call.Fun.(*ast.SelectorExpr); ok && len(call.Args) > 0 {
				switch sel.Sel.Name {
				case "String", "Bool", "Int", "Int64", "Duration", "Uint", "Uint64", "Float64":
					if lit, ok := call.Args[0].(*ast.BasicLit); ok && lit.Kind == token.STRING {
						if name, err := strconv.Unquote(lit.Value); err == nil && name != "" {
							flags = append(flags, name)
						}
					}
				}
			}
		}
		if lit, ok := n.(*ast.BasicLit); ok && lit.Kind == token.STRING {
			if s, err := strconv.Unquote(lit.Value); err == nil && strings.HasPrefix(s, "usage: ") {
				usages = append(usages, s)
			}
		}
		return true
	})
	return flags, usages
}

// switchCaseVerbs returns every string literal appearing in a case clause of
// the top-level switch inside the named function, e.g. `case "pause", "resume":`
// yields both verbs. Help arms are skipped: they are routing, not verbs.
func switchCaseVerbs(t *testing.T, file *ast.File, fnName string) []string {
	t.Helper()
	var verbs []string
	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Name.Name != fnName {
			continue
		}
		ast.Inspect(fn, func(n ast.Node) bool {
			clause, ok := n.(*ast.CaseClause)
			if !ok {
				return true
			}
			for _, expr := range clause.List {
				lit, ok := expr.(*ast.BasicLit)
				if !ok || lit.Kind != token.STRING {
					continue
				}
				verb, err := strconv.Unquote(lit.Value)
				if err != nil || isHelpArg(verb) {
					continue
				}
				verbs = append(verbs, verb)
			}
			return true
		})
	}
	return verbs
}

// `loco cron --help` used to error with `unknown cron command "--help"`, which
// broke the natural discovery path: the bare usage line was stale, so the one
// way left to find `cron remove` was to guess it.
func TestSubcommandGroupsRouteHelpToUsage(t *testing.T) {
	root := t.TempDir()
	for _, group := range []string{"daemon", "registry", "cron", "svc", "events"} {
		for _, helpArg := range []string{"help", "--help", "-h"} {
			var stdout, stderr bytes.Buffer
			code := run([]string{"--config-dir", root, group, helpArg}, &stdout, &stderr)
			if code != 0 {
				t.Errorf("loco %s %s: exit %d, want 0 (stderr=%q)",
					group, helpArg, code, strings.TrimSpace(stderr.String()))
			}
			if !strings.Contains(stdout.String(), "usage: loco "+group) {
				t.Errorf("loco %s %s: stdout lacks usage line, got %q",
					group, helpArg, strings.TrimSpace(stdout.String()))
			}
		}
	}
}

// F-14: commands that reject bad args must print usage, not exit silently.
func TestBadArgsPrintUsageNotSilence(t *testing.T) {
	root := t.TempDir()
	cases := [][]string{
		{"--config-dir", root, "cron", "run-now", "job", "extra"},
		{"--config-dir", root, "svc", "start", "svc1", "extra"},
		{"--config-dir", root, "notify", "T", "B", "extra"},
	}
	for _, args := range cases {
		var stdout, stderr bytes.Buffer
		code := run(args, &stdout, &stderr)
		if code != 1 {
			t.Fatalf("%v: expected exit 1, got %d", args, code)
		}
		if strings.TrimSpace(stderr.String()) == "" {
			t.Fatalf("%v: exited 1 with empty stderr (silent failure)", args)
		}
	}
}

// B2/A2: `loco version` prints the version and exits 0.
func TestVersionCommand(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := run([]string{"version"}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("version exit=%d stderr=%s", code, stderr.String())
	}
	if !strings.HasPrefix(stdout.String(), "loco ") {
		t.Fatalf("version output = %q", stdout.String())
	}
}

// B2: `loco cron remove` on a missing job reports not-found (exit 1), not silence.
func TestCronRemoveMissingJob(t *testing.T) {
	root := t.TempDir()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	p, err := paths.Resolve(root)
	if err != nil {
		t.Fatal(err)
	}
	go func() { done <- daemon.Run(ctx, p, "remove-test") }()
	waitForDaemon(t, p.Socket)
	defer func() { cancel(); <-done }()

	var stdout, stderr bytes.Buffer
	code := run([]string{"--config-dir", root, "cron", "remove", "ghost"}, &stdout, &stderr)
	if code != 1 || !strings.Contains(stderr.String(), "not found") {
		t.Fatalf("remove missing: code=%d stderr=%q", code, stderr.String())
	}
}

// An invalid --channel-topics value fails fast with a clear error rather than starting
// a half-configured server.
func TestMCPServeRejectsInvalidChannelTopic(t *testing.T) {
	root := t.TempDir()
	var stdout, stderr bytes.Buffer
	code := run([]string{"--config-dir", root, "mcp", "serve", "--channel-topics", "../bad"}, &stdout, &stderr)
	if code != 1 || !strings.Contains(stderr.String(), "invalid channel topic") {
		t.Fatalf("code=%d stderr=%q", code, stderr.String())
	}
}
