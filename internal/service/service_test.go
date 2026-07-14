package service

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/amitray007/loco/internal/paths"
	"github.com/amitray007/loco/internal/registry"
)

func TestKilledServiceRestartsAndRecordsLifecycle(t *testing.T) {
	p := serviceTestPaths(t)
	store, err := registry.Open(p.Registry)
	if err != nil {
		t.Fatal(err)
	}
	spec, _ := json.Marshal(Spec{Command: []string{"/bin/sh", "-c", "while :; do sleep 1; done"}, Restart: "always", Enabled: true, BackoffInitial: 10, BackoffMax: 20})
	if _, err := store.Put(registry.PutInput{Name: "worker", Kind: "svc", Spec: spec}); err != nil {
		t.Fatal(err)
	}
	manager := New(store, p)
	if err := manager.Start(); err != nil {
		t.Fatal(err)
	}
	defer manager.Stop()

	first := waitForService(t, manager, "worker", func(s Status) bool { return s.State == "running" && s.PID > 0 })
	process, err := os.FindProcess(first.PID)
	if err != nil {
		t.Fatal(err)
	}
	if err := process.Kill(); err != nil {
		t.Fatal(err)
	}
	second := waitForService(t, manager, "worker", func(s Status) bool { return s.State == "running" && s.PID > 0 && s.PID != first.PID && s.Restarts >= 1 })
	if second.PID == first.PID {
		t.Fatalf("PID did not change: %d", first.PID)
	}

	records, err := filepath.Glob(filepath.Join(p.RunsDir, "worker", "*.json"))
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for len(records) == 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
		records, _ = filepath.Glob(filepath.Join(p.RunsDir, "worker", "*.json"))
	}
	if len(records) == 0 {
		t.Fatal("killed execution was not recorded")
	}
	data, err := os.ReadFile(records[0])
	if err != nil {
		t.Fatal(err)
	}
	var record struct {
		Status   string `json:"status"`
		ExitCode int    `json:"exit_code"`
		Trigger  string `json:"trigger"`
	}
	if err := json.Unmarshal(data, &record); err != nil {
		t.Fatal(err)
	}
	if record.Status != "failed" || record.Trigger != "supervisor" {
		t.Fatalf("record = %+v", record)
	}
}

func TestStoppedStateSurvivesManagerRestart(t *testing.T) {
	p := serviceTestPaths(t)
	store, err := registry.Open(p.Registry)
	if err != nil {
		t.Fatal(err)
	}
	spec, _ := json.Marshal(Spec{Command: []string{"sleep", "60"}, Restart: "always", Enabled: true, BackoffInitial: 10, BackoffMax: 20})
	if _, err := store.Put(registry.PutInput{Name: "durable", Kind: "svc", Spec: spec}); err != nil {
		t.Fatal(err)
	}
	first := New(store, p)
	if err := first.Start(); err != nil {
		t.Fatal(err)
	}
	waitForService(t, first, "durable", func(s Status) bool { return s.State == "running" })
	if _, err := first.SetEnabled("durable", false); err != nil {
		t.Fatal(err)
	}
	first.Stop()

	reloaded, err := registry.Open(p.Registry)
	if err != nil {
		t.Fatal(err)
	}
	second := New(reloaded, p)
	if err := second.Start(); err != nil {
		t.Fatal(err)
	}
	defer second.Stop()
	status := second.List().Services[0]
	if status.State != "stopped" || status.Enabled {
		t.Fatalf("status after restart = %+v", status)
	}
}

func TestDecodeSpecValidation(t *testing.T) {
	for _, raw := range []string{`{}`, `{"command":["true"],"restart":"sometimes"}`, `{"command":["true"],"backoff_initial_ms":20,"backoff_max_ms":10}`} {
		if _, err := DecodeSpec(json.RawMessage(raw)); err == nil {
			t.Fatalf("DecodeSpec(%s) succeeded", raw)
		}
	}
}

func waitForService(t *testing.T, manager *Manager, name string, ready func(Status) bool) Status {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		for _, status := range manager.List().Services {
			if status.Name == name && ready(status) {
				return status
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("service %s did not reach expected state: %+v", name, manager.List())
	return Status{}
}

func serviceTestPaths(t *testing.T) paths.Paths {
	t.Helper()
	root := t.TempDir()
	return paths.Paths{ConfigDir: root, RunDir: filepath.Join(root, "run"), LogDir: filepath.Join(root, "logs"), RunsDir: filepath.Join(root, "runs"), EventsDir: filepath.Join(root, "events"), Registry: filepath.Join(root, "registry.json")}
}

// F-09: an orphaned child from a previous daemon (recorded pid) must be reaped at
// startup so we never run two copies of a service after an unclean restart.
func TestReconcileOrphansKillsRecordedChild(t *testing.T) {
	p := serviceTestPaths(t)
	store, err := registry.Open(p.Registry)
	if err != nil {
		t.Fatal(err)
	}
	// Simulate a previous daemon: spawn a long-lived process in its own group and
	// record its pid the way the supervisor would.
	cmd := exec.Command("/bin/sh", "-c", "while :; do sleep 1; done")
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	orphanPID := cmd.Process.Pid
	go cmd.Wait() // reap when it dies so it doesn't linger as a zombie

	manager := New(store, p)
	manager.writePID("ghost", pidRecord{PID: orphanPID, StartedAt: time.Now().UTC().Format(time.RFC3339Nano), Command: "sh"})

	// The orphan is alive now.
	if syscall.Kill(orphanPID, 0) != nil {
		t.Fatal("orphan should be alive before reconciliation")
	}
	manager.reconcileOrphans()

	// After reconciliation it must be gone, and its pid record cleared.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if syscall.Kill(orphanPID, 0) != nil {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if syscall.Kill(orphanPID, 0) == nil {
		_ = syscall.Kill(-orphanPID, syscall.SIGKILL)
		t.Fatal("orphan survived reconciliation — would run as a duplicate")
	}
	if _, err := os.Stat(manager.pidPath("ghost")); !os.IsNotExist(err) {
		t.Fatal("pid record not cleared after reconciliation")
	}
}
