package cronjob

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/amitray007/loco/internal/paths"
	"github.com/amitray007/loco/internal/registry"
)

func TestRunNowRecordsOutputAndExitStatus(t *testing.T) {
	p := cronTestPaths(t)
	store, err := registry.Open(p.Registry)
	if err != nil {
		t.Fatal(err)
	}
	spec, _ := json.Marshal(Spec{Schedule: "*/5 * * * *", Command: []string{"/bin/sh", "-c", "printf hello"}})
	if _, err := store.Put(registry.PutInput{Name: "demo", Kind: "cron", Spec: spec}); err != nil {
		t.Fatal(err)
	}
	manager := New(store, p)
	if err := manager.Start(); err != nil {
		t.Fatal(err)
	}
	result, err := manager.RunNow("demo")
	if err != nil {
		t.Fatal(err)
	}

	recordPath := filepath.Join(p.RunsDir, "demo", result.RunID+".json")
	var data []byte
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		data, err = os.ReadFile(recordPath)
		if err == nil {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	manager.Stop()
	if err != nil {
		t.Fatal(err)
	}
	var record RunRecord
	if err := json.Unmarshal(data, &record); err != nil {
		t.Fatal(err)
	}
	if record.Status != "success" || record.ExitCode != 0 || record.Trigger != "manual" {
		t.Fatalf("record = %+v", record)
	}
	stdout, err := os.ReadFile(record.StdoutPath)
	if err != nil {
		t.Fatal(err)
	}
	if string(stdout) != "hello" {
		t.Fatalf("stdout = %q", stdout)
	}
}

func TestScheduleReloadsFromRegistry(t *testing.T) {
	p := cronTestPaths(t)
	store, err := registry.Open(p.Registry)
	if err != nil {
		t.Fatal(err)
	}
	spec, _ := json.Marshal(Spec{Schedule: "*/5 * * * *", Command: []string{"true"}})
	if _, err := store.Put(registry.PutInput{Name: "persisted", Kind: "cron", Spec: spec}); err != nil {
		t.Fatal(err)
	}

	first := New(store, p)
	if err := first.Start(); err != nil {
		t.Fatal(err)
	}
	if len(first.entries) != 1 {
		t.Fatalf("first entries = %d", len(first.entries))
	}
	first.Stop()
	reloadedStore, err := registry.Open(p.Registry)
	if err != nil {
		t.Fatal(err)
	}
	second := New(reloadedStore, p)
	if err := second.Start(); err != nil {
		t.Fatal(err)
	}
	defer second.Stop()
	if len(second.entries) != 1 {
		t.Fatalf("reloaded entries = %d", len(second.entries))
	}
	entry := second.scheduler.Entry(second.entries[0])
	if !entry.Next.After(time.Now()) {
		t.Fatalf("next run = %s", entry.Next)
	}
}

func TestDecodeSpecRequiresFiveFieldsAndCommand(t *testing.T) {
	for _, raw := range []string{
		`{"schedule":"* * * * * *","command":["true"]}`,
		`{"schedule":"* * * * *","command":[]}`,
	} {
		if _, err := DecodeSpec(json.RawMessage(raw)); err == nil {
			t.Fatalf("DecodeSpec(%s) succeeded", raw)
		}
	}
}

func cronTestPaths(t *testing.T) paths.Paths {
	t.Helper()
	root := t.TempDir()
	return paths.Paths{
		ConfigDir: root, RunDir: filepath.Join(root, "run"), LogDir: filepath.Join(root, "logs"),
		RunsDir: filepath.Join(root, "runs"), EventsDir: filepath.Join(root, "events"), Registry: filepath.Join(root, "registry.json"),
	}
}

// F-03: a job that floods stdout must be capped and the record flagged truncated.
func TestJobOutputIsCappedAndFlagged(t *testing.T) {
	p := cronTestPaths(t)
	store, err := registry.Open(p.Registry)
	if err != nil {
		t.Fatal(err)
	}
	// Emit ~20 MiB, well over the 8 MiB cap.
	spec, _ := json.Marshal(Spec{Schedule: "*/5 * * * *", Command: []string{"/bin/sh", "-c", "yes ABCDEFGHIJKLMNOPQRSTUVWXYZ | head -c 20000000"}})
	store.Put(registry.PutInput{Name: "flood", Kind: "cron", Spec: spec})
	manager := New(store, p)
	manager.Start()
	result, _ := manager.RunNow("flood")
	recordPath := filepath.Join(p.RunsDir, "flood", result.RunID+".json")
	var data []byte
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if data, err = os.ReadFile(recordPath); err == nil {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	manager.Stop()
	var record RunRecord
	if err := json.Unmarshal(data, &record); err != nil {
		t.Fatal(err)
	}
	if !record.Truncated {
		t.Fatal("expected truncated flag on flooding job")
	}
	info, err := os.Stat(record.StdoutPath)
	if err != nil {
		t.Fatal(err)
	}
	if info.Size() > maxJobLogBytes+1024 {
		t.Fatalf("stdout log %d bytes exceeds cap %d", info.Size(), maxJobLogBytes)
	}
}

// F-13: run records are pruned to runHistoryLimit, deleting old records and their logs.
func TestRunHistoryIsPruned(t *testing.T) {
	p := cronTestPaths(t)
	store, _ := registry.Open(p.Registry)
	manager := New(store, p)
	dir := filepath.Join(p.RunsDir, "job")
	os.MkdirAll(dir, 0o700)
	logDir := filepath.Join(p.LogDir, "jobs", "job")
	os.MkdirAll(logDir, 0o700)
	// Seed runHistoryLimit + 50 fake records, each with a log file.
	total := runHistoryLimit + 50
	for i := 0; i < total; i++ {
		id := fmt.Sprintf("20260101T%09d", i)
		logPath := filepath.Join(logDir, id+".stdout.log")
		os.WriteFile(logPath, []byte("x"), 0o600)
		rec := RunRecord{SchemaVersion: SchemaVersion, ID: id, Job: "job", StdoutPath: logPath}
		data, _ := json.MarshalIndent(rec, "", "  ")
		os.WriteFile(filepath.Join(dir, id+".json"), data, 0o600)
	}
	manager.pruneRuns("job")
	entries, _ := os.ReadDir(dir)
	kept := 0
	for _, e := range entries {
		if filepath.Ext(e.Name()) == ".json" {
			kept++
		}
	}
	if kept != runHistoryLimit {
		t.Fatalf("kept %d run records, want %d", kept, runHistoryLimit)
	}
	// Oldest logs should be gone too.
	logs, _ := os.ReadDir(logDir)
	if len(logs) != runHistoryLimit {
		t.Fatalf("kept %d logs, want %d", len(logs), runHistoryLimit)
	}
}

// F-18: a job command must not inherit the daemon's secrets.
func TestChildEnvExcludesDaemonSecrets(t *testing.T) {
	os.Setenv("LOCO_TEST_SECRET", "topsecret")
	defer os.Unsetenv("LOCO_TEST_SECRET")
	env := childEnv(map[string]string{"MY_VAR": "1"}, nil)
	for _, kv := range env {
		if len(kv) >= 17 && kv[:17] == "LOCO_TEST_SECRET=" {
			t.Fatal("child env leaked the daemon secret")
		}
	}
	var sawMyVar, sawPath bool
	for _, kv := range env {
		if kv == "MY_VAR=1" {
			sawMyVar = true
		}
		if len(kv) >= 5 && kv[:5] == "PATH=" {
			sawPath = true
		}
	}
	if !sawMyVar || !sawPath {
		t.Fatalf("child env missing declared var or PATH: %v", env)
	}
}

// B1/F-18: declared --env reaches the child; --env-passthrough inherits a named daemon
// var; unlisted daemon secrets are still excluded.
func TestChildEnvDeclaredAndPassthrough(t *testing.T) {
	os.Setenv("LOCO_TEST_PASS", "inherited")
	os.Setenv("LOCO_TEST_SECRET", "should-not-leak")
	defer os.Unsetenv("LOCO_TEST_PASS")
	defer os.Unsetenv("LOCO_TEST_SECRET")

	env := childEnv(map[string]string{"DECLARED": "yes"}, []string{"LOCO_TEST_PASS"})
	has := func(kv string) bool {
		for _, e := range env {
			if e == kv {
				return true
			}
		}
		return false
	}
	if !has("DECLARED=yes") {
		t.Fatal("declared env var missing")
	}
	if !has("LOCO_TEST_PASS=inherited") {
		t.Fatal("passthrough var not inherited")
	}
	for _, e := range env {
		if strings.HasPrefix(e, "LOCO_TEST_SECRET=") {
			t.Fatal("unlisted daemon secret leaked into child env")
		}
	}
}

// A hung job with a timeout is killed and recorded as "timeout" (fixes the lock-forever
// bug where a wedged run silently skips all future firings).
func TestRunTimeoutKillsHungJob(t *testing.T) {
	p := cronTestPaths(t)
	store, err := registry.Open(p.Registry)
	if err != nil {
		t.Fatal(err)
	}
	spec, _ := json.Marshal(Spec{Schedule: "*/5 * * * *", Command: []string{"/bin/sh", "-c", "sleep 30"}, TimeoutMS: 500})
	store.Put(registry.PutInput{Name: "hang", Kind: "cron", Spec: spec})
	manager := New(store, p)
	manager.Start()
	result, _ := manager.RunNow("hang")
	recordPath := filepath.Join(p.RunsDir, "hang", result.RunID+".json")
	var data []byte
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if data, err = os.ReadFile(recordPath); err == nil {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	manager.Stop()
	var record RunRecord
	if err := json.Unmarshal(data, &record); err != nil {
		t.Fatal(err)
	}
	if record.Status != "timeout" {
		t.Fatalf("status = %q, want timeout", record.Status)
	}
	// Killed near the 500ms deadline, not after the full 30s sleep.
	if record.DurationMS > 5000 {
		t.Fatalf("timed-out run took %dms; expected ~500ms", record.DurationMS)
	}
}

// A negative timeout disables the cap; runTimeout resolves the three cases.
func TestRunTimeoutResolution(t *testing.T) {
	if got := runTimeout(Spec{TimeoutMS: 0}); got != defaultRunTimeout {
		t.Fatalf("default = %v, want %v", got, defaultRunTimeout)
	}
	if got := runTimeout(Spec{TimeoutMS: -1}); got != 0 {
		t.Fatalf("disabled = %v, want 0", got)
	}
	if got := runTimeout(Spec{TimeoutMS: 2000}); got != 2*time.Second {
		t.Fatalf("explicit = %v, want 2s", got)
	}
}
