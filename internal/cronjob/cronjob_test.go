package cronjob

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
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
	env := childEnv(map[string]string{"MY_VAR": "1"})
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
