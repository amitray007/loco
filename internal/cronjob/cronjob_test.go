package cronjob

import (
	"encoding/json"
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
		RunsDir: filepath.Join(root, "runs"), Registry: filepath.Join(root, "registry.json"),
	}
}
