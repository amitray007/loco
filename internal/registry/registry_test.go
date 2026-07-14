package registry

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestPutIsAtomicIdempotentAndReloadable(t *testing.T) {
	path := filepath.Join(t.TempDir(), "registry.json")
	store, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	firstTime := time.Date(2026, 7, 14, 12, 0, 0, 0, time.UTC)
	store.now = func() time.Time { return firstTime }

	first, err := store.Put(PutInput{
		Name: "demo.job",
		Kind: "cron",
		Spec: json.RawMessage(`{"schedule":"*/5 * * * *","command":["echo","ok"]}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	if !first.Changed || first.Revision != 1 {
		t.Fatalf("first put = %+v", first)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("registry mode = %o, want 600", info.Mode().Perm())
	}

	store.now = func() time.Time { return firstTime.Add(time.Hour) }
	second, err := store.Put(PutInput{
		Name: "demo.job",
		Kind: "cron",
		Spec: json.RawMessage(`{ "command": ["echo", "ok"], "schedule": "*/5 * * * *" }`),
	})
	if err != nil {
		t.Fatal(err)
	}
	if second.Changed || second.Revision != 1 {
		t.Fatalf("idempotent put = %+v", second)
	}
	if second.Job.UpdatedAt != first.Job.UpdatedAt {
		t.Fatalf("updated_at changed on no-op: %q != %q", second.Job.UpdatedAt, first.Job.UpdatedAt)
	}

	reloaded, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	snapshot := reloaded.Snapshot()
	if snapshot.Revision != 1 || snapshot.Jobs["demo.job"].Kind != "cron" {
		t.Fatalf("reloaded snapshot = %+v", snapshot)
	}
}

func TestOpenRejectsCorruptOrUnsupportedRegistry(t *testing.T) {
	tests := map[string]string{
		"corrupt":     `{`,
		"unsupported": `{"schema_version":99,"jobs":{}}`,
	}
	for name, contents := range tests {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "registry.json")
			if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
				t.Fatal(err)
			}
			if _, err := Open(path); err == nil {
				t.Fatal("Open succeeded, want error")
			}
		})
	}
}

func TestValidateRejectsInvalidDefinitions(t *testing.T) {
	tests := []PutInput{
		{Name: "bad name", Kind: "cron", Spec: json.RawMessage(`{}`)},
		{Name: "ok", Kind: "other", Spec: json.RawMessage(`{}`)},
		{Name: "ok", Kind: "svc", Spec: json.RawMessage(`[]`)},
	}
	for _, input := range tests {
		if err := Validate(input); err == nil {
			t.Errorf("Validate(%+v) succeeded", input)
		}
	}
}
