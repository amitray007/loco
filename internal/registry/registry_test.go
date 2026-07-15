package registry

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
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

// Put must reject a spec the kind cannot use. `loco cron add` validated schedules but
// `loco registry put` — a documented, first-class command — did not, so the identical
// spec that `cron add` refused could be registered through it: exit 0, a job in
// `cron list`, and it never fires. Validation belongs at the boundary every write
// crosses, not at one of several doors.
func TestPutRejectsASpecTheValidatorRefuses(t *testing.T) {
	store, err := Open(filepath.Join(t.TempDir(), "registry.json"))
	if err != nil {
		t.Fatal(err)
	}
	store.SetSpecValidator(func(kind string, spec []byte) error {
		if kind == "cron" && strings.Contains(string(spec), "@every") {
			return errors.New("parser does not accept descriptors")
		}
		return nil
	})

	_, err = store.Put(PutInput{Name: "badjob", Kind: "cron", Spec: json.RawMessage(`{"schedule":"@every 5m"}`)})
	if err == nil {
		t.Fatal("Put accepted an unschedulable spec; it would register and never fire")
	}
	if !strings.Contains(err.Error(), "badjob") {
		t.Errorf("error %q does not name the job", err)
	}
	if _, ok := store.Snapshot().Jobs["badjob"]; ok {
		t.Error("a rejected job was stored anyway")
	}

	// A valid spec must still go through, and other kinds are not the cron validator's
	// business.
	if _, err := store.Put(PutInput{Name: "good", Kind: "cron", Spec: json.RawMessage(`{"schedule":"*/5 * * * *"}`)}); err != nil {
		t.Errorf("Put rejected a valid spec: %v", err)
	}
	if _, err := store.Put(PutInput{Name: "svc1", Kind: "svc", Spec: json.RawMessage(`{"command":["x"]}`)}); err != nil {
		t.Errorf("Put rejected a svc spec via the cron validator: %v", err)
	}
}

// An unschedulable job already on disk must not stop the daemon from starting. Load
// deliberately does not run the spec validator: one bad job would fail Open and take
// every healthy job on the machine down with it. Registered-but-invalid is a legal,
// visible state — surfaced by cron list/status rather than fatal at load.
func TestLoadKeepsAnInvalidSpecRatherThanBlockingStartup(t *testing.T) {
	path := filepath.Join(t.TempDir(), "registry.json")
	document := Document{SchemaVersion: SchemaVersion, Revision: 1, Jobs: map[string]Job{
		"badjob":  {Name: "badjob", Kind: "cron", Spec: json.RawMessage(`{"schedule":"@every 5m"}`)},
		"healthy": {Name: "healthy", Kind: "cron", Spec: json.RawMessage(`{"schedule":"*/5 * * * *"}`)},
	}}
	data, err := json.MarshalIndent(document, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}

	store, err := Open(path)
	if err != nil {
		t.Fatalf("a job already on disk blocked Open; the daemon would not start: %v", err)
	}
	// Installing the validator afterwards must not retroactively evict it either.
	store.SetSpecValidator(func(string, []byte) error { return errors.New("nope") })
	jobs := store.Snapshot().Jobs
	if _, ok := jobs["badjob"]; !ok {
		t.Error("badjob was dropped from the registry; nothing should delete it")
	}
	if _, ok := jobs["healthy"]; !ok {
		t.Error("healthy job lost")
	}
}

// A passthrough var missing from the environment jobs are spawned with is skipped
// without a word, so the job runs with it unset and quietly does nothing useful. The
// warning must survive the unchanged path too: re-running an add with the var still
// missing has to say so again, or the second attempt reads as confirmation that the
// first one worked.
func TestPutWarningsRideOnBothTheChangedAndUnchangedPaths(t *testing.T) {
	store, err := Open(filepath.Join(t.TempDir(), "registry.json"))
	if err != nil {
		t.Fatal(err)
	}
	store.SetSpecInspector(func(kind string, spec []byte) []string {
		if strings.Contains(string(spec), "MISSING_VAR") {
			return []string{"MISSING_VAR is not in the daemon's environment"}
		}
		return nil
	})

	input := PutInput{Name: "job", Kind: "cron", Spec: json.RawMessage(`{"passthrough_env":["MISSING_VAR"]}`)}
	first, err := store.Put(input)
	if err != nil {
		t.Fatal(err)
	}
	if !first.Changed {
		t.Fatal("first put reported unchanged")
	}
	if len(first.Warnings) != 1 {
		t.Fatalf("stored job carried %d warnings, want 1: %+v", len(first.Warnings), first.Warnings)
	}

	// Identical re-put: unchanged, but the problem has not gone away.
	second, err := store.Put(input)
	if err != nil {
		t.Fatal(err)
	}
	if second.Changed {
		t.Fatal("re-put of an identical spec reported changed")
	}
	if len(second.Warnings) != 1 {
		t.Errorf("unchanged put dropped the warning; the retry looks like it worked: %+v", second.Warnings)
	}

	// A spec with nothing to warn about must stay silent.
	clean, err := store.Put(PutInput{Name: "clean", Kind: "cron", Spec: json.RawMessage(`{"schedule":"*/5 * * * *"}`)})
	if err != nil {
		t.Fatal(err)
	}
	if len(clean.Warnings) != 0 {
		t.Errorf("clean spec warned anyway: %+v", clean.Warnings)
	}
}

// Delete removes a job and is idempotent.
func TestDeleteRemovesJobAndIsIdempotent(t *testing.T) {
	store, err := Open(filepath.Join(t.TempDir(), "registry.json"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Put(PutInput{Name: "job1", Kind: "cron", Spec: []byte(`{"schedule":"* * * * *","command":["/bin/true"]}`)}); err != nil {
		t.Fatal(err)
	}
	del, err := store.Delete("job1")
	if err != nil {
		t.Fatal(err)
	}
	if !del.Removed {
		t.Fatalf("expected removed, got %+v", del)
	}
	if _, ok := store.Snapshot().Jobs["job1"]; ok {
		t.Fatal("job still present after delete")
	}
	// Second delete is a no-op, not an error.
	again, err := store.Delete("job1")
	if err != nil {
		t.Fatal(err)
	}
	if again.Removed {
		t.Fatal("second delete should report Removed=false")
	}
	// Delete survives reopen (persisted).
	reopened, err := Open(store.path)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := reopened.Snapshot().Jobs["job1"]; ok {
		t.Fatal("delete did not persist")
	}
}

// ValidName rejects traversal/control names used to guard runs/logs path joins.
func TestValidName(t *testing.T) {
	for _, ok := range []string{"job1", "my.job", "a-b_c", "J"} {
		if !ValidName(ok) {
			t.Fatalf("%q should be valid", ok)
		}
	}
	for _, bad := range []string{"../etc", "a/b", "", ".hidden", "a b"} {
		if ValidName(bad) {
			t.Fatalf("%q should be invalid", bad)
		}
	}
}
