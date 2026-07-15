package registry

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sync"
	"time"
)

const SchemaVersion = 1

var namePattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$`)

type Job struct {
	Name      string          `json:"name"`
	Kind      string          `json:"kind"`
	Spec      json.RawMessage `json:"spec"`
	CreatedAt string          `json:"created_at"`
	UpdatedAt string          `json:"updated_at"`
}

type Document struct {
	SchemaVersion int            `json:"schema_version"`
	Revision      uint64         `json:"revision"`
	Jobs          map[string]Job `json:"jobs"`
}

type PutInput struct {
	Name string          `json:"name"`
	Kind string          `json:"kind"`
	Spec json.RawMessage `json:"spec"`
}

type PutResult struct {
	SchemaVersion int    `json:"schema_version"`
	Revision      uint64 `json:"revision"`
	Changed       bool   `json:"changed"`
	Job           Job    `json:"job"`
	// Warnings are non-fatal problems with an accepted job — e.g. a passthrough var
	// that is not in the daemon's environment and will be unset at spawn. The job is
	// still stored; these exist so the caller learns at the point of the mistake
	// rather than from a job that quietly does nothing.
	Warnings []string `json:"warnings,omitempty"`
}

type Store struct {
	mu            sync.RWMutex
	path          string
	doc           Document
	now           func() time.Time
	specValidator SpecValidator
	specInspector SpecInspector
}

func Open(path string) (*Store, error) {
	doc, err := load(path)
	if err != nil {
		return nil, err
	}
	store := &Store{path: path, doc: doc, now: time.Now}
	if _, err := os.Stat(path); errors.Is(err, os.ErrNotExist) {
		if err := store.persist(doc); err != nil {
			return nil, fmt.Errorf("create registry: %w", err)
		}
	} else if err != nil {
		return nil, fmt.Errorf("inspect registry: %w", err)
	}
	return store, nil
}

func (s *Store) Put(input PutInput) (PutResult, error) {
	if err := Validate(input); err != nil {
		return PutResult{}, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	// Reject a spec that cannot be scheduled, at the boundary every write crosses.
	// Load does not do this: an unschedulable job already on disk is surfaced as
	// invalid rather than blocking the daemon's start.
	if s.specValidator != nil {
		if err := s.specValidator(input.Kind, input.Spec); err != nil {
			return PutResult{}, fmt.Errorf("invalid %s spec for job %q: %w", input.Kind, input.Name, err)
		}
	}
	// Warnings ride on the unchanged path too: re-running an add with a var that is
	// still missing must say so again, or the second attempt looks like confirmation
	// that the first one worked.
	var warnings []string
	if s.specInspector != nil {
		warnings = s.specInspector(input.Kind, input.Spec)
	}

	if current, ok := s.doc.Jobs[input.Name]; ok && current.Kind == input.Kind && jsonEqual(current.Spec, input.Spec) {
		return PutResult{
			SchemaVersion: SchemaVersion,
			Revision:      s.doc.Revision,
			Changed:       false,
			Job:           cloneJob(current),
			Warnings:      warnings,
		}, nil
	}

	now := s.now().UTC().Format(time.RFC3339Nano)
	createdAt := now
	if current, ok := s.doc.Jobs[input.Name]; ok {
		createdAt = current.CreatedAt
	}
	job := Job{
		Name:      input.Name,
		Kind:      input.Kind,
		Spec:      cloneJSON(input.Spec),
		CreatedAt: createdAt,
		UpdatedAt: now,
	}
	next := cloneDocument(s.doc)
	next.Revision++
	next.Jobs[input.Name] = job
	if err := s.persist(next); err != nil {
		return PutResult{}, fmt.Errorf("persist registry: %w", err)
	}
	s.doc = next
	return PutResult{
		SchemaVersion: SchemaVersion,
		Revision:      next.Revision,
		Changed:       true,
		Job:           cloneJob(job),
		Warnings:      warnings,
	}, nil
}

// DeleteResult reports the outcome of a Delete.
type DeleteResult struct {
	SchemaVersion int    `json:"schema_version"`
	Revision      uint64 `json:"revision"`
	Removed       bool   `json:"removed"`
	Name          string `json:"name"`
}

// Delete removes a job by name. Removed is false (with no error) if it did not exist, so
// delete is idempotent.
func (s *Store) Delete(name string) (DeleteResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.doc.Jobs[name]; !ok {
		return DeleteResult{SchemaVersion: SchemaVersion, Revision: s.doc.Revision, Removed: false, Name: name}, nil
	}
	next := cloneDocument(s.doc)
	next.Revision++
	delete(next.Jobs, name)
	if err := s.persist(next); err != nil {
		return DeleteResult{}, fmt.Errorf("persist registry: %w", err)
	}
	s.doc = next
	return DeleteResult{SchemaVersion: SchemaVersion, Revision: next.Revision, Removed: true, Name: name}, nil
}

func (s *Store) Snapshot() Document {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return cloneDocument(s.doc)
}

// ValidName reports whether name is a well-formed job name (letters, digits, dots,
// underscores, hyphens; 1-128 chars). Callers that turn a job name into a filesystem
// path (runs/logs) use this to reject traversal (e.g. "../x") before joining.
func ValidName(name string) bool { return namePattern.MatchString(name) }

// Validate checks a job's shape: name, kind, and that the spec is a JSON object. It
// deliberately does NOT interpret the spec's contents — see SpecValidator.
//
// This runs on both the write path (Put) and the load path, and those need different
// answers about a spec that cannot be scheduled. A caller writing one should be told
// no; a daemon starting up with one already on disk must not refuse to start and take
// every healthy job down with it. Contents are therefore checked via the store's
// SpecValidator, which Put consults and load does not.
func Validate(input PutInput) error {
	if !namePattern.MatchString(input.Name) {
		return fmt.Errorf("invalid job name %q: use 1-128 letters, digits, dots, underscores, or hyphens", input.Name)
	}
	if input.Kind != "cron" && input.Kind != "svc" {
		return fmt.Errorf("invalid job kind %q: must be cron or svc", input.Kind)
	}
	if len(input.Spec) == 0 {
		return errors.New("job spec is required")
	}
	var object map[string]json.RawMessage
	if err := json.Unmarshal(input.Spec, &object); err != nil {
		return fmt.Errorf("job spec must be a JSON object: %w", err)
	}
	if object == nil {
		return errors.New("job spec must be a JSON object")
	}
	return nil
}

// SpecValidator reports whether a job spec of the given kind is usable — for a cron
// job, that its schedule parses. Registry cannot check this itself: interpreting a
// spec means importing cronjob, and cronjob already imports registry.
//
// It lives here rather than at the CLI because the CLI is not the only write path.
// `loco registry put` is a documented, first-class command, and it registered specs
// that `loco cron add` rejects — leaving a job that shows up in `cron list` and never
// fires. Validation belongs at the boundary every write crosses, which is Put.
type SpecValidator func(kind string, spec []byte) error

// SetSpecValidator installs the per-kind spec check used by Put. Wired by the daemon,
// which imports both this package and cronjob. Nil disables the check, which is what
// tests and any caller that only needs shape validation get.
func (s *Store) SetSpecValidator(validator SpecValidator) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.specValidator = validator
}

// SpecInspector returns non-fatal warnings about an otherwise-valid spec. Like
// SpecValidator it is injected, but it must also run in the daemon's process for a
// second reason: the thing worth warning about — a passthrough var missing from the
// environment jobs are spawned with — is only observable there. Checking in the CLI
// would inspect the user's shell, which is exactly the environment that does NOT
// apply, and would stay silent in the case that actually bites.
type SpecInspector func(kind string, spec []byte) []string

// SetSpecInspector installs the warning hook used by Put. Nil disables warnings.
func (s *Store) SetSpecInspector(inspector SpecInspector) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.specInspector = inspector
}

func load(path string) (Document, error) {
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return Document{SchemaVersion: SchemaVersion, Jobs: make(map[string]Job)}, nil
	}
	if err != nil {
		return Document{}, fmt.Errorf("read registry: %w", err)
	}
	var doc Document
	if err := json.Unmarshal(data, &doc); err != nil {
		return Document{}, fmt.Errorf("decode registry: %w", err)
	}
	if doc.SchemaVersion != SchemaVersion {
		return Document{}, fmt.Errorf("unsupported registry schema version %d", doc.SchemaVersion)
	}
	if doc.Jobs == nil {
		doc.Jobs = make(map[string]Job)
	}
	for name, job := range doc.Jobs {
		if name != job.Name {
			return Document{}, fmt.Errorf("registry job key %q does not match name %q", name, job.Name)
		}
		if err := Validate(PutInput{Name: job.Name, Kind: job.Kind, Spec: job.Spec}); err != nil {
			return Document{}, fmt.Errorf("invalid registry job %q: %w", name, err)
		}
	}
	return doc, nil
}

func (s *Store) persist(doc Document) error {
	data, err := json.MarshalIndent(doc, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')
	if err := os.MkdirAll(filepath.Dir(s.path), 0o700); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(s.path), ".registry-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer func() { _ = os.Remove(tmpName) }()
	if err := tmp.Chmod(0o600); err != nil {
		_ = tmp.Close()
		return err
	}
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmpName, s.path)
}

func cloneDocument(doc Document) Document {
	clone := Document{
		SchemaVersion: doc.SchemaVersion,
		Revision:      doc.Revision,
		Jobs:          make(map[string]Job, len(doc.Jobs)),
	}
	for name, job := range doc.Jobs {
		clone.Jobs[name] = cloneJob(job)
	}
	return clone
}

func cloneJob(job Job) Job {
	job.Spec = cloneJSON(job.Spec)
	return job
}

func cloneJSON(value json.RawMessage) json.RawMessage {
	return append(json.RawMessage(nil), value...)
}

func jsonEqual(left, right json.RawMessage) bool {
	var leftValue, rightValue any
	if json.Unmarshal(left, &leftValue) != nil || json.Unmarshal(right, &rightValue) != nil {
		return bytes.Equal(left, right)
	}
	return valuesEqual(leftValue, rightValue)
}

func valuesEqual(left, right any) bool {
	leftJSON, leftErr := json.Marshal(left)
	rightJSON, rightErr := json.Marshal(right)
	return leftErr == nil && rightErr == nil && bytes.Equal(leftJSON, rightJSON)
}
