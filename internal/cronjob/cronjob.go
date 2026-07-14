package cronjob

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	cronlib "github.com/robfig/cron/v3"

	"github.com/amitray007/loco/internal/paths"
	"github.com/amitray007/loco/internal/registry"
)

const SchemaVersion = 1

// maxJobLogBytes caps how much a single run may write to each of stdout/stderr, so a
// chatty or runaway command cannot fill the disk. Output past the cap is dropped and
// the run record is flagged truncated.
const maxJobLogBytes = 8 << 20 // 8 MiB per stream

// runHistoryLimit bounds how many run records (and their logs) are retained per job.
const runHistoryLimit = 200

// defaultRunTimeout caps a run when a job doesn't specify one, so a hung command cannot
// hold the per-job lock forever (which would silently skip every future firing).
const defaultRunTimeout = time.Hour

// timeoutKillGrace is how long a timed-out run's process group has to exit on SIGTERM
// before it is SIGKILLed.
const timeoutKillGrace = 5 * time.Second

// cappedWriter writes up to limit bytes to w, then discards the rest and remembers it
// truncated.
type cappedWriter struct {
	w         *os.File
	limit     int64
	written   int64
	truncated bool
}

func (c *cappedWriter) Write(p []byte) (int, error) {
	if c.written >= c.limit {
		c.truncated = true
		return len(p), nil // pretend success so the child isn't killed by EPIPE
	}
	remaining := c.limit - c.written
	if int64(len(p)) > remaining {
		c.truncated = true
		if _, err := c.w.Write(p[:remaining]); err != nil {
			return 0, err
		}
		c.written = c.limit
		return len(p), nil
	}
	n, err := c.w.Write(p)
	c.written += int64(n)
	return n, err
}

type Spec struct {
	Schedule string   `json:"schedule"`
	Command  []string `json:"command"`
	// Paused, when true, keeps the job definition but stops it from firing (the
	// scheduler skips it on Reload). Lets an operator silence a job without deleting it.
	Paused     bool              `json:"paused,omitempty"`
	WorkingDir string            `json:"working_dir,omitempty"`
	Env        map[string]string `json:"env,omitempty"`
	// PassthroughEnv names environment variables to inherit from the daemon's own
	// environment at spawn time. The default child env is minimal (no inherited
	// secrets); this is the explicit opt-in for jobs that need one.
	PassthroughEnv []string `json:"passthrough_env,omitempty"`
	// TimeoutMS bounds a single run's wall-clock time. 0 uses the default
	// (defaultRunTimeout); a negative value disables the timeout entirely. Without a
	// timeout, a hung run holds the per-job lock forever and every future firing is
	// skipped until the daemon restarts.
	TimeoutMS int64 `json:"timeout_ms,omitempty"`
}

type TriggerResult struct {
	SchemaVersion int    `json:"schema_version"`
	Accepted      bool   `json:"accepted"`
	RunID         string `json:"run_id"`
	Job           string `json:"job"`
}

type RunRecord struct {
	SchemaVersion int    `json:"schema_version"`
	ID            string `json:"id"`
	Job           string `json:"job"`
	Trigger       string `json:"trigger"`
	Status        string `json:"status"`
	StartedAt     string `json:"started_at"`
	FinishedAt    string `json:"finished_at"`
	DurationMS    int64  `json:"duration_ms"`
	ExitCode      int    `json:"exit_code"`
	Error         string `json:"error,omitempty"`
	StdoutPath    string `json:"stdout_path,omitempty"`
	StderrPath    string `json:"stderr_path,omitempty"`
	Truncated     bool   `json:"truncated,omitempty"`
}

type Manager struct {
	store     *registry.Store
	paths     paths.Paths
	scheduler *cronlib.Cron

	mu        sync.Mutex
	entries   []cronlib.EntryID
	locks     map[string]*sync.Mutex
	ctx       context.Context
	cancel    context.CancelFunc
	wg        sync.WaitGroup
	sequence  atomic.Uint64
	onFailure func(string, RunRecord)
}

func (m *Manager) SetFailureHandler(handler func(string, RunRecord)) { m.onFailure = handler }

func New(store *registry.Store, p paths.Paths) *Manager {
	parser := cronlib.NewParser(cronlib.Minute | cronlib.Hour | cronlib.Dom | cronlib.Month | cronlib.Dow)
	ctx, cancel := context.WithCancel(context.Background())
	return &Manager{
		store: store, paths: p, locks: make(map[string]*sync.Mutex), ctx: ctx, cancel: cancel,
		scheduler: cronlib.New(cronlib.WithParser(parser)),
	}
}

func (m *Manager) Start() error {
	if err := m.Reload(); err != nil {
		return err
	}
	m.scheduler.Start()
	return nil
}

func (m *Manager) Stop() {
	m.cancel()
	stopped := m.scheduler.Stop()
	<-stopped.Done()
	m.wg.Wait()
}

func (m *Manager) Reload() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, id := range m.entries {
		m.scheduler.Remove(id)
	}
	m.entries = nil

	doc := m.store.Snapshot()
	names := make([]string, 0, len(doc.Jobs))
	for name := range doc.Jobs {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		job := doc.Jobs[name]
		if job.Kind != "cron" {
			continue
		}
		spec, err := DecodeSpec(job.Spec)
		if err != nil {
			continue
		}
		if spec.Paused {
			continue // keep the definition, don't schedule it
		}
		jobName, jobSpec := name, spec
		id, err := m.scheduler.AddFunc(spec.Schedule, func() { m.Trigger(jobName, jobSpec, "scheduled") })
		if err != nil {
			continue
		}
		m.entries = append(m.entries, id)
	}
	return nil
}

func (m *Manager) RunNow(name string) (TriggerResult, error) {
	job, ok := m.store.Snapshot().Jobs[name]
	if !ok || job.Kind != "cron" {
		return TriggerResult{}, fmt.Errorf("cron job %q not found", name)
	}
	spec, err := DecodeSpec(job.Spec)
	if err != nil {
		return TriggerResult{}, err
	}
	return m.Trigger(name, spec, "manual"), nil
}

func (m *Manager) Trigger(name string, spec Spec, trigger string) TriggerResult {
	runID := fmt.Sprintf("%s-%d-%d", time.Now().UTC().Format("20060102T150405.000000000Z"), os.Getpid(), m.sequence.Add(1))
	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		m.execute(runID, name, spec, trigger)
	}()
	return TriggerResult{SchemaVersion: SchemaVersion, Accepted: true, RunID: runID, Job: name}
}

func (m *Manager) execute(runID, name string, spec Spec, trigger string) {
	lock := m.jobLock(name)
	started := time.Now().UTC()
	if !lock.TryLock() {
		m.writeRecord(RunRecord{
			SchemaVersion: SchemaVersion, ID: runID, Job: name, Trigger: trigger, Status: "skipped",
			StartedAt: started.Format(time.RFC3339Nano), FinishedAt: started.Format(time.RFC3339Nano),
			Error: "previous run is still active", ExitCode: -1,
		})
		return
	}
	defer lock.Unlock()

	logDir := filepath.Join(m.paths.LogDir, "jobs", name)
	_ = os.MkdirAll(logDir, 0o700)
	stdoutPath := filepath.Join(logDir, runID+".stdout.log")
	stderrPath := filepath.Join(logDir, runID+".stderr.log")
	stdout, stdoutErr := os.OpenFile(stdoutPath, os.O_CREATE|os.O_WRONLY|os.O_EXCL, 0o600)
	stderr, stderrErr := os.OpenFile(stderrPath, os.O_CREATE|os.O_WRONLY|os.O_EXCL, 0o600)
	record := RunRecord{
		SchemaVersion: SchemaVersion, ID: runID, Job: name, Trigger: trigger, Status: "failed",
		StartedAt: started.Format(time.RFC3339Nano), ExitCode: -1, StdoutPath: stdoutPath, StderrPath: stderrPath,
	}
	if stdoutErr != nil || stderrErr != nil {
		record.Error = errors.Join(stdoutErr, stderrErr).Error()
		if stdout != nil {
			_ = stdout.Close()
		}
		if stderr != nil {
			_ = stderr.Close()
		}
		m.finishRecord(&record, started)
		return
	}
	defer stdout.Close()
	defer stderr.Close()

	outCap := &cappedWriter{w: stdout, limit: maxJobLogBytes}
	errCap := &cappedWriter{w: stderr, limit: maxJobLogBytes}

	// Bound the run so a hung command cannot hold the per-job lock forever. The run
	// context is derived from the manager context (so daemon shutdown still cancels)
	// plus a per-run deadline.
	runCtx := m.ctx
	var cancel context.CancelFunc
	timeout := runTimeout(spec)
	if timeout > 0 {
		runCtx, cancel = context.WithTimeout(m.ctx, timeout)
		defer cancel()
	}
	cmd := exec.CommandContext(runCtx, spec.Command[0], spec.Command[1:]...)
	// Own a process group so a timeout kills the whole tree, not just the shell.
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	// On timeout/cancel, escalate SIGTERM -> SIGKILL to the group instead of the
	// default single-process SIGKILL, so grandchildren are reaped too.
	cmd.Cancel = func() error {
		pgid := cmd.Process.Pid
		_ = syscall.Kill(-pgid, syscall.SIGTERM)
		time.AfterFunc(timeoutKillGrace, func() {
			if syscall.Kill(-pgid, 0) == nil {
				_ = syscall.Kill(-pgid, syscall.SIGKILL)
			}
		})
		return nil
	}
	cmd.Stdout, cmd.Stderr = outCap, errCap
	cmd.Dir = spec.WorkingDir
	cmd.Env = childEnv(spec.Env, spec.PassthroughEnv)
	err := cmd.Run()
	switch {
	case err == nil:
		record.Status = "success"
		record.ExitCode = 0
	case errors.Is(runCtx.Err(), context.DeadlineExceeded):
		record.Status = "timeout"
		record.Error = fmt.Sprintf("run exceeded timeout of %s", timeout)
		record.ExitCode = -1
	default:
		record.Error = err.Error()
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) {
			record.ExitCode = exitErr.ExitCode()
		}
	}
	record.Truncated = outCap.truncated || errCap.truncated
	m.finishRecord(&record, started)
}

func DecodeSpec(raw json.RawMessage) (Spec, error) {
	var spec Spec
	if err := json.Unmarshal(raw, &spec); err != nil {
		return Spec{}, fmt.Errorf("decode cron spec: %w", err)
	}
	if len(spec.Command) == 0 || spec.Command[0] == "" {
		return Spec{}, errors.New("command must contain an executable")
	}
	parser := cronlib.NewParser(cronlib.Minute | cronlib.Hour | cronlib.Dom | cronlib.Month | cronlib.Dow)
	if _, err := parser.Parse(spec.Schedule); err != nil {
		return Spec{}, fmt.Errorf("invalid 5-field schedule: %w", err)
	}
	return spec, nil
}

func (m *Manager) finishRecord(record *RunRecord, started time.Time) {
	finished := time.Now().UTC()
	record.FinishedAt = finished.Format(time.RFC3339Nano)
	record.DurationMS = finished.Sub(started).Milliseconds()
	m.writeRecord(*record)
	if (record.Status == "failed" || record.Status == "timeout") && m.onFailure != nil {
		m.onFailure(record.Job, *record)
	}
}

func (m *Manager) writeRecord(record RunRecord) {
	dir := filepath.Join(m.paths.RunsDir, record.Job)
	if os.MkdirAll(dir, 0o700) != nil {
		return
	}
	data, err := json.MarshalIndent(record, "", "  ")
	if err != nil {
		return
	}
	data = append(data, '\n')
	tmp, err := os.CreateTemp(dir, ".run-*")
	if err != nil {
		return
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	if err := tmp.Chmod(0o600); err != nil {
		_ = tmp.Close()
		return
	}
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return
	}
	if err := tmp.Close(); err != nil {
		return
	}
	_ = os.Rename(tmpName, filepath.Join(dir, record.ID+".json"))
	m.pruneRuns(record.Job)
}

// runTimeout resolves the effective per-run timeout for a spec: 0 -> default, negative
// -> disabled (returns 0 so no deadline is applied), positive -> that duration.
func runTimeout(spec Spec) time.Duration {
	switch {
	case spec.TimeoutMS < 0:
		return 0
	case spec.TimeoutMS == 0:
		return defaultRunTimeout
	default:
		return time.Duration(spec.TimeoutMS) * time.Millisecond
	}
}

// childEnv builds the environment for a job command. It deliberately does NOT inherit
// the daemon's full environment (which may hold operator secrets); it seeds a minimal,
// predictable base, adds any explicitly passed-through daemon vars, then overlays the
// job's declared Env.
func childEnv(extra map[string]string, passthrough []string) []string {
	base := map[string]string{}
	for _, key := range []string{"PATH", "HOME", "USER", "SHELL", "TMPDIR", "LANG", "TZ"} {
		if value, ok := os.LookupEnv(key); ok {
			base[key] = value
		}
	}
	if base["PATH"] == "" {
		base["PATH"] = "/usr/bin:/bin:/usr/sbin:/sbin"
	}
	// Explicit passthrough: inherit named vars from the daemon environment.
	for _, key := range passthrough {
		if value, ok := os.LookupEnv(key); ok {
			base[key] = value
		}
	}
	for key, value := range extra {
		base[key] = value
	}
	keys := make([]string, 0, len(base))
	for key := range base {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	result := make([]string, 0, len(keys))
	for _, key := range keys {
		result = append(result, key+"="+base[key])
	}
	return result
}

// pruneRuns keeps only the newest runHistoryLimit run records for a job and deletes the
// older records together with their stdout/stderr logs, so history cannot grow without
// bound. Best-effort: failures are ignored, the just-written record is never removed.
func (m *Manager) pruneRuns(job string) {
	dir := filepath.Join(m.paths.RunsDir, job)
	entries, err := os.ReadDir(dir)
	if err != nil {
		return
	}
	records := make([]string, 0, len(entries))
	for _, entry := range entries {
		if !entry.IsDir() && filepath.Ext(entry.Name()) == ".json" {
			records = append(records, entry.Name())
		}
	}
	if len(records) <= runHistoryLimit {
		return
	}
	sort.Strings(records) // run ids are time-sortable
	for _, name := range records[:len(records)-runHistoryLimit] {
		path := filepath.Join(dir, name)
		if data, err := os.ReadFile(path); err == nil {
			var record RunRecord
			if json.Unmarshal(data, &record) == nil {
				if record.StdoutPath != "" {
					_ = os.Remove(record.StdoutPath)
				}
				if record.StderrPath != "" {
					_ = os.Remove(record.StderrPath)
				}
			}
		}
		_ = os.Remove(path)
	}
}

func (m *Manager) jobLock(name string) *sync.Mutex {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.locks[name] == nil {
		m.locks[name] = &sync.Mutex{}
	}
	return m.locks[name]
}
