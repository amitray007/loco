package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/amitray007/loco/internal/cronjob"
	"github.com/amitray007/loco/internal/paths"
	"github.com/amitray007/loco/internal/registry"
)

const SchemaVersion = 1

type Spec struct {
	Command        []string          `json:"command"`
	WorkingDir     string            `json:"working_dir,omitempty"`
	Env            map[string]string `json:"env,omitempty"`
	PassthroughEnv []string          `json:"passthrough_env,omitempty"`
	Restart        string            `json:"restart"`
	Enabled        bool              `json:"enabled"`
	BackoffInitial int64             `json:"backoff_initial_ms,omitempty"`
	BackoffMax     int64             `json:"backoff_max_ms,omitempty"`
}

type Status struct {
	Name         string `json:"name"`
	State        string `json:"state"`
	Enabled      bool   `json:"enabled"`
	PID          int    `json:"pid,omitempty"`
	Restarts     int    `json:"restarts"`
	LastExitCode *int   `json:"last_exit_code,omitempty"`
	LastError    string `json:"last_error,omitempty"`
	StartedAt    string `json:"started_at,omitempty"`
}

type ListResult struct {
	SchemaVersion int      `json:"schema_version"`
	Services      []Status `json:"services"`
}

type ControlResult struct {
	SchemaVersion int    `json:"schema_version"`
	Name          string `json:"name"`
	State         string `json:"state"`
}

type Manager struct {
	store *registry.Store
	paths paths.Paths

	ctx       context.Context
	cancel    context.CancelFunc
	reloadMu  sync.Mutex
	mu        sync.RWMutex
	services  map[string]*process
	sequence  atomic.Uint64
	stopping  atomic.Bool
	onFailure func(string, cronjob.RunRecord)
}

type process struct {
	name      string
	spec      Spec
	paths     paths.Paths
	ctx       context.Context
	cancel    context.CancelFunc
	done      chan struct{}
	nextID    func() string
	onFailure func(string, cronjob.RunRecord)
	writePID  func(string, pidRecord)
	clearPID  func(string)

	mu     sync.RWMutex
	status Status
	cmd    *exec.Cmd
}

func New(store *registry.Store, p paths.Paths) *Manager {
	ctx, cancel := context.WithCancel(context.Background())
	return &Manager{store: store, paths: p, ctx: ctx, cancel: cancel, services: make(map[string]*process)}
}
func (m *Manager) SetFailureHandler(handler func(string, cronjob.RunRecord)) { m.onFailure = handler }

// pidRecord tracks a spawned child so a later daemon (after a SIGKILL that skipped
// graceful shutdown) can find and reap the orphan instead of leaving it running and
// spawning a duplicate.
type pidRecord struct {
	PID       int    `json:"pid"`
	StartedAt string `json:"started_at"`
	Command   string `json:"command"`
}

func (m *Manager) pidPath(name string) string {
	return filepath.Join(m.paths.RunDir, "svc-pids", name+".json")
}

func (m *Manager) writePID(name string, rec pidRecord) {
	path := m.pidPath(name)
	if os.MkdirAll(filepath.Dir(path), 0o700) != nil {
		return
	}
	data, err := json.Marshal(rec)
	if err != nil {
		return
	}
	_ = os.WriteFile(path, data, 0o600)
}

func (m *Manager) clearPID(name string) { _ = os.Remove(m.pidPath(name)) }

// reconcileOrphans runs once at startup: any recorded child process group still alive
// belongs to a previous daemon that died without cleaning up. Kill it so we do not run
// two copies of a service after an unclean restart.
func (m *Manager) reconcileOrphans() {
	dir := filepath.Join(m.paths.RunDir, "svc-pids")
	entries, err := os.ReadDir(dir)
	if err != nil {
		return
	}
	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".json" {
			continue
		}
		path := filepath.Join(dir, entry.Name())
		data, err := os.ReadFile(path)
		if err != nil {
			continue
		}
		var rec pidRecord
		if json.Unmarshal(data, &rec) != nil || rec.PID <= 0 {
			_ = os.Remove(path)
			continue
		}
		// Signal 0 probes existence; if the process group is alive, terminate it.
		if syscall.Kill(-rec.PID, 0) == nil {
			_ = syscall.Kill(-rec.PID, syscall.SIGTERM)
			// Give it a moment, then SIGKILL any survivors.
			time.Sleep(200 * time.Millisecond)
			_ = syscall.Kill(-rec.PID, syscall.SIGKILL)
		}
		_ = os.Remove(path)
	}
}

func (m *Manager) Start() error {
	m.reconcileOrphans()
	return m.Reload()
}

func (m *Manager) Stop() {
	if !m.stopping.CompareAndSwap(false, true) {
		return
	}
	m.mu.RLock()
	processes := make([]*process, 0, len(m.services))
	for _, proc := range m.services {
		processes = append(processes, proc)
	}
	m.mu.RUnlock()
	for _, proc := range processes {
		proc.stop()
	}
	m.cancel()
}

func (m *Manager) Reload() error {
	m.reloadMu.Lock()
	defer m.reloadMu.Unlock()
	if m.stopping.Load() {
		return nil
	}
	doc := m.store.Snapshot()
	desired := make(map[string]Spec)
	for name, job := range doc.Jobs {
		if job.Kind != "svc" {
			continue
		}
		spec, err := DecodeSpec(job.Spec)
		if err != nil {
			continue
		}
		if spec.Enabled {
			desired[name] = spec
		}
	}

	m.mu.RLock()
	current := make(map[string]*process, len(m.services))
	for name, proc := range m.services {
		current[name] = proc
	}
	m.mu.RUnlock()
	for name, proc := range current {
		spec, wanted := desired[name]
		if !wanted || !specEqual(proc.spec, spec) {
			proc.stop()
			m.mu.Lock()
			delete(m.services, name)
			m.mu.Unlock()
		}
	}
	for name, spec := range desired {
		m.mu.RLock()
		existing := m.services[name]
		m.mu.RUnlock()
		if existing != nil {
			continue
		}
		ctx, cancel := context.WithCancel(m.ctx)
		proc := &process{
			name: name, spec: spec, paths: m.paths, ctx: ctx, cancel: cancel, done: make(chan struct{}),
			nextID: func() string {
				return fmt.Sprintf("%s-%d-%d", time.Now().UTC().Format("20060102T150405.000000000Z"), os.Getpid(), m.sequence.Add(1))
			},
			status:    Status{Name: name, State: "starting", Enabled: true},
			onFailure: m.onFailure,
			writePID:  m.writePID,
			clearPID:  m.clearPID,
		}
		m.mu.Lock()
		m.services[name] = proc
		m.mu.Unlock()
		go proc.run()
	}
	return nil
}

func (m *Manager) SetEnabled(name string, enabled bool) (ControlResult, error) {
	job, ok := m.store.Snapshot().Jobs[name]
	if !ok || job.Kind != "svc" {
		return ControlResult{}, fmt.Errorf("service %q not found", name)
	}
	spec, err := DecodeSpec(job.Spec)
	if err != nil {
		return ControlResult{}, err
	}
	spec.Enabled = enabled
	raw, err := json.Marshal(spec)
	if err != nil {
		return ControlResult{}, err
	}
	if _, err := m.store.Put(registry.PutInput{Name: name, Kind: "svc", Spec: raw}); err != nil {
		return ControlResult{}, err
	}
	if err := m.Reload(); err != nil {
		return ControlResult{}, err
	}
	state := "stopped"
	if enabled {
		state = "starting"
	}
	return ControlResult{SchemaVersion: SchemaVersion, Name: name, State: state}, nil
}

func (m *Manager) Restart(name string) (ControlResult, error) {
	job, ok := m.store.Snapshot().Jobs[name]
	if !ok || job.Kind != "svc" {
		return ControlResult{}, fmt.Errorf("service %q not found", name)
	}
	spec, err := DecodeSpec(job.Spec)
	if err != nil {
		return ControlResult{}, err
	}
	if !spec.Enabled {
		return m.SetEnabled(name, true)
	}
	m.mu.RLock()
	proc := m.services[name]
	m.mu.RUnlock()
	if proc != nil {
		proc.stop()
		m.mu.Lock()
		delete(m.services, name)
		m.mu.Unlock()
	}
	if err := m.Reload(); err != nil {
		return ControlResult{}, err
	}
	return ControlResult{SchemaVersion: SchemaVersion, Name: name, State: "starting"}, nil
}

func (m *Manager) List() ListResult {
	doc := m.store.Snapshot()
	names := make([]string, 0)
	for name, job := range doc.Jobs {
		if job.Kind == "svc" {
			names = append(names, name)
		}
	}
	sort.Strings(names)
	result := ListResult{SchemaVersion: SchemaVersion, Services: make([]Status, 0, len(names))}
	for _, name := range names {
		job := doc.Jobs[name]
		spec, err := DecodeSpec(job.Spec)
		if err != nil {
			result.Services = append(result.Services, Status{Name: name, State: "invalid", LastError: err.Error()})
			continue
		}
		m.mu.RLock()
		proc := m.services[name]
		m.mu.RUnlock()
		if proc == nil {
			result.Services = append(result.Services, Status{Name: name, State: "stopped", Enabled: spec.Enabled})
			continue
		}
		result.Services = append(result.Services, proc.snapshot())
	}
	return result
}

func DecodeSpec(raw json.RawMessage) (Spec, error) {
	var spec Spec
	if err := json.Unmarshal(raw, &spec); err != nil {
		return Spec{}, fmt.Errorf("decode service spec: %w", err)
	}
	if len(spec.Command) == 0 || spec.Command[0] == "" {
		return Spec{}, errors.New("command must contain an executable")
	}
	if spec.Restart == "" {
		spec.Restart = "always"
	}
	if spec.Restart != "always" && spec.Restart != "on-failure" && spec.Restart != "never" {
		return Spec{}, fmt.Errorf("restart must be always, on-failure, or never")
	}
	if spec.BackoffInitial <= 0 {
		spec.BackoffInitial = 1000
	}
	if spec.BackoffMax <= 0 {
		spec.BackoffMax = 30000
	}
	if spec.BackoffMax < spec.BackoffInitial {
		return Spec{}, errors.New("backoff max must be greater than or equal to initial")
	}
	return spec, nil
}

func (p *process) run() {
	defer close(p.done)
	backoff := time.Duration(p.spec.BackoffInitial) * time.Millisecond
	maxBackoff := time.Duration(p.spec.BackoffMax) * time.Millisecond
	for {
		if p.ctx.Err() != nil {
			p.setStopped()
			return
		}
		runID := p.nextID()
		started := time.Now().UTC()
		logDir := filepath.Join(p.paths.LogDir, "jobs", p.name)
		_ = os.MkdirAll(logDir, 0o700)
		stdoutPath := filepath.Join(logDir, runID+".stdout.log")
		stderrPath := filepath.Join(logDir, runID+".stderr.log")
		stdout, outErr := os.OpenFile(stdoutPath, os.O_CREATE|os.O_WRONLY|os.O_EXCL, 0o600)
		stderr, errErr := os.OpenFile(stderrPath, os.O_CREATE|os.O_WRONLY|os.O_EXCL, 0o600)
		if outErr != nil || errErr != nil {
			if stdout != nil {
				_ = stdout.Close()
			}
			if stderr != nil {
				_ = stderr.Close()
			}
			p.setError(errors.Join(outErr, errErr))
			p.setStopped()
			return
		}
		cmd := exec.CommandContext(p.ctx, p.spec.Command[0], p.spec.Command[1:]...)
		cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
		cmd.Dir, cmd.Stdout, cmd.Stderr, cmd.Env = p.spec.WorkingDir, stdout, stderr, mergedEnv(p.spec.Env, p.spec.PassthroughEnv)
		if err := cmd.Start(); err != nil {
			_ = stdout.Close()
			_ = stderr.Close()
			p.setError(err)
			p.writeRecord(runID, started, time.Now().UTC(), -1, "failed", err.Error(), stdoutPath, stderrPath)
			if !p.shouldRestart(-1) {
				p.setStopped()
				return
			}
		} else {
			p.setRunning(cmd, started)
			pgid := cmd.Process.Pid
			err := cmd.Wait()
			// Reap any grandchildren still in the group BEFORE the leader's pid can be
			// recycled. Kill(0) confirms the group still exists; we do not blindly
			// signal a pid that Wait may have already freed.
			if syscall.Kill(-pgid, 0) == nil {
				_ = syscall.Kill(-pgid, syscall.SIGKILL)
			}
			_ = stdout.Close()
			_ = stderr.Close()
			finished := time.Now().UTC()
			exitCode, status, message := 0, "success", ""
			if err != nil {
				status, message = "failed", err.Error()
				exitCode = -1
				var exitErr *exec.ExitError
				if errors.As(err, &exitErr) {
					exitCode = exitErr.ExitCode()
				}
			}
			if p.ctx.Err() != nil {
				status, message = "stopped", "service stopped"
			}
			p.writeRecord(runID, started, finished, exitCode, status, message, stdoutPath, stderrPath)
			p.setExited(exitCode, message)
			if p.ctx.Err() != nil || !p.shouldRestart(exitCode) {
				p.setStopped()
				return
			}
			if finished.Sub(started) >= 30*time.Second {
				backoff = time.Duration(p.spec.BackoffInitial) * time.Millisecond
			}
		}
		p.setBackoff()
		timer := time.NewTimer(backoff)
		select {
		case <-p.ctx.Done():
			timer.Stop()
			p.setStopped()
			return
		case <-timer.C:
		}
		if backoff < maxBackoff {
			backoff *= 2
			if backoff > maxBackoff {
				backoff = maxBackoff
			}
		}
		p.mu.Lock()
		p.status.Restarts++
		p.mu.Unlock()
	}
}

func (p *process) stop() {
	p.mu.RLock()
	cmd := p.cmd
	p.mu.RUnlock()
	if cmd != nil && cmd.Process != nil {
		pgid := cmd.Process.Pid
		_ = syscall.Kill(-pgid, syscall.SIGTERM)
		// Escalate to SIGKILL if the child ignores SIGTERM, so stop() cannot hang
		// forever on an unresponsive service.
		go func() {
			timer := time.NewTimer(5 * time.Second)
			defer timer.Stop()
			select {
			case <-p.done:
			case <-timer.C:
				if syscall.Kill(-pgid, 0) == nil {
					_ = syscall.Kill(-pgid, syscall.SIGKILL)
				}
			}
		}()
	}
	p.cancel()
	<-p.done
}
func (p *process) snapshot() Status { p.mu.RLock(); defer p.mu.RUnlock(); return p.status }
func (p *process) setRunning(cmd *exec.Cmd, started time.Time) {
	p.mu.Lock()
	p.cmd = cmd
	p.status.State = "running"
	p.status.PID = cmd.Process.Pid
	p.status.StartedAt = started.Format(time.RFC3339Nano)
	p.status.LastError = ""
	p.mu.Unlock()
	if p.writePID != nil {
		p.writePID(p.name, pidRecord{PID: cmd.Process.Pid, StartedAt: started.Format(time.RFC3339Nano), Command: strings.Join(p.spec.Command, " ")})
	}
}
func (p *process) setExited(code int, message string) {
	p.mu.Lock()
	p.cmd = nil
	p.status.PID = 0
	p.status.LastExitCode = &code
	p.status.LastError = message
	p.mu.Unlock()
	// The child is reaped; drop its pid record so a future daemon won't try to kill a
	// stale (possibly recycled) pid.
	if p.clearPID != nil {
		p.clearPID(p.name)
	}
}
func (p *process) setBackoff() {
	p.mu.Lock()
	p.status.State = "backoff"
	p.status.PID = 0
	p.mu.Unlock()
}
func (p *process) setStopped() {
	p.mu.Lock()
	p.status.State = "stopped"
	p.status.PID = 0
	p.cmd = nil
	p.mu.Unlock()
}
func (p *process) setError(err error) { p.mu.Lock(); p.status.LastError = err.Error(); p.mu.Unlock() }
func (p *process) shouldRestart(code int) bool {
	return p.spec.Restart == "always" || (p.spec.Restart == "on-failure" && code != 0)
}

func (p *process) writeRecord(id string, started, finished time.Time, exitCode int, status, message, stdout, stderr string) {
	record := cronjob.RunRecord{SchemaVersion: cronjob.SchemaVersion, ID: id, Job: p.name, Trigger: "supervisor", Status: status, StartedAt: started.Format(time.RFC3339Nano), FinishedAt: finished.Format(time.RFC3339Nano), DurationMS: finished.Sub(started).Milliseconds(), ExitCode: exitCode, Error: message, StdoutPath: stdout, StderrPath: stderr}
	dir := filepath.Join(p.paths.RunsDir, p.name)
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
	name := tmp.Name()
	defer func() { _ = os.Remove(name) }()
	if tmp.Chmod(0o600) != nil {
		_ = tmp.Close()
		return
	}
	if _, err = tmp.Write(data); err != nil {
		_ = tmp.Close()
		return
	}
	if tmp.Sync() != nil {
		_ = tmp.Close()
		return
	}
	if tmp.Close() != nil {
		return
	}
	_ = os.Rename(name, filepath.Join(dir, id+".json"))
	if status == "failed" && p.onFailure != nil {
		p.onFailure(p.name, record)
	}
}

// mergedEnv builds the environment for a supervised child. It deliberately does NOT
// inherit the daemon's full environment (which may hold operator secrets); it seeds a
// minimal, predictable base and overlays the service's declared Env.
func mergedEnv(extra map[string]string, passthrough []string) []string {
	values := make(map[string]string)
	for _, key := range []string{"PATH", "HOME", "USER", "SHELL", "TMPDIR", "LANG", "TZ"} {
		if value, ok := os.LookupEnv(key); ok {
			values[key] = value
		}
	}
	if values["PATH"] == "" {
		values["PATH"] = "/usr/bin:/bin:/usr/sbin:/sbin"
	}
	for _, key := range passthrough {
		if value, ok := os.LookupEnv(key); ok {
			values[key] = value
		}
	}
	for key, value := range extra {
		values[key] = value
	}
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	result := make([]string, 0, len(keys))
	for _, key := range keys {
		result = append(result, key+"="+values[key])
	}
	return result
}

func specEqual(a, b Spec) bool {
	left, _ := json.Marshal(a)
	right, _ := json.Marshal(b)
	return string(left) == string(right)
}
