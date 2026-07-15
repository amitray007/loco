package daemon

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/amitray007/loco/internal/cronjob"
	"github.com/amitray007/loco/internal/events"
	"github.com/amitray007/loco/internal/notification"
	"github.com/amitray007/loco/internal/paths"
	"github.com/amitray007/loco/internal/registry"
	"github.com/amitray007/loco/internal/service"
)

var ErrAlreadyRunning = errors.New("daemon is already running")

type runtimeState struct {
	SchemaVersion int    `json:"schema_version"`
	PID           int    `json:"pid"`
	StartedAt     string `json:"started_at"`
	Version       string `json:"version"`
}

// maxUnixSocketPath is the portable ceiling for a unix socket path (sun_path is 104
// bytes on macOS/BSD, 108 on Linux). We validate against the smaller bound so a config
// dir that works on one OS works on both.
const maxUnixSocketPath = 104

func Run(ctx context.Context, p paths.Paths, version string) error {
	// Preflight the socket path length: a too-long path fails deep in the net stack
	// with an opaque "bind: invalid argument". Catch it here with an actionable message.
	if len(p.Socket) >= maxUnixSocketPath {
		return fmt.Errorf("socket path is too long (%d >= %d bytes): choose a shorter --config-dir\n  %s",
			len(p.Socket), maxUnixSocketPath, p.Socket)
	}
	if err := p.EnsureRuntimeDirs(); err != nil {
		return err
	}

	lock, err := acquireLock(p.Lock)
	if err != nil {
		return err
	}
	defer func() {
		_ = syscall.Flock(int(lock.Fd()), syscall.LOCK_UN)
		_ = lock.Close()
	}()
	registryStore, err := registry.Open(p.Registry)
	if err != nil {
		return fmt.Errorf("open registry: %w", err)
	}
	notifier := notification.New(p.NotifyState, p.Alerts, notification.DesktopSender())
	automaticChannel := os.Getenv("LOCO_NOTIFY_CHANNEL")
	if automaticChannel == "" {
		automaticChannel = "desktop"
	}
	failureHandler := func(name string, record cronjob.RunRecord) {
		_, _ = notifier.Send(context.Background(), notification.Input{Title: "loco job failed", Body: fmt.Sprintf("%s exited with code %d", name, record.ExitCode), Channel: automaticChannel, Group: "job-failure:" + name})
	}
	cronManager := cronjob.New(registryStore, p)
	cronManager.SetFailureHandler(failureHandler)
	if err := cronManager.Start(); err != nil {
		return fmt.Errorf("start cron manager: %w", err)
	}
	defer cronManager.Stop()
	serviceManager := service.New(registryStore, p)
	serviceManager.SetFailureHandler(failureHandler)
	if err := serviceManager.Start(); err != nil {
		return fmt.Errorf("start service manager: %w", err)
	}
	defer serviceManager.Stop()
	eventStore := events.New(p.EventsDir, p.Alerts)
	if _, err := eventStore.CompactAll(); err != nil {
		return fmt.Errorf("maintain event store: %w", err)
	}

	// Only the process holding daemon.lock may remove a stale socket.
	if err := os.Remove(p.Socket); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("remove stale socket: %w", err)
	}
	listener, err := net.Listen("unix", p.Socket)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", p.Socket, err)
	}
	defer func() {
		_ = listener.Close()
		_ = os.Remove(p.Socket)
		_ = os.Remove(p.State)
	}()
	if err := os.Chmod(p.Socket, 0o600); err != nil {
		return fmt.Errorf("secure daemon socket: %w", err)
	}

	started := time.Now().UTC()
	state := runtimeState{
		SchemaVersion: SchemaVersion,
		PID:           os.Getpid(),
		StartedAt:     started.Format(time.RFC3339Nano),
		Version:       version,
	}
	if err := writeJSONAtomic(p.State, state, 0o600); err != nil {
		return fmt.Errorf("write runtime state: %w", err)
	}
	maintenanceCtx, stopMaintenance := context.WithCancel(ctx)
	maintenanceDone := make(chan struct{})
	go func() {
		defer close(maintenanceDone)
		ticker := time.NewTicker(6 * time.Hour)
		defer ticker.Stop()
		for {
			select {
			case <-maintenanceCtx.Done():
				return
			case <-ticker.C:
				_, _ = eventStore.CompactAll()
			}
		}
	}()
	defer func() { stopMaintenance(); <-maintenanceDone }()

	var connections sync.WaitGroup
	done := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			_ = listener.Close()
		case <-done:
		}
	}()
	defer close(done)

	for {
		conn, err := listener.Accept()
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, net.ErrClosed) {
				connections.Wait()
				return ctx.Err()
			}
			return fmt.Errorf("accept connection: %w", err)
		}
		connections.Add(1)
		go func() {
			defer connections.Done()
			serveConnection(conn, state, started, p, registryStore, cronManager, serviceManager, eventStore, notifier)
		}()
	}
}

// maxRequestBytes caps a single RPC request body so one socket-local client cannot
// exhaust daemon memory with an unbounded JSON payload. The largest legitimate body is
// an event emit, itself capped at 1 MiB; 2 MiB leaves headroom for envelope overhead.
const maxRequestBytes = 2 << 20

func serveConnection(conn net.Conn, state runtimeState, started time.Time, p paths.Paths, registryStore *registry.Store, cronManager *cronjob.Manager, serviceManager *service.Manager, eventStore *events.Store, notifier *notification.Manager) {
	defer func() { _ = conn.Close() }()
	_ = conn.SetDeadline(time.Now().Add(2 * time.Second))
	// Bound the whole connection: the command word plus its JSON body may not exceed
	// the request cap.
	reader := bufio.NewReader(io.LimitReader(conn, maxRequestBytes))
	command, err := reader.ReadString('\n')
	if err != nil {
		return
	}
	encoder := json.NewEncoder(conn)
	switch strings.TrimSpace(command) {
	case "status":
		_ = encoder.Encode(Health{
			SchemaVersion: SchemaVersion,
			Healthy:       true,
			PID:           state.PID,
			StartedAt:     state.StartedAt,
			UptimeSeconds: int64(time.Since(started).Seconds()),
			Version:       state.Version,
		})
	case "status.overview":
		result, err := buildOverview(state, started, p, registryStore, serviceManager, eventStore)
		if err != nil {
			_ = encoder.Encode(wireError{SchemaVersion: SchemaVersion, Error: err.Error()})
			return
		}
		_ = encoder.Encode(result)
	case "registry.put":
		var input registry.PutInput
		if err := json.NewDecoder(reader).Decode(&input); err != nil {
			_ = encoder.Encode(wireError{SchemaVersion: SchemaVersion, Error: "decode registry entry: " + err.Error()})
			return
		}
		result, err := registryStore.Put(input)
		if err != nil {
			_ = encoder.Encode(wireError{SchemaVersion: SchemaVersion, Error: err.Error()})
			return
		}
		if result.Changed {
			_ = cronManager.Reload()
			_ = serviceManager.Reload()
		}
		_ = encoder.Encode(result)
	case "service.list":
		_ = encoder.Encode(serviceManager.List())
	case "service.control":
		var request struct {
			Name   string `json:"name"`
			Action string `json:"action"`
		}
		if err := json.NewDecoder(reader).Decode(&request); err != nil {
			_ = encoder.Encode(wireError{SchemaVersion: SchemaVersion, Error: "decode service request: " + err.Error()})
			return
		}
		var result service.ControlResult
		var err error
		switch request.Action {
		case "start":
			result, err = serviceManager.SetEnabled(request.Name, true)
		case "stop":
			result, err = serviceManager.SetEnabled(request.Name, false)
		case "restart":
			result, err = serviceManager.Restart(request.Name)
		default:
			err = fmt.Errorf("unknown service action %q", request.Action)
		}
		if err != nil {
			_ = encoder.Encode(wireError{SchemaVersion: SchemaVersion, Error: err.Error()})
			return
		}
		_ = cronManager.Reload()
		_ = encoder.Encode(result)
	case "events.emit":
		var input events.EmitInput
		if err := json.NewDecoder(reader).Decode(&input); err != nil {
			_ = encoder.Encode(wireError{SchemaVersion: SchemaVersion, Error: "decode event: " + err.Error()})
			return
		}
		result, err := eventStore.Emit(input)
		if err != nil {
			_ = encoder.Encode(wireError{SchemaVersion: SchemaVersion, Error: err.Error()})
			return
		}
		_ = encoder.Encode(result)
	case "events.read":
		var request struct {
			Topic  string `json:"topic"`
			Cursor string `json:"cursor"`
			Limit  int    `json:"limit"`
		}
		if err := json.NewDecoder(reader).Decode(&request); err != nil {
			_ = encoder.Encode(wireError{SchemaVersion: SchemaVersion, Error: "decode read request: " + err.Error()})
			return
		}
		result, err := eventStore.Read(request.Topic, request.Cursor, request.Limit)
		if err != nil {
			_ = encoder.Encode(wireError{SchemaVersion: SchemaVersion, Error: err.Error()})
			return
		}
		_ = encoder.Encode(result)
	case "events.peek":
		var request struct {
			Topic   string `json:"topic"`
			Tail    int    `json:"tail"`
			FromSeq uint64 `json:"from_seq"`
		}
		if err := json.NewDecoder(reader).Decode(&request); err != nil {
			_ = encoder.Encode(wireError{SchemaVersion: SchemaVersion, Error: "decode peek request: " + err.Error()})
			return
		}
		result, err := eventStore.Peek(request.Topic, request.Tail, request.FromSeq)
		if err != nil {
			_ = encoder.Encode(wireError{SchemaVersion: SchemaVersion, Error: err.Error()})
			return
		}
		_ = encoder.Encode(result)
	case "events.ack":
		var request struct {
			Topic   string `json:"topic"`
			Cursor  string `json:"cursor"`
			Through string `json:"through"`
		}
		if err := json.NewDecoder(reader).Decode(&request); err != nil {
			_ = encoder.Encode(wireError{SchemaVersion: SchemaVersion, Error: "decode ack request: " + err.Error()})
			return
		}
		result, err := eventStore.Ack(request.Topic, request.Cursor, request.Through)
		if err != nil {
			_ = encoder.Encode(wireError{SchemaVersion: SchemaVersion, Error: err.Error()})
			return
		}
		// Compaction is O(n) in topic size; running it on every ack made ack latency
		// grow linearly with history. It now runs only on the 6-hour maintenance loop
		// (and CompactAll at startup), keeping ack cheap.
		_ = encoder.Encode(result)
	case "notify.send":
		var input notification.Input
		if err := json.NewDecoder(reader).Decode(&input); err != nil {
			_ = encoder.Encode(wireError{SchemaVersion: SchemaVersion, Error: "decode notification: " + err.Error()})
			return
		}
		result, err := notifier.Send(context.Background(), input)
		if err != nil {
			_ = encoder.Encode(wireError{SchemaVersion: SchemaVersion, Error: err.Error()})
			return
		}
		_ = encoder.Encode(result)
	case "registry.delete":
		var request struct {
			Name string `json:"name"`
		}
		if err := json.NewDecoder(reader).Decode(&request); err != nil {
			_ = encoder.Encode(wireError{SchemaVersion: SchemaVersion, Error: "decode delete request: " + err.Error()})
			return
		}
		result, err := registryStore.Delete(request.Name)
		if err != nil {
			_ = encoder.Encode(wireError{SchemaVersion: SchemaVersion, Error: err.Error()})
			return
		}
		if result.Removed {
			// Drop the now-absent job from the running managers (stops a deleted service).
			_ = cronManager.Reload()
			_ = serviceManager.Reload()
		}
		_ = encoder.Encode(result)
	case "registry.list":
		_ = encoder.Encode(registryStore.Snapshot())
	case "cron.run-now":
		var request struct {
			Name string `json:"name"`
		}
		if err := json.NewDecoder(reader).Decode(&request); err != nil {
			_ = encoder.Encode(wireError{SchemaVersion: SchemaVersion, Error: "decode cron request: " + err.Error()})
			return
		}
		result, err := cronManager.RunNow(request.Name)
		if err != nil {
			_ = encoder.Encode(wireError{SchemaVersion: SchemaVersion, Error: err.Error()})
			return
		}
		_ = encoder.Encode(result)
	default:
		_ = encoder.Encode(wireError{SchemaVersion: SchemaVersion, Error: "unknown daemon operation"})
	}
}

func acquireLock(path string) (*os.File, error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open daemon lock: %w", err)
	}
	if err := syscall.Flock(int(file.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		_ = file.Close()
		if errors.Is(err, syscall.EWOULDBLOCK) {
			return nil, ErrAlreadyRunning
		}
		return nil, fmt.Errorf("lock daemon state: %w", err)
	}
	return file, nil
}

func writeJSONAtomic(path string, value any, mode os.FileMode) error {
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')
	tmp, err := os.CreateTemp(filepath.Dir(path), ".daemon-state-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer func() { _ = os.Remove(tmpName) }()
	if err := tmp.Chmod(mode); err != nil {
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
	return os.Rename(tmpName, path)
}
