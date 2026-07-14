package daemon

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"github.com/amitray007/loco/internal/paths"
)

var ErrAlreadyRunning = errors.New("daemon is already running")

type runtimeState struct {
	SchemaVersion int    `json:"schema_version"`
	PID           int    `json:"pid"`
	StartedAt     string `json:"started_at"`
	Version       string `json:"version"`
}

func Run(ctx context.Context, p paths.Paths, version string) error {
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
			serveConnection(conn, state, started)
		}()
	}
}

func serveConnection(conn net.Conn, state runtimeState, started time.Time) {
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(2 * time.Second))
	scanner := bufio.NewScanner(conn)
	if !scanner.Scan() || scanner.Text() != "status" {
		return
	}
	status := Health{
		SchemaVersion: SchemaVersion,
		Healthy:       true,
		PID:           state.PID,
		StartedAt:     state.StartedAt,
		UptimeSeconds: int64(time.Since(started).Seconds()),
		Version:       state.Version,
	}
	_ = json.NewEncoder(conn).Encode(status)
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
	defer os.Remove(tmpName)
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
