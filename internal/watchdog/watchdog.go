package watchdog

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/amitray007/loco/internal/notification"
	"os"
	"path/filepath"
	"time"
)

const SchemaVersion = 1

type Checker func(context.Context) error
type State struct {
	SchemaVersion int    `json:"schema_version"`
	Failures      int    `json:"failures"`
	Alerted       bool   `json:"alerted"`
	LastCheck     string `json:"last_check"`
	LastError     string `json:"last_error,omitempty"`
}
type Result struct {
	SchemaVersion int  `json:"schema_version"`
	Healthy       bool `json:"healthy"`
	Failures      int  `json:"failures"`
	Alerted       bool `json:"alerted"`
	AlertSent     bool `json:"alert_sent"`
}

func Check(ctx context.Context, statePath string, threshold int, checker Checker, sender notification.Sender) (Result, error) {
	if threshold < 1 {
		return Result{}, errors.New("watchdog threshold must be positive")
	}
	state, err := load(statePath)
	if err != nil {
		return Result{}, err
	}
	now := time.Now().UTC()
	checkCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	checkErr := checker(checkCtx)
	cancel()
	state.LastCheck = now.Format(time.RFC3339Nano)
	if checkErr == nil {
		state.Failures = 0
		state.Alerted = false
		state.LastError = ""
		if err := save(statePath, state); err != nil {
			return Result{}, err
		}
		return Result{SchemaVersion: SchemaVersion, Healthy: true}, nil
	}
	state.Failures++
	state.LastError = checkErr.Error()
	if err := save(statePath, state); err != nil {
		return Result{}, err
	}
	sent := false
	if state.Failures >= threshold && !state.Alerted {
		if sender == nil {
			return Result{}, errors.New("watchdog notification sender unavailable")
		}
		sendCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		err := sender.Send(sendCtx, "loco daemon is down", fmt.Sprintf("loco has failed %d consecutive health checks", state.Failures))
		cancel()
		if err != nil {
			return Result{}, err
		}
		state.Alerted = true
		sent = true
		if err := save(statePath, state); err != nil {
			return Result{}, err
		}
	}
	return Result{SchemaVersion: SchemaVersion, Healthy: false, Failures: state.Failures, Alerted: state.Alerted, AlertSent: sent}, nil
}
func load(path string) (State, error) {
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return State{SchemaVersion: SchemaVersion}, nil
	}
	if err != nil {
		return State{}, err
	}
	var state State
	// A corrupt or schema-mismatched state file must never permanently disable the
	// watchdog — the one thing meant to guard the daemon. Reset to a fresh state and
	// keep watching, rather than returning an error that stops all future checks.
	if err := json.Unmarshal(data, &state); err != nil {
		return State{SchemaVersion: SchemaVersion}, nil
	}
	if state.SchemaVersion != SchemaVersion {
		return State{SchemaVersion: SchemaVersion}, nil
	}
	return state, nil
}
func save(path string, state State) error {
	data, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(path), ".watchdog-*")
	if err != nil {
		return err
	}
	name := tmp.Name()
	defer func() { _ = os.Remove(name) }()
	if err = tmp.Chmod(0o600); err != nil {
		_ = tmp.Close()
		return err
	}
	if _, err = tmp.Write(data); err != nil {
		_ = tmp.Close()
		return err
	}
	if err = tmp.Sync(); err != nil {
		_ = tmp.Close()
		return err
	}
	if err = tmp.Close(); err != nil {
		return err
	}
	return os.Rename(name, path)
}
