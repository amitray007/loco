package notification

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

const SchemaVersion = 1
const DefaultGroupWindow = time.Hour

type Input struct {
	Title   string `json:"title"`
	Body    string `json:"body"`
	Channel string `json:"channel,omitempty"`
	Group   string `json:"group,omitempty"`
}
type Result struct {
	SchemaVersion int    `json:"schema_version"`
	Delivered     bool   `json:"delivered"`
	Suppressed    bool   `json:"suppressed"`
	Group         string `json:"group,omitempty"`
	Timestamp     string `json:"ts"`
}
type Alert struct {
	SchemaVersion int    `json:"schema_version"`
	ID            string `json:"id"`
	Timestamp     string `json:"ts"`
	Kind          string `json:"kind"`
	Title         string `json:"title"`
	Message       string `json:"message"`
	Channel       string `json:"channel"`
	Group         string `json:"group,omitempty"`
}
type groupState struct {
	SchemaVersion int               `json:"schema_version"`
	Groups        map[string]string `json:"groups"`
}

type Sender interface {
	Send(context.Context, string, string) error
}

// FileSender writes notifications to the alerts log instead of the desktop. It lets the
// watchdog (and any headless/CI context) honor LOCO_NOTIFY_CHANNEL=file so a background
// health check never posts a real macOS banner or requires a GUI session.
type FileSender struct{ AlertsPath string }

func (f FileSender) Send(_ context.Context, title, body string) error {
	return appendJSONL(f.AlertsPath, Alert{SchemaVersion: SchemaVersion, ID: fmt.Sprintf("notify-%d", time.Now().UnixNano()), Timestamp: time.Now().UTC().Format(time.RFC3339Nano), Kind: "notification", Title: title, Message: body, Channel: "file"})
}

// SenderForChannel returns the sender matching LOCO_NOTIFY_CHANNEL semantics: "file"
// routes to the alerts log, anything else (default) to the OS desktop notifier
// (osascript on macOS, notify-send on Linux).
func SenderForChannel(channel, alertsPath string) Sender {
	if channel == "file" {
		return FileSender{AlertsPath: alertsPath}
	}
	return DesktopSender()
}

// DesktopChannelName is the platform-appropriate label for the desktop channel, used in
// help text so macOS users see "macos"/"desktop" and Linux users see "desktop".
func DesktopChannelName() string { return "desktop" }

type Manager struct {
	mu                    sync.Mutex
	statePath, alertsPath string
	sender                Sender
	now                   func() time.Time
	window                time.Duration
	id                    func() string
}

func New(statePath, alertsPath string, sender Sender) *Manager {
	return &Manager{statePath: statePath, alertsPath: alertsPath, sender: sender, now: time.Now, window: DefaultGroupWindow, id: func() string { return fmt.Sprintf("notify-%d", time.Now().UnixNano()) }}
}

func (m *Manager) Send(ctx context.Context, input Input) (Result, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	input.Title = strings.TrimSpace(input.Title)
	input.Body = strings.TrimSpace(input.Body)
	if input.Title == "" || input.Body == "" {
		return Result{}, errors.New("title and body are required")
	}
	if input.Channel == "" {
		input.Channel = "desktop"
	}
	// "macos" is accepted as a back-compat alias for the OS desktop channel.
	if input.Channel == "macos" {
		input.Channel = "desktop"
	}
	if input.Channel != "desktop" && input.Channel != "file" {
		return Result{}, fmt.Errorf("unsupported notification channel %q (use desktop or file)", input.Channel)
	}
	state, err := m.load()
	if err != nil {
		return Result{}, err
	}
	now := m.now().UTC()
	if input.Group != "" {
		if value := state.Groups[input.Group]; value != "" {
			last, err := time.Parse(time.RFC3339Nano, value)
			if err != nil {
				return Result{}, err
			}
			if now.Sub(last) < m.window {
				return Result{SchemaVersion: SchemaVersion, Suppressed: true, Group: input.Group, Timestamp: now.Format(time.RFC3339Nano)}, nil
			}
		}
	}
	if input.Channel == "desktop" {
		if m.sender == nil {
			return Result{}, errors.New("desktop notification sender is unavailable")
		}
		sendCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		err = m.sender.Send(sendCtx, input.Title, input.Body)
		cancel()
		if err != nil {
			return Result{}, err
		}
	}
	if input.Group != "" {
		state.Groups[input.Group] = now.Format(time.RFC3339Nano)
		if err := writeJSONAtomic(m.statePath, state); err != nil {
			return Result{}, err
		}
	}
	alert := Alert{SchemaVersion: SchemaVersion, ID: m.id(), Timestamp: now.Format(time.RFC3339Nano), Kind: "notification", Title: input.Title, Message: input.Body, Channel: input.Channel, Group: input.Group}
	if err := appendJSONL(m.alertsPath, alert); err != nil {
		return Result{}, err
	}
	return Result{SchemaVersion: SchemaVersion, Delivered: true, Group: input.Group, Timestamp: now.Format(time.RFC3339Nano)}, nil
}

func (m *Manager) load() (groupState, error) {
	data, err := os.ReadFile(m.statePath)
	if errors.Is(err, os.ErrNotExist) {
		return groupState{SchemaVersion: SchemaVersion, Groups: map[string]string{}}, nil
	}
	if err != nil {
		return groupState{}, err
	}
	var state groupState
	if err := json.Unmarshal(data, &state); err != nil {
		return groupState{}, err
	}
	if state.SchemaVersion != SchemaVersion {
		return groupState{}, fmt.Errorf("unsupported notification schema %d", state.SchemaVersion)
	}
	if state.Groups == nil {
		state.Groups = map[string]string{}
	}
	return state, nil
}
func writeJSONAtomic(path string, value any) error {
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(path), ".notify-*")
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
func appendJSONL(path string, value any) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	data = append(data, '\n')
	file, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	if _, err = file.Write(data); err == nil {
		err = file.Sync()
	}
	closeErr := file.Close()
	if err == nil {
		err = closeErr
	}
	return err
}
