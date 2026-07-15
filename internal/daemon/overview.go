package daemon

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/amitray007/loco/internal/cronjob"
	"github.com/amitray007/loco/internal/events"
	"github.com/amitray007/loco/internal/paths"
	"github.com/amitray007/loco/internal/registry"
	"github.com/amitray007/loco/internal/service"
)

type RunOverview struct {
	ID         string `json:"id"`
	Status     string `json:"status"`
	FinishedAt string `json:"finished_at"`
	ExitCode   int    `json:"exit_code"`
	DurationMS int64  `json:"duration_ms"`
}
type JobOverview struct {
	Name    string       `json:"name"`
	Kind    string       `json:"kind"`
	State   string       `json:"state"`
	LastRun *RunOverview `json:"last_run,omitempty"`
}
type Overview struct {
	SchemaVersion int                  `json:"schema_version"`
	Daemon        Health               `json:"daemon"`
	Jobs          []JobOverview        `json:"jobs"`
	Topics        []events.TopicStatus `json:"topics"`
	Alerts        []events.Alert       `json:"alerts"`
}

func buildOverview(state runtimeState, started time.Time, p paths.Paths, store *registry.Store, services *service.Manager, eventStore *events.Store) (Overview, error) {
	result := Overview{SchemaVersion: SchemaVersion, Daemon: Health{SchemaVersion: SchemaVersion, Healthy: true, PID: state.PID, StartedAt: state.StartedAt, UptimeSeconds: int64(time.Since(started).Seconds()), Version: state.Version}, Jobs: make([]JobOverview, 0), Topics: make([]events.TopicStatus, 0), Alerts: make([]events.Alert, 0)}
	serviceStates := make(map[string]string)
	for _, item := range services.List().Services {
		serviceStates[item.Name] = item.State
	}
	doc := store.Snapshot()
	names := make([]string, 0, len(doc.Jobs))
	for name := range doc.Jobs {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		job := doc.Jobs[name]
		state := "scheduled"
		if job.Kind == "svc" {
			state = serviceStates[name]
			if state == "" {
				state = "stopped"
			}
		}
		last, err := lastRun(p.RunsDir, name)
		if err != nil {
			return Overview{}, err
		}
		result.Jobs = append(result.Jobs, JobOverview{Name: name, Kind: job.Kind, State: state, LastRun: last})
	}
	var err error
	result.Topics, err = eventStore.Overview()
	if err != nil {
		return Overview{}, err
	}
	result.Alerts, err = eventStore.RecentAlerts(time.Now().UTC().Add(-24*time.Hour), 10)
	if err != nil {
		return Overview{}, err
	}
	return result, nil
}

func lastRun(root, job string) (*RunOverview, error) {
	entries, err := os.ReadDir(filepath.Join(root, job))
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		if !entry.IsDir() && filepath.Ext(entry.Name()) == ".json" {
			names = append(names, entry.Name())
		}
	}
	if len(names) == 0 {
		return nil, nil
	}
	sort.Sort(sort.Reverse(sort.StringSlice(names)))
	data, err := os.ReadFile(filepath.Join(root, job, names[0]))
	if err != nil {
		return nil, err
	}
	var record cronjob.RunRecord
	if err := json.Unmarshal(data, &record); err != nil {
		return nil, fmt.Errorf("decode last run for %s: %w", job, err)
	}
	return &RunOverview{ID: record.ID, Status: record.Status, FinishedAt: record.FinishedAt, ExitCode: record.ExitCode, DurationMS: record.DurationMS}, nil
}

func StatusOverview(ctx context.Context, socket string, timeout time.Duration) (Overview, error) {
	conn, err := dial(ctx, socket, timeout)
	if err != nil {
		return Overview{}, err
	}
	defer func() { _ = conn.Close() }()
	if _, err = conn.Write([]byte("status.overview\n")); err != nil {
		return Overview{}, err
	}
	var raw json.RawMessage
	if err = json.NewDecoder(conn).Decode(&raw); err != nil {
		return Overview{}, err
	}
	if err = responseError(raw); err != nil {
		return Overview{}, err
	}
	var result Overview
	if err = json.Unmarshal(raw, &result); err != nil {
		return Overview{}, err
	}
	return result, nil
}
