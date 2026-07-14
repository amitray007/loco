package daemon

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"time"
)

const SchemaVersion = 1

type Health struct {
	SchemaVersion int    `json:"schema_version"`
	Healthy       bool   `json:"healthy"`
	PID           int    `json:"pid,omitempty"`
	StartedAt     string `json:"started_at,omitempty"`
	UptimeSeconds int64  `json:"uptime_seconds"`
	Version       string `json:"version"`
	Error         string `json:"error,omitempty"`
}

func UnhealthyStatus(version string, err error) Health {
	return Health{
		SchemaVersion: SchemaVersion,
		Healthy:       false,
		Version:       version,
		Error:         err.Error(),
	}
}

func Status(ctx context.Context, socket string, timeout time.Duration) (Health, error) {
	dialer := net.Dialer{Timeout: timeout}
	conn, err := dialer.DialContext(ctx, "unix", socket)
	if err != nil {
		return Health{}, fmt.Errorf("connect to %s: %w", socket, err)
	}
	defer conn.Close()

	deadline := time.Now().Add(timeout)
	if err := conn.SetDeadline(deadline); err != nil {
		return Health{}, fmt.Errorf("set status deadline: %w", err)
	}
	if _, err := conn.Write([]byte("status\n")); err != nil {
		return Health{}, fmt.Errorf("request status: %w", err)
	}

	var status Health
	if err := json.NewDecoder(conn).Decode(&status); err != nil {
		return Health{}, fmt.Errorf("decode status: %w", err)
	}
	if !status.Healthy {
		return status, fmt.Errorf("daemon reported unhealthy")
	}
	return status, nil
}
