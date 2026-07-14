package daemon

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"time"

	"github.com/amitray007/loco/internal/cronjob"
	"github.com/amitray007/loco/internal/registry"
)

type wireError struct {
	SchemaVersion int    `json:"schema_version"`
	Error         string `json:"error"`
}

func CronRunNow(ctx context.Context, socket string, timeout time.Duration, name string) (cronjob.TriggerResult, error) {
	conn, err := dial(ctx, socket, timeout)
	if err != nil {
		return cronjob.TriggerResult{}, err
	}
	defer conn.Close()
	if _, err := conn.Write([]byte("cron.run-now\n")); err != nil {
		return cronjob.TriggerResult{}, err
	}
	if err := json.NewEncoder(conn).Encode(struct {
		Name string `json:"name"`
	}{Name: name}); err != nil {
		return cronjob.TriggerResult{}, err
	}
	var raw json.RawMessage
	if err := json.NewDecoder(conn).Decode(&raw); err != nil {
		return cronjob.TriggerResult{}, err
	}
	if err := responseError(raw); err != nil {
		return cronjob.TriggerResult{}, err
	}
	var result cronjob.TriggerResult
	if err := json.Unmarshal(raw, &result); err != nil {
		return cronjob.TriggerResult{}, err
	}
	return result, nil
}

func RegistryPut(ctx context.Context, socket string, timeout time.Duration, input registry.PutInput) (registry.PutResult, error) {
	conn, err := dial(ctx, socket, timeout)
	if err != nil {
		return registry.PutResult{}, err
	}
	defer conn.Close()
	if _, err := conn.Write([]byte("registry.put\n")); err != nil {
		return registry.PutResult{}, fmt.Errorf("request registry put: %w", err)
	}
	if err := json.NewEncoder(conn).Encode(input); err != nil {
		return registry.PutResult{}, fmt.Errorf("encode registry entry: %w", err)
	}
	var raw json.RawMessage
	if err := json.NewDecoder(conn).Decode(&raw); err != nil {
		return registry.PutResult{}, fmt.Errorf("decode registry response: %w", err)
	}
	if err := responseError(raw); err != nil {
		return registry.PutResult{}, err
	}
	var result registry.PutResult
	if err := json.Unmarshal(raw, &result); err != nil {
		return registry.PutResult{}, fmt.Errorf("decode registry result: %w", err)
	}
	return result, nil
}

func RegistryDelete(ctx context.Context, socket string, timeout time.Duration, name string) (registry.DeleteResult, error) {
	conn, err := dial(ctx, socket, timeout)
	if err != nil {
		return registry.DeleteResult{}, err
	}
	defer conn.Close()
	if _, err := conn.Write([]byte("registry.delete\n")); err != nil {
		return registry.DeleteResult{}, fmt.Errorf("request registry delete: %w", err)
	}
	if err := json.NewEncoder(conn).Encode(struct {
		Name string `json:"name"`
	}{Name: name}); err != nil {
		return registry.DeleteResult{}, fmt.Errorf("encode delete request: %w", err)
	}
	var raw json.RawMessage
	if err := json.NewDecoder(conn).Decode(&raw); err != nil {
		return registry.DeleteResult{}, fmt.Errorf("decode delete response: %w", err)
	}
	if err := responseError(raw); err != nil {
		return registry.DeleteResult{}, err
	}
	var result registry.DeleteResult
	if err := json.Unmarshal(raw, &result); err != nil {
		return registry.DeleteResult{}, fmt.Errorf("decode delete result: %w", err)
	}
	return result, nil
}

func RegistryList(ctx context.Context, socket string, timeout time.Duration) (registry.Document, error) {
	conn, err := dial(ctx, socket, timeout)
	if err != nil {
		return registry.Document{}, err
	}
	defer conn.Close()
	if _, err := conn.Write([]byte("registry.list\n")); err != nil {
		return registry.Document{}, fmt.Errorf("request registry list: %w", err)
	}
	var raw json.RawMessage
	if err := json.NewDecoder(conn).Decode(&raw); err != nil {
		return registry.Document{}, fmt.Errorf("decode registry response: %w", err)
	}
	if err := responseError(raw); err != nil {
		return registry.Document{}, err
	}
	var document registry.Document
	if err := json.Unmarshal(raw, &document); err != nil {
		return registry.Document{}, fmt.Errorf("decode registry document: %w", err)
	}
	return document, nil
}

func dial(ctx context.Context, socket string, timeout time.Duration) (net.Conn, error) {
	dialer := net.Dialer{Timeout: timeout}
	conn, err := dialer.DialContext(ctx, "unix", socket)
	if err != nil {
		return nil, fmt.Errorf("connect to %s: %w", socket, err)
	}
	if err := conn.SetDeadline(time.Now().Add(timeout)); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("set daemon deadline: %w", err)
	}
	return conn, nil
}

func responseError(raw json.RawMessage) error {
	var response wireError
	if err := json.Unmarshal(raw, &response); err == nil && response.Error != "" {
		return fmt.Errorf("daemon: %s", response.Error)
	}
	return nil
}
