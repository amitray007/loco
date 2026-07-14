package daemon

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/amitray007/loco/internal/service"
)

func ServiceList(ctx context.Context, socket string, timeout time.Duration) (service.ListResult, error) {
	conn, err := dial(ctx, socket, timeout)
	if err != nil {
		return service.ListResult{}, err
	}
	defer conn.Close()
	if _, err := conn.Write([]byte("service.list\n")); err != nil {
		return service.ListResult{}, fmt.Errorf("request service list: %w", err)
	}
	var raw json.RawMessage
	if err := json.NewDecoder(conn).Decode(&raw); err != nil {
		return service.ListResult{}, fmt.Errorf("decode service list: %w", err)
	}
	if err := responseError(raw); err != nil {
		return service.ListResult{}, err
	}
	var result service.ListResult
	if err := json.Unmarshal(raw, &result); err != nil {
		return service.ListResult{}, err
	}
	return result, nil
}

func ServiceControl(ctx context.Context, socket string, timeout time.Duration, name, action string) (service.ControlResult, error) {
	conn, err := dial(ctx, socket, timeout)
	if err != nil {
		return service.ControlResult{}, err
	}
	defer conn.Close()
	if _, err := conn.Write([]byte("service.control\n")); err != nil {
		return service.ControlResult{}, fmt.Errorf("request service control: %w", err)
	}
	request := struct {
		Name   string `json:"name"`
		Action string `json:"action"`
	}{Name: name, Action: action}
	if err := json.NewEncoder(conn).Encode(request); err != nil {
		return service.ControlResult{}, err
	}
	var raw json.RawMessage
	if err := json.NewDecoder(conn).Decode(&raw); err != nil {
		return service.ControlResult{}, fmt.Errorf("decode service control: %w", err)
	}
	if err := responseError(raw); err != nil {
		return service.ControlResult{}, err
	}
	var result service.ControlResult
	if err := json.Unmarshal(raw, &result); err != nil {
		return service.ControlResult{}, err
	}
	return result, nil
}
