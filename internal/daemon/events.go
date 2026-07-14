package daemon

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/amitray007/loco/internal/events"
)

func EventsEmit(ctx context.Context, socket string, timeout time.Duration, input events.EmitInput) (events.EmitResult, error) {
	var result events.EmitResult
	err := eventRequest(ctx, socket, timeout, "events.emit", input, &result)
	return result, err
}
func EventsRead(ctx context.Context, socket string, timeout time.Duration, topic, cursor string, limit int) (events.ReadResult, error) {
	request := struct {
		Topic  string `json:"topic"`
		Cursor string `json:"cursor"`
		Limit  int    `json:"limit"`
	}{topic, cursor, limit}
	var result events.ReadResult
	err := eventRequest(ctx, socket, timeout, "events.read", request, &result)
	return result, err
}
func EventsAck(ctx context.Context, socket string, timeout time.Duration, topic, cursor, through string) (events.AckResult, error) {
	request := struct {
		Topic   string `json:"topic"`
		Cursor  string `json:"cursor"`
		Through string `json:"through"`
	}{topic, cursor, through}
	var result events.AckResult
	err := eventRequest(ctx, socket, timeout, "events.ack", request, &result)
	return result, err
}
func eventRequest(ctx context.Context, socket string, timeout time.Duration, operation string, request any, result any) error {
	conn, err := dial(ctx, socket, timeout)
	if err != nil {
		return err
	}
	defer conn.Close()
	if _, err = conn.Write([]byte(operation + "\n")); err != nil {
		return fmt.Errorf("request %s: %w", operation, err)
	}
	if err = json.NewEncoder(conn).Encode(request); err != nil {
		return err
	}
	var raw json.RawMessage
	if err = json.NewDecoder(conn).Decode(&raw); err != nil {
		return fmt.Errorf("decode %s response: %w", operation, err)
	}
	if err = responseError(raw); err != nil {
		return err
	}
	if err = json.Unmarshal(raw, result); err != nil {
		return fmt.Errorf("decode %s result: %w", operation, err)
	}
	return nil
}
