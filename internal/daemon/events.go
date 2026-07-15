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
func EventsRead(ctx context.Context, socket string, timeout time.Duration, topic, cursor, holder string, limit int) (events.ReadResult, error) {
	request := struct {
		Topic  string `json:"topic"`
		Cursor string `json:"cursor"`
		Limit  int    `json:"limit"`
		Holder string `json:"holder"`
	}{topic, cursor, limit, holder}
	var result events.ReadResult
	err := eventRequest(ctx, socket, timeout, "events.read", request, &result)
	return result, err
}
func EventsPeek(ctx context.Context, socket string, timeout time.Duration, topic string, tail int, fromSeq uint64) (events.ReadResult, error) {
	request := struct {
		Topic   string `json:"topic"`
		Tail    int    `json:"tail"`
		FromSeq uint64 `json:"from_seq"`
	}{topic, tail, fromSeq}
	var result events.ReadResult
	err := eventRequest(ctx, socket, timeout, "events.peek", request, &result)
	return result, err
}

// EventsAck acks by watermark (through, everything at or below it) or by processed ids
// (ids, the longest contiguous run). Exactly one of through/ids is set; ids wins if both
// are, though the CLI rejects that combination before it reaches here.
func EventsAck(ctx context.Context, socket string, timeout time.Duration, topic, cursor, through string, ids []string, allowSkip bool) (events.AckResult, error) {
	request := struct {
		Topic     string   `json:"topic"`
		Cursor    string   `json:"cursor"`
		Through   string   `json:"through"`
		IDs       []string `json:"ids,omitempty"`
		AllowSkip bool     `json:"allow_skip"`
	}{topic, cursor, through, ids, allowSkip}
	var result events.AckResult
	err := eventRequest(ctx, socket, timeout, "events.ack", request, &result)
	return result, err
}
func eventRequest(ctx context.Context, socket string, timeout time.Duration, operation string, request any, result any) error {
	conn, err := dial(ctx, socket, timeout)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()
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
