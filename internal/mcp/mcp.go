// Package mcp implements a Model Context Protocol (MCP) stdio server that exposes
// loco's daemon to AI agents as MCP tools. It speaks newline-delimited JSON-RPC 2.0
// over stdin/stdout, which is the MCP stdio transport. No third-party dependency is
// used: the JSON-RPC envelope and MCP method set are implemented directly against
// encoding/json.
package mcp

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"time"

	"github.com/amitray007/loco/internal/daemon"
	"github.com/amitray007/loco/internal/events"
	"github.com/amitray007/loco/internal/registry"
)

// callTimeout bounds every daemon RPC issued on behalf of a tool call.
const callTimeout = 5 * time.Second

// JSON-RPC 2.0 error codes used by this server. -32700/-32601/-32602/-32603 are
// reserved by the spec; -32000 is in the implementation-defined server-error range.
const (
	codeParseError     = -32700
	codeMethodNotFound = -32601
	codeInvalidParams  = -32602
	codeInternalError  = -32603
)

// request is a JSON-RPC 2.0 request or notification. Notifications omit ID.
// ID may be a number, string, or null per the spec, so it is decoded as raw JSON and
// echoed back verbatim in the response.
type request struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id,omitempty"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params,omitempty"`
}

// isNotification reports whether this request carries no id (a JSON-RPC notification,
// which must never receive a response).
func (r request) isNotification() bool { return len(r.ID) == 0 }

// response is a JSON-RPC 2.0 response. Exactly one of Result/Error is set.
type response struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id"`
	Result  any             `json:"result,omitempty"`
	Error   *rpcError       `json:"error,omitempty"`
}

type rpcError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

// tool describes one MCP tool as returned from tools/list.
type tool struct {
	Name        string         `json:"name"`
	Description string         `json:"description"`
	InputSchema map[string]any `json:"inputSchema"`
}

// toolCallParams is the params payload of a tools/call request.
type toolCallParams struct {
	Name      string          `json:"name"`
	Arguments json.RawMessage `json:"arguments"`
}

// toolContent is one block of an MCP tool result's content array.
type toolContent struct {
	Type string `json:"type"`
	Text string `json:"text"`
}

// toolCallResult is the result payload of a tools/call response.
type toolCallResult struct {
	Content []toolContent `json:"content"`
	IsError bool          `json:"isError,omitempty"`
}

// server holds everything a running MCP session needs to dispatch tool calls against
// the loco daemon.
type server struct {
	socket  string
	version string
	out     *bufio.Writer
}

// Serve reads newline-delimited JSON-RPC 2.0 messages from in, dispatches MCP methods
// against the loco daemon reachable at socket, and writes newline-delimited JSON-RPC
// responses to out, flushing after each one. It runs until in is exhausted (EOF), ctx
// is canceled, or a fatal I/O error occurs.
func Serve(ctx context.Context, socket string, in io.Reader, out io.Writer) error {
	return ServeVersion(ctx, socket, "devel", in, out)
}

// ServeVersion is Serve with an explicit server version to report from `initialize`.
func ServeVersion(ctx context.Context, socket, version string, in io.Reader, out io.Writer) error {
	s := &server{socket: socket, version: version, out: bufio.NewWriter(out)}

	scanner := bufio.NewScanner(in)
	// MCP payloads (tool results, registry documents) can exceed the default 64KiB
	// token limit; allow generous per-line buffering.
	scanner.Buffer(make([]byte, 64*1024), 16<<20)

	for scanner.Scan() {
		if err := ctx.Err(); err != nil {
			return err
		}
		line := scanner.Bytes()
		// Skip blank lines some clients emit between messages.
		if len(trimSpace(line)) == 0 {
			continue
		}
		s.handleLine(ctx, line)
	}
	return scanner.Err()
}

// trimSpace avoids importing bytes/strings just for whitespace trimming of a line.
func trimSpace(b []byte) []byte {
	start := 0
	for start < len(b) && isSpace(b[start]) {
		start++
	}
	end := len(b)
	for end > start && isSpace(b[end-1]) {
		end--
	}
	return b[start:end]
}

func isSpace(c byte) bool { return c == ' ' || c == '\t' || c == '\r' || c == '\n' }

// handleLine parses one line as a JSON-RPC request and dispatches it, writing a
// response unless the line was a notification (no id) or was unparseable with no
// recoverable id.
func (s *server) handleLine(ctx context.Context, line []byte) {
	var req request
	if err := json.Unmarshal(line, &req); err != nil {
		// A malformed line has no recoverable id; per JSON-RPC, a parse error response
		// carries id: null.
		s.writeResponse(response{JSONRPC: "2.0", ID: json.RawMessage("null"), Error: &rpcError{Code: codeParseError, Message: "parse error: " + err.Error()}})
		return
	}

	result, rpcErr := s.dispatch(ctx, req)
	if req.isNotification() {
		// Notifications (e.g. notifications/initialized) never receive a response, even
		// on error.
		return
	}
	resp := response{JSONRPC: "2.0", ID: req.ID}
	if rpcErr != nil {
		resp.Error = rpcErr
	} else {
		resp.Result = result
	}
	s.writeResponse(resp)
}

func (s *server) writeResponse(resp response) {
	data, err := json.Marshal(resp)
	if err != nil {
		// Should be unreachable given the types we construct, but never let a marshal
		// failure crash the server loop.
		return
	}
	_, _ = s.out.Write(data)
	_, _ = s.out.Write([]byte("\n"))
	_ = s.out.Flush()
}

// dispatch routes a single request to its MCP method handler.
func (s *server) dispatch(ctx context.Context, req request) (any, *rpcError) {
	switch req.Method {
	case "initialize":
		return s.handleInitialize(), nil
	case "notifications/initialized":
		// Client acknowledgement notification; nothing to do. handleLine already skips
		// the response for notifications, so the return value here is unused.
		return nil, nil
	case "tools/list":
		return s.handleToolsList(), nil
	case "tools/call":
		return s.handleToolsCall(ctx, req.Params)
	default:
		return nil, &rpcError{Code: codeMethodNotFound, Message: fmt.Sprintf("method not found: %s", req.Method)}
	}
}

func (s *server) handleInitialize() any {
	return map[string]any{
		"protocolVersion": "2024-11-05",
		"capabilities": map[string]any{
			"tools": map[string]any{},
		},
		"serverInfo": map[string]any{
			"name":    "loco",
			"version": s.version,
		},
	}
}

func (s *server) handleToolsList() any {
	return map[string]any{"tools": toolDefinitions()}
}

// toolDefinitions is the fixed catalog of MCP tools this server exposes. Each maps to
// one loco daemon RPC.
func toolDefinitions() []tool {
	return []tool{
		{
			Name:        "loco_status",
			Description: "Get situational awareness of the loco daemon: health, jobs, event topics, and recent alerts.",
			InputSchema: map[string]any{
				"type":       "object",
				"properties": map[string]any{},
			},
		},
		{
			Name:        "loco_events_emit",
			Description: "Emit an idempotent event onto a durable loco event topic.",
			InputSchema: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"topic":      map[string]any{"type": "string", "description": "Event topic name"},
					"data":       map[string]any{"type": "object", "description": "Arbitrary JSON payload for the event"},
					"id":         map[string]any{"type": "string", "description": "Optional idempotency key; generated if omitted"},
					"source_job": map[string]any{"type": "string", "description": "Optional name of the job emitting this event"},
				},
				"required": []string{"topic", "data"},
			},
		},
		{
			Name:        "loco_events_read",
			Description: "Read unread events from a topic for a named cursor, without advancing the cursor.",
			InputSchema: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"topic":  map[string]any{"type": "string", "description": "Event topic name"},
					"cursor": map[string]any{"type": "string", "description": "Consumer cursor name"},
					"limit":  map[string]any{"type": "integer", "description": "Maximum number of events to return (default 100)"},
				},
				"required": []string{"topic", "cursor"},
			},
		},
		{
			Name:        "loco_events_ack",
			Description: "Advance a topic's cursor through the given event ID, marking prior events consumed.",
			InputSchema: map[string]any{
				"type": "object",
				"properties": map[string]any{
					"topic":   map[string]any{"type": "string", "description": "Event topic name"},
					"cursor":  map[string]any{"type": "string", "description": "Consumer cursor name"},
					"through": map[string]any{"type": "string", "description": "Event ID to acknowledge through"},
				},
				"required": []string{"topic", "cursor", "through"},
			},
		},
		{
			Name:        "loco_cron_list",
			Description: "List all cron jobs registered with the loco daemon.",
			InputSchema: map[string]any{
				"type":       "object",
				"properties": map[string]any{},
			},
		},
		{
			Name:        "loco_svc_list",
			Description: "List all supervised services and their current state, PID, and restart count.",
			InputSchema: map[string]any{
				"type":       "object",
				"properties": map[string]any{},
			},
		},
	}
}

func (s *server) handleToolsCall(ctx context.Context, rawParams json.RawMessage) (any, *rpcError) {
	var params toolCallParams
	if err := json.Unmarshal(rawParams, &params); err != nil {
		return nil, &rpcError{Code: codeInvalidParams, Message: "invalid params: " + err.Error()}
	}

	result, err := s.callTool(ctx, params.Name, params.Arguments)
	if err != nil {
		// Tool execution errors are reported as MCP tool errors (isError: true) inside a
		// normal JSON-RPC success response, not as a JSON-RPC error — the call itself
		// succeeded in reaching the tool.
		return toolCallResult{Content: []toolContent{{Type: "text", Text: err.Error()}}, IsError: true}, nil
	}
	text, marshalErr := json.Marshal(result)
	if marshalErr != nil {
		return toolCallResult{Content: []toolContent{{Type: "text", Text: marshalErr.Error()}}, IsError: true}, nil
	}
	return toolCallResult{Content: []toolContent{{Type: "text", Text: string(text)}}}, nil
}

// callTool dispatches a single tool invocation to the matching loco daemon client
// call, decoding arguments per-tool.
func (s *server) callTool(ctx context.Context, name string, rawArgs json.RawMessage) (any, error) {
	switch name {
	case "loco_status":
		return daemon.StatusOverview(ctx, s.socket, callTimeout)

	case "loco_events_emit":
		var args struct {
			Topic     string          `json:"topic"`
			Data      json.RawMessage `json:"data"`
			ID        string          `json:"id"`
			SourceJob string          `json:"source_job"`
		}
		if err := json.Unmarshal(rawArgs, &args); err != nil {
			return nil, fmt.Errorf("invalid arguments: %w", err)
		}
		if args.Topic == "" {
			return nil, fmt.Errorf("topic is required")
		}
		if len(args.Data) == 0 {
			return nil, fmt.Errorf("data is required")
		}
		return daemon.EventsEmit(ctx, s.socket, callTimeout, events.EmitInput{
			Topic:     args.Topic,
			ID:        args.ID,
			SourceJob: args.SourceJob,
			Payload:   args.Data,
		})

	case "loco_events_read":
		var args struct {
			Topic  string `json:"topic"`
			Cursor string `json:"cursor"`
			Limit  *int   `json:"limit"`
		}
		if err := json.Unmarshal(rawArgs, &args); err != nil {
			return nil, fmt.Errorf("invalid arguments: %w", err)
		}
		if args.Topic == "" || args.Cursor == "" {
			return nil, fmt.Errorf("topic and cursor are required")
		}
		limit := 100
		if args.Limit != nil {
			limit = *args.Limit
		}
		return daemon.EventsRead(ctx, s.socket, callTimeout, args.Topic, args.Cursor, limit)

	case "loco_events_ack":
		var args struct {
			Topic   string `json:"topic"`
			Cursor  string `json:"cursor"`
			Through string `json:"through"`
		}
		if err := json.Unmarshal(rawArgs, &args); err != nil {
			return nil, fmt.Errorf("invalid arguments: %w", err)
		}
		if args.Topic == "" || args.Cursor == "" || args.Through == "" {
			return nil, fmt.Errorf("topic, cursor, and through are required")
		}
		return daemon.EventsAck(ctx, s.socket, callTimeout, args.Topic, args.Cursor, args.Through)

	case "loco_cron_list":
		document, err := daemon.RegistryList(ctx, s.socket, callTimeout)
		if err != nil {
			return nil, err
		}
		return filterByKind(document, "cron"), nil

	case "loco_svc_list":
		return daemon.ServiceList(ctx, s.socket, callTimeout)

	default:
		return nil, fmt.Errorf("unknown tool: %s", name)
	}
}

// filterByKind narrows a registry document down to jobs of the given kind, preserving
// the document's schema version and revision.
func filterByKind(document registry.Document, kind string) registry.Document {
	filtered := document
	filtered.Jobs = make(map[string]registry.Job, len(document.Jobs))
	for name, job := range document.Jobs {
		if job.Kind == kind {
			filtered.Jobs[name] = job
		}
	}
	return filtered
}
