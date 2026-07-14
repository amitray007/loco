package mcp

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/amitray007/loco/internal/daemon"
	"github.com/amitray007/loco/internal/paths"
)

// TestMain ensures no desktop notification channel is ever exercised by a daemon
// started under test.
func TestMain(m *testing.M) {
	_ = os.Setenv("LOCO_NOTIFY_CHANNEL", "file")
	os.Exit(m.Run())
}

// rpcResponse mirrors the wire shape of a JSON-RPC response for test assertions.
type rpcResponse struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id"`
	Result  json.RawMessage `json:"result"`
	Error   *rpcError       `json:"error"`
}

// runLines drives Serve over an in-memory buffer with the given request lines (already
// JSON-encoded, one per line) and returns the decoded responses in order.
func runLines(t *testing.T, socket string, lines []string) []rpcResponse {
	t.Helper()
	var in bytes.Buffer
	for _, line := range lines {
		in.WriteString(line)
		in.WriteString("\n")
	}
	var out bytes.Buffer
	if err := Serve(context.Background(), socket, &in, &out); err != nil {
		t.Fatalf("Serve: %v", err)
	}
	var responses []rpcResponse
	decoder := json.NewDecoder(&out)
	for decoder.More() {
		var resp rpcResponse
		if err := decoder.Decode(&resp); err != nil {
			t.Fatalf("decode response: %v", err)
		}
		responses = append(responses, resp)
	}
	return responses
}

func TestInitializeReturnsServerInfo(t *testing.T) {
	responses := runLines(t, "/nonexistent.sock", []string{
		`{"jsonrpc":"2.0","id":1,"method":"initialize","params":{}}`,
	})
	if len(responses) != 1 {
		t.Fatalf("got %d responses, want 1", len(responses))
	}
	if responses[0].Error != nil {
		t.Fatalf("unexpected error: %+v", responses[0].Error)
	}
	var result struct {
		ProtocolVersion string `json:"protocolVersion"`
		ServerInfo      struct {
			Name    string `json:"name"`
			Version string `json:"version"`
		} `json:"serverInfo"`
		Capabilities map[string]any `json:"capabilities"`
	}
	if err := json.Unmarshal(responses[0].Result, &result); err != nil {
		t.Fatalf("decode result: %v", err)
	}
	if result.ServerInfo.Name != "loco" {
		t.Fatalf("serverInfo.name = %q, want loco", result.ServerInfo.Name)
	}
	if result.ProtocolVersion == "" {
		t.Fatal("protocolVersion is empty")
	}
	if _, ok := result.Capabilities["tools"]; !ok {
		t.Fatalf("capabilities missing tools: %+v", result.Capabilities)
	}
}

func TestNotificationsInitializedGetsNoResponse(t *testing.T) {
	responses := runLines(t, "/nonexistent.sock", []string{
		`{"jsonrpc":"2.0","method":"notifications/initialized"}`,
	})
	if len(responses) != 0 {
		t.Fatalf("got %d responses for a notification, want 0: %+v", len(responses), responses)
	}
}

func TestToolsListReturnsAllSixTools(t *testing.T) {
	responses := runLines(t, "/nonexistent.sock", []string{
		`{"jsonrpc":"2.0","id":1,"method":"tools/list","params":{}}`,
	})
	if len(responses) != 1 {
		t.Fatalf("got %d responses, want 1", len(responses))
	}
	if responses[0].Error != nil {
		t.Fatalf("unexpected error: %+v", responses[0].Error)
	}
	var result struct {
		Tools []tool `json:"tools"`
	}
	if err := json.Unmarshal(responses[0].Result, &result); err != nil {
		t.Fatalf("decode result: %v", err)
	}
	wantNames := map[string]bool{
		"loco_status": false, "loco_events_emit": false, "loco_events_read": false,
		"loco_events_ack": false, "loco_cron_list": false, "loco_svc_list": false,
	}
	if len(result.Tools) != len(wantNames) {
		t.Fatalf("got %d tools, want %d: %+v", len(result.Tools), len(wantNames), result.Tools)
	}
	for _, tl := range result.Tools {
		if _, ok := wantNames[tl.Name]; !ok {
			t.Fatalf("unexpected tool %q", tl.Name)
		}
		wantNames[tl.Name] = true
		if tl.Description == "" {
			t.Fatalf("tool %q has empty description", tl.Name)
		}
		if len(tl.InputSchema) == 0 {
			t.Fatalf("tool %q has empty inputSchema", tl.Name)
		}
		if tl.InputSchema["type"] != "object" {
			t.Fatalf("tool %q inputSchema.type = %v, want object", tl.Name, tl.InputSchema["type"])
		}
	}
	for name, seen := range wantNames {
		if !seen {
			t.Fatalf("tool %q missing from tools/list", name)
		}
	}
}

func TestUnknownMethodReturnsMethodNotFound(t *testing.T) {
	responses := runLines(t, "/nonexistent.sock", []string{
		`{"jsonrpc":"2.0","id":1,"method":"bogus/method","params":{}}`,
	})
	if len(responses) != 1 {
		t.Fatalf("got %d responses, want 1", len(responses))
	}
	if responses[0].Error == nil {
		t.Fatal("expected an error, got none")
	}
	if responses[0].Error.Code != codeMethodNotFound {
		t.Fatalf("error code = %d, want %d", responses[0].Error.Code, codeMethodNotFound)
	}
}

func TestMalformedJSONReturnsParseError(t *testing.T) {
	var in bytes.Buffer
	in.WriteString("{not valid json\n")
	var out bytes.Buffer
	if err := Serve(context.Background(), "/nonexistent.sock", &in, &out); err != nil {
		t.Fatalf("Serve: %v", err)
	}
	var resp rpcResponse
	if err := json.Unmarshal(out.Bytes(), &resp); err != nil {
		t.Fatalf("decode response: %v; raw=%s", err, out.String())
	}
	if resp.Error == nil || resp.Error.Code != codeParseError {
		t.Fatalf("resp = %+v, want parse error", resp)
	}
}

// startTestDaemon boots a real loco daemon on a temp config dir and returns its socket
// path, waiting for it to become healthy. Mirrors the pattern in cmd/loco/main_test.go.
// It deliberately uses a short-prefixed os.MkdirTemp under /tmp rather than t.TempDir():
// t.TempDir() embeds the full (often long) test function name in the path, and the
// daemon refuses to bind a unix socket path >= 104 bytes (see daemon.maxUnixSocketPath).
func startTestDaemon(t *testing.T) paths.Paths {
	t.Helper()
	root, err := os.MkdirTemp("/tmp", "loco-mcp-test-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(root) })
	p, err := paths.Resolve(root)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- daemon.Run(ctx, p, "mcp-test") }()
	t.Cleanup(func() {
		cancel()
		<-done
	})
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := daemon.Status(context.Background(), p.Socket, 50*time.Millisecond); err == nil {
			return p
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("daemon did not become healthy")
	return paths.Paths{}
}

func TestToolsCallLocoStatusAgainstRealDaemon(t *testing.T) {
	p := startTestDaemon(t)
	responses := runLines(t, p.Socket, []string{
		`{"jsonrpc":"2.0","id":1,"method":"initialize","params":{}}`,
		`{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"loco_status","arguments":{}}}`,
	})
	if len(responses) != 2 {
		t.Fatalf("got %d responses, want 2", len(responses))
	}
	if responses[1].Error != nil {
		t.Fatalf("unexpected JSON-RPC error: %+v", responses[1].Error)
	}
	var result toolCallResult
	if err := json.Unmarshal(responses[1].Result, &result); err != nil {
		t.Fatalf("decode tool result: %v", err)
	}
	if result.IsError {
		t.Fatalf("tool reported an error: %+v", result.Content)
	}
	if len(result.Content) != 1 || result.Content[0].Type != "text" {
		t.Fatalf("content = %+v", result.Content)
	}
	var overview daemon.Overview
	if err := json.Unmarshal([]byte(result.Content[0].Text), &overview); err != nil {
		t.Fatalf("decode overview from content text: %v; text=%s", err, result.Content[0].Text)
	}
	if !overview.Daemon.Healthy {
		t.Fatalf("overview.daemon.healthy = false: %+v", overview.Daemon)
	}
}

func TestToolsCallEventsEmitThenReadRoundTrip(t *testing.T) {
	p := startTestDaemon(t)
	emitParams := `{"name":"loco_events_emit","arguments":{"topic":"mcp.demo","id":"evt-1","data":{"value":42}}}`
	readParams := `{"name":"loco_events_read","arguments":{"topic":"mcp.demo","cursor":"agent-1"}}`
	responses := runLines(t, p.Socket, []string{
		fmt.Sprintf(`{"jsonrpc":"2.0","id":1,"method":"tools/call","params":%s}`, emitParams),
		fmt.Sprintf(`{"jsonrpc":"2.0","id":2,"method":"tools/call","params":%s}`, readParams),
	})
	if len(responses) != 2 {
		t.Fatalf("got %d responses, want 2", len(responses))
	}

	var emitResult toolCallResult
	if err := json.Unmarshal(responses[0].Result, &emitResult); err != nil {
		t.Fatalf("decode emit tool result: %v", err)
	}
	if emitResult.IsError {
		t.Fatalf("emit reported an error: %+v", emitResult.Content)
	}
	var emitted struct {
		Emitted bool `json:"emitted"`
		Event   struct {
			ID string `json:"id"`
		} `json:"event"`
	}
	if err := json.Unmarshal([]byte(emitResult.Content[0].Text), &emitted); err != nil {
		t.Fatalf("decode emit payload: %v", err)
	}
	if !emitted.Emitted || emitted.Event.ID != "evt-1" {
		t.Fatalf("emitted = %+v", emitted)
	}

	var readResult toolCallResult
	if err := json.Unmarshal(responses[1].Result, &readResult); err != nil {
		t.Fatalf("decode read tool result: %v", err)
	}
	if readResult.IsError {
		t.Fatalf("read reported an error: %+v", readResult.Content)
	}
	var read struct {
		Events []struct {
			ID      string          `json:"id"`
			Payload json.RawMessage `json:"payload"`
		} `json:"events"`
	}
	if err := json.Unmarshal([]byte(readResult.Content[0].Text), &read); err != nil {
		t.Fatalf("decode read payload: %v", err)
	}
	if len(read.Events) != 1 || read.Events[0].ID != "evt-1" {
		t.Fatalf("read = %+v", read)
	}
	if string(read.Events[0].Payload) != `{"value":42}` {
		t.Fatalf("payload = %s", read.Events[0].Payload)
	}
}

func TestToolsCallUnknownToolIsMCPError(t *testing.T) {
	p := startTestDaemon(t)
	responses := runLines(t, p.Socket, []string{
		`{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"loco_bogus","arguments":{}}}`,
	})
	if len(responses) != 1 {
		t.Fatalf("got %d responses, want 1", len(responses))
	}
	if responses[0].Error != nil {
		t.Fatalf("expected a tool-level error, not a JSON-RPC error: %+v", responses[0].Error)
	}
	var result toolCallResult
	if err := json.Unmarshal(responses[0].Result, &result); err != nil {
		t.Fatalf("decode tool result: %v", err)
	}
	if !result.IsError {
		t.Fatal("expected isError=true for an unknown tool")
	}
}
