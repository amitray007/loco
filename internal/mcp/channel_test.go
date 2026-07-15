package mcp

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/amitray007/loco/internal/daemon"
	"github.com/amitray007/loco/internal/events"
)

// collector is a concurrency-safe pushFunc sink for assertions.
type collector struct {
	mu  sync.Mutex
	got []string // "topic/id"
}

func (c *collector) push(topic string, ev events.Event) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.got = append(c.got, topic+"/"+ev.ID)
}

func (c *collector) snapshot() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]string(nil), c.got...)
}

// The tail starts at the current tail: events that existed BEFORE it started are
// never pushed (no backlog replay), and events appended after are pushed exactly once.
func TestTailStartsAtTailAndPushesNewEvents(t *testing.T) {
	p := startTestDaemon(t)
	ctx := context.Background()

	// Pre-existing backlog — must NOT be replayed.
	for _, id := range []string{"old1", "old2", "old3"} {
		if _, err := daemon.EventsEmit(ctx, p.Socket, 5*time.Second,
			events.EmitInput{Topic: "chan.t", ID: id, Payload: json.RawMessage(`{}`)}); err != nil {
			t.Fatal(err)
		}
	}

	tailCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	c := &collector{}
	go runTail(tailCtx, p.Socket, ChannelConfig{Topics: []string{"chan.t"}, Poll: 50 * time.Millisecond}, c.push)

	// Let the tail establish its high-water mark before emitting.
	time.Sleep(300 * time.Millisecond)
	if got := c.snapshot(); len(got) != 0 {
		t.Fatalf("backlog was replayed: %v", got)
	}

	if _, err := daemon.EventsEmit(ctx, p.Socket, 5*time.Second,
		events.EmitInput{Topic: "chan.t", ID: "new1", Payload: json.RawMessage(`{"x":1}`)}); err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if len(c.snapshot()) > 0 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	got := c.snapshot()
	if len(got) != 1 || got[0] != "chan.t/new1" {
		t.Fatalf("want exactly [chan.t/new1], got %v", got)
	}
}

// Regression for the "daemon down at startup" backlog-replay bug (finding 1): if the
// tail cannot reach the daemon to establish its high-water mark, it must retry rather
// than defaulting to a false 0 — because a false 0 would make the first successful poll
// replay the topic's entire pre-existing backlog into a fresh session. This is exactly
// the failure D4 exists to prevent.
//
// The backlog is real (emitted against a genuinely running daemon), but runTail is
// pointed at a socket path where NOTHING is listening — a directory that is never
// created, so every dial deterministically fails with the same "no such file or
// directory" error a down daemon would produce. Because latestSeq now retries forever
// on error, runTail must never get past startup, and so must never push anything.
//
// This also proves finding 2 (ctx-aware startup): cancelling ctx must make runTail
// return promptly — bounded by the retry backoff (~200ms), not by callTimeout (5s) or
// any unbounded hang — even though the daemon never becomes reachable.
func TestTailDownAtStartupNeverReplaysBacklog(t *testing.T) {
	p := startTestDaemon(t)
	ctx := context.Background()

	// A real backlog exists on the topic before the tail ever starts.
	for _, id := range []string{"old1", "old2", "old3"} {
		if _, err := daemon.EventsEmit(ctx, p.Socket, 5*time.Second,
			events.EmitInput{Topic: "chan.down", ID: id, Payload: json.RawMessage(`{}`)}); err != nil {
			t.Fatal(err)
		}
	}

	// A socket path in a directory that is never created: every dial fails exactly like
	// it would against a down/restarting daemon (registry.dial's "connect to %s" error).
	bogusSocket := filepath.Join(t.TempDir(), "nowhere", "daemon.sock")
	if _, err := os.Stat(filepath.Dir(bogusSocket)); !os.IsNotExist(err) {
		t.Fatalf("test setup: %s must not exist", filepath.Dir(bogusSocket))
	}

	tailCtx, cancel := context.WithCancel(ctx)
	c := &collector{}
	done := make(chan struct{})
	go func() {
		runTail(tailCtx, bogusSocket, ChannelConfig{Topics: []string{"chan.down"}, Poll: 50 * time.Millisecond}, c.push)
		close(done)
	}()

	// Give it several retry cycles against the unreachable daemon. It must still be
	// stuck in startup (never reached the ticker loop, so never pushed anything).
	time.Sleep(700 * time.Millisecond)
	if got := c.snapshot(); len(got) != 0 {
		t.Fatalf("tail pushed events before ever establishing a high-water mark: %v", got)
	}
	select {
	case <-done:
		t.Fatal("runTail returned before ctx was cancelled; it should still be retrying startup")
	default:
	}

	// Cancel and require prompt return — this is the finding-2 assertion: startup must
	// be ctx-aware, not blocked on an unbounded retry loop.
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("runTail did not return promptly after ctx cancellation during startup")
	}

	if got := c.snapshot(); len(got) != 0 {
		t.Fatalf("tail that never established a high-water mark pushed events: %v", got)
	}
}

// The tail must never create a cursor — a channel cursor would pin retention for 30d.
func TestTailCreatesNoCursor(t *testing.T) {
	p := startTestDaemon(t)
	ctx := context.Background()
	if _, err := daemon.EventsEmit(ctx, p.Socket, 5*time.Second,
		events.EmitInput{Topic: "chan.nc", ID: "a", Payload: json.RawMessage(`{}`)}); err != nil {
		t.Fatal(err)
	}
	tailCtx, cancel := context.WithCancel(ctx)
	c := &collector{}
	go runTail(tailCtx, p.Socket, ChannelConfig{Topics: []string{"chan.nc"}, Poll: 50 * time.Millisecond}, c.push)
	time.Sleep(300 * time.Millisecond)
	daemon.EventsEmit(ctx, p.Socket, 5*time.Second,
		events.EmitInput{Topic: "chan.nc", ID: "b", Payload: json.RawMessage(`{}`)})
	time.Sleep(300 * time.Millisecond)
	cancel()

	overview, err := daemon.StatusOverview(ctx, p.Socket, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	for _, tp := range overview.Topics {
		if tp.Name == "chan.nc" && len(tp.Cursors) != 0 {
			t.Fatalf("tail created %d cursors on chan.nc; want 0", len(tp.Cursors))
		}
	}
}

// Only configured topics are bridged.
func TestTailIgnoresUnconfiguredTopics(t *testing.T) {
	p := startTestDaemon(t)
	ctx := context.Background()
	tailCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	c := &collector{}
	go runTail(tailCtx, p.Socket, ChannelConfig{Topics: []string{"chan.yes"}, Poll: 50 * time.Millisecond}, c.push)
	time.Sleep(300 * time.Millisecond)

	daemon.EventsEmit(ctx, p.Socket, 5*time.Second,
		events.EmitInput{Topic: "chan.no", ID: "n1", Payload: json.RawMessage(`{}`)})
	daemon.EventsEmit(ctx, p.Socket, 5*time.Second,
		events.EmitInput{Topic: "chan.yes", ID: "y1", Payload: json.RawMessage(`{}`)})

	time.Sleep(600 * time.Millisecond)
	for _, g := range c.snapshot() {
		if g == "chan.no/n1" {
			t.Fatal("pushed an unconfigured topic")
		}
	}
	found := false
	for _, g := range c.snapshot() {
		if g == "chan.yes/y1" {
			found = true
		}
	}
	if !found {
		t.Fatalf("configured topic not pushed: %v", c.snapshot())
	}
}
