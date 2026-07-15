package mcp

import (
	"context"
	"encoding/json"
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
