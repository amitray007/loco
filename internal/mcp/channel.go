package mcp

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/amitray007/loco/internal/daemon"
	"github.com/amitray007/loco/internal/events"
)

// defaultChannelPoll is how often the tail asks the daemon for new events. The daemon
// serves peeks from its in-memory topic cache, so a 1s tick is cheap and meets the
// "seconds latency" goal without a daemon-side subscribe RPC.
const defaultChannelPoll = time.Second

// peekTail bounds each poll's page size. A burst larger than this is delivered across
// consecutive ticks (seq order is preserved), never dropped.
const peekTail = 200

// ChannelConfig selects which topics a session bridges. An empty Topics list disables
// the channel entirely: the server then behaves exactly as a pure tools server.
type ChannelConfig struct {
	Topics []string
	Poll   time.Duration
}

func (c ChannelConfig) enabled() bool { return len(c.Topics) > 0 }

func (c ChannelConfig) poll() time.Duration {
	if c.Poll <= 0 {
		return defaultChannelPoll
	}
	return c.Poll
}

// pushFunc receives every event appended after the tail started.
type pushFunc func(topic string, ev events.Event)

// runTail polls the daemon for new events on each configured topic and invokes push for
// anything newer than the high-water mark captured at startup. It is deliberately
// STATELESS: it uses the cursor-free EventsPeek and never creates or advances a cursor,
// so a closed session leaves nothing behind and cannot pin compaction. Missing a push
// costs latency, never correctness — the consumer's own cursor is the ledger.
// It blocks until ctx is done.
func runTail(ctx context.Context, socket string, cfg ChannelConfig, push pushFunc) {
	// high[topic] = the highest seq already seen. Established at startup so pre-existing
	// events are never replayed into a fresh session.
	high := make(map[string]uint64, len(cfg.Topics))
	for _, topic := range cfg.Topics {
		high[topic] = latestSeq(ctx, socket, topic)
	}

	ticker := time.NewTicker(cfg.poll())
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		for _, topic := range cfg.Topics {
			result, err := daemon.EventsPeek(ctx, socket, callTimeout, topic, peekTail, high[topic]+1)
			if err != nil {
				// The daemon may be restarting, or the topic may be unreadable. Keep
				// polling the others; never kill the MCP session over a tail error.
				fmt.Fprintf(os.Stderr, "loco channel: peek %s: %v\n", topic, err)
				continue
			}
			for _, ev := range result.Events {
				if ev.Seq <= high[topic] {
					continue
				}
				high[topic] = ev.Seq
				push(topic, ev)
			}
		}
	}
}

// latestSeq returns the highest seq currently on a topic, or 0 if the topic does not
// exist yet or cannot be read (in which case the tail simply starts from the beginning
// of that topic's future).
func latestSeq(ctx context.Context, socket, topic string) uint64 {
	result, err := daemon.EventsPeek(ctx, socket, callTimeout, topic, 1, 0)
	if err != nil || len(result.Events) == 0 {
		return 0
	}
	return result.Events[len(result.Events)-1].Seq
}
