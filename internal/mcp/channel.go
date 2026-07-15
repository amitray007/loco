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
	// events are never replayed into a fresh session. If ctx is cancelled while we are
	// still establishing high-water marks (e.g. the session ends while the daemon is
	// down), abort immediately rather than proceeding with a false 0 for any topic.
	high := make(map[string]uint64, len(cfg.Topics))
	for _, topic := range cfg.Topics {
		seq, ok := latestSeq(ctx, socket, topic)
		if !ok {
			return
		}
		high[topic] = seq
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

// latestSeqRetryBackoff bounds how long latestSeq waits between retries while the daemon
// is unreachable at startup. Short, because the daemon dials fail fast (registry.dial
// returns immediately on ECONNREFUSED/ENOENT) — this is purely pacing, not a timeout.
const latestSeqRetryBackoff = 200 * time.Millisecond

// latestSeq returns the highest seq currently on a topic. An EMPTY topic (no error, zero
// events — the topic does not exist yet or has never been written to) correctly returns
// (0, true): the tail starts from the beginning of that topic's future, which is exactly
// what "no backlog exists" means. An ERROR (daemon down/restarting) is a DIFFERENT state
// and must never be treated as empty: doing so would establish a false high-water mark
// of 0, and the first successful poll would then replay the topic's entire retained
// backlog into a fresh session — precisely what D4 forbids. So on error, this retries
// with a bounded backoff until the daemon answers, honoring ctx cancellation throughout
// (the session may end while the daemon is still down). Returns ok=false only when ctx
// is done before a real answer was obtained; callers must not use the returned seq (0)
// in that case.
func latestSeq(ctx context.Context, socket, topic string) (uint64, bool) {
	for {
		result, err := daemon.EventsPeek(ctx, socket, callTimeout, topic, 1, 0)
		if err == nil {
			if len(result.Events) == 0 {
				return 0, true
			}
			return result.Events[len(result.Events)-1].Seq, true
		}
		fmt.Fprintf(os.Stderr, "loco channel: establishing high-water mark for %s: %v; retrying\n", topic, err)
		select {
		case <-ctx.Done():
			return 0, false
		case <-time.After(latestSeqRetryBackoff):
		}
	}
}
