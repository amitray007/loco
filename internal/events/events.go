package events

import (
	"bufio"
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"
)

const (
	SchemaVersion    = 1
	RetentionFloor   = 14 * 24 * time.Hour
	CursorIdleWindow = 30 * 24 * time.Hour
	maxPayloadBytes  = 1 << 20
)

var componentPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$`)

type Event struct {
	SchemaVersion int             `json:"schema_version"`
	ID            string          `json:"id"`
	Seq           uint64          `json:"seq"`
	Timestamp     string          `json:"ts"`
	SourceJob     string          `json:"source_job,omitempty"`
	Payload       json.RawMessage `json:"payload"`
}

type EmitInput struct {
	Topic     string          `json:"topic"`
	ID        string          `json:"id,omitempty"`
	SourceJob string          `json:"source_job,omitempty"`
	Payload   json.RawMessage `json:"payload"`
}
type EmitResult struct {
	SchemaVersion int  `json:"schema_version"`
	Emitted       bool `json:"emitted"`
	// Expired is set when the id was recognized as a duplicate but its event has been
	// compacted away — the emit is correctly suppressed, but the original payload is
	// no longer retained. Distinguishes a real dedup hit from a compacted one.
	Expired bool  `json:"expired,omitempty"`
	Event   Event `json:"event"`
}
type ReadResult struct {
	SchemaVersion int     `json:"schema_version"`
	Topic         string  `json:"topic"`
	Cursor        string  `json:"cursor"`
	Events        []Event `json:"events"`
}
type AckResult struct {
	SchemaVersion int    `json:"schema_version"`
	Topic         string `json:"topic"`
	Cursor        string `json:"cursor"`
	ThroughID     string `json:"through_id"`
	ThroughSeq    uint64 `json:"through_seq"`
	Changed       bool   `json:"changed"`
	// AckedCount is how many events this ack consumed — the blast radius. Reported
	// whether or not the ack skipped anything, so a log makes a post-mortem possible.
	AckedCount int `json:"acked_count"`
	// SkippedUnread is how many of those were never returned by a read on this cursor.
	// Non-zero means the ack consumed events the consumer cannot have processed.
	SkippedUnread int `json:"skipped_unread"`
}
type CompactResult struct {
	SchemaVersion  int      `json:"schema_version"`
	Topic          string   `json:"topic"`
	Removed        int      `json:"removed"`
	RetiredCursors []string `json:"retired_cursors"`
}
type CursorStatus struct {
	Name         string `json:"name"`
	Status       string `json:"status"`
	LastAckedSeq uint64 `json:"last_acked_seq"`
	Unread       int    `json:"unread"`
	Warning      string `json:"warning,omitempty"`
}
type TopicStatus struct {
	Name      string         `json:"name"`
	LatestSeq uint64         `json:"latest_seq"`
	Retained  int            `json:"retained"`
	Cursors   []CursorStatus `json:"cursors"`
	Error     string         `json:"error,omitempty"`
}

type topicMeta struct {
	SchemaVersion int               `json:"schema_version"`
	NextSeq       uint64            `json:"next_seq"`
	FloorSeq      uint64            `json:"floor_seq"`
	IDs           map[string]uint64 `json:"ids"`
	// Tombstones records ids whose events have been compacted away, keyed to the time
	// they were retired. They keep idempotency durable (a retried producer still
	// dedups) without pinning the full event, and are themselves pruned once older
	// than the retention floor so the map cannot grow without bound.
	Tombstones map[string]string `json:"tombstones,omitempty"`
}
type Cursor struct {
	SchemaVersion int    `json:"schema_version"`
	Topic         string `json:"topic"`
	Name          string `json:"name"`
	LastAckedSeq  uint64 `json:"last_acked_seq"`
	CreatedAt     string `json:"created_at"`
	LastReadAt    string `json:"last_read_at,omitempty"`
	// LastReadSeq is the highest seq this cursor has ever been handed by a read. Ack
	// compares against it to tell "the consumer processed these and is acking them"
	// from "the consumer is acking past events it never saw". Absent on cursors written
	// before this field existed, which decodes to 0 — strict then cannot fire on them,
	// which is the right default: loco does not know what they read.
	LastReadSeq uint64 `json:"last_read_seq,omitempty"`
	LastAckAt   string `json:"last_ack_at,omitempty"`
	Status      string `json:"status"`
	RetiredAt   string `json:"retired_at,omitempty"`
}
type Alert struct {
	SchemaVersion int    `json:"schema_version"`
	ID            string `json:"id"`
	Timestamp     string `json:"ts"`
	Kind          string `json:"kind"`
	Message       string `json:"message"`
}

type topicCacheEntry struct {
	meta   topicMeta
	events []Event
}

type Store struct {
	root, alerts string
	mu           sync.Mutex
	now          func() time.Time
	id           func() string
	// cache holds the parsed meta+events per topic so emit/read/ack do not re-read and
	// re-parse the entire log on every call (was O(n) per operation). Guarded by mu.
	// Entries are appended to on emit and dropped on any rewrite/compaction.
	cache map[string]*topicCacheEntry
}

func New(root, alerts string) *Store {
	return &Store{root: root, alerts: alerts, now: time.Now, id: randomID, cache: make(map[string]*topicCacheEntry)}
}

func (s *Store) Emit(input EmitInput) (EmitResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := validateComponent("topic", input.Topic); err != nil {
		return EmitResult{}, err
	}
	if len(input.Payload) == 0 || len(input.Payload) > maxPayloadBytes || !json.Valid(input.Payload) {
		return EmitResult{}, errors.New("data must be valid JSON no larger than 1 MiB")
	}
	if input.ID == "" {
		input.ID = s.id()
	}
	if err := validateID(input.ID); err != nil {
		return EmitResult{}, err
	}
	meta, existing, err := s.loadTopic(input.Topic)
	if err != nil {
		return EmitResult{}, err
	}
	if seq, ok := meta.IDs[input.ID]; ok {
		for _, event := range existing {
			if event.Seq == seq {
				return EmitResult{SchemaVersion: SchemaVersion, Emitted: false, Event: event}, nil
			}
		}
		return EmitResult{SchemaVersion: SchemaVersion, Emitted: false, Event: Event{SchemaVersion: SchemaVersion, ID: input.ID, Seq: seq}}, nil
	}
	// The id's event has been compacted away, but idempotency still holds: suppress the
	// re-emit and say so explicitly rather than injecting a duplicate.
	if _, ok := meta.Tombstones[input.ID]; ok {
		return EmitResult{SchemaVersion: SchemaVersion, Emitted: false, Expired: true, Event: Event{SchemaVersion: SchemaVersion, ID: input.ID}}, nil
	}
	event := Event{SchemaVersion: SchemaVersion, ID: input.ID, Seq: meta.NextSeq, Timestamp: s.now().UTC().Format(time.RFC3339Nano), SourceJob: input.SourceJob, Payload: append(json.RawMessage(nil), input.Payload...)}
	line, err := json.Marshal(event)
	if err != nil {
		return EmitResult{}, err
	}
	line = append(line, '\n')
	if err := os.MkdirAll(s.root, 0o700); err != nil {
		return EmitResult{}, err
	}
	file, err := os.OpenFile(s.topicPath(input.Topic), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return EmitResult{}, err
	}
	if _, err = file.Write(line); err == nil {
		err = file.Sync()
	}
	closeErr := file.Close()
	if err == nil {
		err = closeErr
	}
	if err != nil {
		return EmitResult{}, fmt.Errorf("append event: %w", err)
	}
	// Durably record the new topic file / grown log before the meta references it.
	if err := syncDir(s.root); err != nil {
		return EmitResult{}, fmt.Errorf("sync event dir: %w", err)
	}
	meta.IDs[event.ID], meta.NextSeq = event.Seq, event.Seq+1
	if err := writeJSONAtomic(s.metaPath(input.Topic), meta); err != nil {
		// Meta is now inconsistent with the appended log; drop the cache so the next
		// load reconciles from disk rather than trusting a stale in-memory copy.
		s.invalidate(input.Topic)
		return EmitResult{}, fmt.Errorf("persist topic metadata: %w", err)
	}
	// Keep the cache warm by appending in place instead of forcing a full re-read.
	s.cache[input.Topic] = &topicCacheEntry{meta: cloneMeta(meta), events: append(append([]Event(nil), existing...), event)}
	return EmitResult{SchemaVersion: SchemaVersion, Emitted: true, Event: event}, nil
}

func (s *Store) Read(topic, cursorName string, limit int) (ReadResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := validateComponent("topic", topic); err != nil {
		return ReadResult{}, err
	}
	if err := validateComponent("cursor", cursorName); err != nil {
		return ReadResult{}, err
	}
	if limit < 0 {
		return ReadResult{}, errors.New("limit cannot be negative")
	}
	if limit > 1000 {
		return ReadResult{}, errors.New("limit cannot exceed 1000")
	}
	// limit == 0 means "no events" (the caller's budget is exhausted); it is a valid,
	// honored request, not a request for the default page size. The CLI supplies its
	// own default (100) for the unset case before it ever reaches the store.
	meta, all, err := s.loadTopic(topic)
	if err != nil {
		return ReadResult{}, err
	}
	cursor, exists, err := s.loadCursor(topic, cursorName)
	if err != nil {
		return ReadResult{}, err
	}
	now := s.now().UTC().Format(time.RFC3339Nano)
	if !exists || cursor.Status == "retired" {
		last := uint64(0)
		if meta.FloorSeq > 0 {
			last = meta.FloorSeq - 1
		}
		cursor = Cursor{SchemaVersion: SchemaVersion, Topic: topic, Name: cursorName, LastAckedSeq: last, CreatedAt: now, Status: "live"}
	}
	cursor.LastReadAt, cursor.Status, cursor.RetiredAt = now, "live", ""
	result := ReadResult{SchemaVersion: SchemaVersion, Topic: topic, Cursor: cursorName, Events: make([]Event, 0)}
	for _, event := range all {
		if len(result.Events) >= limit {
			break
		}
		if event.Seq > cursor.LastAckedSeq {
			result.Events = append(result.Events, event)
		}
	}
	// Record how far this consumer has actually been shown, so an ack can tell a
	// processed batch from a watermark sweeping past events nobody read. The cursor is
	// saved after the batch is chosen, not before: the mark must describe what is being
	// handed over. Reads still never advance LastAckedSeq — this is not a read receipt,
	// only a bound on what the consumer could possibly have processed.
	if len(result.Events) > 0 {
		if high := result.Events[len(result.Events)-1].Seq; high > cursor.LastReadSeq {
			cursor.LastReadSeq = high
		}
	}
	if err := s.saveCursor(cursor); err != nil {
		return ReadResult{}, err
	}
	return result, nil
}

// Peek returns the most recent events on a topic WITHOUT creating or touching any
// cursor. Unlike Read, it has no side effects — so inspecting a topic can never mint a
// cursor that silently pins retention for 30 days. tail<=0 defaults to 20; fromSeq>0
// returns events with Seq>=fromSeq (oldest-first) instead of the tail.
func (s *Store) Peek(topic string, tail int, fromSeq uint64) (ReadResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := validateComponent("topic", topic); err != nil {
		return ReadResult{}, err
	}
	if tail < 0 {
		return ReadResult{}, errors.New("tail cannot be negative")
	}
	if tail == 0 {
		tail = 20
	}
	if tail > 1000 {
		return ReadResult{}, errors.New("tail cannot exceed 1000")
	}
	_, all, err := s.loadTopic(topic)
	if err != nil {
		return ReadResult{}, err
	}
	result := ReadResult{SchemaVersion: SchemaVersion, Topic: topic, Cursor: "", Events: make([]Event, 0)}
	if fromSeq > 0 {
		for _, event := range all {
			if event.Seq >= fromSeq {
				result.Events = append(result.Events, event)
				if len(result.Events) >= tail {
					break
				}
			}
		}
		return result, nil
	}
	// Tail: the last `tail` events in order.
	start := 0
	if len(all) > tail {
		start = len(all) - tail
	}
	result.Events = append(result.Events, all[start:]...)
	return result, nil
}

// ErrAckSkipsUnread is returned when a strict ack would consume events the cursor was
// never handed by a read. Callers that mean it pass allowSkip.
var ErrAckSkipsUnread = errors.New("ack would consume events that were never read")

// Ack advances a cursor's watermark to throughID's seq, consuming every event at or
// below it.
//
// allowSkip=false (strict) refuses an ack that would consume events this cursor has
// never been handed by a read. `ack --through ID` reads like "ack this event"; it means
// "ack this event and everything before it, read or not". A consumer that drains 5,
// fails on the 3rd, and acks the batch's last id — the natural thing to write — silently
// consumes the failure. At-least-once delivery then quietly depends on every consumer
// knowing to ack the highest CONTIGUOUSLY-succeeded id. That invariant belongs here.
func (s *Store) Ack(topic, cursorName, throughID string, allowSkip bool) (AckResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := validateComponent("topic", topic); err != nil {
		return AckResult{}, err
	}
	if err := validateComponent("cursor", cursorName); err != nil {
		return AckResult{}, err
	}
	meta, all, err := s.loadTopic(topic)
	if err != nil {
		return AckResult{}, err
	}
	seq, ok := meta.IDs[throughID]
	if !ok {
		return AckResult{}, fmt.Errorf("event id %q not found on topic %q", throughID, topic)
	}
	cursor, exists, err := s.loadCursor(topic, cursorName)
	if err != nil {
		return AckResult{}, err
	}
	if !exists {
		return AckResult{}, fmt.Errorf("cursor %q does not exist; read the topic first", cursorName)
	}
	changed := seq > cursor.LastAckedSeq

	// Blast radius: how many events this ack consumes, and how many of those the
	// consumer was never shown. Counting actual events rather than differencing seqs
	// keeps both honest across a compacted topic, where seqs are sparse.
	ackedCount, skippedUnread := 0, 0
	if changed {
		for _, event := range all {
			if event.Seq > cursor.LastAckedSeq && event.Seq <= seq {
				ackedCount++
				if event.Seq > cursor.LastReadSeq {
					skippedUnread++
				}
			}
		}
	}
	// A cursor with no recorded read history predates LastReadSeq: it was written before
	// the field existed, so 0 means "unknown", not "read nothing". Enforcing strict
	// against it would reject every existing consumer's first ack after an upgrade —
	// a louder failure than the silent one being fixed, and on data loco cannot judge.
	// The count is still reported; only the refusal is withheld.
	unknownReadHistory := cursor.LastReadSeq == 0
	if skippedUnread > 0 && !allowSkip && !unknownReadHistory {
		return AckResult{
				SchemaVersion: SchemaVersion, Topic: topic, Cursor: cursorName,
				ThroughID: throughID, ThroughSeq: seq, Changed: false,
				AckedCount: ackedCount, SkippedUnread: skippedUnread,
			}, fmt.Errorf("%w: %d of %d events through %q (seq %d) were never returned by a read on cursor %q; ack the highest contiguously-processed id, or pass allow-skip to consume them anyway",
				ErrAckSkipsUnread, skippedUnread, ackedCount, throughID, seq, cursorName)
	}

	if changed {
		cursor.LastAckedSeq = seq
	}
	cursor.LastAckAt, cursor.Status, cursor.RetiredAt = s.now().UTC().Format(time.RFC3339Nano), "live", ""
	if err := s.saveCursor(cursor); err != nil {
		return AckResult{}, err
	}
	return AckResult{
		SchemaVersion: SchemaVersion, Topic: topic, Cursor: cursorName,
		ThroughID: throughID, ThroughSeq: seq, Changed: changed,
		AckedCount: ackedCount, SkippedUnread: skippedUnread,
	}, nil
}

func (s *Store) Compact(topic string) (CompactResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := validateComponent("topic", topic); err != nil {
		return CompactResult{}, err
	}
	meta, all, err := s.loadTopic(topic)
	if err != nil {
		return CompactResult{}, err
	}
	cursors, err := s.loadCursors(topic)
	if err != nil {
		return CompactResult{}, err
	}
	now := s.now().UTC()
	result := CompactResult{SchemaVersion: SchemaVersion, Topic: topic, RetiredCursors: make([]string, 0)}
	// A topic with no cursor at all has no consumer yet — its events are unread, not
	// acked. Retain everything until a cursor exists so a late-wired consumer never
	// finds its early history silently compacted away.
	if len(cursors) == 0 {
		sort.Strings(result.RetiredCursors)
		return result, nil
	}
	minAck, haveLive := ^uint64(0), false
	for _, cursor := range cursors {
		activity, err := cursorActivity(cursor)
		if err != nil {
			return CompactResult{}, err
		}
		if cursor.Status != "retired" && now.Sub(activity) > CursorIdleWindow {
			cursor.Status, cursor.RetiredAt = "retired", now.Format(time.RFC3339Nano)
			if err := s.saveCursor(cursor); err != nil {
				return CompactResult{}, err
			}
			result.RetiredCursors = append(result.RetiredCursors, cursor.Name)
			_ = s.appendAlert(Alert{SchemaVersion: SchemaVersion, ID: s.id(), Timestamp: now.Format(time.RFC3339Nano), Kind: "cursor_retired", Message: fmt.Sprintf("cursor %s on topic %s retired after 30d idle", cursor.Name, topic)})
		}
		if cursor.Status == "live" {
			haveLive = true
			if cursor.LastAckedSeq < minAck {
				minAck = cursor.LastAckedSeq
			}
		}
	}
	kept := make([]Event, 0, len(all))
	cutoff := now.Add(-RetentionFloor)
	for _, event := range all {
		ts, err := time.Parse(time.RFC3339Nano, event.Timestamp)
		if err != nil {
			return CompactResult{}, err
		}
		acked := !haveLive || event.Seq <= minAck
		if acked && ts.Before(cutoff) {
			result.Removed++
			continue
		}
		kept = append(kept, event)
	}
	if result.Removed > 0 {
		if err := s.rewriteTopic(topic, kept); err != nil {
			return CompactResult{}, err
		}
		if len(kept) > 0 {
			meta.FloorSeq = kept[0].Seq
		} else {
			meta.FloorSeq = meta.NextSeq
		}
		// Rebuild the id->seq index over retained events only, and move the ids of
		// dropped events into time-stamped tombstones so idempotency stays durable
		// (a retried producer still dedups) without pinning the full event. Prune
		// tombstones older than the retention floor so the map cannot grow unbounded.
		retainedIDs := make(map[string]uint64, len(kept))
		for _, event := range kept {
			retainedIDs[event.ID] = event.Seq
		}
		if meta.Tombstones == nil {
			meta.Tombstones = make(map[string]string)
		}
		nowStamp := now.Format(time.RFC3339Nano)
		for id := range meta.IDs {
			if _, stillLive := retainedIDs[id]; !stillLive {
				meta.Tombstones[id] = nowStamp
			}
		}
		for id, stamp := range meta.Tombstones {
			ts, perr := time.Parse(time.RFC3339Nano, stamp)
			if perr != nil || ts.Before(cutoff) {
				delete(meta.Tombstones, id)
			}
		}
		meta.IDs = retainedIDs
		if err := writeJSONAtomic(s.metaPath(topic), meta); err != nil {
			s.invalidate(topic)
			return CompactResult{}, err
		}
		// The log and meta were rewritten out of band of the emit path; refresh the
		// cache from the compacted state.
		s.cache[topic] = &topicCacheEntry{meta: cloneMeta(meta), events: append([]Event(nil), kept...)}
	}
	sort.Strings(result.RetiredCursors)
	return result, nil
}

func (s *Store) CompactAll() ([]CompactResult, error) {
	entries, err := os.ReadDir(filepath.Join(s.root, ".meta"))
	if errors.Is(err, os.ErrNotExist) {
		return []CompactResult{}, nil
	}
	if err != nil {
		return nil, err
	}
	results := make([]CompactResult, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".json" {
			continue
		}
		topic := strings.TrimSuffix(entry.Name(), ".json")
		result, err := s.Compact(topic)
		if err != nil {
			// One corrupt topic must never abort startup maintenance or take down
			// every unrelated cron job and service. Record an alert and carry on.
			_ = s.appendAlert(Alert{SchemaVersion: SchemaVersion, ID: s.id(), Timestamp: s.now().UTC().Format(time.RFC3339Nano), Kind: "topic_unreadable", Message: fmt.Sprintf("skipped compaction of topic %s: %v", topic, err)})
			continue
		}
		results = append(results, result)
	}
	return results, nil
}

func (s *Store) Overview() ([]TopicStatus, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	entries, err := os.ReadDir(filepath.Join(s.root, ".meta"))
	if errors.Is(err, os.ErrNotExist) {
		return []TopicStatus{}, nil
	}
	if err != nil {
		return nil, err
	}
	result := make([]TopicStatus, 0, len(entries))
	now := s.now().UTC()
	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".json" {
			continue
		}
		topic := strings.TrimSuffix(entry.Name(), ".json")
		meta, all, err := s.loadTopic(topic)
		if err != nil {
			// Isolate the failure to this one topic so `status` still reports every
			// healthy topic, job, and service during a partial-corruption incident.
			result = append(result, TopicStatus{Name: topic, Cursors: make([]CursorStatus, 0), Error: err.Error()})
			continue
		}
		cursors, err := s.loadCursors(topic)
		if err != nil {
			result = append(result, TopicStatus{Name: topic, Cursors: make([]CursorStatus, 0), Error: err.Error()})
			continue
		}
		item := TopicStatus{Name: topic, LatestSeq: meta.NextSeq - 1, Retained: len(all), Cursors: make([]CursorStatus, 0, len(cursors))}
		sort.Slice(cursors, func(i, j int) bool { return cursors[i].Name < cursors[j].Name })
		for _, cursor := range cursors {
			cs := CursorStatus{Name: cursor.Name, Status: cursor.Status, LastAckedSeq: cursor.LastAckedSeq}
			var oldest time.Time
			for _, event := range all {
				if event.Seq > cursor.LastAckedSeq {
					cs.Unread++
					if oldest.IsZero() {
						oldest, _ = time.Parse(time.RFC3339Nano, event.Timestamp)
					}
				}
			}
			if cursor.Status == "live" && !oldest.IsZero() && now.Sub(oldest) > RetentionFloor {
				cs.Warning = "unacked events older than 14d"
			}
			item.Cursors = append(item.Cursors, cs)
		}
		result = append(result, item)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Name < result[j].Name })
	return result, nil
}

func (s *Store) RecentAlerts(since time.Time, limit int) ([]Alert, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if limit <= 0 {
		limit = 10
	}
	file, err := os.Open(s.alerts)
	if errors.Is(err, os.ErrNotExist) {
		return []Alert{}, nil
	}
	if err != nil {
		return nil, err
	}
	defer func() { _ = file.Close() }()
	scanner := bufio.NewScanner(file)
	result := make([]Alert, 0)
	for scanner.Scan() {
		var alert Alert
		if err := json.Unmarshal(scanner.Bytes(), &alert); err != nil {
			return nil, err
		}
		ts, err := time.Parse(time.RFC3339Nano, alert.Timestamp)
		if err != nil {
			return nil, err
		}
		if !ts.Before(since) {
			result = append(result, alert)
			if len(result) > limit {
				result = result[1:]
			}
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return result, nil
}

// loadTopic returns the topic's meta and events, served from an in-memory cache when
// warm. Callers receive copies so they can never mutate cached state. Must be called
// with s.mu held.
func (s *Store) loadTopic(topic string) (topicMeta, []Event, error) {
	if entry, ok := s.cache[topic]; ok {
		return cloneMeta(entry.meta), append([]Event(nil), entry.events...), nil
	}
	meta, events, err := s.loadTopicFromDisk(topic)
	if err != nil {
		return topicMeta{}, nil, err
	}
	s.cache[topic] = &topicCacheEntry{meta: cloneMeta(meta), events: append([]Event(nil), events...)}
	return meta, events, nil
}

func cloneMeta(m topicMeta) topicMeta {
	out := m
	out.IDs = make(map[string]uint64, len(m.IDs))
	for k, v := range m.IDs {
		out.IDs[k] = v
	}
	if m.Tombstones != nil {
		out.Tombstones = make(map[string]string, len(m.Tombstones))
		for k, v := range m.Tombstones {
			out.Tombstones[k] = v
		}
	}
	return out
}

// invalidate drops the cached copy of a topic after its on-disk state changes out of
// band of the incremental emit path (rewrite, compaction, meta reconciliation).
func (s *Store) invalidate(topic string) { delete(s.cache, topic) }

func (s *Store) loadTopicFromDisk(topic string) (topicMeta, []Event, error) {
	meta := topicMeta{SchemaVersion: SchemaVersion, NextSeq: 1, FloorSeq: 1, IDs: make(map[string]uint64)}
	if data, err := os.ReadFile(s.metaPath(topic)); err == nil {
		if err := json.Unmarshal(data, &meta); err != nil {
			return topicMeta{}, nil, fmt.Errorf("decode topic metadata: %w", err)
		}
		if meta.SchemaVersion != SchemaVersion {
			return topicMeta{}, nil, fmt.Errorf("unsupported topic schema %d", meta.SchemaVersion)
		}
		if meta.NextSeq == 0 || meta.FloorSeq == 0 {
			return topicMeta{}, nil, errors.New("invalid zero sequence in topic metadata")
		}
		if meta.IDs == nil {
			meta.IDs = make(map[string]uint64)
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return topicMeta{}, nil, err
	}
	events, err := readEventFile(s.topicPath(topic))
	if err != nil {
		return topicMeta{}, nil, err
	}
	changed := false
	for _, event := range events {
		if previous, ok := meta.IDs[event.ID]; ok && previous != event.Seq {
			return topicMeta{}, nil, fmt.Errorf("duplicate event id %q has conflicting sequences", event.ID)
		}
		if _, ok := meta.IDs[event.ID]; !ok {
			meta.IDs[event.ID], changed = event.Seq, true
		}
		if event.Seq >= meta.NextSeq {
			meta.NextSeq, changed = event.Seq+1, true
		}
	}
	if len(events) > 0 && meta.FloorSeq != events[0].Seq {
		meta.FloorSeq, changed = events[0].Seq, true
	}
	if changed {
		if err := writeJSONAtomic(s.metaPath(topic), meta); err != nil {
			return topicMeta{}, nil, err
		}
	}
	return meta, events, nil
}

// readEventFile parses a topic log line by line. A crash or power loss mid-append
// leaves a torn final line (partial JSON, no trailing newline); because the append
// path fsyncs every complete record, only the very last line can be torn. We tolerate
// exactly that case: if the final line fails to parse, we truncate it on disk and
// return the recovered prefix so one interrupted write can never brick a topic or the
// daemon. Any earlier corrupt line is still a hard error — that is real damage.
func readEventFile(path string) ([]Event, error) {
	file, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return []Event{}, nil
	}
	if err != nil {
		return nil, err
	}
	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 64*1024), maxPayloadBytes+64*1024)
	result := make([]Event, 0)
	var previous uint64
	// Track the byte offset of the start of the current line so we can truncate a
	// torn tail back to the last known-good record.
	var goodOffset int64
	torn := false
	for scanner.Scan() {
		raw := scanner.Bytes()
		lineLen := int64(len(raw)) + 1 // + newline consumed by the scanner
		// A blank/whitespace final line is a benign torn tail (partial write with no
		// content yet). Skip it only if it is genuinely last.
		if len(bytes.TrimSpace(raw)) == 0 {
			if scanner.Scan() || scanner.Err() != nil {
				_ = file.Close()
				return nil, fmt.Errorf("decode event log %s: unexpected blank line", path)
			}
			torn = true
			break
		}
		event, perr := parseEventLine(raw, previous)
		if perr != nil {
			// Only the final line may be torn. If more data follows, this is
			// corruption in the middle of the log — refuse to guess.
			if scanner.Scan() || scanner.Err() != nil {
				_ = file.Close()
				return nil, fmt.Errorf("decode event log %s: %w", path, perr)
			}
			torn = true
			break
		}
		previous = event.Seq
		goodOffset += lineLen
		result = append(result, event)
	}
	scanErr := scanner.Err()
	_ = file.Close()
	if scanErr != nil && !torn {
		return nil, scanErr
	}
	if torn {
		// Repair: truncate the torn trailing bytes so the next append starts clean.
		if err := os.Truncate(path, goodOffset); err != nil {
			return nil, fmt.Errorf("repair torn event log %s: %w", path, err)
		}
	}
	return result, nil
}

func parseEventLine(raw []byte, previous uint64) (Event, error) {
	var event Event
	if err := json.Unmarshal(raw, &event); err != nil {
		return Event{}, err
	}
	if event.SchemaVersion != SchemaVersion || event.Seq == 0 || (previous > 0 && event.Seq <= previous) {
		return Event{}, errors.New("invalid event sequence")
	}
	if err := validateID(event.ID); err != nil {
		return Event{}, fmt.Errorf("invalid event id: %w", err)
	}
	if !json.Valid(event.Payload) {
		return Event{}, errors.New("invalid event payload")
	}
	if _, err := time.Parse(time.RFC3339Nano, event.Timestamp); err != nil {
		return Event{}, fmt.Errorf("invalid event timestamp: %w", err)
	}
	return event, nil
}

func (s *Store) loadCursor(topic, name string) (Cursor, bool, error) {
	data, err := os.ReadFile(s.cursorPath(topic, name))
	if errors.Is(err, os.ErrNotExist) {
		return Cursor{}, false, nil
	}
	if err != nil {
		return Cursor{}, false, err
	}
	var cursor Cursor
	if err := json.Unmarshal(data, &cursor); err != nil {
		return Cursor{}, false, err
	}
	if cursor.SchemaVersion != SchemaVersion {
		return Cursor{}, false, fmt.Errorf("unsupported cursor schema %d", cursor.SchemaVersion)
	}
	if cursor.Topic != topic || cursor.Name != name {
		return Cursor{}, false, errors.New("cursor file identity does not match its path")
	}
	if cursor.Status != "live" && cursor.Status != "retired" {
		return Cursor{}, false, fmt.Errorf("invalid cursor status %q", cursor.Status)
	}
	return cursor, true, nil
}
func (s *Store) loadCursors(topic string) ([]Cursor, error) {
	dir := s.cursorDir(topic)
	entries, err := os.ReadDir(dir)
	if errors.Is(err, os.ErrNotExist) {
		return []Cursor{}, nil
	}
	if err != nil {
		return nil, err
	}
	result := make([]Cursor, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".json" {
			continue
		}
		data, err := os.ReadFile(filepath.Join(dir, entry.Name()))
		if err != nil {
			return nil, err
		}
		var cursor Cursor
		if json.Unmarshal(data, &cursor) != nil {
			return nil, fmt.Errorf("decode cursor %s", entry.Name())
		}
		if cursor.Topic != topic || cursor.Name+".json" != entry.Name() || (cursor.Status != "live" && cursor.Status != "retired") {
			return nil, fmt.Errorf("invalid cursor %s", entry.Name())
		}
		result = append(result, cursor)
	}
	return result, nil
}
func (s *Store) saveCursor(cursor Cursor) error {
	return writeJSONAtomic(s.cursorPath(cursor.Topic, cursor.Name), cursor)
}
func (s *Store) rewriteTopic(topic string, events []Event) error {
	var buffer bytes.Buffer
	encoder := json.NewEncoder(&buffer)
	for _, event := range events {
		if err := encoder.Encode(event); err != nil {
			return err
		}
	}
	return writeBytesAtomic(s.topicPath(topic), buffer.Bytes())
}
func (s *Store) appendAlert(alert Alert) error {
	if s.alerts == "" {
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(s.alerts), 0o700); err != nil {
		return err
	}
	data, _ := json.Marshal(alert)
	data = append(data, '\n')
	file, err := os.OpenFile(s.alerts, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	defer func() { _ = file.Close() }()
	if _, err = file.Write(data); err != nil {
		return err
	}
	return file.Sync()
}
func cursorActivity(cursor Cursor) (time.Time, error) {
	var latest time.Time
	for _, value := range []string{cursor.CreatedAt, cursor.LastReadAt, cursor.LastAckAt} {
		if value == "" {
			continue
		}
		parsed, err := time.Parse(time.RFC3339Nano, value)
		if err != nil {
			return time.Time{}, fmt.Errorf("parse cursor activity: %w", err)
		}
		if parsed.After(latest) {
			latest = parsed
		}
	}
	if latest.IsZero() {
		return time.Time{}, errors.New("cursor has no activity timestamp")
	}
	return latest, nil
}

func (s *Store) topicPath(topic string) string { return filepath.Join(s.root, topic+".jsonl") }
func (s *Store) metaPath(topic string) string  { return filepath.Join(s.root, ".meta", topic+".json") }
func (s *Store) cursorDir(topic string) string { return filepath.Join(s.root, ".cursors", topic) }
func (s *Store) cursorPath(topic, name string) string {
	return filepath.Join(s.cursorDir(topic), name+".json")
}

func validateComponent(kind, value string) error {
	if !componentPattern.MatchString(value) {
		return fmt.Errorf("invalid %s %q: use 1-128 letters, digits, dots, underscores, or hyphens", kind, value)
	}
	return nil
}
func validateID(id string) error {
	if len(id) == 0 || len(id) > 256 || strings.ContainsAny(id, "\r\n\x00") {
		return errors.New("event id must be 1-256 characters without control line breaks")
	}
	return nil
}
func randomID() string {
	var raw [12]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return fmt.Sprintf("%d", time.Now().UnixNano())
	}
	return hex.EncodeToString(raw[:])
}

func writeJSONAtomic(path string, value any) error {
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')
	return writeBytesAtomic(path, data)
}
func writeBytesAtomic(path string, data []byte) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(path), ".events-*")
	if err != nil {
		return err
	}
	name := tmp.Name()
	defer func() { _ = os.Remove(name) }()
	if err := tmp.Chmod(0o600); err != nil {
		_ = tmp.Close()
		return err
	}
	if _, err = tmp.Write(data); err != nil {
		_ = tmp.Close()
		return err
	}
	if err = tmp.Sync(); err != nil {
		_ = tmp.Close()
		return err
	}
	if err = tmp.Close(); err != nil {
		return err
	}
	if err = os.Rename(name, path); err != nil {
		return err
	}
	return syncDir(filepath.Dir(path))
}

// syncDir fsyncs a directory so a create/rename within it is durable across a power
// loss. Without this, a just-committed rename can silently revert on a hard crash.
func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	if err := d.Sync(); err != nil {
		_ = d.Close()
		return err
	}
	return d.Close()
}
