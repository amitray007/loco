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
	SchemaVersion int   `json:"schema_version"`
	Emitted       bool  `json:"emitted"`
	Event         Event `json:"event"`
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
}
type CompactResult struct {
	SchemaVersion  int      `json:"schema_version"`
	Topic          string   `json:"topic"`
	Removed        int      `json:"removed"`
	RetiredCursors []string `json:"retired_cursors"`
}

type topicMeta struct {
	SchemaVersion int               `json:"schema_version"`
	NextSeq       uint64            `json:"next_seq"`
	FloorSeq      uint64            `json:"floor_seq"`
	IDs           map[string]uint64 `json:"ids"`
}
type Cursor struct {
	SchemaVersion int    `json:"schema_version"`
	Topic         string `json:"topic"`
	Name          string `json:"name"`
	LastAckedSeq  uint64 `json:"last_acked_seq"`
	CreatedAt     string `json:"created_at"`
	LastReadAt    string `json:"last_read_at,omitempty"`
	LastAckAt     string `json:"last_ack_at,omitempty"`
	Status        string `json:"status"`
	RetiredAt     string `json:"retired_at,omitempty"`
}
type Alert struct {
	SchemaVersion int    `json:"schema_version"`
	ID            string `json:"id"`
	Timestamp     string `json:"ts"`
	Kind          string `json:"kind"`
	Message       string `json:"message"`
}

type Store struct {
	root, alerts string
	mu           sync.Mutex
	now          func() time.Time
	id           func() string
}

func New(root, alerts string) *Store {
	return &Store{root: root, alerts: alerts, now: time.Now, id: randomID}
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
	meta.IDs[event.ID], meta.NextSeq = event.Seq, event.Seq+1
	if err := writeJSONAtomic(s.metaPath(input.Topic), meta); err != nil {
		return EmitResult{}, fmt.Errorf("persist topic metadata: %w", err)
	}
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
	if limit <= 0 {
		limit = 100
	}
	if limit > 1000 {
		return ReadResult{}, errors.New("limit cannot exceed 1000")
	}
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
	if err := s.saveCursor(cursor); err != nil {
		return ReadResult{}, err
	}
	result := ReadResult{SchemaVersion: SchemaVersion, Topic: topic, Cursor: cursorName, Events: make([]Event, 0)}
	for _, event := range all {
		if event.Seq > cursor.LastAckedSeq {
			result.Events = append(result.Events, event)
			if len(result.Events) == limit {
				break
			}
		}
	}
	return result, nil
}

func (s *Store) Ack(topic, cursorName, throughID string) (AckResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := validateComponent("topic", topic); err != nil {
		return AckResult{}, err
	}
	if err := validateComponent("cursor", cursorName); err != nil {
		return AckResult{}, err
	}
	meta, _, err := s.loadTopic(topic)
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
	if changed {
		cursor.LastAckedSeq = seq
	}
	cursor.LastAckAt, cursor.Status, cursor.RetiredAt = s.now().UTC().Format(time.RFC3339Nano), "live", ""
	if err := s.saveCursor(cursor); err != nil {
		return AckResult{}, err
	}
	return AckResult{SchemaVersion: SchemaVersion, Topic: topic, Cursor: cursorName, ThroughID: throughID, ThroughSeq: seq, Changed: changed}, nil
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
		if err := writeJSONAtomic(s.metaPath(topic), meta); err != nil {
			return CompactResult{}, err
		}
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
			return nil, err
		}
		results = append(results, result)
	}
	return results, nil
}

func (s *Store) loadTopic(topic string) (topicMeta, []Event, error) {
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

func readEventFile(path string) ([]Event, error) {
	file, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return []Event{}, nil
	}
	if err != nil {
		return nil, err
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 64*1024), maxPayloadBytes+64*1024)
	result := make([]Event, 0)
	var previous uint64
	for scanner.Scan() {
		var event Event
		if err := json.Unmarshal(scanner.Bytes(), &event); err != nil {
			return nil, fmt.Errorf("decode event log %s: %w", path, err)
		}
		if event.SchemaVersion != SchemaVersion || event.Seq == 0 || (previous > 0 && event.Seq <= previous) {
			return nil, fmt.Errorf("invalid event sequence in %s", path)
		}
		if err := validateID(event.ID); err != nil {
			return nil, fmt.Errorf("invalid event in %s: %w", path, err)
		}
		if !json.Valid(event.Payload) {
			return nil, fmt.Errorf("invalid event payload in %s", path)
		}
		if _, err := time.Parse(time.RFC3339Nano, event.Timestamp); err != nil {
			return nil, fmt.Errorf("invalid event timestamp in %s: %w", path, err)
		}
		previous = event.Seq
		result = append(result, event)
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return result, nil
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
	defer file.Close()
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
	defer os.Remove(name)
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
	return os.Rename(name, path)
}
