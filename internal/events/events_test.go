package events

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func TestEmitReadAckAndDedupe(t *testing.T) {
	store := testStore(t)
	first, err := store.Emit(EmitInput{Topic: "demo.topic", ID: "same-id", Payload: json.RawMessage(`{"ok":true}`)})
	if err != nil {
		t.Fatal(err)
	}
	if !first.Emitted || first.Event.Seq != 1 {
		t.Fatalf("first=%+v", first)
	}
	duplicate, err := store.Emit(EmitInput{Topic: "demo.topic", ID: "same-id", Payload: json.RawMessage(`{"ok":false}`)})
	if err != nil {
		t.Fatal(err)
	}
	if duplicate.Emitted || duplicate.Event.Seq != 1 {
		t.Fatalf("duplicate=%+v", duplicate)
	}
	read, err := store.Read("demo.topic", "c1", 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(read.Events) != 1 || read.Events[0].ID != "same-id" {
		t.Fatalf("read=%+v", read)
	}
	again, _ := store.Read("demo.topic", "c1", 100)
	if len(again.Events) != 1 {
		t.Fatal("read advanced cursor without ack")
	}
	ack, err := store.Ack("demo.topic", "c1", "same-id")
	if err != nil {
		t.Fatal(err)
	}
	if !ack.Changed || ack.ThroughSeq != 1 {
		t.Fatalf("ack=%+v", ack)
	}
	empty, _ := store.Read("demo.topic", "c1", 100)
	if len(empty.Events) != 0 {
		t.Fatalf("after ack=%+v", empty)
	}
	secondCursor, _ := store.Read("demo.topic", "c2", 100)
	if len(secondCursor.Events) != 1 {
		t.Fatal("broadcast cursor did not receive event independently")
	}
}

func TestSequenceAndDedupeRecoverFromStaleMetadata(t *testing.T) {
	store := testStore(t)
	if _, err := store.Emit(EmitInput{Topic: "recovery", ID: "one", Payload: json.RawMessage(`1`)}); err != nil {
		t.Fatal(err)
	}
	stale := topicMeta{SchemaVersion: SchemaVersion, NextSeq: 1, FloorSeq: 1, IDs: map[string]uint64{}}
	if err := writeJSONAtomic(store.metaPath("recovery"), stale); err != nil {
		t.Fatal(err)
	}
	reopened := New(store.root, store.alerts)
	second, err := reopened.Emit(EmitInput{Topic: "recovery", ID: "two", Payload: json.RawMessage(`2`)})
	if err != nil {
		t.Fatal(err)
	}
	if second.Event.Seq != 2 {
		t.Fatalf("recovered seq=%d", second.Event.Seq)
	}
	dupe, err := reopened.Emit(EmitInput{Topic: "recovery", ID: "one", Payload: json.RawMessage(`3`)})
	if err != nil {
		t.Fatal(err)
	}
	if dupe.Emitted || dupe.Event.Seq != 1 {
		t.Fatalf("recovered dedupe=%+v", dupe)
	}
}

func TestCompactionWaitsForEveryLiveCursorAndPreservesWatermark(t *testing.T) {
	store := testStore(t)
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	store.now = func() time.Time { return now }
	if _, err := store.Emit(EmitInput{Topic: "compact", ID: "old", Payload: json.RawMessage(`{}`)}); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"c1", "c2"} {
		if _, err := store.Read("compact", name, 100); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := store.Ack("compact", "c1", "old"); err != nil {
		t.Fatal(err)
	}
	now = now.Add(15 * 24 * time.Hour)
	result, err := store.Compact("compact")
	if err != nil {
		t.Fatal(err)
	}
	if result.Removed != 0 {
		t.Fatalf("removed with live unacked cursor: %+v", result)
	}
	if _, err := store.Ack("compact", "c2", "old"); err != nil {
		t.Fatal(err)
	}
	result, err = store.Compact("compact")
	if err != nil {
		t.Fatal(err)
	}
	if result.Removed != 1 {
		t.Fatalf("result=%+v", result)
	}
	dupe, err := store.Emit(EmitInput{Topic: "compact", ID: "old", Payload: json.RawMessage(`{"again":true}`)})
	if err != nil {
		t.Fatal(err)
	}
	if dupe.Emitted || dupe.Event.Seq != 1 {
		t.Fatalf("dedupe after compaction=%+v", dupe)
	}
	newEvent, err := store.Emit(EmitInput{Topic: "compact", ID: "new", Payload: json.RawMessage(`{}`)})
	if err != nil {
		t.Fatal(err)
	}
	if newEvent.Event.Seq != 2 {
		t.Fatalf("new seq=%d", newEvent.Event.Seq)
	}
	read, err := store.Read("compact", "c1", 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(read.Events) != 1 || read.Events[0].Seq != 2 {
		t.Fatalf("watermark read=%+v", read)
	}
}

func TestIdleCursorRetiresAndReturnsAtCompactionFloor(t *testing.T) {
	store := testStore(t)
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	store.now = func() time.Time { return now }
	if _, err := store.Emit(EmitInput{Topic: "retire", ID: "old", Payload: json.RawMessage(`{}`)}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Read("retire", "abandoned", 100); err != nil {
		t.Fatal(err)
	}
	now = now.Add(31 * 24 * time.Hour)
	result, err := store.Compact("retire")
	if err != nil {
		t.Fatal(err)
	}
	if result.Removed != 1 || len(result.RetiredCursors) != 1 || result.RetiredCursors[0] != "abandoned" {
		t.Fatalf("compact=%+v", result)
	}
	read, err := store.Read("retire", "abandoned", 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(read.Events) != 0 {
		t.Fatalf("retired cursor replayed compacted event: %+v", read)
	}
	alerts, err := os.ReadFile(store.alerts)
	if err != nil {
		t.Fatal(err)
	}
	var alert Alert
	if err := json.Unmarshal(bytes.TrimSpace(alerts), &alert); err != nil {
		t.Fatal(err)
	}
	if alert.Kind != "cursor_retired" {
		t.Fatalf("alert=%+v", alert)
	}
}

func TestValidationAndAckRequiresRead(t *testing.T) {
	store := testStore(t)
	if _, err := store.Emit(EmitInput{Topic: "bad/topic", Payload: json.RawMessage(`{}`)}); err == nil {
		t.Fatal("invalid topic accepted")
	}
	if _, err := store.Emit(EmitInput{Topic: "good", ID: "x", Payload: json.RawMessage(`{broken`)}); err == nil {
		t.Fatal("invalid JSON accepted")
	}
	if _, err := store.Emit(EmitInput{Topic: "good", ID: "x", Payload: json.RawMessage(`{}`)}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Ack("good", "missing", "x"); err == nil {
		t.Fatal("ack created a cursor")
	}
}

func TestLimitAndAckThroughMiddleSequence(t *testing.T) {
	store := testStore(t)
	for _, id := range []string{"one", "two", "three"} {
		if _, err := store.Emit(EmitInput{Topic: "ordered", ID: id, Payload: json.RawMessage(`{}`)}); err != nil {
			t.Fatal(err)
		}
	}
	read, err := store.Read("ordered", "consumer", 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(read.Events) != 2 || read.Events[1].ID != "two" {
		t.Fatalf("limited read=%+v", read)
	}
	if _, err := store.Ack("ordered", "consumer", "two"); err != nil {
		t.Fatal(err)
	}
	remaining, err := store.Read("ordered", "consumer", 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(remaining.Events) != 1 || remaining.Events[0].ID != "three" {
		t.Fatalf("remaining=%+v", remaining)
	}
}

func TestConcurrentEmitsRemainMonotonicAndIdempotent(t *testing.T) {
	store := testStore(t)
	const count = 40
	var wg sync.WaitGroup
	errorsCh := make(chan error, count)
	for i := 0; i < count; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, err := store.Emit(EmitInput{Topic: "concurrent", ID: fmt.Sprintf("id-%d", i), Payload: json.RawMessage(`{}`)})
			errorsCh <- err
		}(i)
	}
	wg.Wait()
	close(errorsCh)
	for err := range errorsCh {
		if err != nil {
			t.Fatal(err)
		}
	}
	read, err := store.Read("concurrent", "reader", 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(read.Events) != count {
		t.Fatalf("count=%d", len(read.Events))
	}
	for i, event := range read.Events {
		if event.Seq != uint64(i+1) {
			t.Fatalf("event %d seq=%d", i, event.Seq)
		}
	}
	emitted := 0
	wg = sync.WaitGroup{}
	results := make(chan EmitResult, 10)
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			result, _ := store.Emit(EmitInput{Topic: "same", ID: "shared", Payload: json.RawMessage(`{}`)})
			results <- result
		}()
	}
	wg.Wait()
	close(results)
	for result := range results {
		if result.Emitted {
			emitted++
		}
	}
	if emitted != 1 {
		t.Fatalf("emitted=%d, want 1", emitted)
	}
}

func testStore(t *testing.T) *Store {
	t.Helper()
	root := t.TempDir()
	return New(filepath.Join(root, "events"), filepath.Join(root, "alerts.jsonl"))
}
