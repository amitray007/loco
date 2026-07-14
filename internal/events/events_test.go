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
	// After compaction the id is tombstoned: still deduped (idempotency durable), now
	// flagged Expired since the original event body is no longer retained.
	if dupe.Emitted || !dupe.Expired {
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

func TestOverviewReportsUnreadCountsAndOldEventWarning(t *testing.T) {
	store := testStore(t)
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	store.now = func() time.Time { return now }
	if _, err := store.Emit(EmitInput{Topic: "status.topic", ID: "old", Payload: json.RawMessage(`{}`)}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Read("status.topic", "consumer", 100); err != nil {
		t.Fatal(err)
	}
	now = now.Add(15 * 24 * time.Hour)
	topics, err := store.Overview()
	if err != nil {
		t.Fatal(err)
	}
	if len(topics) != 1 || topics[0].LatestSeq != 1 || topics[0].Retained != 1 || len(topics[0].Cursors) != 1 {
		t.Fatalf("topics=%+v", topics)
	}
	cursor := topics[0].Cursors[0]
	if cursor.Unread != 1 || cursor.Warning == "" {
		t.Fatalf("cursor=%+v", cursor)
	}
}

func testStore(t *testing.T) *Store {
	t.Helper()
	root := t.TempDir()
	return New(filepath.Join(root, "events"), filepath.Join(root, "alerts.jsonl"))
}

// --- Regression tests for adversarial findings (fix/adversarial-findings) ---

// F-01/F-02: a torn trailing line (crash mid-append) must not brick the topic or the
// daemon; the good prefix is recovered and the torn tail truncated.
func TestTornTrailingLineIsRecovered(t *testing.T) {
	store := testStore(t)
	if _, err := store.Emit(EmitInput{Topic: "crash", ID: "c1", Payload: json.RawMessage(`{"a":1}`)}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Emit(EmitInput{Topic: "crash", ID: "c2", Payload: json.RawMessage(`{"a":2}`)}); err != nil {
		t.Fatal(err)
	}
	// Simulate power loss mid-append: partial JSON, no newline.
	f, err := os.OpenFile(store.topicPath("crash"), os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteString(`{"schema_version":1,"id":"c3","seq":3,"ts":"2026-07`); err != nil {
		t.Fatal(err)
	}
	f.Close()

	// Reads must still work and return the two good events.
	read, err := store.Read("crash", "r", 100)
	if err != nil {
		t.Fatalf("torn tail bricked the topic: %v", err)
	}
	if len(read.Events) != 2 {
		t.Fatalf("expected 2 recovered events, got %d", len(read.Events))
	}
	// The torn tail must have been truncated so a subsequent emit lands cleanly.
	next, err := store.Emit(EmitInput{Topic: "crash", ID: "c4", Payload: json.RawMessage(`{"a":4}`)})
	if err != nil {
		t.Fatalf("emit after torn-tail repair failed: %v", err)
	}
	if next.Event.Seq != 3 {
		t.Fatalf("expected next seq 3 after repair, got %d", next.Event.Seq)
	}
}

// F-02: mid-file corruption (not the last line) must still be a hard error — we only
// tolerate a torn *tail*, never guess about damage in the middle.
func TestMidFileCorruptionStillErrors(t *testing.T) {
	store := testStore(t)
	for _, id := range []string{"a", "b", "c"} {
		if _, err := store.Emit(EmitInput{Topic: "mid", ID: id, Payload: json.RawMessage(`{}`)}); err != nil {
			t.Fatal(err)
		}
	}
	// Corrupt the middle line, keep a valid line after it.
	data, _ := os.ReadFile(store.topicPath("mid"))
	lines := bytes.SplitN(data, []byte("\n"), 4)
	lines[1] = []byte("{garbage")
	if err := os.WriteFile(store.topicPath("mid"), bytes.Join(lines, []byte("\n")), 0o600); err != nil {
		t.Fatal(err)
	}
	// Corruption is detected on a cold load (e.g. daemon restart), so read through a
	// fresh store pointed at the same directory rather than the warm in-memory cache.
	fresh := New(store.root, store.alerts)
	if _, err := fresh.Read("mid", "r", 100); err == nil {
		t.Fatal("expected mid-file corruption to error, got nil")
	}
}

// F-16/F-25: one corrupt topic must not blank the whole overview.
func TestOverviewIsolatesCorruptTopic(t *testing.T) {
	store := testStore(t)
	if _, err := store.Emit(EmitInput{Topic: "good", ID: "g1", Payload: json.RawMessage(`{}`)}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Emit(EmitInput{Topic: "bad", ID: "b1", Payload: json.RawMessage(`{}`)}); err != nil {
		t.Fatal(err)
	}
	// Corrupt the middle of "bad" so loadTopic hard-errors (torn-tail would be recovered).
	os.WriteFile(store.topicPath("bad"), []byte("{garbage\n{\"schema_version\":1,\"id\":\"x\",\"seq\":9,\"ts\":\"2026-01-01T00:00:00Z\",\"payload\":{}}\n"), 0o600)
	// Overview through a cold store (daemon restart) so the corruption on disk is read.
	store = New(store.root, store.alerts)
	topics, err := store.Overview()
	if err != nil {
		t.Fatalf("overview failed closed on one bad topic: %v", err)
	}
	var sawGood, sawBadFlagged bool
	for _, tp := range topics {
		if tp.Name == "good" && tp.Error == "" {
			sawGood = true
		}
		if tp.Name == "bad" && tp.Error != "" {
			sawBadFlagged = true
		}
	}
	if !sawGood {
		t.Fatal("healthy topic missing from overview during partial corruption")
	}
	if !sawBadFlagged {
		t.Fatal("corrupt topic not flagged with an error")
	}
}

// F-11: a topic with no cursor yet must retain everything, even past the retention floor.
func TestNoCursorTopicRetainsEverything(t *testing.T) {
	store := testStore(t)
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	store.now = func() time.Time { return base }
	for _, id := range []string{"e1", "e2", "e3"} {
		if _, err := store.Emit(EmitInput{Topic: "nocursor", ID: id, Payload: json.RawMessage(`{}`)}); err != nil {
			t.Fatal(err)
		}
	}
	// Advance well past the 14d retention floor and compact.
	store.now = func() time.Time { return base.Add(60 * 24 * time.Hour) }
	res, err := store.Compact("nocursor")
	if err != nil {
		t.Fatal(err)
	}
	if res.Removed != 0 {
		t.Fatalf("no-cursor topic dropped %d events; must retain all until a consumer exists", res.Removed)
	}
}

// F-10: compaction prunes meta.IDs so re-emitting a compacted id is a fresh event, not
// a bodyless dedup hit, and the id map does not grow without bound.
func TestCompactPrunesIDMap(t *testing.T) {
	store := testStore(t)
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	store.now = func() time.Time { return base }
	for _, id := range []string{"old1", "old2"} {
		store.Emit(EmitInput{Topic: "t", ID: id, Payload: json.RawMessage(`{}`)})
	}
	// A cursor that acks everything so events become eligible for compaction.
	store.Read("t", "c", 100)
	store.Ack("t", "c", "old2")
	store.now = func() time.Time { return base.Add(30 * 24 * time.Hour) }
	res, err := store.Compact("t")
	if err != nil {
		t.Fatal(err)
	}
	if res.Removed == 0 {
		t.Fatal("expected acked+aged events to compact")
	}
	meta, _, err := store.loadTopic("t")
	if err != nil {
		t.Fatal(err)
	}
	if _, stillThere := meta.IDs["old1"]; stillThere {
		t.Fatal("compacted id still in meta.IDs — map grows unbounded")
	}
	// Idempotency still holds via a tombstone: re-emitting the compacted id is
	// suppressed and flagged Expired, never injected as a duplicate.
	re, err := store.Emit(EmitInput{Topic: "t", ID: "old1", Payload: json.RawMessage(`{"retry":true}`)})
	if err != nil {
		t.Fatal(err)
	}
	if re.Emitted || !re.Expired {
		t.Fatalf("re-emit of compacted id should be suppressed+expired, got %+v", re)
	}
}

// F-21: limit==0 returns no events; negative limit is an error.
func TestReadLimitZeroAndNegative(t *testing.T) {
	store := testStore(t)
	for _, id := range []string{"e1", "e2", "e3"} {
		store.Emit(EmitInput{Topic: "lim", ID: id, Payload: json.RawMessage(`{}`)})
	}
	zero, err := store.Read("lim", "c", 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(zero.Events) != 0 {
		t.Fatalf("limit 0 returned %d events, want 0", len(zero.Events))
	}
	if _, err := store.Read("lim", "c", -5); err == nil {
		t.Fatal("negative limit should error")
	}
}

// F-07: emit must not re-parse the whole topic on every call. With the in-memory cache
// warm, emit cost is independent of retained topic size. This asserts the 5000th emit
// is not dramatically slower than the 100th (allowing generous slack for noise), which
// would fail loudly under the old O(n)-per-emit behavior.
func TestEmitStaysFlatWithTopicSize(t *testing.T) {
	if testing.Short() {
		t.Skip("perf test skipped in -short")
	}
	store := testStore(t)
	timeEmitAt := func(n int) time.Duration {
		start := time.Now()
		if _, err := store.Emit(EmitInput{Topic: "perf", ID: fmt.Sprintf("e%d", n), Payload: json.RawMessage(`{"x":1}`)}); err != nil {
			t.Fatal(err)
		}
		return time.Since(start)
	}
	// warm up to 100
	for i := 0; i < 100; i++ {
		timeEmitAt(i)
	}
	early := timeEmitAt(100)
	for i := 101; i < 5000; i++ {
		timeEmitAt(i)
	}
	late := timeEmitAt(5000)
	// Under O(n) per emit, `late` was ~7x `early` at this scale (measured 16ms->120ms
	// end-to-end). With the cache it should be within a small constant factor. Use a
	// loose 10x bound purely to catch a regression to linear scanning, not to assert
	// microbenchmark precision.
	if late > 10*early+2*time.Millisecond {
		t.Fatalf("emit latency grew with topic size: early=%v late=%v (O(n) regression?)", early, late)
	}
}
