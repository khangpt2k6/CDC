package main

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/khangpt2k6/CDC/internal/batch"
	"github.com/khangpt2k6/CDC/internal/config"
	"github.com/khangpt2k6/CDC/internal/consumer"
	"github.com/khangpt2k6/CDC/internal/metrics"
)

// TestMapRecordClassification pins the parse/map → skip-vs-poison decision that
// Issue 2.4 hinges on: intentionally-ignored records must report errSkip (advance
// the offset, no DLQ), while genuinely bad records must report a non-errSkip
// error (route to the DLQ). Valid records must map cleanly with a nil error.
func TestMapRecordClassification(t *testing.T) {
	tests := []struct {
		name     string
		topic    string
		value    []byte
		wantSkip bool // errors.Is(err, errSkip)
		wantErr  bool // err != nil (poison when true && !wantSkip)
		wantOK   bool // err == nil, a real Item
	}{
		{
			name:     "tombstone (null value) is a skip, not poison",
			value:    []byte("null"),
			wantSkip: true,
		},
		{
			name:     "unsupported op (truncate) is a skip, not poison",
			value:    []byte(`{"op":"t","source":{"schema":"public","table":"orders"}}`),
			wantSkip: true,
		},
		{
			name:     "untracked table is a skip, not poison",
			value:    []byte(`{"op":"c","after":{"x":1},"source":{"schema":"public","table":"not_tracked"}}`),
			wantSkip: true,
		},
		{
			name:    "malformed JSON is poison",
			value:   []byte(`{not json`),
			wantErr: true,
		},
		{
			name:    "missing op is poison",
			value:   []byte(`{"after":{"id":1},"source":{"table":"customers"}}`),
			wantErr: true,
		},
		{
			name:   "valid customer insert maps cleanly",
			value:  []byte(`{"op":"c","after":{"id":1,"email":"a@b.c","full_name":"A","country":"US","created_at":"2026-01-01T00:00:00Z","updated_at":"2026-01-01T00:00:00Z"},"source":{"schema":"public","table":"customers"}}`),
			wantOK: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := mapRecord(&kgo.Record{Topic: tc.topic, Value: tc.value})
			switch {
			case tc.wantSkip:
				if !errors.Is(err, errSkip) {
					t.Fatalf("got err %v, want errSkip", err)
				}
			case tc.wantOK:
				if err != nil {
					t.Fatalf("got err %v, want nil (valid record)", err)
				}
			case tc.wantErr:
				if err == nil || errors.Is(err, errSkip) {
					t.Fatalf("got err %v, want a non-skip poison error", err)
				}
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Loop-level fakes: in-memory stand-ins for the consumer / DLQ / batcher so the
// real run() consume loop can be driven without Kafka or ClickHouse. They
// implement the poller / deadLetterer / adder interfaces run() depends on.
// ---------------------------------------------------------------------------

// fakePoller yields one scripted batch of records per Poll call, then returns
// consumer.ErrClosed so run() exits cleanly. It records the records handed to
// Commit so a test can assert how far the offset advanced.
type fakePoller struct {
	batches   [][]*kgo.Record
	next      int
	committed []*kgo.Record
}

func (f *fakePoller) Poll(context.Context) ([]*kgo.Record, error) {
	if f.next >= len(f.batches) {
		return nil, consumer.ErrClosed
	}
	b := f.batches[f.next]
	f.next++
	return b, nil
}

func (f *fakePoller) Commit(_ context.Context, recs []*kgo.Record) error {
	f.committed = append(f.committed, recs...)
	return nil
}

// fakeDLQ records every record routed to it and, when err is set, fails the
// send so the "DLQ failure blocks the offset" path can be exercised.
type fakeDLQ struct {
	sent  []*kgo.Record
	cause []error
	err   error
}

func (f *fakeDLQ) Send(_ context.Context, rec *kgo.Record, cause error) error {
	if f.err != nil {
		return f.err
	}
	f.sent = append(f.sent, rec)
	f.cause = append(f.cause, cause)
	return nil
}

// fakeBatcher collects added items so a test can confirm that good records
// (including ones after a poison record) still reach the sink. It never reaches
// the size threshold, so Add never auto-flushes.
type fakeBatcher struct {
	added []batch.Item
}

func (f *fakeBatcher) Add(_ context.Context, it batch.Item) (int64, bool, error) {
	f.added = append(f.added, it)
	return 0, false, nil
}

func (f *fakeBatcher) Flush(context.Context) (int64, bool, error) { return 0, false, nil }
func (f *fakeBatcher) Len() int                                   { return 0 }

// record builds a Kafka record on a tracked topic with the given value bytes.
func record(value string) *kgo.Record {
	return &kgo.Record{Topic: "cdc.public.customers", Value: []byte(value)}
}

// fmtRecord fills the goodCustomer template's id/email verbs with n.
func fmtRecord(tmpl string, n int) string {
	return fmt.Sprintf(tmpl, n, n)
}

const (
	goodCustomer = `{"op":"c","after":{"id":%d,"email":"a%d@b.c","full_name":"A","country":"US","created_at":"2026-01-01T00:00:00Z","updated_at":"2026-01-01T00:00:00Z"},"source":{"schema":"public","table":"customers"}}`
	malformed    = `{not json`
)

// runLoop drives the real run() over a single scripted poll batch with a
// background (uncancelled) context, returning when the fake poller reports
// ErrClosed. testCfg keeps the flush interval short.
func runLoop(t *testing.T, recs []*kgo.Record, dl deadLetterer, b adder) (*fakePoller, error) {
	t.Helper()
	fp := &fakePoller{batches: [][]*kgo.Record{recs}}
	cfg := config.Config{BatchSize: 1000, FlushInterval: 10 * time.Millisecond}
	err := run(context.Background(), cfg, fp, dl, b)
	return fp, err
}

// ---------------------------------------------------------------------------
// AC#1 (behavior only): "A malformed event is routed to a DLQ and the consumer
// keeps running." These tests assert the loop's routing/survival behavior; the
// dlq_total metric is covered separately by the AC#2 tests.
// ---------------------------------------------------------------------------

// TestRunPoisonRoutedAndLoopContinues feeds a malformed record between two good
// ones and asserts: the malformed record is routed to the DLQ, the loop keeps
// running (no error return), and both good records still reach the batcher.
func TestRunPoisonRoutedAndLoopContinues(t *testing.T) {
	dl := &fakeDLQ{}
	b := &fakeBatcher{}

	recs := []*kgo.Record{
		record(fmtRecord(goodCustomer, 1)),
		record(malformed),
		record(fmtRecord(goodCustomer, 2)),
	}
	if _, err := runLoop(t, recs, dl, b); err != nil {
		t.Fatalf("run returned %v, want nil (clean ErrClosed exit)", err)
	}

	if len(dl.sent) != 1 {
		t.Fatalf("DLQ received %d records, want 1", len(dl.sent))
	}
	if string(dl.sent[0].Value) != malformed {
		t.Errorf("DLQ got value %q, want the malformed record", dl.sent[0].Value)
	}
	if dl.cause[0] == nil {
		t.Error("DLQ cause is nil, want the parse error")
	}
	if len(b.added) != 2 {
		t.Errorf("batcher got %d items, want 2 (both good records survived the poison one)", len(b.added))
	}
}

// TestRunSkipsAreNotDeadLettered guards the errSkip-vs-poison boundary at the
// loop level: tombstone / unsupported op / untracked table are correct skips,
// not poison, so they must not be routed to the DLQ.
func TestRunSkipsAreNotDeadLettered(t *testing.T) {
	dl := &fakeDLQ{}
	b := &fakeBatcher{}

	recs := []*kgo.Record{
		record("null"), // tombstone
		record(`{"op":"t","source":{"schema":"public","table":"orders"}}`),               // unsupported op
		record(`{"op":"c","after":{"x":1},"source":{"schema":"public","table":"nope"}}`), // untracked table
	}
	if _, err := runLoop(t, recs, dl, b); err != nil {
		t.Fatalf("run returned %v, want nil", err)
	}

	if len(dl.sent) != 0 {
		t.Errorf("DLQ received %d records, want 0 (intentional skips are not poison)", len(dl.sent))
	}
}

// TestRunDLQFailureBlocksOffset proves the no-drop contract: if the DLQ send
// fails, run() returns the error and does NOT commit past the poison record, so
// it replays on the next start rather than being silently lost.
func TestRunDLQFailureBlocksOffset(t *testing.T) {
	sendErr := errors.New("dlq unavailable")
	dl := &fakeDLQ{err: sendErr}
	b := &fakeBatcher{}

	fp, err := runLoop(t, []*kgo.Record{record(malformed)}, dl, b)
	if !errors.Is(err, sendErr) {
		t.Fatalf("run returned %v, want the DLQ send error", err)
	}
	if len(fp.committed) != 0 {
		t.Errorf("committed %d records despite DLQ failure, want 0 (offset must not advance)", len(fp.committed))
	}
}

// ---------------------------------------------------------------------------
// AC#2 (metrics only): "A dlq_total metric increments on each bad event." These
// tests assert only the counter, reading it via a before/after delta so they do
// not depend on other tests' contributions to the process-global counter.
// ---------------------------------------------------------------------------

// TestDLQTotalIncrementsPerBadEvent proves the metric moves by exactly one per
// poison event: three malformed records yield a delta of three.
func TestDLQTotalIncrementsPerBadEvent(t *testing.T) {
	before := testutil.ToFloat64(metrics.DLQTotal)

	recs := []*kgo.Record{record(malformed), record(malformed), record(malformed)}
	if _, err := runLoop(t, recs, &fakeDLQ{}, &fakeBatcher{}); err != nil {
		t.Fatalf("run returned %v, want nil", err)
	}

	if got := testutil.ToFloat64(metrics.DLQTotal) - before; got != 3 {
		t.Errorf("dlq_total incremented by %v, want 3 (one per bad event)", got)
	}
}

// TestDLQTotalUnchangedOnSkips proves intentional skips (tombstone / unsupported
// op / untracked table) and valid records do not touch the counter -- only true
// poison events do.
func TestDLQTotalUnchangedOnSkips(t *testing.T) {
	before := testutil.ToFloat64(metrics.DLQTotal)

	recs := []*kgo.Record{
		record("null"), // tombstone
		record(`{"op":"t","source":{"schema":"public","table":"orders"}}`),               // unsupported op
		record(`{"op":"c","after":{"x":1},"source":{"schema":"public","table":"nope"}}`), // untracked table
		record(fmtRecord(goodCustomer, 1)),                                               // valid record
	}
	if _, err := runLoop(t, recs, &fakeDLQ{}, &fakeBatcher{}); err != nil {
		t.Fatalf("run returned %v, want nil", err)
	}

	if got := testutil.ToFloat64(metrics.DLQTotal) - before; got != 0 {
		t.Errorf("dlq_total moved by %v on non-poison events, want 0", got)
	}
}
