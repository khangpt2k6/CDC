package batch_test

import (
	"context"
	"errors"
	"testing"

	"github.com/khangpt2k6/CDC/internal/batch"
)

// fakeSink records WriteBatch calls and can be told to fail.
type fakeSink struct {
	calls []call
	err   error
}

type call struct {
	table string
	rows  [][]any
}

func (f *fakeSink) WriteBatch(_ context.Context, table string, rows [][]any) error {
	if f.err != nil {
		return f.err
	}
	// Copy rows so later buffer reuse cannot mutate the record.
	cp := make([][]any, len(rows))
	copy(cp, rows)
	f.calls = append(f.calls, call{table: table, rows: cp})
	return nil
}

func item(table string, id int, offset int64) batch.Item {
	return batch.Item{Table: table, Row: []any{int64(id)}, Offset: offset}
}

func TestAddBelowThresholdDoesNotFlush(t *testing.T) {
	sink := &fakeSink{}
	b := batch.New(sink, 3)

	_, flushed, err := b.Add(context.Background(), item("orders", 1, 10))
	if err != nil || flushed {
		t.Fatalf("Add() = flushed %v, err %v; want false, nil", flushed, err)
	}
	if _, flushed, _ := b.Add(context.Background(), item("orders", 2, 11)); flushed {
		t.Fatal("Add() flushed before reaching threshold")
	}
	if len(sink.calls) != 0 {
		t.Errorf("sink called %d times, want 0", len(sink.calls))
	}
	if b.Len() != 2 {
		t.Errorf("Len() = %d, want 2", b.Len())
	}
}

func TestAddReachingThresholdFlushes(t *testing.T) {
	sink := &fakeSink{}
	b := batch.New(sink, 2)

	_, _, _ = b.Add(context.Background(), item("orders", 1, 10))
	off, flushed, err := b.Add(context.Background(), item("orders", 2, 11))
	if err != nil {
		t.Fatalf("Add() error = %v", err)
	}
	if !flushed {
		t.Fatal("Add() did not flush at threshold")
	}
	if off != 11 {
		t.Errorf("flushed offset = %d, want 11", off)
	}
	if len(sink.calls) != 1 || len(sink.calls[0].rows) != 2 {
		t.Errorf("sink calls = %+v, want one call of 2 rows", sink.calls)
	}
	if b.Len() != 0 {
		t.Errorf("Len() = %d after flush, want 0", b.Len())
	}
}

func TestFlushGroupsByTable(t *testing.T) {
	sink := &fakeSink{}
	b := batch.New(sink, 100)

	_, _, _ = b.Add(context.Background(), item("orders", 1, 10))
	_, _, _ = b.Add(context.Background(), item("customers", 1, 11))
	_, _, _ = b.Add(context.Background(), item("orders", 2, 12))

	off, flushed, err := b.Flush(context.Background())
	if err != nil || !flushed {
		t.Fatalf("Flush() = flushed %v, err %v; want true, nil", flushed, err)
	}
	if off != 12 {
		t.Errorf("flushed offset = %d, want 12", off)
	}
	if len(sink.calls) != 2 {
		t.Fatalf("sink calls = %d, want 2 (one per table)", len(sink.calls))
	}
	byTable := map[string]int{}
	for _, c := range sink.calls {
		byTable[c.table] = len(c.rows)
	}
	if byTable["orders"] != 2 || byTable["customers"] != 1 {
		t.Errorf("rows per table = %v, want orders:2 customers:1", byTable)
	}
}

func TestFlushEmptyIsNoop(t *testing.T) {
	sink := &fakeSink{}
	b := batch.New(sink, 10)

	_, flushed, err := b.Flush(context.Background())
	if err != nil || flushed {
		t.Fatalf("Flush() empty = flushed %v, err %v; want false, nil", flushed, err)
	}
	if len(sink.calls) != 0 {
		t.Errorf("sink called %d times on empty flush", len(sink.calls))
	}
}

func TestFlushErrorKeepsBuffer(t *testing.T) {
	sink := &fakeSink{err: errors.New("clickhouse down")}
	b := batch.New(sink, 10)
	_, _, _ = b.Add(context.Background(), item("orders", 1, 10))

	_, _, err := b.Flush(context.Background())
	if err == nil {
		t.Fatal("Flush() error = nil, want the sink error")
	}
	if b.Len() != 1 {
		t.Errorf("Len() = %d after failed flush, want 1 (buffer intact)", b.Len())
	}
}
