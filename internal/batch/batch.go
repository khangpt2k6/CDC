// Package batch buffers mapped rows and flushes them to a Sink in per-table
// batches, either when a size threshold is reached or when the caller flushes
// on a timer.
package batch

import "context"

// Sink writes a batch of rows for one table.
type Sink interface {
	WriteBatch(ctx context.Context, table string, rows [][]any) error
}

// Item is one mapped row tagged with its source table and Kafka offset.
type Item struct {
	Table  string
	Row    []any
	Offset int64
}

// Batcher accumulates Items and flushes them grouped by table. It is not safe
// for concurrent use; drive it from a single consumer goroutine.
type Batcher struct {
	sink    Sink
	maxRows int
	buf     []Item
	highest int64
}

// New returns a Batcher that flushes once maxRows items are buffered.
func New(sink Sink, maxRows int) *Batcher {
	return &Batcher{sink: sink, maxRows: maxRows}
}

// Len reports the number of buffered items.
func (b *Batcher) Len() int { return len(b.buf) }

// Add buffers it and reports whether the buffer is now full. It does NOT flush:
// when the buffer reaches maxRows it returns ready=true so the caller can drive
// the flush (the caller wraps it in retry/backoff and commits offsets after it
// succeeds). Keeping the flush out of Add gives the loop a single flush
// chokepoint and leaves the buffer intact for replay if that flush fails.
func (b *Batcher) Add(_ context.Context, it Item) (ready bool) {
	b.buf = append(b.buf, it)
	if it.Offset > b.highest {
		b.highest = it.Offset
	}
	return len(b.buf) >= b.maxRows
}

// Flush writes all buffered rows grouped by table and, on success, clears the
// buffer and returns the highest offset covered. On a sink error the buffer is
// left intact so the caller does not advance its committed offset; the events
// replay and ReplacingMergeTree collapses any duplicates by version.
func (b *Batcher) Flush(ctx context.Context) (offset int64, flushed bool, err error) {
	if len(b.buf) == 0 {
		return 0, false, nil
	}

	grouped := make(map[string][][]any)
	order := make([]string, 0)
	for _, it := range b.buf {
		if _, seen := grouped[it.Table]; !seen {
			order = append(order, it.Table)
		}
		grouped[it.Table] = append(grouped[it.Table], it.Row)
	}

	for _, table := range order {
		if werr := b.sink.WriteBatch(ctx, table, grouped[table]); werr != nil {
			return 0, false, werr
		}
	}

	high := b.highest
	b.buf = b.buf[:0]
	b.highest = 0
	return high, true, nil
}
