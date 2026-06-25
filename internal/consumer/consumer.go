// Package consumer is a thin franz-go wrapper that polls Debezium change-event
// records from Kafka and commits offsets explicitly, so the caller can commit
// only after a batch has been durably written downstream.
package consumer

import (
	"context"
	"errors"
	"fmt"

	"github.com/twmb/franz-go/pkg/kgo"
)

// ErrClosed is returned by Poll after the underlying client has been closed.
var ErrClosed = errors.New("consumer: client closed")

// Consumer wraps a kgo.Client configured as a manual-commit consumer group.
type Consumer struct {
	cl *kgo.Client
}

// New dials brokers and joins group consuming topics, with auto-commit disabled.
func New(brokers []string, group string, topics []string) (*Consumer, error) {
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(topics...),
		kgo.DisableAutoCommit(),
	)
	if err != nil {
		return nil, fmt.Errorf("consumer: new client: %w", err)
	}
	return &Consumer{cl: cl}, nil
}

// Poll returns the next batch of records, blocking until some arrive or ctx is
// done. A ctx deadline or cancellation yields the records fetched so far
// (possibly none) with a nil error, so the caller can use a deadline to drive a
// periodic flush. Non-context fetch errors are returned. It returns ErrClosed
// once the client is closed.
func (c *Consumer) Poll(ctx context.Context) ([]*kgo.Record, error) {
	fetches := c.cl.PollFetches(ctx)
	if fetches.IsClientClosed() {
		return nil, ErrClosed
	}

	var fetchErr error
	fetches.EachError(func(t string, p int32, err error) {
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
			return // expected when a poll deadline drives the flush timer
		}
		if fetchErr == nil {
			fetchErr = fmt.Errorf("consumer: fetch %s[%d]: %w", t, p, err)
		}
	})
	if fetchErr != nil {
		return nil, fetchErr
	}

	var recs []*kgo.Record
	fetches.EachRecord(func(r *kgo.Record) { recs = append(recs, r) })
	return recs, nil
}

// Commit commits offsets for recs (the highest offset per partition, plus one),
// persisting the group's position. It is a no-op for an empty slice.
func (c *Consumer) Commit(ctx context.Context, recs []*kgo.Record) error {
	if len(recs) == 0 {
		return nil
	}
	if err := c.cl.CommitRecords(ctx, recs...); err != nil {
		return fmt.Errorf("consumer: commit: %w", err)
	}
	return nil
}

// Close flushes and closes the client.
func (c *Consumer) Close() { c.cl.Close() }
