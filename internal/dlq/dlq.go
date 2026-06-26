// Package dlq publishes change events the worker could not parse or map to a
// dead-letter Kafka topic, so a single poison message is routed aside instead of
// wedging the consumer (Issue 2.4). The raw message bytes are preserved verbatim
// and the failure cause plus source coordinates ride along as Kafka headers, so
// the original record can be inspected or replayed later without guessing why it
// was rejected.
package dlq

import (
	"context"
	"fmt"

	"github.com/twmb/franz-go/pkg/kgo"
)

// Producer is a thin franz-go producer that mirrors one source record onto its
// dead-letter topic. It runs as an idempotent producer so a retried send cannot
// duplicate a poison message, and it does not share the consumer's client so a
// DLQ publish never perturbs consume/commit state.
type Producer struct {
	cl     *kgo.Client
	suffix string
}

// New dials brokers for producing. suffix is appended to a source record's topic
// to derive its dead-letter topic (e.g. ".dlq" -> "cdc.public.orders.dlq"), so
// each source keeps its own quarantine and ordering is preserved per source.
func New(brokers []string, suffix string) (*Producer, error) {
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ProducerLinger(0),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.ProducerBatchMaxBytes(16<<20),
	)
	if err != nil {
		return nil, fmt.Errorf("dlq: new client: %w", err)
	}
	return &Producer{cl: cl, suffix: suffix}, nil
}

// Send publishes rec's raw bytes to its dead-letter topic and blocks until the
// broker acknowledges (or ctx is done). cause is the parse/map error that
// condemned the record. It returns an error if the publish fails, so the caller
// can decline to advance its committed offset and let the record replay rather
// than silently drop a poison message.
func (p *Producer) Send(ctx context.Context, rec *kgo.Record, cause error) error {
	dl := &kgo.Record{
		Topic:   rec.Topic + p.suffix,
		Key:     rec.Key,
		Value:   rec.Value,
		Headers: append(headers(rec, cause), rec.Headers...),
	}
	if err := p.cl.ProduceSync(ctx, dl).FirstErr(); err != nil {
		return fmt.Errorf("dlq: produce to %s: %w", dl.Topic, err)
	}
	return nil
}

// Close flushes any buffered records and closes the client.
func (p *Producer) Close() { p.cl.Close() }

// headers builds the dead-letter diagnostic headers describing why and from
// where the record was dead-lettered. The "dlq-" prefix avoids colliding with
// any headers the source record already carries (which are appended after these).
func headers(rec *kgo.Record, cause error) []kgo.RecordHeader {
	return []kgo.RecordHeader{
		{Key: "dlq-error", Value: []byte(cause.Error())},
		{Key: "dlq-source-topic", Value: []byte(rec.Topic)},
		{Key: "dlq-source-partition", Value: []byte(fmt.Sprintf("%d", rec.Partition))},
		{Key: "dlq-source-offset", Value: []byte(fmt.Sprintf("%d", rec.Offset))},
	}
}
