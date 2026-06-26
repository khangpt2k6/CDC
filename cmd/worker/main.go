// Command worker runs the CDC sink: it consumes Debezium change events from
// Kafka, maps them to rows, and lands them in ClickHouse in batches, committing
// Kafka offsets only after each batch is durably written.
package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/khangpt2k6/CDC/internal/batch"
	"github.com/khangpt2k6/CDC/internal/config"
	"github.com/khangpt2k6/CDC/internal/consumer"
	"github.com/khangpt2k6/CDC/internal/debezium"
	"github.com/khangpt2k6/CDC/internal/dlq"
	"github.com/khangpt2k6/CDC/internal/lag"
	"github.com/khangpt2k6/CDC/internal/metrics"
	"github.com/khangpt2k6/CDC/internal/retry"
	"github.com/khangpt2k6/CDC/internal/sink/clickhouse"
)

// version is overridden at build time via -ldflags "-X main.version=...".
var version = "dev"

func main() {
	cfg, err := config.Load(os.Getenv)
	if err != nil {
		slog.Error("invalid configuration", "err", err)
		os.Exit(1)
	}

	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: parseLevel(cfg.LogLevel),
	})))

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	sink, err := clickhouse.Open(ctx, cfg.ClickHouseDSN, cfg.ClickHouseDialTimeout, cfg.ClickHouseReadTimeout)
	if err != nil {
		slog.Error("connect ClickHouse", "err", err)
		os.Exit(1)
	}
	defer func() { _ = sink.Close() }()
	if err := sink.ApplyDDL(ctx); err != nil {
		slog.Error("apply ClickHouse schema", "err", err)
		os.Exit(1)
	}

	cons, err := consumer.New(cfg.KafkaBrokers, cfg.KafkaGroup, cfg.KafkaTopics)
	if err != nil {
		slog.Error("connect Kafka", "err", err)
		os.Exit(1)
	}
	defer cons.Close()

	deadletter, err := dlq.New(cfg.KafkaBrokers, cfg.DLQTopicSuffix)
	if err != nil {
		slog.Error("connect DLQ producer", "err", err)
		os.Exit(1)
	}
	defer deadletter.Close()

	// Serve Prometheus metrics in the background. A bind failure is logged but
	// non-fatal: it must not stop the data pipeline.
	go serveMetrics(cfg.MetricsAddr)

	// Sample consumer-group lag on a timer into the cdc_consumer_lag gauge, so
	// /metrics shows how far behind the worker is and that it returns to zero
	// once caught up. Stops with ctx on shutdown.
	go lag.Run(ctx, cons, cfg.LagInterval)

	slog.Info("cdc worker running",
		"version", version,
		"kafka_brokers", cfg.KafkaBrokers,
		"kafka_group", cfg.KafkaGroup,
		"kafka_topics", cfg.KafkaTopics,
		"batch_size", cfg.BatchSize,
		"flush_interval", cfg.FlushInterval.String(),
		"metrics_addr", cfg.MetricsAddr,
		"dlq_topic_suffix", cfg.DLQTopicSuffix,
	)

	if err := run(ctx, cfg, cons, deadletter, batch.New(sink, cfg.BatchSize)); err != nil && !errors.Is(err, context.Canceled) {
		slog.Error("worker stopped with error", "err", err)
		os.Exit(1)
	}
	slog.Info("cdc worker stopped cleanly")
}

// poller is the consume-side of Kafka the loop depends on: fetch records and
// commit their offsets. *consumer.Consumer satisfies it; tests supply a fake.
type poller interface {
	Poll(context.Context) ([]*kgo.Record, error)
	Commit(context.Context, []*kgo.Record) error
}

// deadLetterer routes a poison record aside. *dlq.Producer satisfies it.
type deadLetterer interface {
	Send(context.Context, *kgo.Record, error) error
}

// adder is the batching sink the loop drives. *batch.Batcher satisfies it. Add
// only buffers and reports whether the buffer is full (ready to flush); the loop
// owns every flush so it can wrap it in retry/backoff.
type adder interface {
	Add(context.Context, batch.Item) (ready bool)
	Flush(context.Context) (int64, bool, error)
	Len() int
}

// run is the consume loop. It polls with a deadline equal to the flush interval
// (so an idle poll still flushes), maps each record, and commits Kafka offsets
// only after the batcher has written the buffered rows to ClickHouse. Its
// dependencies are interfaces so the loop can be driven by in-memory fakes in
// tests; main wires the concrete consumer/dlq/batcher.
func run(ctx context.Context, cfg config.Config, cons poller, deadletter deadLetterer, batcher adder) error {
	var pending []*kgo.Record

	commit := func(cctx context.Context) error {
		if len(pending) == 0 {
			return nil
		}
		if err := cons.Commit(cctx, pending); err != nil {
			return err
		}
		pending = pending[:0]
		return nil
	}

	// flushWithRetry is the single flush chokepoint: it retries the batcher flush
	// with capped backoff until ClickHouse accepts it (or fctx is cancelled). A
	// failed flush leaves the buffer intact, so a retry re-drives the same rows;
	// because the blocked loop stops polling Kafka while it waits, the buffer
	// cannot grow (backpressure) and the pipeline resumes the moment the sink
	// recovers (Issue 2.5).
	retryCfg := retry.Config{Base: cfg.RetryBase, Max: cfg.RetryMax}
	flushWithRetry := func(fctx context.Context) error {
		return retry.Do(fctx, retryCfg, func(attempt int, delay time.Duration, err error) {
			metrics.SinkRetries.Inc()
			slog.Warn("clickhouse flush failed; backing off",
				"attempt", attempt, "delay", delay.String(), "err", err)
		}, func() error {
			// Capture the buffer depth before the flush clears it so a successful
			// flush can attribute the right row count.
			n := batcher.Len()
			start := time.Now()
			_, flushed, ferr := batcher.Flush(fctx)
			metrics.FlushDuration.Observe(time.Since(start).Seconds())
			if ferr == nil && flushed {
				metrics.BatchesFlushed.Inc()
				metrics.RowsWritten.Add(float64(n))
			}
			return ferr
		})
	}

	for {
		if err := ctx.Err(); err != nil {
			// Best-effort final flush + commit on shutdown, off the cancelled ctx.
			shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			if ferr := flushWithRetry(shutCtx); ferr == nil && batcher.Len() == 0 {
				_ = commit(shutCtx)
			}
			cancel()
			return err
		}

		pollCtx, cancel := context.WithTimeout(ctx, cfg.FlushInterval)
		recs, err := cons.Poll(pollCtx)
		cancel()
		if errors.Is(err, consumer.ErrClosed) {
			return nil
		}
		if err != nil {
			return err
		}
		metrics.EventsConsumed.Add(float64(len(recs)))

		for _, r := range recs {
			pending = append(pending, r)
			it, err := mapRecord(r)
			if errors.Is(err, errSkip) {
				continue // tombstone / unsupported / untracked; committed with the batch
			}
			if err != nil {
				// A poison message: route it aside and keep going. The DLQ send
				// must succeed before this record's offset is allowed to advance,
				// so a send failure is fatal to the iteration (no commit -> the
				// record replays), exactly like a sink-write failure. This keeps
				// the produce->write->commit ordering contract: nothing is
				// dropped, and a malformed event never wedges the consumer.
				if derr := deadletter.Send(ctx, r, err); derr != nil {
					return derr
				}
				metrics.DLQTotal.Inc()
				slog.Warn("routed bad event to DLQ",
					"topic", r.Topic, "partition", r.Partition, "offset", r.Offset, "err", err)
				continue
			}
			if ready := batcher.Add(ctx, it); ready {
				// Buffer is full: flush it (retrying past a stalled sink) and
				// commit only after it lands.
				if ferr := flushWithRetry(ctx); ferr != nil {
					return ferr
				}
				if err := commit(ctx); err != nil {
					return err
				}
			}
		}

		// Time-based flush; the buffer being empty afterwards means every
		// pending record is now either written or intentionally skipped.
		if ferr := flushWithRetry(ctx); ferr != nil {
			return ferr
		}
		if batcher.Len() == 0 {
			if err := commit(ctx); err != nil {
				return err
			}
		}
		metrics.BufferedRows.Set(float64(batcher.Len()))
	}
}

// errSkip marks a record that is intentionally not written and is NOT an error:
// a delete tombstone, an unsupported op, or a change for an untracked table.
// Callers branch on it with errors.Is to distinguish a correct skip (advance the
// offset, no DLQ) from a genuine poison message (route to the DLQ).
var errSkip = errors.New("record intentionally skipped")

// mapRecord parses and maps one Kafka record into a batch.Item. The returned
// error means:
//   - nil: the Item is valid and should be batched.
//   - errSkip (via errors.Is): the record is correctly ignored (tombstone /
//     unsupported op / untracked table); advance the offset, do not dead-letter.
//   - any other error: the record is a poison message (unparseable or
//     unmappable) and the caller routes it to the DLQ.
func mapRecord(r *kgo.Record) (batch.Item, error) {
	ev, err := debezium.Parse(r.Value)
	if err != nil {
		switch {
		case errors.Is(err, debezium.ErrTombstone):
			return batch.Item{}, errSkip // expected after a delete
		case errors.Is(err, debezium.ErrUnsupportedOp):
			slog.Debug("skip unsupported op", "topic", r.Topic, "offset", r.Offset)
			return batch.Item{}, errSkip
		default:
			return batch.Item{}, err // malformed: dead-letter it
		}
	}

	spec, ok := clickhouse.Specs[ev.Table]
	if !ok {
		slog.Debug("skip record for untracked table", "table", ev.Table)
		return batch.Item{}, errSkip
	}
	row, err := clickhouse.MapRow(spec, ev)
	if err != nil {
		return batch.Item{}, err // unmappable: dead-letter it
	}

	return batch.Item{Table: ev.Table, Row: row, Offset: r.Offset}, nil
}

// serveMetrics exposes Prometheus metrics on addr at /metrics. It blocks, so run
// it in a goroutine. A serve error (e.g. the port is in use) is logged but not
// fatal: metrics are observability, and losing them must not stop the pipeline.
func serveMetrics(addr string) {
	mux := http.NewServeMux()
	mux.Handle("/metrics", metrics.Handler())
	srv := &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		slog.Warn("metrics server stopped", "addr", addr, "err", err)
	}
}

// parseLevel maps a log level string to a slog.Level, defaulting to info.
func parseLevel(s string) slog.Level {
	switch s {
	case "debug":
		return slog.LevelDebug
	case "warn":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}
