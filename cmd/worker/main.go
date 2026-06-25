// Command worker runs the CDC sink: it consumes Debezium change events from
// Kafka, maps them to rows, and lands them in ClickHouse in batches, committing
// Kafka offsets only after each batch is durably written.
package main

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/khangpt2k6/CDC/internal/batch"
	"github.com/khangpt2k6/CDC/internal/config"
	"github.com/khangpt2k6/CDC/internal/consumer"
	"github.com/khangpt2k6/CDC/internal/debezium"
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

	sink, err := clickhouse.Open(ctx, cfg.ClickHouseDSN)
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

	slog.Info("cdc worker running",
		"version", version,
		"kafka_brokers", cfg.KafkaBrokers,
		"kafka_group", cfg.KafkaGroup,
		"kafka_topics", cfg.KafkaTopics,
		"batch_size", cfg.BatchSize,
		"flush_interval", cfg.FlushInterval.String(),
	)

	if err := run(ctx, cfg, cons, batch.New(sink, cfg.BatchSize)); err != nil && !errors.Is(err, context.Canceled) {
		slog.Error("worker stopped with error", "err", err)
		os.Exit(1)
	}
	slog.Info("cdc worker stopped cleanly")
}

// run is the consume loop. It polls with a deadline equal to the flush interval
// (so an idle poll still flushes), maps each record, and commits Kafka offsets
// only after the batcher has written the buffered rows to ClickHouse.
func run(ctx context.Context, cfg config.Config, cons *consumer.Consumer, batcher *batch.Batcher) error {
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

	for {
		if err := ctx.Err(); err != nil {
			// Best-effort final flush + commit on shutdown, off the cancelled ctx.
			shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			if _, _, ferr := batcher.Flush(shutCtx); ferr == nil && batcher.Len() == 0 {
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

		for _, r := range recs {
			pending = append(pending, r)
			it, ok := mapRecord(r)
			if !ok {
				continue // tombstone / unsupported / unmappable; committed with the batch
			}
			_, flushed, ferr := batcher.Add(ctx, it)
			if ferr != nil {
				return ferr
			}
			if flushed {
				if err := commit(ctx); err != nil {
					return err
				}
			}
		}

		// Time-based flush; the buffer being empty afterwards means every
		// pending record is now either written or intentionally skipped.
		if _, _, ferr := batcher.Flush(ctx); ferr != nil {
			return ferr
		}
		if batcher.Len() == 0 {
			if err := commit(ctx); err != nil {
				return err
			}
		}
	}
}

// mapRecord parses and maps one Kafka record into a batch.Item. It returns
// ok=false (logging the reason) for records that are intentionally skipped:
// delete tombstones, unsupported ops, untracked tables, or unmappable rows.
func mapRecord(r *kgo.Record) (batch.Item, bool) {
	ev, err := debezium.Parse(r.Value)
	if err != nil {
		switch {
		case errors.Is(err, debezium.ErrTombstone):
			// expected after a delete; silently skip
		case errors.Is(err, debezium.ErrUnsupportedOp):
			slog.Debug("skip unsupported op", "topic", r.Topic, "offset", r.Offset)
		default:
			slog.Warn("skip unparseable record", "topic", r.Topic, "offset", r.Offset, "err", err)
		}
		return batch.Item{}, false
	}

	spec, ok := clickhouse.Specs[ev.Table]
	if !ok {
		slog.Debug("skip record for untracked table", "table", ev.Table)
		return batch.Item{}, false
	}
	row, err := clickhouse.MapRow(spec, ev)
	if err != nil {
		slog.Warn("skip unmappable record", "table", ev.Table, "offset", r.Offset, "err", err)
		return batch.Item{}, false
	}

	return batch.Item{Table: ev.Table, Row: row, Offset: r.Offset}, true
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
