// Package lag periodically samples consumer-group lag and publishes it to the
// cdc_consumer_lag gauge, so /metrics shows how far behind the worker is and that
// it returns to zero once caught up (Issue 3.1). It is decoupled from Kafka via
// the Source interface, so the consumer supplies real lag and tests a fake one.
package lag

import (
	"context"
	"log/slog"
	"strconv"
	"time"

	"github.com/khangpt2k6/CDC/internal/metrics"
)

// Source reports current consumer-group lag per topic and partition. The
// consumer.Consumer satisfies it.
type Source interface {
	Lag(ctx context.Context) (map[string]map[int32]int64, error)
}

// Report samples src once and sets the cdc_consumer_lag gauge for each
// topic/partition. A sample error is logged and skipped (lag is observability;
// a transient metadata error must not be fatal), leaving the last value in place.
func Report(ctx context.Context, src Source) {
	byTopic, err := src.Lag(ctx)
	if err != nil {
		slog.Warn("sample consumer lag", "err", err)
		return
	}
	for topic, parts := range byTopic {
		for partition, lag := range parts {
			metrics.ConsumerLag.
				WithLabelValues(topic, strconv.Itoa(int(partition))).
				Set(float64(lag))
		}
	}
}

// Run samples src every interval until ctx is cancelled, calling Report each
// tick. It blocks, so run it in a goroutine. It takes one sample immediately so
// the gauge is populated without waiting a full interval.
func Run(ctx context.Context, src Source, interval time.Duration) {
	Report(ctx, src)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			Report(ctx, src)
		}
	}
}
