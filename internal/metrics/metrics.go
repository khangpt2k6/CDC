// Package metrics holds the worker's Prometheus instruments and the HTTP handler
// that exposes them. It is intentionally small: only the counters the current
// phase needs are registered here, and more are added as later phases (3.1)
// flesh out throughput, lag, and latency metrics.
package metrics

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// DLQTotal counts change events the worker could not parse or map and therefore
// routed to the dead-letter topic. It increments once per bad event (Issue 2.4);
// a steadily rising value means upstream is producing messages the sink can't
// handle.
var DLQTotal = promauto.NewCounter(prometheus.CounterOpts{
	Name: "cdc_dlq_total",
	Help: "Total change events routed to the dead-letter topic (unparseable or unmappable).",
})

// SinkRetries counts ClickHouse flush attempts that failed and were retried with
// backoff (Issue 2.5). Each increment is one backed-off retry; a rising value
// means the sink is slow or stalled and the consumer is applying backpressure
// (not polling Kafka) until it recovers.
var SinkRetries = promauto.NewCounter(prometheus.CounterOpts{
	Name: "cdc_sink_retries_total",
	Help: "Total ClickHouse flush attempts retried after a failure (sink slow or stalled).",
})

// The instruments below expose pipeline health (Issue 3.1): throughput, write
// volume, batch behavior, and consumer lag. Errors are already covered by
// DLQTotal and SinkRetries above.

// EventsConsumed counts Debezium records polled from Kafka, including ones later
// skipped or dead-lettered. rate() over it is the consume throughput.
var EventsConsumed = promauto.NewCounter(prometheus.CounterOpts{
	Name: "cdc_events_consumed_total",
	Help: "Total Kafka records polled from the source topics.",
})

// RowsWritten counts rows landed in ClickHouse across all successful flushes.
var RowsWritten = promauto.NewCounter(prometheus.CounterOpts{
	Name: "cdc_rows_written_total",
	Help: "Total rows written to ClickHouse by successful batch flushes.",
})

// BatchesFlushed counts successful (non-empty) flushes to ClickHouse.
var BatchesFlushed = promauto.NewCounter(prometheus.CounterOpts{
	Name: "cdc_batches_flushed_total",
	Help: "Total successful batch flushes to ClickHouse.",
})

// FlushDuration observes how long each flush attempt takes (including the final
// successful attempt after any retries), so dashboards can show batch latency
// quantiles.
var FlushDuration = promauto.NewHistogram(prometheus.HistogramOpts{
	Name:    "cdc_flush_duration_seconds",
	Help:    "Duration of a ClickHouse batch flush in seconds.",
	Buckets: prometheus.DefBuckets,
})

// BufferedRows reports the batcher's current buffer depth, so a rising value
// signals backpressure (the sink is keeping up slower than the source).
var BufferedRows = promauto.NewGauge(prometheus.GaugeOpts{
	Name: "cdc_buffered_rows",
	Help: "Rows currently buffered in the batcher awaiting a flush.",
})

// ConsumerLag reports the consumer group's lag per topic/partition: how many
// records have been produced but not yet committed. It goes to zero when the
// worker is caught up (Issue 3.1). Set periodically by the lag reporter.
var ConsumerLag = promauto.NewGaugeVec(prometheus.GaugeOpts{
	Name: "cdc_consumer_lag",
	Help: "Consumer group lag (uncommitted records) per topic and partition.",
}, []string{"topic", "partition"})

// Handler returns an http.Handler serving the registered metrics in Prometheus
// text format, for mounting at /metrics.
func Handler() http.Handler {
	return promhttp.Handler()
}
