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

// Handler returns an http.Handler serving the registered metrics in Prometheus
// text format, for mounting at /metrics.
func Handler() http.Handler {
	return promhttp.Handler()
}
