// Package config loads worker configuration from the environment.
package config

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

// Config holds everything the worker needs to connect its Kafka source to its
// ClickHouse sink. Every field comes from a CDC_* environment variable, with
// the defaults applied by Load.
type Config struct {
	KafkaBrokers   []string      // CDC_KAFKA_BROKERS (comma-separated host:port)
	KafkaGroup     string        // CDC_KAFKA_GROUP (consumer group id)
	KafkaTopics    []string      // CDC_KAFKA_TOPICS (comma-separated topics)
	ClickHouseDSN  string        // CDC_CLICKHOUSE_DSN
	BatchSize      int           // CDC_BATCH_SIZE (rows per flush)
	FlushInterval  time.Duration // CDC_FLUSH_INTERVAL (max time between flushes)
	MetricsAddr    string        // CDC_METRICS_ADDR (host:port serving /metrics)
	DLQTopicSuffix string        // CDC_DLQ_TOPIC_SUFFIX (appended to a source topic to form its dead-letter topic)
	LogLevel       string        // CDC_LOG_LEVEL (debug|info|warn|error)
}

// Load builds a Config from getenv, applying a default for any variable that
// getenv returns empty. getenv has os.Getenv semantics (empty string when
// unset); injecting it keeps Load testable without touching the real
// environment. It returns an error when a numeric or duration value is
// malformed or out of range.
func Load(getenv func(string) string) (Config, error) {
	cfg := Config{
		KafkaBrokers:   splitList(get(getenv, "CDC_KAFKA_BROKERS", "localhost:29092")),
		KafkaGroup:     get(getenv, "CDC_KAFKA_GROUP", "cdc-clickhouse-sink"),
		KafkaTopics:    splitList(get(getenv, "CDC_KAFKA_TOPICS", "cdc.public.customers,cdc.public.orders")),
		ClickHouseDSN:  get(getenv, "CDC_CLICKHOUSE_DSN", "clickhouse://default:@localhost:9000/cdc"),
		MetricsAddr:    get(getenv, "CDC_METRICS_ADDR", ":9100"),
		DLQTopicSuffix: get(getenv, "CDC_DLQ_TOPIC_SUFFIX", ".dlq"),
		LogLevel:       get(getenv, "CDC_LOG_LEVEL", "info"),
	}

	batch, err := strconv.Atoi(get(getenv, "CDC_BATCH_SIZE", "1000"))
	if err != nil {
		return Config{}, fmt.Errorf("CDC_BATCH_SIZE: %w", err)
	}
	if batch <= 0 {
		return Config{}, fmt.Errorf("CDC_BATCH_SIZE must be positive, got %d", batch)
	}
	cfg.BatchSize = batch

	flush, err := time.ParseDuration(get(getenv, "CDC_FLUSH_INTERVAL", "1s"))
	if err != nil {
		return Config{}, fmt.Errorf("CDC_FLUSH_INTERVAL: %w", err)
	}
	if flush <= 0 {
		return Config{}, fmt.Errorf("CDC_FLUSH_INTERVAL must be positive, got %s", flush)
	}
	cfg.FlushInterval = flush

	return cfg, nil
}

// get returns getenv(key), or def when the variable is empty or unset.
func get(getenv func(string) string, key, def string) string {
	if v := getenv(key); v != "" {
		return v
	}
	return def
}

// splitList splits a comma-separated list, trimming spaces and dropping empties.
func splitList(s string) []string {
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}
