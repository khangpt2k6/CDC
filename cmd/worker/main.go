// Command worker runs the CDC sink: it consumes Debezium change events from
// Kafka and lands them in ClickHouse.
//
// At this stage it loads configuration and logs startup; the Kafka consumer
// and ClickHouse sink are wired in by later issues.
package main

import (
	"log/slog"
	"os"

	"github.com/khangpt2k6/CDC/internal/config"
)

// version is overridden at build time via -ldflags "-X main.version=...".
var version = "dev"

func main() {
	cfg, err := config.Load(os.Getenv)
	if err != nil {
		slog.Error("invalid configuration", "err", err)
		os.Exit(1)
	}

	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: parseLevel(cfg.LogLevel),
	}))
	slog.SetDefault(logger)

	// The ClickHouse DSN carries a password, so it is intentionally not logged.
	slog.Info("cdc worker starting",
		"version", version,
		"kafka_brokers", cfg.KafkaBrokers,
		"kafka_group", cfg.KafkaGroup,
		"kafka_topics", cfg.KafkaTopics,
		"batch_size", cfg.BatchSize,
		"flush_interval", cfg.FlushInterval.String(),
		"metrics_addr", cfg.MetricsAddr,
		"log_level", cfg.LogLevel,
	)

	// Consumer and sink are wired in by later issues.
	slog.Info("cdc worker has no pipeline wired yet; exiting")
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
