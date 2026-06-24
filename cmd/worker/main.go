// Command worker is the entrypoint for a CDC pipeline worker.
//
// At this stage it is a buildable placeholder: it parses configuration from
// flags and environment variables and logs startup. Capture, snapshot, and
// sink logic are wired in by later phases.
package main

import (
	"flag"
	"log/slog"
	"os"
)

// version is overridden at build time via -ldflags "-X main.version=...".
var version = "dev"

func main() {
	var (
		logLevel = flag.String("log-level", env("CDC_LOG_LEVEL", "info"), "log level: debug|info|warn|error")
		tenant   = flag.String("tenant", env("CDC_TENANT", ""), "tenant id this worker serves")
	)
	flag.Parse()

	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: parseLevel(*logLevel),
	}))
	slog.SetDefault(logger)

	slog.Info("cdc worker starting",
		"version", version,
		"tenant", *tenant,
		"log_level", *logLevel,
	)

	// No pipeline yet - exit cleanly so the binary is runnable in CI.
	slog.Info("cdc worker has no work configured yet; exiting")
}

// env returns the value of the environment variable key, or def when unset.
func env(key, def string) string {
	if v, ok := os.LookupEnv(key); ok {
		return v
	}
	return def
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
