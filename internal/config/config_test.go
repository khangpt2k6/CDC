package config_test

import (
	"reflect"
	"testing"
	"time"

	"github.com/khangpt2k6/CDC/internal/config"
)

// noEnv simulates a process with no CDC_* variables set.
func noEnv(string) string { return "" }

func TestLoadDefaults(t *testing.T) {
	cfg, err := config.Load(noEnv)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}

	want := config.Config{
		KafkaBrokers:   []string{"localhost:29092"},
		KafkaGroup:     "cdc-clickhouse-sink",
		KafkaTopics:    []string{"cdc.public.customers", "cdc.public.orders"},
		ClickHouseDSN:  "clickhouse://default:@localhost:9000/cdc",
		BatchSize:      1000,
		FlushInterval:  time.Second,
		MetricsAddr:    ":9100",
		DLQTopicSuffix: ".dlq",
		LogLevel:       "info",
	}
	if !reflect.DeepEqual(cfg, want) {
		t.Errorf("Load() defaults =\n  %+v\nwant\n  %+v", cfg, want)
	}
}

func TestLoadOverrides(t *testing.T) {
	env := map[string]string{
		"CDC_KAFKA_BROKERS":    "broker1:9092,broker2:9092",
		"CDC_KAFKA_GROUP":      "g1",
		"CDC_KAFKA_TOPICS":     "t1,t2",
		"CDC_CLICKHOUSE_DSN":   "clickhouse://h:9000/db",
		"CDC_BATCH_SIZE":       "500",
		"CDC_FLUSH_INTERVAL":   "250ms",
		"CDC_METRICS_ADDR":     ":1234",
		"CDC_DLQ_TOPIC_SUFFIX": ".deadletter",
		"CDC_LOG_LEVEL":        "debug",
	}
	cfg, err := config.Load(func(k string) string { return env[k] })
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}

	if !reflect.DeepEqual(cfg.KafkaBrokers, []string{"broker1:9092", "broker2:9092"}) {
		t.Errorf("KafkaBrokers = %v", cfg.KafkaBrokers)
	}
	if !reflect.DeepEqual(cfg.KafkaTopics, []string{"t1", "t2"}) {
		t.Errorf("KafkaTopics = %v", cfg.KafkaTopics)
	}
	if cfg.KafkaGroup != "g1" {
		t.Errorf("KafkaGroup = %q, want g1", cfg.KafkaGroup)
	}
	if cfg.BatchSize != 500 {
		t.Errorf("BatchSize = %d, want 500", cfg.BatchSize)
	}
	if cfg.FlushInterval != 250*time.Millisecond {
		t.Errorf("FlushInterval = %v, want 250ms", cfg.FlushInterval)
	}
	if cfg.LogLevel != "debug" {
		t.Errorf("LogLevel = %q, want debug", cfg.LogLevel)
	}
	if cfg.DLQTopicSuffix != ".deadletter" {
		t.Errorf("DLQTopicSuffix = %q, want .deadletter", cfg.DLQTopicSuffix)
	}
}

func TestLoadRejectsInvalidBatchSize(t *testing.T) {
	_, err := config.Load(func(k string) string {
		if k == "CDC_BATCH_SIZE" {
			return "not-a-number"
		}
		return ""
	})
	if err == nil {
		t.Fatal("Load() error = nil, want error for non-numeric CDC_BATCH_SIZE")
	}
}

func TestLoadRejectsNonPositiveBatchSize(t *testing.T) {
	_, err := config.Load(func(k string) string {
		if k == "CDC_BATCH_SIZE" {
			return "0"
		}
		return ""
	})
	if err == nil {
		t.Fatal("Load() error = nil, want error for non-positive CDC_BATCH_SIZE")
	}
}

func TestLoadRejectsInvalidFlushInterval(t *testing.T) {
	_, err := config.Load(func(k string) string {
		if k == "CDC_FLUSH_INTERVAL" {
			return "soon"
		}
		return ""
	})
	if err == nil {
		t.Fatal("Load() error = nil, want error for invalid CDC_FLUSH_INTERVAL")
	}
}
