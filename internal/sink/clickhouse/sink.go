package clickhouse

import (
	"context"
	_ "embed"
	"fmt"
	"strings"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

//go:embed schema.sql
var schemaSQL string

// Sink writes mapped rows to ClickHouse. It satisfies batch.Sink.
type Sink struct {
	conn driver.Conn
}

// Open connects to ClickHouse using a clickhouse:// DSN and verifies the
// connection with a ping.
func Open(ctx context.Context, dsn string) (*Sink, error) {
	opts, err := clickhouse.ParseDSN(dsn)
	if err != nil {
		return nil, fmt.Errorf("clickhouse: parse dsn: %w", err)
	}
	conn, err := clickhouse.Open(opts)
	if err != nil {
		return nil, fmt.Errorf("clickhouse: open: %w", err)
	}
	if err := conn.Ping(ctx); err != nil {
		return nil, fmt.Errorf("clickhouse: ping: %w", err)
	}
	return &Sink{conn: conn}, nil
}

// ApplyDDL runs schema.sql (CREATE TABLE IF NOT EXISTS) so the target tables
// exist before the first write.
func (s *Sink) ApplyDDL(ctx context.Context) error {
	for _, stmt := range statements(schemaSQL) {
		if err := s.conn.Exec(ctx, stmt); err != nil {
			return fmt.Errorf("clickhouse: apply ddl: %w", err)
		}
	}
	return nil
}

// WriteBatch inserts rows into cdc.<table> in a single prepared batch. rows
// must be ordered to match the table's columns (see MapRow).
func (s *Sink) WriteBatch(ctx context.Context, table string, rows [][]any) error {
	b, err := s.conn.PrepareBatch(ctx, "INSERT INTO cdc."+table)
	if err != nil {
		return fmt.Errorf("clickhouse: prepare batch for %s: %w", table, err)
	}
	for _, row := range rows {
		if err := b.Append(row...); err != nil {
			return fmt.Errorf("clickhouse: append to %s: %w", table, err)
		}
	}
	if err := b.Send(); err != nil {
		return fmt.Errorf("clickhouse: send batch for %s: %w", table, err)
	}
	return nil
}

// Close closes the underlying connection.
func (s *Sink) Close() error { return s.conn.Close() }

// statements splits a multi-statement SQL string on ";" and strips full-line
// comments, so each CREATE TABLE can be issued on its own (Exec runs one
// statement at a time).
func statements(sql string) []string {
	var out []string
	for _, raw := range strings.Split(sql, ";") {
		var b strings.Builder
		for _, line := range strings.Split(raw, "\n") {
			if strings.HasPrefix(strings.TrimSpace(line), "--") {
				continue
			}
			b.WriteString(line)
			b.WriteString("\n")
		}
		if s := strings.TrimSpace(b.String()); s != "" {
			out = append(out, s)
		}
	}
	return out
}
