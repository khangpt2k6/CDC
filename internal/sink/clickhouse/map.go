// Package clickhouse maps change events into ClickHouse rows and writes them.
package clickhouse

import (
	"fmt"
	"time"

	"github.com/shopspring/decimal"

	"github.com/khangpt2k6/CDC/internal/model"
)

// Kind is the ClickHouse-facing type of a source column, used to convert the
// raw decoded JSON value into the Go type the driver expects.
type Kind int

// Column kinds for the supported ClickHouse target types.
const (
	KindInt64 Kind = iota
	KindString
	KindDecimal
	KindDateTime
)

// Column is one business column of a target table.
type Column struct {
	Name string
	Kind Kind
}

// TableSpec is a target table's business columns in DDL order. MapRow appends
// the _version and _is_deleted pipeline columns after these.
type TableSpec struct {
	Name    string
	Columns []Column
}

// Specs maps a source table name to its ClickHouse column layout. It mirrors
// the DDL in schema.sql.
var Specs = map[string]TableSpec{
	"customers": {Name: "customers", Columns: []Column{
		{"id", KindInt64}, {"email", KindString}, {"full_name", KindString},
		{"country", KindString}, {"created_at", KindDateTime}, {"updated_at", KindDateTime},
	}},
	"orders": {Name: "orders", Columns: []Column{
		{"id", KindInt64}, {"customer_id", KindInt64}, {"status", KindString},
		{"total_amount", KindDecimal}, {"currency", KindString},
		{"placed_at", KindDateTime}, {"updated_at", KindDateTime},
	}},
}

// MapRow converts a ChangeEvent into the ordered values for a ClickHouse insert:
// the spec's business columns, then _version (the source LSN) and _is_deleted
// (1 for a delete, else 0). Deletes are mapped from Before (After is empty);
// every other op is mapped from After.
func MapRow(spec TableSpec, ev model.ChangeEvent) ([]any, error) {
	src := ev.After
	if ev.Op == model.OpDelete {
		src = ev.Before
	}

	row := make([]any, 0, len(spec.Columns)+2)
	for _, col := range spec.Columns {
		v, err := convert(col.Kind, src[col.Name])
		if err != nil {
			return nil, fmt.Errorf("column %q: %w", col.Name, err)
		}
		row = append(row, v)
	}

	var deleted uint8
	if ev.Op == model.OpDelete {
		deleted = 1
	}
	row = append(row, ev.LSN, deleted)
	return row, nil
}

// convert turns a raw decoded JSON value into the Go type the ClickHouse driver
// expects for the column kind. A nil input maps to the kind's zero value.
func convert(kind Kind, v any) (any, error) {
	switch kind {
	case KindInt64:
		if v == nil {
			return int64(0), nil
		}
		f, ok := v.(float64)
		if !ok {
			return nil, fmt.Errorf("expected number, got %T", v)
		}
		return int64(f), nil
	case KindString:
		if v == nil {
			return "", nil
		}
		s, ok := v.(string)
		if !ok {
			return nil, fmt.Errorf("expected string, got %T", v)
		}
		return s, nil
	case KindDecimal:
		if v == nil {
			return decimal.Zero, nil
		}
		s, ok := v.(string)
		if !ok {
			return nil, fmt.Errorf("expected decimal string, got %T", v)
		}
		d, err := decimal.NewFromString(s)
		if err != nil {
			return nil, fmt.Errorf("decimal %q: %w", s, err)
		}
		return d, nil
	case KindDateTime:
		if v == nil {
			return time.Time{}, nil
		}
		s, ok := v.(string)
		if !ok {
			return nil, fmt.Errorf("expected timestamp string, got %T", v)
		}
		t, err := time.Parse(time.RFC3339Nano, s)
		if err != nil {
			return nil, fmt.Errorf("timestamp %q: %w", s, err)
		}
		return t.UTC(), nil
	default:
		return nil, fmt.Errorf("unknown column kind %d", kind)
	}
}
