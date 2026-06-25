package clickhouse_test

import (
	"testing"
	"time"

	"github.com/shopspring/decimal"

	"github.com/khangpt2k6/CDC/internal/model"
	ch "github.com/khangpt2k6/CDC/internal/sink/clickhouse"
)

func TestMapRowInsertOrders(t *testing.T) {
	ev := model.ChangeEvent{
		Op: model.OpCreate, Schema: "public", Table: "orders", LSN: 555,
		After: map[string]any{
			"id": float64(1), "customer_id": float64(7), "status": "paid",
			"total_amount": "42.50", "currency": "USD",
			"placed_at": "2026-06-25T09:00:00Z", "updated_at": "2026-06-25T09:00:00Z",
		},
	}

	row, err := ch.MapRow(ch.Specs["orders"], ev)
	if err != nil {
		t.Fatalf("MapRow() error = %v", err)
	}
	// Column order: id, customer_id, status, total_amount, currency,
	// placed_at, updated_at, _version, _is_deleted.
	if len(row) != 9 {
		t.Fatalf("len(row) = %d, want 9", len(row))
	}
	if row[0] != int64(1) {
		t.Errorf("id = %v (%T), want int64(1)", row[0], row[0])
	}
	if row[1] != int64(7) {
		t.Errorf("customer_id = %v, want int64(7)", row[1])
	}
	if row[2] != "paid" {
		t.Errorf("status = %v, want paid", row[2])
	}
	if d, ok := row[3].(decimal.Decimal); !ok || !d.Equal(decimal.RequireFromString("42.50")) {
		t.Errorf("total_amount = %v (%T), want decimal 42.50", row[3], row[3])
	}
	if row[4] != "USD" {
		t.Errorf("currency = %v, want USD", row[4])
	}
	wantTime := time.Date(2026, 6, 25, 9, 0, 0, 0, time.UTC)
	if tt, ok := row[5].(time.Time); !ok || !tt.Equal(wantTime) {
		t.Errorf("placed_at = %v (%T), want %v", row[5], row[5], wantTime)
	}
	if row[7] != uint64(555) {
		t.Errorf("_version = %v, want uint64(555)", row[7])
	}
	if row[8] != uint8(0) {
		t.Errorf("_is_deleted = %v, want uint8(0)", row[8])
	}
}

func TestMapRowDeleteUsesBeforeAndTombstone(t *testing.T) {
	ev := model.ChangeEvent{
		Op: model.OpDelete, Table: "orders", LSN: 600,
		Before: map[string]any{
			"id": float64(2), "customer_id": float64(3), "status": "pending",
			"total_amount": "9.99", "currency": "EUR",
			"placed_at": "2026-06-25T10:00:00Z", "updated_at": "2026-06-25T10:00:00Z",
		},
	}

	row, err := ch.MapRow(ch.Specs["orders"], ev)
	if err != nil {
		t.Fatalf("MapRow() error = %v", err)
	}
	if row[0] != int64(2) {
		t.Errorf("id = %v, want int64(2) from Before", row[0])
	}
	if row[7] != uint64(600) {
		t.Errorf("_version = %v, want uint64(600)", row[7])
	}
	if row[8] != uint8(1) {
		t.Errorf("_is_deleted = %v, want uint8(1)", row[8])
	}
}

func TestMapRowCustomersSnapshot(t *testing.T) {
	ev := model.ChangeEvent{
		Op: model.OpSnapshot, Table: "customers", LSN: 10,
		After: map[string]any{
			"id": float64(5), "email": "emma@example.com", "full_name": "Emma Schmidt",
			"country": "DE", "created_at": "2026-06-01T00:00:00Z", "updated_at": "2026-06-01T00:00:00Z",
		},
	}

	row, err := ch.MapRow(ch.Specs["customers"], ev)
	if err != nil {
		t.Fatalf("MapRow() error = %v", err)
	}
	if len(row) != 8 { // 6 business columns + _version + _is_deleted
		t.Fatalf("len(row) = %d, want 8", len(row))
	}
	if row[1] != "emma@example.com" {
		t.Errorf("email = %v, want emma@example.com", row[1])
	}
	if row[7] != uint8(0) {
		t.Errorf("_is_deleted = %v, want uint8(0)", row[7])
	}
}

func TestMapRowRejectsBadDecimal(t *testing.T) {
	ev := model.ChangeEvent{
		Op: model.OpCreate, Table: "orders",
		After: map[string]any{
			"id": float64(1), "customer_id": float64(1), "status": "x",
			"total_amount": "not-a-number", "currency": "USD",
			"placed_at": "2026-06-25T09:00:00Z", "updated_at": "2026-06-25T09:00:00Z",
		},
	}

	if _, err := ch.MapRow(ch.Specs["orders"], ev); err == nil {
		t.Fatal("MapRow() error = nil, want error for malformed decimal")
	}
}

func TestMapRowRejectsBadTimestamp(t *testing.T) {
	ev := model.ChangeEvent{
		Op: model.OpCreate, Table: "customers",
		After: map[string]any{
			"id": float64(1), "email": "a@b.c", "full_name": "A B", "country": "US",
			"created_at": "not-a-time", "updated_at": "2026-06-01T00:00:00Z",
		},
	}

	if _, err := ch.MapRow(ch.Specs["customers"], ev); err == nil {
		t.Fatal("MapRow() error = nil, want error for malformed timestamp")
	}
}
