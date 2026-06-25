package debezium_test

import (
	"errors"
	"testing"

	"github.com/khangpt2k6/CDC/internal/debezium"
	"github.com/khangpt2k6/CDC/internal/model"
)

// insertValue is a Debezium Postgres change event for an INSERT on orders,
// in the schemas.enable=false shape the connector is configured to emit.
const insertValue = `{
  "before": null,
  "after": {
    "id": 1, "customer_id": 1, "status": "paid",
    "total_amount": "42.50", "currency": "USD",
    "placed_at": 1718000000000000, "updated_at": 1718000000000000
  },
  "source": {
    "version": "2.7.3.Final", "connector": "postgresql", "name": "cdc",
    "ts_ms": 1718000000000, "snapshot": "false", "db": "cdc",
    "schema": "public", "table": "orders", "txId": 789, "lsn": 123456789
  },
  "op": "c",
  "ts_ms": 1718000000050
}`

const updateValue = `{
  "before": {"id": 1, "status": "paid", "total_amount": "42.50"},
  "after":  {"id": 1, "status": "shipped", "total_amount": "42.50"},
  "source": {"schema": "public", "table": "orders", "ts_ms": 1718000001000, "lsn": 123456800},
  "op": "u",
  "ts_ms": 1718000001050
}`

const deleteValue = `{
  "before": {"id": 2, "status": "pending", "total_amount": "99.00"},
  "after": null,
  "source": {"schema": "public", "table": "orders", "ts_ms": 1718000002000, "lsn": 123456900},
  "op": "d",
  "ts_ms": 1718000002050
}`

const snapshotValue = `{
  "before": null,
  "after": {"id": 5, "email": "emma@example.com", "full_name": "Emma Schmidt"},
  "source": {"schema": "public", "table": "customers", "ts_ms": 1717999990000, "lsn": 123450000},
  "op": "r",
  "ts_ms": 1717999990050
}`

func TestParseInsert(t *testing.T) {
	ev, err := debezium.Parse([]byte(insertValue))
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}
	if ev.Op != model.OpCreate {
		t.Errorf("Op = %q, want c", ev.Op)
	}
	if ev.Schema != "public" || ev.Table != "orders" {
		t.Errorf("Schema/Table = %q/%q, want public/orders", ev.Schema, ev.Table)
	}
	if ev.Before != nil {
		t.Errorf("Before = %v, want nil for insert", ev.Before)
	}
	if ev.After["status"] != "paid" {
		t.Errorf("After[status] = %v, want paid", ev.After["status"])
	}
	if ev.LSN != 123456789 {
		t.Errorf("LSN = %d, want 123456789", ev.LSN)
	}
	if ev.TsMs != 1718000000000 {
		t.Errorf("TsMs = %d, want source.ts_ms 1718000000000", ev.TsMs)
	}
}

func TestParseUpdate(t *testing.T) {
	ev, err := debezium.Parse([]byte(updateValue))
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}
	if ev.Op != model.OpUpdate {
		t.Errorf("Op = %q, want u", ev.Op)
	}
	if ev.Before["status"] != "paid" {
		t.Errorf("Before[status] = %v, want paid", ev.Before["status"])
	}
	if ev.After["status"] != "shipped" {
		t.Errorf("After[status] = %v, want shipped", ev.After["status"])
	}
}

func TestParseDelete(t *testing.T) {
	ev, err := debezium.Parse([]byte(deleteValue))
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}
	if ev.Op != model.OpDelete {
		t.Errorf("Op = %q, want d", ev.Op)
	}
	if ev.After != nil {
		t.Errorf("After = %v, want nil for delete", ev.After)
	}
	if ev.Before["id"] != float64(2) {
		t.Errorf("Before[id] = %v, want 2", ev.Before["id"])
	}
}

func TestParseSnapshotRead(t *testing.T) {
	ev, err := debezium.Parse([]byte(snapshotValue))
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}
	if ev.Op != model.OpSnapshot {
		t.Errorf("Op = %q, want r", ev.Op)
	}
	if ev.Table != "customers" {
		t.Errorf("Table = %q, want customers", ev.Table)
	}
	if ev.After["email"] != "emma@example.com" {
		t.Errorf("After[email] = %v", ev.After["email"])
	}
}

func TestParseMalformedJSON(t *testing.T) {
	_, err := debezium.Parse([]byte(`{not json`))
	if err == nil {
		t.Fatal("Parse() error = nil, want error for malformed JSON")
	}
}

func TestParseEmptyValueIsTombstone(t *testing.T) {
	_, err := debezium.Parse([]byte("null"))
	if !errors.Is(err, debezium.ErrTombstone) {
		t.Fatalf("Parse(null) error = %v, want ErrTombstone", err)
	}
}

func TestParseUnsupportedOp(t *testing.T) {
	truncate := `{"op":"t","source":{"schema":"public","table":"orders","ts_ms":1,"lsn":1}}`
	_, err := debezium.Parse([]byte(truncate))
	if !errors.Is(err, debezium.ErrUnsupportedOp) {
		t.Fatalf("Parse(op=t) error = %v, want ErrUnsupportedOp", err)
	}
}
