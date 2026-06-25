// Package debezium parses Debezium Postgres change events (JSON with the
// converter's schema disabled) into the internal model.ChangeEvent.
package debezium

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/khangpt2k6/CDC/internal/model"
)

// ErrTombstone is returned for a null message value. Debezium emits a null
// value as a log-compaction tombstone after a delete; the consumer skips it.
var ErrTombstone = errors.New("debezium: tombstone (null value)")

// ErrUnsupportedOp is returned for change events whose op is not one of the row
// operations the sink handles (c, u, d, r), such as truncate ("t") or logical
// message ("m") events.
var ErrUnsupportedOp = errors.New("debezium: unsupported op")

// envelope is the wire shape of a change-event value with schemas disabled.
type envelope struct {
	Op     *string        `json:"op"`
	Before map[string]any `json:"before"`
	After  map[string]any `json:"after"`
	Source source         `json:"source"`
}

type source struct {
	Schema string `json:"schema"`
	Table  string `json:"table"`
	LSN    uint64 `json:"lsn"`
	TsMs   int64  `json:"ts_ms"`
}

// Parse decodes one Debezium change-event value into a model.ChangeEvent. It
// returns ErrTombstone for a null value and ErrUnsupportedOp for ops the sink
// does not handle.
func Parse(value []byte) (model.ChangeEvent, error) {
	trimmed := bytes.TrimSpace(value)
	if len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")) {
		return model.ChangeEvent{}, ErrTombstone
	}

	var env envelope
	if err := json.Unmarshal(trimmed, &env); err != nil {
		return model.ChangeEvent{}, fmt.Errorf("debezium: decode value: %w", err)
	}
	if env.Op == nil {
		return model.ChangeEvent{}, errors.New("debezium: missing op")
	}

	op := model.Op(*env.Op)
	switch op {
	case model.OpCreate, model.OpUpdate, model.OpDelete, model.OpSnapshot:
		// handled below
	default:
		return model.ChangeEvent{}, fmt.Errorf("%w: %q", ErrUnsupportedOp, *env.Op)
	}

	return model.ChangeEvent{
		Op:     op,
		Schema: env.Source.Schema,
		Table:  env.Source.Table,
		Before: env.Before,
		After:  env.After,
		LSN:    env.Source.LSN,
		TsMs:   env.Source.TsMs,
	}, nil
}
