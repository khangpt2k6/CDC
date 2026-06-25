// Package model holds the internal, source-agnostic representation of a single
// row change that flows from the capture side to the sink side.
package model

// Op is the kind of change a ChangeEvent carries.
type Op string

// The change operations a ChangeEvent can carry.
const (
	OpCreate   Op = "c" // insert
	OpUpdate   Op = "u" // update
	OpDelete   Op = "d" // delete
	OpSnapshot Op = "r" // snapshot read (existing row at connector start)
)

// ChangeEvent is one decoded row change. Before is nil for inserts and
// snapshot reads; After is nil for deletes. Column values are kept as raw
// decoded JSON (map[string]any) so the sink can apply its own type mapping.
type ChangeEvent struct {
	Op     Op
	Schema string         // source schema, e.g. "public"
	Table  string         // source table, e.g. "orders"
	Before map[string]any // row state before the change (nil for c/r)
	After  map[string]any // row state after the change (nil for d)
	LSN    uint64         // log sequence number; monotonic, used as the sink version
	TsMs   int64          // source commit time, epoch milliseconds
}
