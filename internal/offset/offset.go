package offset

import (
	"context"
	"errors"
)

// ErrNoOffset is returned by OffsetStore.Load when no position has been
// committed yet for the stream key (the first run for that stream). Callers
// branch on this - via errors.Is - to start the source from its natural
// beginning rather than treating it as a failure.
var ErrNoOffset = errors.New("offset: no committed offset for stream key")

// OffsetStore persists the last durably-acked source position per stream.
//
// A key identifies one (tenant, source) stream. The ordering contract is
// produce -> ack -> commit: a position is only ever committed here after the
// sink has durably acknowledged the corresponding data (see sink.Sink.Write).
type OffsetStore interface { //nolint:revive // OffsetStore is the canonical contract name (Source/Sink/OffsetStore).
	// Load returns the last committed position for key, or ErrNoOffset if none
	// has been committed yet.
	Load(ctx context.Context, key string) (position string, err error)

	// Commit durably records position as the last acked position for key. It
	// must be atomic: a partial/torn commit is not allowed.
	Commit(ctx context.Context, key, position string) error
}
