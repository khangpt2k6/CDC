package source

import (
	"context"

	cdcv1 "github.com/khangpt2k6/CDC/internal/gen/cdc/v1"
)

// Source captures changes from one upstream database and streams them as
// change-event envelopes.
//
// Lifecycle: Start opens the stream; the returned envelope channel and error
// channel are both closed when the source stops (either via Stop or because
// the upstream ended). A non-nil send on the error channel is terminal for the
// stream.
type Source interface {
	// Start begins streaming change events from fromPosition (exclusive).
	//
	// An empty fromPosition means "start from the source's natural beginning /
	// configured start point" (e.g. the slot's confirmed point, or a fresh
	// snapshot). It returns the envelope stream, an error stream, and a
	// start-up error for failures that occur before streaming begins.
	Start(ctx context.Context, fromPosition string) (<-chan *cdcv1.Envelope, <-chan error, error)

	// Stop halts streaming and releases resources. It is safe to call after the
	// stream has already ended.
	Stop(ctx context.Context) error
}
