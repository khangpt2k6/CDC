package sink

import (
	"context"

	cdcv1 "github.com/khangpt2k6/CDC/internal/gen/cdc/v1"
)

// Sink delivers batches of change-event envelopes to a target system.
type Sink interface {
	// Write delivers batch and returns the highest source position that is
	// durably acknowledged by the target.
	//
	// Ordering contract: the pipeline must follow produce -> ack -> commit. It
	// calls Write, takes the returned ackedPosition, and only then commits that
	// position to the OffsetStore - never the reverse. Returning an error means
	// nothing in the batch may be treated as acked, so no offset advance may
	// happen.
	Write(ctx context.Context, batch []*cdcv1.Envelope) (ackedPosition string, err error)
}
