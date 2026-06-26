// Package retry runs an operation with capped exponential backoff until it
// succeeds or the context is cancelled. The worker uses it to ride out a slow or
// paused ClickHouse: the single consume goroutine blocks here while flushes fail,
// which doubles as backpressure (Kafka is not polled, so no offset advances and
// the buffer cannot grow) and resumes automatically the moment a flush succeeds.
package retry

import (
	"context"
	"time"
)

// Config controls the backoff schedule. The nth retry waits Base*2^(n-1),
// clamped to Max. No jitter: a single consumer is not a thundering-herd source,
// and a deterministic schedule keeps the behavior easy to reason about and test.
type Config struct {
	Base time.Duration // delay before the first retry
	Max  time.Duration // upper bound on any single delay
}

// Do calls fn repeatedly until it returns nil or ctx is done. Between attempts it
// sleeps with exponential backoff (see Config), calling onRetry(attempt, delay,
// err) just before each wait so the caller can log or measure the stall; onRetry
// may be nil. The first call to fn is attempt 0 and is not preceded by onRetry. A
// ctx cancelled during a backoff sleep returns ctx.Err() promptly so shutdown is
// never blocked by a long delay.
func Do(ctx context.Context, cfg Config, onRetry func(attempt int, delay time.Duration, err error), fn func() error) error {
	for attempt := 0; ; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}

		err := fn()
		if err == nil {
			return nil
		}

		delay := backoff(cfg, attempt)
		if onRetry != nil {
			onRetry(attempt+1, delay, err)
		}

		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}

// backoff returns the delay before the retry following the given zero-based
// attempt, equal to Base*2^attempt clamped to Max. Doubling stops once it reaches
// Max so very large attempt counts cannot overflow the shift.
func backoff(cfg Config, attempt int) time.Duration {
	d := cfg.Base
	for i := 0; i < attempt; i++ {
		if d >= cfg.Max {
			return cfg.Max
		}
		d *= 2
	}
	if d > cfg.Max {
		return cfg.Max
	}
	return d
}
