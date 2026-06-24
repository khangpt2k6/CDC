// Package offset holds durable offset/position persistence for capture streams.
//
// The OffsetStore interface (Issue 0.4) records the last durably-acked source
// position per (tenant, source) stream so workers resume without loss.
package offset
