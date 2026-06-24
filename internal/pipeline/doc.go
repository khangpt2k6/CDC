// Package pipeline wires a Source through a Sink and an OffsetStore following
// the produce -> ack -> commit ordering contract.
//
// The concrete streaming pipeline is built in Phase 1. For now this package
// hosts the contract test (Issue 0.4) that exercises the three core
// interfaces end to end with in-memory fakes.
package pipeline
