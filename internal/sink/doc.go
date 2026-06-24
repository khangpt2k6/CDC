// Package sink holds CDC delivery sinks (Postgres, HTTP/webhook, S3/Iceberg).
//
// Each sink consumes change-event envelopes and delivers them to a target
// system. The common Sink interface is defined in Issue 0.4.
package sink
