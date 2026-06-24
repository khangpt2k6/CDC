// Package source holds CDC capture sources (PostgreSQL, MySQL, MongoDB).
//
// Each source reads a database's replication stream and emits change-event
// envelopes. The common Source interface is defined in Issue 0.4.
package source
