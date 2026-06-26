package main

import (
	"errors"
	"testing"

	"github.com/twmb/franz-go/pkg/kgo"
)

// TestMapRecordClassification pins the parse/map → skip-vs-poison decision that
// Issue 2.4 hinges on: intentionally-ignored records must report errSkip (advance
// the offset, no DLQ), while genuinely bad records must report a non-errSkip
// error (route to the DLQ). Valid records must map cleanly with a nil error.
func TestMapRecordClassification(t *testing.T) {
	tests := []struct {
		name     string
		topic    string
		value    []byte
		wantSkip bool // errors.Is(err, errSkip)
		wantErr  bool // err != nil (poison when true && !wantSkip)
		wantOK   bool // err == nil, a real Item
	}{
		{
			name:     "tombstone (null value) is a skip, not poison",
			value:    []byte("null"),
			wantSkip: true,
		},
		{
			name:     "unsupported op (truncate) is a skip, not poison",
			value:    []byte(`{"op":"t","source":{"schema":"public","table":"orders"}}`),
			wantSkip: true,
		},
		{
			name:     "untracked table is a skip, not poison",
			value:    []byte(`{"op":"c","after":{"x":1},"source":{"schema":"public","table":"not_tracked"}}`),
			wantSkip: true,
		},
		{
			name:    "malformed JSON is poison",
			value:   []byte(`{not json`),
			wantErr: true,
		},
		{
			name:    "missing op is poison",
			value:   []byte(`{"after":{"id":1},"source":{"table":"customers"}}`),
			wantErr: true,
		},
		{
			name:   "valid customer insert maps cleanly",
			value:  []byte(`{"op":"c","after":{"id":1,"email":"a@b.c","full_name":"A","country":"US","created_at":"2026-01-01T00:00:00Z","updated_at":"2026-01-01T00:00:00Z"},"source":{"schema":"public","table":"customers"}}`),
			wantOK: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := mapRecord(&kgo.Record{Topic: tc.topic, Value: tc.value})
			switch {
			case tc.wantSkip:
				if !errors.Is(err, errSkip) {
					t.Fatalf("got err %v, want errSkip", err)
				}
			case tc.wantOK:
				if err != nil {
					t.Fatalf("got err %v, want nil (valid record)", err)
				}
			case tc.wantErr:
				if err == nil || errors.Is(err, errSkip) {
					t.Fatalf("got err %v, want a non-skip poison error", err)
				}
			}
		})
	}
}
