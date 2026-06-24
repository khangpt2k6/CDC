package pipeline_test

import (
	"context"
	"errors"
	"testing"

	cdcv1 "github.com/khangpt2k6/CDC/internal/gen/cdc/v1"
	"github.com/khangpt2k6/CDC/internal/offset"
	"github.com/khangpt2k6/CDC/internal/sink"
	"github.com/khangpt2k6/CDC/internal/source"
)

// Compile-time assertions that the fakes satisfy the core interfaces.
var (
	_ source.Source      = (*fakeSource)(nil)
	_ sink.Sink          = (*fakeSink)(nil)
	_ offset.OffsetStore = (*fakeOffsetStore)(nil)
)

// fakeSource emits its envelopes in order, skipping any at or before
// fromPosition so a resumed Start replays only newer events.
type fakeSource struct {
	envs []*cdcv1.Envelope
}

func (f *fakeSource) Start(ctx context.Context, fromPosition string) (<-chan *cdcv1.Envelope, <-chan error, error) {
	out := make(chan *cdcv1.Envelope)
	errc := make(chan error, 1)
	go func() {
		defer close(out)
		defer close(errc)
		for _, e := range f.envs {
			if fromPosition != "" && e.GetPosition() <= fromPosition {
				continue
			}
			select {
			case out <- e:
			case <-ctx.Done():
				errc <- ctx.Err()
				return
			}
		}
	}()
	return out, errc, nil
}

func (f *fakeSource) Stop(context.Context) error { return nil }

// fakeSink records what it received and returns the highest position in the
// batch as the acked position.
type fakeSink struct {
	ops *[]string
	got []*cdcv1.Envelope
}

func (s *fakeSink) Write(_ context.Context, batch []*cdcv1.Envelope) (string, error) {
	*s.ops = append(*s.ops, "write")
	s.got = append(s.got, batch...)
	acked := ""
	for _, e := range batch {
		if e.GetPosition() > acked {
			acked = e.GetPosition()
		}
	}
	return acked, nil
}

// fakeOffsetStore is an in-memory OffsetStore.
type fakeOffsetStore struct {
	ops *[]string
	m   map[string]string
}

func (o *fakeOffsetStore) Load(_ context.Context, key string) (string, error) {
	if v, ok := o.m[key]; ok {
		return v, nil
	}
	return "", offset.ErrNoOffset
}

func (o *fakeOffsetStore) Commit(_ context.Context, key, position string) error {
	*o.ops = append(*o.ops, "commit")
	o.m[key] = position
	return nil
}

func env(pos string) *cdcv1.Envelope {
	return &cdcv1.Envelope{
		Op:       cdcv1.Op_OP_INSERT,
		Position: pos,
		TenantId: "t1",
	}
}

// runBatch performs one load -> stream -> write -> commit cycle, consuming at
// most limit envelopes, and returns what the sink received this cycle.
func runBatch(t *testing.T, src source.Source, snk *fakeSink, store offset.OffsetStore, key string, limit int) []*cdcv1.Envelope {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	from, err := store.Load(ctx, key)
	if err != nil && !errors.Is(err, offset.ErrNoOffset) {
		t.Fatalf("Load: unexpected error: %v", err)
	}

	stream, errc, err := src.Start(ctx, from)
	if err != nil {
		t.Fatalf("Start: %v", err)
	}

	var batch []*cdcv1.Envelope
	for len(batch) < limit {
		select {
		case e, ok := <-stream:
			if !ok {
				stream = nil
			} else {
				batch = append(batch, e)
				continue
			}
		case err := <-errc:
			if err != nil {
				t.Fatalf("stream error: %v", err)
			}
		}
		if stream == nil {
			break
		}
	}
	cancel()
	if err := src.Stop(context.Background()); err != nil {
		t.Fatalf("Stop: %v", err)
	}

	if len(batch) == 0 {
		return nil
	}

	acked, err := snk.Write(ctx, batch)
	if err != nil {
		t.Fatalf("Write: %v", err)
	}
	want := batch[len(batch)-1].GetPosition()
	if acked != want {
		t.Fatalf("acked position = %q, want highest in batch %q", acked, want)
	}
	if err := store.Commit(context.Background(), key, acked); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	return batch
}

// TestPipelineLoadStreamWriteCommit exercises the three core interfaces end to
// end: first run starts from ErrNoOffset (natural beginning), a later run
// resumes from the committed position and replays only newer events. It also
// asserts the produce -> ack -> commit ordering.
func TestPipelineLoadStreamWriteCommit(t *testing.T) {
	const key = "tenant1/pg-main"
	ops := []string{}
	snk := &fakeSink{ops: &ops}
	store := &fakeOffsetStore{ops: &ops, m: map[string]string{}}

	// First run: no committed offset yet, so Load returns ErrNoOffset and the
	// source streams from the beginning. Consume the first 3 of 5 events.
	src1 := &fakeSource{envs: []*cdcv1.Envelope{
		env("00000001"), env("00000002"), env("00000003"),
		env("00000004"), env("00000005"),
	}}
	got := runBatch(t, src1, snk, store, key, 3)
	if len(got) != 3 {
		t.Fatalf("first run consumed %d events, want 3", len(got))
	}
	if pos := store.m[key]; pos != "00000003" {
		t.Fatalf("committed offset = %q, want 00000003", pos)
	}

	// Ordering: write must come before commit.
	if len(ops) != 2 || ops[0] != "write" || ops[1] != "commit" {
		t.Fatalf("op order = %v, want [write commit]", ops)
	}

	// Second run: Load now returns 00000003, so the source replays only the
	// newer events 4 and 5.
	src2 := &fakeSource{envs: []*cdcv1.Envelope{
		env("00000001"), env("00000002"), env("00000003"),
		env("00000004"), env("00000005"),
	}}
	got = runBatch(t, src2, snk, store, key, 10)
	if len(got) != 2 {
		t.Fatalf("resumed run consumed %d events, want 2 (only newer)", len(got))
	}
	if got[0].GetPosition() != "00000004" || got[1].GetPosition() != "00000005" {
		t.Fatalf("resumed events = %q,%q, want 00000004,00000005", got[0].GetPosition(), got[1].GetPosition())
	}
	if pos := store.m[key]; pos != "00000005" {
		t.Fatalf("committed offset after resume = %q, want 00000005", pos)
	}

	// All 5 distinct events were delivered to the sink across the two runs.
	if len(snk.got) != 5 {
		t.Fatalf("sink received %d events total, want 5", len(snk.got))
	}
}
