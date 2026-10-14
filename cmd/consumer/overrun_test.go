package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"testing"

	"github.com/ErnestoCM-13/proyecto-sistema-resiliente-con-backpressure/internal/stream"
)

type unansweredReads struct {
	*stream.Reader
	calls      int
	unanswered func(call int) bool
}

func (u *unansweredReads) Read(ctx context.Context) ([]stream.Message, error) {
	u.calls++
	if u.unanswered(u.calls) {
		return nil, fmt.Errorf("xreadgroup: %w", stream.ErrBlockOverran)
	}
	return u.Reader.Read(ctx)
}

func (f *consumerFixture) consumeWithUnansweredReads(t *testing.T, unanswered func(call int) bool) (*Report, error) {
	t.Helper()
	ctx := context.Background()
	reader := f.reader(f.cfg.consumerName)
	if err := reader.EnsureGroup(ctx); err != nil {
		t.Fatalf("ensure group: %v", err)
	}
	source := &unansweredReads{Reader: reader, unanswered: unanswered}
	p, err := newPipeline(source, f.cfg, newTimeline(io.Discard))
	if err != nil {
		t.Fatalf("newPipeline: %v", err)
	}
	return consume(ctx, p, f.cfg)
}

func TestConsumeReadsAgainWhenRedisLeavesAReadUnanswered(t *testing.T) {
	stub := newInventoryStub(t, func(int, string) answer { return available })
	f := newConsumerFixture(t, stub.URL)
	f.publish(t, oneOfEach())

	rep, err := f.consumeWithUnansweredReads(t, func(call int) bool {
		return call == 1 || call == 4 || call == 5 || call == 9 || call == 10
	})
	if err != nil {
		t.Fatalf("consume: %v", err)
	}

	assertEveryEventOnce(t, rep)
	assertEveryReservationCheckedOnce(t, stub, testReserved)
	if rep.ReadOverruns != 5 {
		t.Fatalf("the report counts %d unanswered reads, want 5", rep.ReadOverruns)
	}
	if rep.Claimed != 0 || rep.DuplicatesDiscarded != 0 {
		t.Fatalf("claimed %d and discarded %d duplicates, want 0 and 0", rep.Claimed, rep.DuplicatesDiscarded)
	}
	if pending := f.pending(t); pending != 0 {
		t.Fatalf("%d messages are still unacknowledged, want 0", pending)
	}

	noted := 0
	for _, entry := range rep.Timeline {
		if entry.What == "redis left a stream read unanswered; reading again" {
			noted++
		}
	}
	if noted != 5 {
		t.Fatalf("the timeline notes %d unanswered reads, want 5", noted)
	}
}

func TestConsumeGivesUpAfterThreeUnansweredReadsInARow(t *testing.T) {
	stub := newInventoryStub(t, func(int, string) answer { return available })
	f := newConsumerFixture(t, stub.URL)
	f.publish(t, oneOfEach())

	rep, err := f.consumeWithUnansweredReads(t, func(call int) bool { return call >= 4 })

	if !errors.Is(err, stream.ErrBlockOverran) {
		t.Fatalf("consume error = %v, want it to wrap stream.ErrBlockOverran", err)
	}
	if rep.ReadOverruns != maxReadOverruns {
		t.Fatalf("the report counts %d unanswered reads, want it to stop at %d", rep.ReadOverruns, maxReadOverruns)
	}
	if rep.Consumed != 30 {
		t.Fatalf("consumed %d messages, want the 30 of the three reads that were answered", rep.Consumed)
	}
	if total := rep.Count.Aggregated + rep.Count.ReadNotFinished; total != 30 {
		t.Fatalf("%d aggregated and %d read but not aggregated, want them to add up to the 30 read",
			rep.Count.Aggregated, rep.Count.ReadNotFinished)
	}
	if pending := f.pending(t); pending != rep.Count.ReadNotFinished {
		t.Fatalf("%d messages are unacknowledged and %d events were read but not aggregated, want them equal",
			pending, rep.Count.ReadNotFinished)
	}
}
