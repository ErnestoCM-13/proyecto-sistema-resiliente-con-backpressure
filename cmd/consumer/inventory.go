package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/ErnestoCM-13/proyecto-sistema-resiliente-con-backpressure/internal/aggregate"
	"github.com/ErnestoCM-13/proyecto-sistema-resiliente-con-backpressure/internal/backpressure"
	"github.com/ErnestoCM-13/proyecto-sistema-resiliente-con-backpressure/internal/breaker"
	"github.com/ErnestoCM-13/proyecto-sistema-resiliente-con-backpressure/internal/clock"
	"github.com/ErnestoCM-13/proyecto-sistema-resiliente-con-backpressure/internal/event"
	"github.com/ErnestoCM-13/proyecto-sistema-resiliente-con-backpressure/internal/inventory"
	"github.com/ErnestoCM-13/proyecto-sistema-resiliente-con-backpressure/internal/pace"
	"github.com/ErnestoCM-13/proyecto-sistema-resiliente-con-backpressure/internal/stream"
)

const (
	halfOpenRetry = 10 * time.Millisecond
	ackTimeout    = 5 * time.Second
)

type checker interface {
	Check(ctx context.Context, orderID, country string) (bool, error)
	Stats() breaker.Stats
}

type pendingCheck struct {
	id    string
	ev    event.Event
	pubNS int64
}

type pipeline struct {
	reader  *stream.Reader
	checker checker
	buffer  *backpressure.Buffer[pendingCheck]
	log     *timeline
}

func newPipeline(reader *stream.Reader, cfg config, log *timeline) (*pipeline, error) {
	client, err := inventory.NewClient(cfg.inventoryURL, cfg.checkTimeout)
	if err != nil {
		return nil, fmt.Errorf("inventory client: %w", err)
	}
	guarded, err := inventory.NewGuarded(client, breaker.Options{
		FailureThreshold: cfg.failures,
		Cooldown:         cfg.cooldown,
		HalfOpenTrials:   cfg.trials,
		OnTransition:     log.breaker,
	})
	if err != nil {
		return nil, fmt.Errorf("circuit breaker: %w", err)
	}
	buffer, err := backpressure.New[pendingCheck](backpressure.Options{
		High:     cfg.bufferHigh,
		Low:      cfg.bufferLow,
		Capacity: cfg.bufferHigh + int(cfg.readCount),
		OnChange: log.backpressure,
	})
	if err != nil {
		return nil, fmt.Errorf("wait buffer: %w", err)
	}
	return &pipeline{reader: reader, checker: guarded, buffer: buffer, log: log}, nil
}

type TimelineEntry struct {
	AtSeconds float64 `json:"at_seconds"`
	What      string  `json:"what"`
}

type timeline struct {
	mu      sync.Mutex
	start   time.Time
	out     io.Writer
	entries []TimelineEntry
}

func newTimeline(out io.Writer) *timeline {
	return &timeline{start: time.Now(), out: out}
}

func (t *timeline) add(at time.Time, what string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	entry := TimelineEntry{AtSeconds: at.Sub(t.start).Seconds(), What: what}
	t.entries = append(t.entries, entry)
	fmt.Fprintf(t.out, "  +%8.3fs  %s\n", entry.AtSeconds, entry.What)
}

func (t *timeline) breaker(tr breaker.Transition) {
	t.add(tr.At, fmt.Sprintf("breaker %s -> %s", tr.From, tr.To))
}

func (t *timeline) backpressure(c backpressure.Change) {
	if c.Engaged {
		t.add(c.At, fmt.Sprintf("backpressure on, %d waiting: reading from the stream stops", c.Depth))
		return
	}
	t.add(c.At, fmt.Sprintf("backpressure off, %d waiting: reading resumes", c.Depth))
}

func (t *timeline) snapshot() []TimelineEntry {
	t.mu.Lock()
	defer t.mu.Unlock()
	return append([]TimelineEntry(nil), t.entries...)
}

type processor struct {
	mu          sync.Mutex
	agg         *aggregate.Aggregate
	latencies   []int64
	verifiedNS  []int64
	outstanding int
	first, last time.Time

	available, unavailable                      int64
	overloaded, timeouts, unreachable, rejected int64
}

func (p *processor) apply(ev *event.Event, latencyNS int64, now time.Time) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.applyLocked(ev, latencyNS, now)
}

func (p *processor) applyLocked(ev *event.Event, latencyNS int64, now time.Time) {
	p.agg.Add(ev)
	p.latencies = append(p.latencies, latencyNS)
	if p.first.IsZero() {
		p.first = now
	}
	p.last = now
}

func (p *processor) applyVerified(ev *event.Event, latencyNS int64, available bool, now time.Time) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.applyLocked(ev, latencyNS, now)
	p.verifiedNS = append(p.verifiedNS, latencyNS)
	if available {
		p.available++
	} else {
		p.unavailable++
	}
}

func (p *processor) hold() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.outstanding++
}

func (p *processor) release() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.outstanding--
}

func (p *processor) idle() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.outstanding == 0
}

func (p *processor) countRejected() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.rejected++
}

func (p *processor) countFailure(kind inventory.Kind) {
	p.mu.Lock()
	defer p.mu.Unlock()
	switch kind {
	case inventory.KindOverloaded:
		p.overloaded++
	case inventory.KindTimeout:
		p.timeouts++
	case inventory.KindUnreachable:
		p.unreachable++
	}
}

func work(ctx context.Context, p *pipeline, state *processor) error {
	for {
		item, err := p.buffer.Take(ctx)
		if err != nil {
			return err
		}
		available, err := verify(ctx, p.checker, item, state)
		if err != nil {
			return err
		}

		nowNS, err := clock.NowNS()
		if err != nil {
			return err
		}
		state.applyVerified(&item.ev, nowNS-item.pubNS, available, time.Now())
		if err := ack(ctx, p.reader, []string{item.id}); err != nil {
			return err
		}
		state.release()
	}
}

func ack(ctx context.Context, reader *stream.Reader, ids []string) error {
	ackCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), ackTimeout)
	defer cancel()
	return reader.Ack(ackCtx, ids)
}

func verify(ctx context.Context, c checker, item pendingCheck, state *processor) (bool, error) {
	for {
		available, err := c.Check(ctx, item.ev.OrderID, string(item.ev.Country))
		if err == nil {
			return available, nil
		}

		var rejected *breaker.RejectedError
		if errors.As(err, &rejected) {
			state.countRejected()
			if err := pace.Wait(ctx, time.Now().Add(max(rejected.RetryAfter, halfOpenRetry))); err != nil {
				return false, err
			}
			continue
		}

		var invErr *inventory.Error
		if !errors.As(err, &invErr) || !invErr.Kind.Retryable() {
			return false, fmt.Errorf("verifying order %s of message %s: %w", item.ev.OrderID, item.id, err)
		}
		state.countFailure(invErr.Kind)
	}
}
