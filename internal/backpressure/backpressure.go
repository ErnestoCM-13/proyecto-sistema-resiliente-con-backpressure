// Holds the work that is waiting for a slow dependency, and says when so much of it has piled up that no more should be taken in.
package backpressure

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

var ErrFull = errors.New("the wait buffer is at capacity")

type Options struct {
	High     int
	Low      int
	Capacity int

	Now      func() time.Time
	OnChange func(Change)
}

type Change struct {
	Engaged bool
	Depth   int
	At      time.Time
}

type Stats struct {
	Depth        int
	MaxDepth     int
	Engaged      bool
	TimesEngaged int
	EngagedFor   time.Duration
	Puts         int64
	Takes        int64
}

type Buffer[T any] struct {
	opts  Options
	items chan T

	mu           sync.Mutex
	depth        int
	maxDepth     int
	engaged      bool
	engagedAt    time.Time
	timesEngaged int
	engagedFor   time.Duration
	puts, takes  int64
	room         chan struct{}
}

func New[T any](opts Options) (*Buffer[T], error) {
	var problems []error
	if opts.High < 1 {
		problems = append(problems, fmt.Errorf("high: %d is not positive", opts.High))
	}
	if opts.Low < 0 {
		problems = append(problems, fmt.Errorf("low: %d is negative", opts.Low))
	}
	if opts.Low >= opts.High && opts.High >= 1 {
		problems = append(problems, fmt.Errorf("low: %d is not below high (%d)", opts.Low, opts.High))
	}
	if opts.Capacity < opts.High {
		problems = append(problems, fmt.Errorf("capacity: %d is below high (%d)", opts.Capacity, opts.High))
	}
	if len(problems) > 0 {
		return nil, errors.Join(problems...)
	}

	if opts.Now == nil {
		opts.Now = time.Now
	}
	room := make(chan struct{})
	close(room)
	return &Buffer[T]{opts: opts, items: make(chan T, opts.Capacity), room: room}, nil
}

func (b *Buffer[T]) Put(item T) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.depth >= b.opts.Capacity {
		return ErrFull
	}
	b.items <- item
	b.depth++
	b.puts++
	b.maxDepth = max(b.maxDepth, b.depth)

	if !b.engaged && b.depth >= b.opts.High {
		b.engaged = true
		b.engagedAt = b.opts.Now()
		b.timesEngaged++
		b.room = make(chan struct{})
		b.changed(b.engagedAt)
	}
	return nil
}

func (b *Buffer[T]) Take(ctx context.Context) (T, error) {
	select {
	case item := <-b.items:
		b.taken()
		return item, nil
	case <-ctx.Done():
		var zero T
		return zero, ctx.Err()
	}
}

func (b *Buffer[T]) taken() {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.depth--
	b.takes++
	if b.engaged && b.depth <= b.opts.Low {
		now := b.opts.Now()
		b.engaged = false
		b.engagedFor += now.Sub(b.engagedAt)
		close(b.room)
		b.changed(now)
	}
}

func (b *Buffer[T]) changed(at time.Time) {
	if b.opts.OnChange != nil {
		b.opts.OnChange(Change{Engaged: b.engaged, Depth: b.depth, At: at})
	}
}

func (b *Buffer[T]) WaitForRoom(ctx context.Context) error {
	b.mu.Lock()
	room := b.room
	b.mu.Unlock()

	select {
	case <-room:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (b *Buffer[T]) Stats() Stats {
	b.mu.Lock()
	defer b.mu.Unlock()

	engagedFor := b.engagedFor
	if b.engaged {
		engagedFor += b.opts.Now().Sub(b.engagedAt)
	}
	return Stats{
		Depth:        b.depth,
		MaxDepth:     b.maxDepth,
		Engaged:      b.engaged,
		TimesEngaged: b.timesEngaged,
		EngagedFor:   engagedFor,
		Puts:         b.puts,
		Takes:        b.takes,
	}
}
