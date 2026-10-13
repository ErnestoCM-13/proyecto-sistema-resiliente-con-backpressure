package backpressure_test

import (
	"context"
	"errors"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ErnestoCM-13/proyecto-sistema-resiliente-con-backpressure/internal/backpressure"
)

const (
	high     = 4
	low      = 2
	capacity = 6
)

type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func newFakeClock() *fakeClock {
	return &fakeClock{now: time.Date(2026, 11, 14, 10, 0, 0, 0, time.UTC)}
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

type recorder struct {
	mu      sync.Mutex
	changes []backpressure.Change
}

func (r *recorder) record(c backpressure.Change) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.changes = append(r.changes, c)
}

func newBuffer(t *testing.T, clock *fakeClock, rec *recorder) *backpressure.Buffer[int] {
	t.Helper()
	b, err := backpressure.New[int](backpressure.Options{
		High:     high,
		Low:      low,
		Capacity: capacity,
		Now:      clock.Now,
		OnChange: rec.record,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return b
}

type action int

const (
	put action = iota
	take
)

type step struct {
	do          action
	wantFull    bool
	wantDepth   int
	wantEngaged bool
}

func puts(depths ...int) []step {
	out := make([]step, len(depths))
	for i, d := range depths {
		out[i] = step{do: put, wantDepth: d, wantEngaged: d >= high}
	}
	return out
}

func TestSignal(t *testing.T) {
	type change struct {
		engaged bool
		depth   int
	}

	cases := []struct {
		name        string
		steps       []step
		wantChanges []change
	}{
		{
			name:  "stays off one item short of high",
			steps: puts(1, 2, 3),
		},
		{
			name:        "turns on exactly at high",
			steps:       puts(1, 2, 3, 4),
			wantChanges: []change{{true, 4}},
		},
		{
			name: "stays on while the depth is between low and high",
			steps: slices.Concat(puts(1, 2, 3, 4), []step{
				{do: take, wantDepth: 3, wantEngaged: true},
			}),
			wantChanges: []change{{true, 4}},
		},
		{
			name: "turns off exactly at low",
			steps: slices.Concat(puts(1, 2, 3, 4), []step{
				{do: take, wantDepth: 3, wantEngaged: true},
				{do: take, wantDepth: 2, wantEngaged: false},
			}),
			wantChanges: []change{{true, 4}, {false, 2}},
		},
		{
			name: "what arrives while it is on is kept, up to capacity",
			steps: slices.Concat(puts(1, 2, 3, 4), []step{
				{do: put, wantDepth: 5, wantEngaged: true},
				{do: put, wantDepth: 6, wantEngaged: true},
			}),
			wantChanges: []change{{true, 4}},
		},
		{
			name: "refuses what does not fit and loses nothing it held",
			steps: slices.Concat(puts(1, 2, 3, 4), []step{
				{do: put, wantDepth: 5, wantEngaged: true},
				{do: put, wantDepth: 6, wantEngaged: true},
				{do: put, wantFull: true, wantDepth: 6, wantEngaged: true},
				{do: take, wantDepth: 5, wantEngaged: true},
				{do: put, wantDepth: 6, wantEngaged: true},
			}),
			wantChanges: []change{{true, 4}},
		},
		{
			name: "rising again between low and high does not turn it on",
			steps: slices.Concat(puts(1, 2, 3, 4), []step{
				{do: take, wantDepth: 3, wantEngaged: true},
				{do: take, wantDepth: 2, wantEngaged: false},
				{do: put, wantDepth: 3, wantEngaged: false},
			}),
			wantChanges: []change{{true, 4}, {false, 2}},
		},
		{
			name: "turns on again when the depth comes back to high",
			steps: slices.Concat(puts(1, 2, 3, 4), []step{
				{do: take, wantDepth: 3, wantEngaged: true},
				{do: take, wantDepth: 2, wantEngaged: false},
				{do: put, wantDepth: 3, wantEngaged: false},
				{do: put, wantDepth: 4, wantEngaged: true},
			}),
			wantChanges: []change{{true, 4}, {false, 2}, {true, 4}},
		},
		{
			name: "emptying a buffer that never reached high changes nothing",
			steps: slices.Concat(puts(1, 2, 3), []step{
				{do: take, wantDepth: 2},
				{do: take, wantDepth: 1},
				{do: take, wantDepth: 0},
			}),
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rec := &recorder{}
			b := newBuffer(t, newFakeClock(), rec)

			next, oldest := 0, 0
			for i, s := range tc.steps {
				switch s.do {
				case put:
					err := b.Put(next)
					switch {
					case s.wantFull && !errors.Is(err, backpressure.ErrFull):
						t.Fatalf("step %d: Put error = %v, want ErrFull", i, err)
					case !s.wantFull && err != nil:
						t.Fatalf("step %d: Put: %v", i, err)
					case !s.wantFull:
						next++
					}
				case take:
					got, err := b.Take(context.Background())
					if err != nil {
						t.Fatalf("step %d: Take: %v", i, err)
					}
					if got != oldest {
						t.Fatalf("step %d: Take = %d, want %d, the oldest item", i, got, oldest)
					}
					oldest++
				}

				stats := b.Stats()
				if stats.Depth != s.wantDepth || stats.Engaged != s.wantEngaged {
					t.Fatalf("step %d: depth %d engaged %t, want depth %d engaged %t",
						i, stats.Depth, stats.Engaged, s.wantDepth, s.wantEngaged)
				}
			}

			var got []change
			for _, c := range rec.changes {
				got = append(got, change{c.Engaged, c.Depth})
			}
			if !slices.Equal(got, tc.wantChanges) {
				t.Fatalf("changes = %v, want %v", got, tc.wantChanges)
			}
		})
	}
}

func TestStatsCountDepthEngagementsAndTimeEngaged(t *testing.T) {
	clock := newFakeClock()
	start := clock.Now()
	rec := &recorder{}
	b := newBuffer(t, clock, rec)

	if got, want := b.Stats(), (backpressure.Stats{}); got != want {
		t.Fatalf("stats of a new buffer = %+v, want %+v", got, want)
	}

	clock.Advance(time.Second)
	for i := range 5 {
		if err := b.Put(i); err != nil {
			t.Fatalf("Put: %v", err)
		}
	}
	clock.Advance(3 * time.Second)

	want := backpressure.Stats{Depth: 5, MaxDepth: 5, Engaged: true, TimesEngaged: 1, EngagedFor: 3 * time.Second, Puts: 5}
	if got := b.Stats(); got != want {
		t.Fatalf("stats while engaged = %+v, want %+v", got, want)
	}

	for range 3 {
		if _, err := b.Take(context.Background()); err != nil {
			t.Fatalf("Take: %v", err)
		}
	}
	clock.Advance(time.Hour)
	for i := range 2 {
		if err := b.Put(i); err != nil {
			t.Fatalf("Put: %v", err)
		}
	}
	clock.Advance(2 * time.Second)

	want = backpressure.Stats{Depth: 4, MaxDepth: 5, Engaged: true, TimesEngaged: 2, EngagedFor: 5 * time.Second, Puts: 7, Takes: 3}
	if got := b.Stats(); got != want {
		t.Fatalf("stats after engaging twice = %+v, want %+v", got, want)
	}

	wantChanges := []backpressure.Change{
		{Engaged: true, Depth: 4, At: start.Add(time.Second)},
		{Engaged: false, Depth: 2, At: start.Add(4 * time.Second)},
		{Engaged: true, Depth: 4, At: start.Add(time.Hour + 4*time.Second)},
	}
	if !slices.Equal(rec.changes, wantChanges) {
		t.Fatalf("changes = %v, want %v", rec.changes, wantChanges)
	}
}

func TestTakeWaitsForAnItem(t *testing.T) {
	b := newBuffer(t, newFakeClock(), &recorder{})

	got := make(chan int, 1)
	go func() {
		item, err := b.Take(context.Background())
		if err != nil {
			t.Errorf("Take: %v", err)
		}
		got <- item
	}()

	select {
	case item := <-got:
		t.Fatalf("Take returned %d from an empty buffer", item)
	case <-time.After(50 * time.Millisecond):
	}

	if err := b.Put(42); err != nil {
		t.Fatalf("Put: %v", err)
	}
	select {
	case item := <-got:
		if item != 42 {
			t.Fatalf("Take = %d, want 42", item)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Take did not return after an item was put")
	}
}

func TestTakeGivesUpWhenTheContextIsCancelled(t *testing.T) {
	b := newBuffer(t, newFakeClock(), &recorder{})

	ctx, cancel := context.WithCancel(context.Background())
	time.AfterFunc(20*time.Millisecond, cancel)

	if _, err := b.Take(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("Take error = %v, want context.Canceled", err)
	}
	if got, want := b.Stats(), (backpressure.Stats{}); got != want {
		t.Fatalf("stats after a Take that gave up = %+v, want %+v", got, want)
	}
}

func TestWaitForRoom(t *testing.T) {
	fill := func(t *testing.T, b *backpressure.Buffer[int], n int) {
		t.Helper()
		for i := range n {
			if err := b.Put(i); err != nil {
				t.Fatalf("Put: %v", err)
			}
		}
	}
	drain := func(t *testing.T, b *backpressure.Buffer[int], n int) {
		t.Helper()
		for range n {
			if _, err := b.Take(context.Background()); err != nil {
				t.Fatalf("Take: %v", err)
			}
		}
	}

	t.Run("returns at once while the signal is off", func(t *testing.T) {
		b := newBuffer(t, newFakeClock(), &recorder{})
		fill(t, b, high-1)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := b.WaitForRoom(ctx); err != nil {
			t.Fatalf("WaitForRoom: %v", err)
		}
	})

	t.Run("holds every waiter until the depth falls to low", func(t *testing.T) {
		b := newBuffer(t, newFakeClock(), &recorder{})
		fill(t, b, high)

		const waiters = 3
		released := make(chan error, waiters)
		for range waiters {
			go func() { released <- b.WaitForRoom(context.Background()) }()
		}

		drain(t, b, high-low-1)
		select {
		case err := <-released:
			t.Fatalf("a waiter was released with %v while the depth was still above low", err)
		case <-time.After(50 * time.Millisecond):
		}

		drain(t, b, 1)
		for range waiters {
			select {
			case err := <-released:
				if err != nil {
					t.Fatalf("WaitForRoom: %v", err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("a waiter was not released after the depth fell to low")
			}
		}
	})

	t.Run("gives up when the context is cancelled", func(t *testing.T) {
		b := newBuffer(t, newFakeClock(), &recorder{})
		fill(t, b, high)

		ctx, cancel := context.WithCancel(context.Background())
		time.AfterFunc(20*time.Millisecond, cancel)
		if err := b.WaitForRoom(ctx); !errors.Is(err, context.Canceled) {
			t.Fatalf("WaitForRoom error = %v, want context.Canceled", err)
		}
	})

	t.Run("holds again after the signal turns back on", func(t *testing.T) {
		b := newBuffer(t, newFakeClock(), &recorder{})
		fill(t, b, high)
		drain(t, b, high-low)
		fill(t, b, high-low)

		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()
		if err := b.WaitForRoom(ctx); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("WaitForRoom error = %v, want it to be still waiting when the deadline passed", err)
		}
	})
}

func TestAReaderThatWaitsForRoomNeverOverfillsTheBuffer(t *testing.T) {
	const (
		items   = 20_000
		workers = 4
	)

	b, err := backpressure.New[int](backpressure.Options{High: 64, Low: 16, Capacity: 64})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var sum atomic.Int64
	var taken atomic.Int64
	var wg sync.WaitGroup
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				item, err := b.Take(ctx)
				if err != nil {
					return
				}
				sum.Add(int64(item))
				if taken.Add(1) == items {
					cancel()
				}
			}
		}()
	}

	for i := 1; i <= items; i++ {
		if err := b.WaitForRoom(context.Background()); err != nil {
			t.Fatalf("WaitForRoom: %v", err)
		}
		if err := b.Put(i); err != nil {
			t.Fatalf("Put(%d): %v", i, err)
		}
	}
	wg.Wait()

	if got := taken.Load(); got != items {
		t.Fatalf("%d items were taken, want %d", got, items)
	}
	if got, want := sum.Load(), int64(items)*(items+1)/2; got != want {
		t.Fatalf("the items taken add up to %d, want %d: one was lost or taken twice", got, want)
	}
	stats := b.Stats()
	if stats.Puts != items || stats.Takes != items || stats.Depth != 0 {
		t.Fatalf("stats = %+v, want %d puts, %d takes and nothing left", stats, items, items)
	}
	if stats.MaxDepth > 64 {
		t.Fatalf("the depth reached %d, above the capacity of 64", stats.MaxDepth)
	}
}

func TestNewRejectsBadOptions(t *testing.T) {
	cases := []struct {
		name         string
		opts         backpressure.Options
		wantProblems int
	}{
		{name: "valid", opts: backpressure.Options{High: 4, Low: 2, Capacity: 6}},
		{name: "low may be zero", opts: backpressure.Options{High: 1, Low: 0, Capacity: 1}},
		{name: "capacity may equal high", opts: backpressure.Options{High: 4, Low: 2, Capacity: 4}},
		{name: "zero high", opts: backpressure.Options{High: 0, Low: 0, Capacity: 6}, wantProblems: 1},
		{name: "negative low", opts: backpressure.Options{High: 4, Low: -1, Capacity: 6}, wantProblems: 1},
		{name: "low equal to high", opts: backpressure.Options{High: 4, Low: 4, Capacity: 6}, wantProblems: 1},
		{name: "low above high", opts: backpressure.Options{High: 4, Low: 5, Capacity: 6}, wantProblems: 1},
		{name: "capacity below high", opts: backpressure.Options{High: 4, Low: 2, Capacity: 3}, wantProblems: 1},
		{name: "low above high and capacity below it", opts: backpressure.Options{High: 4, Low: 5, Capacity: 3}, wantProblems: 2},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			b, err := backpressure.New[int](tc.opts)

			if tc.wantProblems == 0 {
				if err != nil {
					t.Fatalf("New: %v", err)
				}
				if got, want := b.Stats(), (backpressure.Stats{}); got != want {
					t.Fatalf("stats of a new buffer = %+v, want %+v", got, want)
				}
				return
			}

			if err == nil {
				t.Fatal("New succeeded, want an error")
			}
			joined, ok := err.(interface{ Unwrap() []error })
			if !ok {
				t.Fatalf("New error is %T, want one built by errors.Join", err)
			}
			if got := len(joined.Unwrap()); got != tc.wantProblems {
				t.Fatalf("New reported %d problems, want %d: %v", got, tc.wantProblems, err)
			}
		})
	}
}
