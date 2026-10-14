package breaker_test

import (
	"errors"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ErnestoCM-13/proyecto-sistema-resiliente-con-backpressure/internal/breaker"
)

const (
	threshold = 5
	cooldown  = 2 * time.Second
	trials    = 3
)

var (
	errFailure = errors.New("the dependency failed")
	errNeutral = errors.New("the caller gave up")
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
	mu          sync.Mutex
	transitions []breaker.Transition
}

func (r *recorder) record(tr breaker.Transition) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.transitions = append(r.transitions, tr)
}

func (r *recorder) states() []breaker.State {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []breaker.State
	for _, tr := range r.transitions {
		out = append(out, tr.To)
	}
	return out
}

func newBreaker(t *testing.T, clock *fakeClock, rec *recorder) *breaker.Breaker {
	t.Helper()
	b, err := breaker.New(breaker.Options{
		FailureThreshold: threshold,
		Cooldown:         cooldown,
		HalfOpenTrials:   trials,
		IsFailure:        func(err error) bool { return errors.Is(err, errFailure) },
		Now:              clock.Now,
		OnTransition:     rec.record,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return b
}

type call int

const (
	noCall call = iota
	succeed
	fail
	neutral
)

func (c call) err() error {
	switch c {
	case fail:
		return errFailure
	case neutral:
		return errNeutral
	default:
		return nil
	}
}

type step struct {
	advance      time.Duration
	call         call
	wantRejected bool
	wantRetry    time.Duration
	wantState    breaker.State
}

func repeat(n int, s step) []step {
	out := make([]step, n)
	for i := range out {
		out[i] = s
	}
	return out
}

func join(parts ...[]step) []step {
	return slices.Concat(parts...)
}

const (
	closed   = breaker.StateClosed
	open     = breaker.StateOpen
	halfOpen = breaker.StateHalfOpen
)

var (
	opens = join(
		repeat(threshold-1, step{call: fail, wantState: closed}),
		[]step{{call: fail, wantState: open}},
	)
	closes = join(
		[]step{{advance: cooldown, call: succeed, wantState: halfOpen}},
		repeat(trials-2, step{call: succeed, wantState: halfOpen}),
		[]step{{call: succeed, wantState: closed}},
	)
)

func TestStateMachine(t *testing.T) {
	cases := []struct {
		name            string
		steps           []step
		wantTransitions []breaker.State
	}{
		{
			name:  "starts closed and lets calls through",
			steps: []step{{wantState: closed}, {call: succeed, wantState: closed}},
		},
		{
			name:  "stays closed one failure short of the threshold",
			steps: repeat(threshold-1, step{call: fail, wantState: closed}),
		},
		{
			name:            "opens at the threshold",
			steps:           opens,
			wantTransitions: []breaker.State{open},
		},
		{
			name: "a success resets the count of failures",
			steps: join(
				repeat(threshold-1, step{call: fail, wantState: closed}),
				[]step{{call: succeed, wantState: closed}},
				repeat(threshold-1, step{call: fail, wantState: closed}),
			),
		},
		{
			name: "an error that is not a failure neither counts nor resets",
			steps: join(
				repeat(threshold-1, step{call: fail, wantState: closed}),
				[]step{{call: neutral, wantState: closed}},
				[]step{{call: fail, wantState: open}},
			),
			wantTransitions: []breaker.State{open},
		},
		{
			name: "rejects without calling while open, and says how long is left",
			steps: join(opens, []step{
				{call: succeed, wantRejected: true, wantRetry: cooldown, wantState: open},
				{advance: 1500 * time.Millisecond, call: succeed, wantRejected: true, wantRetry: 500 * time.Millisecond, wantState: open},
			}),
			wantTransitions: []breaker.State{open},
		},
		{
			name: "still rejects one nanosecond before the cooldown ends",
			steps: join(opens, []step{
				{advance: cooldown - time.Nanosecond, call: succeed, wantRejected: true, wantRetry: time.Nanosecond, wantState: open},
			}),
			wantTransitions: []breaker.State{open},
		},
		{
			name: "lets a trial through exactly when the cooldown ends",
			steps: join(opens, []step{
				{advance: cooldown, call: succeed, wantState: halfOpen},
			}),
			wantTransitions: []breaker.State{open, halfOpen},
		},
		{
			name:            "closes after every trial succeeds",
			steps:           join(opens, closes),
			wantTransitions: []breaker.State{open, halfOpen, closed},
		},
		{
			name: "a failed first trial reopens and restarts the cooldown",
			steps: join(opens, []step{
				{advance: cooldown, call: fail, wantState: open},
				{advance: cooldown - time.Nanosecond, call: succeed, wantRejected: true, wantRetry: time.Nanosecond, wantState: open},
				{advance: time.Nanosecond, call: succeed, wantState: halfOpen},
			}),
			wantTransitions: []breaker.State{open, halfOpen, open, halfOpen},
		},
		{
			name: "a failed last trial reopens",
			steps: join(opens, []step{
				{advance: cooldown, call: succeed, wantState: halfOpen},
				{call: succeed, wantState: halfOpen},
				{call: fail, wantState: open},
			}),
			wantTransitions: []breaker.State{open, halfOpen, open},
		},
		{
			name: "after a failed trial, the next round needs every trial again",
			steps: join(opens, []step{
				{advance: cooldown, call: succeed, wantState: halfOpen},
				{call: succeed, wantState: halfOpen},
				{call: fail, wantState: open},
				{advance: cooldown, call: succeed, wantState: halfOpen},
				{call: succeed, wantState: halfOpen},
				{call: succeed, wantState: closed},
			}),
			wantTransitions: []breaker.State{open, halfOpen, open, halfOpen, closed},
		},
		{
			name: "a trial that is neither success nor failure decides nothing",
			steps: join(opens, []step{
				{advance: cooldown, call: neutral, wantState: halfOpen},
				{call: succeed, wantState: halfOpen},
				{call: neutral, wantState: halfOpen},
				{call: succeed, wantState: halfOpen},
				{call: succeed, wantState: closed},
			}),
			wantTransitions: []breaker.State{open, halfOpen, closed},
		},
		{
			name: "once closed again, failures are counted from zero",
			steps: join(opens, closes,
				repeat(threshold-1, step{call: fail, wantState: closed}),
				[]step{{call: fail, wantState: open}},
			),
			wantTransitions: []breaker.State{open, halfOpen, closed, open},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			clock := newFakeClock()
			rec := &recorder{}
			b := newBreaker(t, clock, rec)

			for i, s := range tc.steps {
				clock.Advance(s.advance)

				if s.call != noCall {
					ran := false
					want := s.call.err()
					err := b.Do(func() error {
						ran = true
						return want
					})

					var rejected *breaker.RejectedError
					switch {
					case s.wantRejected:
						if ran {
							t.Fatalf("step %d: the call ran, want it rejected", i)
						}
						if !errors.As(err, &rejected) {
							t.Fatalf("step %d: error = %v, want a *breaker.RejectedError", i, err)
						}
						if rejected.State != open || rejected.RetryAfter != s.wantRetry {
							t.Fatalf("step %d: rejected in %s for %s, want open for %s",
								i, rejected.State, rejected.RetryAfter, s.wantRetry)
						}
					case !ran:
						t.Fatalf("step %d: the call did not run: %v", i, err)
					case err != want:
						t.Fatalf("step %d: Do returned %v, want the call's own result %v", i, err, want)
					}
				}

				if got := b.State(); got != s.wantState {
					t.Fatalf("step %d: state = %s, want %s", i, got, s.wantState)
				}
			}

			if got := rec.states(); !slices.Equal(got, tc.wantTransitions) {
				t.Fatalf("transitions went to %v, want %v", got, tc.wantTransitions)
			}
		})
	}
}

func run(t *testing.T, b *breaker.Breaker, clock *fakeClock, steps []step) {
	t.Helper()
	for i, s := range steps {
		clock.Advance(s.advance)
		if s.call != noCall {
			b.Do(s.call.err)
		}
		if got := b.State(); got != s.wantState {
			t.Fatalf("step %d: state = %s, want %s", i, got, s.wantState)
		}
	}
}

func TestTransitionsCarryWhereTheyCameFromAndWhen(t *testing.T) {
	clock := newFakeClock()
	start := clock.Now()
	rec := &recorder{}
	b := newBreaker(t, clock, rec)

	clock.Advance(time.Second)
	run(t, b, clock, join(opens, closes))

	want := []breaker.Transition{
		{From: closed, To: open, At: start.Add(time.Second)},
		{From: open, To: halfOpen, At: start.Add(time.Second + cooldown)},
		{From: halfOpen, To: closed, At: start.Add(time.Second + cooldown)},
	}
	if !slices.Equal(rec.transitions, want) {
		t.Fatalf("transitions = %v, want %v", rec.transitions, want)
	}
}

func TestStatsCountOpeningsRejectionsAndTimeOpen(t *testing.T) {
	clock := newFakeClock()
	b := newBreaker(t, clock, &recorder{})

	if got, want := b.Stats(), (breaker.Stats{State: closed}); got != want {
		t.Fatalf("stats of a new breaker = %+v, want %+v", got, want)
	}

	run(t, b, clock, join(opens, []step{
		{advance: time.Second, call: succeed, wantState: open},
	}))
	want := breaker.Stats{State: open, Opened: 1, OpenFor: time.Second, Rejected: 1}
	if got := b.Stats(); got != want {
		t.Fatalf("stats while open = %+v, want %+v", got, want)
	}

	run(t, b, clock, []step{
		{advance: time.Second, call: fail, wantState: open},
		{advance: 3 * time.Second, call: succeed, wantState: halfOpen},
		{advance: 7 * time.Second, call: succeed, wantState: halfOpen},
		{call: succeed, wantState: closed},
		{advance: time.Hour, call: succeed, wantState: closed},
	})
	want = breaker.Stats{State: closed, Opened: 2, OpenFor: cooldown + 3*time.Second, Rejected: 1}
	if got := b.Stats(); got != want {
		t.Fatalf("stats after closing = %+v, want %+v", got, want)
	}
}

func TestHalfOpenRunsOneTrialAtATime(t *testing.T) {
	clock := newFakeClock()
	b := newBreaker(t, clock, &recorder{})
	run(t, b, clock, opens)
	clock.Advance(cooldown)

	started := make(chan struct{})
	release := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- b.Do(func() error {
			close(started)
			<-release
			return nil
		})
	}()
	<-started

	ran := false
	err := b.Do(func() error {
		ran = true
		return nil
	})
	var rejected *breaker.RejectedError
	if !errors.As(err, &rejected) || ran {
		t.Fatalf("a second trial ran=%t with error %v, want it rejected while the first is in flight", ran, err)
	}
	if rejected.State != halfOpen || rejected.RetryAfter != 0 {
		t.Fatalf("rejected in %s for %s, want half-open with no wait to report", rejected.State, rejected.RetryAfter)
	}

	close(release)
	if err := <-done; err != nil {
		t.Fatalf("the first trial: %v", err)
	}

	run(t, b, clock, []step{
		{call: succeed, wantState: halfOpen},
		{call: succeed, wantState: closed},
	})
}

func TestACallLetThroughBeforeATransitionDoesNotCountAfterIt(t *testing.T) {
	cases := []struct {
		name   string
		result error
	}{
		{name: "its failure does not reopen a half-open breaker", result: errFailure},
		{name: "its success does not count as a trial", result: nil},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			clock := newFakeClock()
			b := newBreaker(t, clock, &recorder{})

			started := make(chan struct{})
			release := make(chan struct{})
			done := make(chan error, 1)
			go func() {
				done <- b.Do(func() error {
					close(started)
					<-release
					return tc.result
				})
			}()
			<-started

			run(t, b, clock, join(opens, []step{
				{advance: cooldown, call: succeed, wantState: halfOpen},
			}))

			close(release)
			if err := <-done; err != tc.result {
				t.Fatalf("the old call returned %v, want %v", err, tc.result)
			}
			if got := b.State(); got != halfOpen {
				t.Fatalf("state after the old call returned = %s, want half-open", got)
			}

			run(t, b, clock, []step{
				{call: succeed, wantState: halfOpen},
				{call: succeed, wantState: closed},
			})
		})
	}
}

func TestEveryCallIsEitherRunOrRejectedUnderConcurrency(t *testing.T) {
	const (
		workers = 16
		perWork = 500
	)

	b, err := breaker.New(breaker.Options{
		FailureThreshold: threshold,
		Cooldown:         time.Millisecond,
		HalfOpenTrials:   trials,
		IsFailure:        func(err error) bool { return errors.Is(err, errFailure) },
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	var ran, rejected atomic.Int64
	var wg sync.WaitGroup
	for w := range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range perWork {
				err := b.Do(func() error {
					ran.Add(1)
					if (w+i)%7 == 0 {
						return nil
					}
					return errFailure
				})
				var rej *breaker.RejectedError
				if errors.As(err, &rej) {
					rejected.Add(1)
					time.Sleep(50 * time.Microsecond)
				}
			}
		}()
	}
	wg.Wait()

	if got := ran.Load() + rejected.Load(); got != workers*perWork {
		t.Fatalf("%d calls ran and %d were rejected, %d in all, want %d",
			ran.Load(), rejected.Load(), got, workers*perWork)
	}
	stats := b.Stats()
	if stats.Rejected != rejected.Load() {
		t.Fatalf("the breaker counted %d rejections, the callers saw %d", stats.Rejected, rejected.Load())
	}
	if stats.Opened == 0 {
		t.Fatal("the breaker never opened although every caller fails six calls in a row")
	}
}

func TestNewRejectsBadOptions(t *testing.T) {
	isFailure := func(error) bool { return true }

	cases := []struct {
		name         string
		opts         breaker.Options
		wantProblems int
	}{
		{name: "valid", opts: breaker.Options{FailureThreshold: 1, Cooldown: time.Nanosecond, HalfOpenTrials: 1, IsFailure: isFailure}},
		{name: "zero threshold", opts: breaker.Options{FailureThreshold: 0, Cooldown: cooldown, HalfOpenTrials: trials, IsFailure: isFailure}, wantProblems: 1},
		{name: "negative threshold", opts: breaker.Options{FailureThreshold: -1, Cooldown: cooldown, HalfOpenTrials: trials, IsFailure: isFailure}, wantProblems: 1},
		{name: "zero cooldown", opts: breaker.Options{FailureThreshold: threshold, Cooldown: 0, HalfOpenTrials: trials, IsFailure: isFailure}, wantProblems: 1},
		{name: "negative cooldown", opts: breaker.Options{FailureThreshold: threshold, Cooldown: -time.Second, HalfOpenTrials: trials, IsFailure: isFailure}, wantProblems: 1},
		{name: "zero trials", opts: breaker.Options{FailureThreshold: threshold, Cooldown: cooldown, HalfOpenTrials: 0, IsFailure: isFailure}, wantProblems: 1},
		{name: "negative trials", opts: breaker.Options{FailureThreshold: threshold, Cooldown: cooldown, HalfOpenTrials: -1, IsFailure: isFailure}, wantProblems: 1},
		{name: "no failure rule", opts: breaker.Options{FailureThreshold: threshold, Cooldown: cooldown, HalfOpenTrials: trials}, wantProblems: 1},
		{name: "nothing set", opts: breaker.Options{}, wantProblems: 4},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			b, err := breaker.New(tc.opts)

			if tc.wantProblems == 0 {
				if err != nil {
					t.Fatalf("New: %v", err)
				}
				if got := b.State(); got != closed {
					t.Fatalf("a new breaker is %s, want closed", got)
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

func TestStateString(t *testing.T) {
	cases := []struct {
		state breaker.State
		want  string
	}{
		{state: closed, want: "closed"},
		{state: open, want: "open"},
		{state: halfOpen, want: "half-open"},
		{state: breaker.State(0), want: "state(0)"},
		{state: breaker.State(9), want: "state(9)"},
	}

	for _, tc := range cases {
		t.Run(tc.want, func(t *testing.T) {
			if got := tc.state.String(); got != tc.want {
				t.Fatalf("String() = %q, want %q", got, tc.want)
			}
		})
	}
}
