// Stops calling a dependency that keeps failing, waits, and then tests with a few calls whether it has come back.
package breaker

import (
	"errors"
	"fmt"
	"sync"
	"time"
)

type State int

const (
	StateClosed State = iota + 1
	StateOpen
	StateHalfOpen
)

func (s State) String() string {
	switch s {
	case StateClosed:
		return "closed"
	case StateOpen:
		return "open"
	case StateHalfOpen:
		return "half-open"
	default:
		return fmt.Sprintf("state(%d)", int(s))
	}
}

type RejectedError struct {
	State      State
	RetryAfter time.Duration
}

func (e *RejectedError) Error() string {
	if e.State == StateOpen {
		return fmt.Sprintf("circuit breaker is open for another %s", e.RetryAfter)
	}
	return "circuit breaker is half-open and already testing the dependency"
}

type Transition struct {
	From State
	To   State
	At   time.Time
}

type Options struct {
	FailureThreshold int
	Cooldown         time.Duration
	HalfOpenTrials   int
	IsFailure        func(error) bool

	Now          func() time.Time
	OnTransition func(Transition)
}

type Stats struct {
	State    State
	Opened   int
	OpenFor  time.Duration
	Rejected int64
}

type Breaker struct {
	opts Options

	mu          sync.Mutex
	state       State
	generation  uint64
	failures    int
	successes   int
	trialActive bool
	openedAt    time.Time
	opened      int
	openFor     time.Duration
	rejected    int64
}

func New(opts Options) (*Breaker, error) {
	var problems []error
	if opts.FailureThreshold < 1 {
		problems = append(problems, fmt.Errorf("failure threshold: %d is not positive", opts.FailureThreshold))
	}
	if opts.Cooldown <= 0 {
		problems = append(problems, fmt.Errorf("cooldown: %s is not positive", opts.Cooldown))
	}
	if opts.HalfOpenTrials < 1 {
		problems = append(problems, fmt.Errorf("half-open trials: %d is not positive", opts.HalfOpenTrials))
	}
	if opts.IsFailure == nil {
		problems = append(problems, errors.New("is failure: missing"))
	}
	if len(problems) > 0 {
		return nil, errors.Join(problems...)
	}

	if opts.Now == nil {
		opts.Now = time.Now
	}
	return &Breaker{opts: opts, state: StateClosed}, nil
}

func (b *Breaker) Do(fn func() error) error {
	generation, err := b.admit()
	if err != nil {
		return err
	}
	err = fn()
	b.record(generation, err)
	return err
}

func (b *Breaker) admit() (uint64, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	switch b.state {
	case StateClosed:
		return b.generation, nil

	case StateOpen:
		now := b.opts.Now()
		if wait := b.openedAt.Add(b.opts.Cooldown).Sub(now); wait > 0 {
			b.rejected++
			return 0, &RejectedError{State: StateOpen, RetryAfter: wait}
		}
		b.transition(StateHalfOpen, now)
		b.trialActive = true
		return b.generation, nil

	case StateHalfOpen:
		if b.trialActive {
			b.rejected++
			return 0, &RejectedError{State: StateHalfOpen}
		}
		b.trialActive = true
		return b.generation, nil

	default:
		return 0, fmt.Errorf("circuit breaker is in %s", b.state)
	}
}

func (b *Breaker) record(generation uint64, err error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if generation != b.generation {
		return
	}

	switch {
	case err == nil:
		switch b.state {
		case StateClosed:
			b.failures = 0
		case StateHalfOpen:
			b.trialActive = false
			b.successes++
			if b.successes >= b.opts.HalfOpenTrials {
				b.transition(StateClosed, b.opts.Now())
			}
		}

	case b.opts.IsFailure(err):
		switch b.state {
		case StateClosed:
			b.failures++
			if b.failures >= b.opts.FailureThreshold {
				b.transition(StateOpen, b.opts.Now())
			}
		case StateHalfOpen:
			b.transition(StateOpen, b.opts.Now())
		}

	default:
		if b.state == StateHalfOpen {
			b.trialActive = false
		}
	}
}

func (b *Breaker) transition(to State, now time.Time) {
	from := b.state
	if from == StateOpen {
		b.openFor += now.Sub(b.openedAt)
	}

	b.state = to
	b.generation++
	b.failures, b.successes, b.trialActive = 0, 0, false
	if to == StateOpen {
		b.openedAt = now
		b.opened++
	}

	if b.opts.OnTransition != nil {
		b.opts.OnTransition(Transition{From: from, To: to, At: now})
	}
}

func (b *Breaker) State() State {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.state
}

func (b *Breaker) Stats() Stats {
	b.mu.Lock()
	defer b.mu.Unlock()

	openFor := b.openFor
	if b.state == StateOpen {
		openFor += b.opts.Now().Sub(b.openedAt)
	}
	return Stats{State: b.state, Opened: b.opened, OpenFor: openFor, Rejected: b.rejected}
}
