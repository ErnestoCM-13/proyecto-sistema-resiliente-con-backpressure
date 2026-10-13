package inventory_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ErnestoCM-13/proyecto-sistema-resiliente-con-backpressure/internal/breaker"
	"github.com/ErnestoCM-13/proyecto-sistema-resiliente-con-backpressure/internal/inventory"
)

const (
	guardThreshold = 5
	guardCooldown  = 2 * time.Second
	guardTrials    = 3
	guardTimeout   = 50 * time.Millisecond
)

type behaviour int32

const (
	answerAvailable behaviour = iota
	answerOverloaded
	answerNotFound
	neverAnswer
)

type switchableServer struct {
	*httptest.Server
	behaviour atomic.Int32
	hits      atomic.Int64
}

func newSwitchableServer(t *testing.T, initial behaviour) *switchableServer {
	t.Helper()
	s := &switchableServer{}
	s.behaviour.Store(int32(initial))

	release := make(chan struct{})
	s.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.hits.Add(1)
		switch behaviour(s.behaviour.Load()) {
		case answerAvailable:
			respond(http.StatusOK, `{"available": true}`)(w, r)
		case answerOverloaded:
			respond(http.StatusServiceUnavailable, `{"error": "overloaded"}`)(w, r)
		case answerNotFound:
			respond(http.StatusNotFound, `{"error": "not found"}`)(w, r)
		case neverAnswer:
			<-release
		}
	}))
	t.Cleanup(func() {
		close(release)
		s.Server.Close()
	})
	return s
}

type guardClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *guardClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *guardClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

type guardFixture struct {
	guarded     *inventory.Guarded
	clock       *guardClock
	transitions []breaker.State
}

func newGuardFixture(t *testing.T, baseURL string) *guardFixture {
	t.Helper()
	f := &guardFixture{clock: &guardClock{now: time.Date(2026, 11, 14, 10, 0, 0, 0, time.UTC)}}

	guarded, err := inventory.NewGuarded(newClient(t, baseURL, guardTimeout), breaker.Options{
		FailureThreshold: guardThreshold,
		Cooldown:         guardCooldown,
		HalfOpenTrials:   guardTrials,
		Now:              f.clock.Now,
		OnTransition:     func(tr breaker.Transition) { f.transitions = append(f.transitions, tr.To) },
	})
	if err != nil {
		t.Fatalf("NewGuarded: %v", err)
	}
	f.guarded = guarded
	return f
}

func TestGuardedOpensOnEachKindOfUnavailability(t *testing.T) {
	cases := []struct {
		name      string
		behaviour behaviour
		closePort bool
		wantKind  inventory.Kind
		wantHits  int64
	}{
		{name: "overloaded", behaviour: answerOverloaded, wantKind: inventory.KindOverloaded, wantHits: guardThreshold},
		{name: "timeout", behaviour: neverAnswer, wantKind: inventory.KindTimeout, wantHits: guardThreshold},
		{name: "unreachable", closePort: true, wantKind: inventory.KindUnreachable, wantHits: 0},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv := newSwitchableServer(t, tc.behaviour)
			f := newGuardFixture(t, srv.URL)
			if tc.closePort {
				srv.Server.CloseClientConnections()
				srv.Listener.Close()
			}

			for i := range guardThreshold {
				_, err := f.guarded.Check(context.Background(), "order-0001", "MX")
				var invErr *inventory.Error
				if !errors.As(err, &invErr) || invErr.Kind != tc.wantKind {
					t.Fatalf("call %d: error = %v, want an inventory error of kind %s", i, err, tc.wantKind)
				}
			}

			_, err := f.guarded.Check(context.Background(), "order-0001", "MX")
			var rejected *breaker.RejectedError
			if !errors.As(err, &rejected) {
				t.Fatalf("call after the threshold: error = %v, want a *breaker.RejectedError", err)
			}
			if rejected.State != breaker.StateOpen || rejected.RetryAfter != guardCooldown {
				t.Fatalf("rejected in %s for %s, want open for %s", rejected.State, rejected.RetryAfter, guardCooldown)
			}
			if got := srv.hits.Load(); got != tc.wantHits {
				t.Fatalf("the server saw %d calls, want %d: a rejected call must not reach it", got, tc.wantHits)
			}

			want := breaker.Stats{State: breaker.StateOpen, Opened: 1, Rejected: 1}
			if got := f.guarded.Stats(); got != want {
				t.Fatalf("stats = %+v, want %+v", got, want)
			}
		})
	}
}

func TestGuardedStaysClosedOnErrorsThatAreNotUnavailability(t *testing.T) {
	cases := []struct {
		name      string
		behaviour behaviour
		cancelled bool
		check     func(t *testing.T, err error)
	}{
		{
			name:      "a status the contract does not name",
			behaviour: answerNotFound,
			check: func(t *testing.T, err error) {
				var invErr *inventory.Error
				if !errors.As(err, &invErr) || invErr.Kind != inventory.KindProtocol {
					t.Fatalf("error = %v, want an inventory error of kind %s", err, inventory.KindProtocol)
				}
			},
		},
		{
			name:      "the caller gave up",
			behaviour: neverAnswer,
			cancelled: true,
			check: func(t *testing.T, err error) {
				if !errors.Is(err, context.Canceled) {
					t.Fatalf("error = %v, want it to wrap context.Canceled", err)
				}
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv := newSwitchableServer(t, tc.behaviour)
			f := newGuardFixture(t, srv.URL)

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if tc.cancelled {
				cancel()
			}

			for range 2 * guardThreshold {
				_, err := f.guarded.Check(ctx, "order-0001", "MX")
				tc.check(t, err)
			}

			want := breaker.Stats{State: breaker.StateClosed}
			if got := f.guarded.Stats(); got != want {
				t.Fatalf("stats = %+v, want %+v", got, want)
			}
		})
	}
}

func TestGuardedClosesAgainOnceTheServiceAnswers(t *testing.T) {
	srv := newSwitchableServer(t, neverAnswer)
	f := newGuardFixture(t, srv.URL)

	for range guardThreshold {
		f.guarded.Check(context.Background(), "order-0001", "MX")
	}
	srv.behaviour.Store(int32(answerAvailable))

	f.clock.Advance(guardCooldown - time.Nanosecond)
	if _, err := f.guarded.Check(context.Background(), "order-0001", "MX"); err == nil {
		t.Fatal("a call one nanosecond before the cooldown ends went through, want it rejected")
	}
	if got := srv.hits.Load(); got != guardThreshold {
		t.Fatalf("the server saw %d calls, want %d", got, guardThreshold)
	}

	f.clock.Advance(time.Nanosecond)
	for i := range guardTrials {
		available, err := f.guarded.Check(context.Background(), "order-0001", "MX")
		if err != nil || !available {
			t.Fatalf("trial %d: available=%t, error = %v, want true and no error", i, available, err)
		}
	}

	wantTransitions := []breaker.State{breaker.StateOpen, breaker.StateHalfOpen, breaker.StateClosed}
	if !slices.Equal(f.transitions, wantTransitions) {
		t.Fatalf("transitions went to %v, want %v", f.transitions, wantTransitions)
	}
	want := breaker.Stats{State: breaker.StateClosed, Opened: 1, OpenFor: guardCooldown, Rejected: 1}
	if got := f.guarded.Stats(); got != want {
		t.Fatalf("stats = %+v, want %+v", got, want)
	}
	if got := srv.hits.Load(); got != guardThreshold+guardTrials {
		t.Fatalf("the server saw %d calls, want %d", got, guardThreshold+guardTrials)
	}
}

func TestNewGuardedRejectsBadConfiguration(t *testing.T) {
	client := newClient(t, "http://localhost:8090", time.Second)
	valid := breaker.Options{FailureThreshold: guardThreshold, Cooldown: guardCooldown, HalfOpenTrials: guardTrials}
	withRule := valid
	withRule.IsFailure = func(error) bool { return true }

	cases := []struct {
		name         string
		client       *inventory.Client
		opts         breaker.Options
		wantProblems int
	}{
		{name: "valid", client: client, opts: valid},
		{name: "no client", client: nil, opts: valid, wantProblems: 1},
		{name: "a failure rule from outside", client: client, opts: withRule, wantProblems: 1},
		{name: "no client and a failure rule from outside", client: nil, opts: withRule, wantProblems: 2},
		{name: "breaker options left empty", client: client, opts: breaker.Options{}, wantProblems: 3},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			guarded, err := inventory.NewGuarded(tc.client, tc.opts)

			if tc.wantProblems == 0 {
				if err != nil {
					t.Fatalf("NewGuarded: %v", err)
				}
				if got := guarded.Stats().State; got != breaker.StateClosed {
					t.Fatalf("a new guard is %s, want closed", got)
				}
				return
			}

			if err == nil {
				t.Fatal("NewGuarded succeeded, want an error")
			}
			joined, ok := err.(interface{ Unwrap() []error })
			if !ok {
				t.Fatalf("NewGuarded error is %T, want one built by errors.Join", err)
			}
			if got := len(joined.Unwrap()); got != tc.wantProblems {
				t.Fatalf("NewGuarded reported %d problems, want %d: %v", got, tc.wantProblems, err)
			}
		})
	}
}
