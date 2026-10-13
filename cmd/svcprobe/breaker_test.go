package main

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ErnestoCM-13/proyecto-sistema-resiliente-con-backpressure/internal/inventory"
)

func TestParseWorkers(t *testing.T) {
	cases := []struct {
		name         string
		in           string
		want         []int
		wantProblems int
	}{
		{name: "one", in: "1", want: []int{1}},
		{name: "several with spaces", in: "1, 2,4", want: []int{1, 2, 4}},
		{name: "the highest allowed", in: "256", want: []int{256}},
		{name: "empty", in: "", wantProblems: 1},
		{name: "not a number", in: "many", wantProblems: 1},
		{name: "zero", in: "0", wantProblems: 1},
		{name: "above the highest allowed", in: "257", wantProblems: 1},
		{name: "two bad among good", in: "1,x,4,0", wantProblems: 2},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parseWorkers(tc.in)

			if tc.wantProblems == 0 {
				if err != nil {
					t.Fatalf("parseWorkers(%q): %v", tc.in, err)
				}
				if !slices.Equal(got, tc.want) {
					t.Fatalf("parseWorkers(%q) = %v, want %v", tc.in, got, tc.want)
				}
				return
			}

			if err == nil {
				t.Fatalf("parseWorkers(%q) = %v, want an error", tc.in, got)
			}
			joined, ok := err.(interface{ Unwrap() []error })
			if !ok {
				t.Fatalf("parseWorkers error is %T, want one built by errors.Join", err)
			}
			if n := len(joined.Unwrap()); n != tc.wantProblems {
				t.Fatalf("parseWorkers(%q) reported %d problems, want %d: %v", tc.in, n, tc.wantProblems, err)
			}
		})
	}
}

type countingServer struct {
	*httptest.Server
	hits atomic.Int64
}

func newCountingServer(t *testing.T, answer func(w http.ResponseWriter, release <-chan struct{})) *countingServer {
	t.Helper()
	s := &countingServer{}
	release := make(chan struct{})
	s.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.hits.Add(1)
		answer(w, release)
	}))
	t.Cleanup(func() {
		close(release)
		s.Server.Close()
	})
	return s
}

func breakerTestConfig(step time.Duration) config {
	return config{
		step:         step,
		checkTimeout: 20 * time.Millisecond,
		failures:     5,
		cooldown:     100 * time.Millisecond,
		trials:       3,
	}
}

func breakerTestClient(t *testing.T, baseURL string, cfg config) *inventory.Client {
	t.Helper()
	client, err := inventory.NewClient(baseURL, cfg.checkTimeout)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	return client
}

func TestBreakerStepAgainstAHealthyServiceNeverOpens(t *testing.T) {
	srv := newCountingServer(t, func(w http.ResponseWriter, _ <-chan struct{}) {
		io.WriteString(w, `{"available": true}`)
	})
	cfg := breakerTestConfig(300 * time.Millisecond)

	step, err := breakerStep(context.Background(), breakerTestClient(t, srv.URL, cfg), cfg, 2)
	if err != nil {
		t.Fatalf("breakerStep: %v", err)
	}

	if step.Calls == 0 {
		t.Fatal("no calls were made")
	}
	if int64(step.Calls) != srv.hits.Load() {
		t.Errorf("%d calls let through, the server saw %d", step.Calls, srv.hits.Load())
	}
	if step.OK != step.Calls {
		t.Errorf("%d of %d calls succeeded, want all", step.OK, step.Calls)
	}
	if step.Rejected != 0 || step.Opened != 0 || len(step.Transitions) != 0 {
		t.Errorf("rejected %d, opened %d, %d transitions, want none of each",
			step.Rejected, step.Opened, len(step.Transitions))
	}
	if step.Workers != 2 || step.OKLatencyMs.N != step.OK {
		t.Errorf("workers = %d with %d latencies for %d successes", step.Workers, step.OKLatencyMs.N, step.OK)
	}
}

func TestBreakerStepAgainstAServiceThatNeverAnswersKeepsItsCallsAway(t *testing.T) {
	srv := newCountingServer(t, func(_ http.ResponseWriter, release <-chan struct{}) {
		<-release
	})
	cfg := breakerTestConfig(450 * time.Millisecond)

	step, err := breakerStep(context.Background(), breakerTestClient(t, srv.URL, cfg), cfg, 2)
	if err != nil {
		t.Fatalf("breakerStep: %v", err)
	}

	if int64(step.Calls) != srv.hits.Load() {
		t.Errorf("%d calls let through, the server saw %d: a rejected call must not reach it", step.Calls, srv.hits.Load())
	}
	if step.OK != 0 || step.Timeout != step.Calls {
		t.Errorf("%d ok and %d timeouts out of %d calls, want every call to time out", step.OK, step.Timeout, step.Calls)
	}
	if step.Opened < 2 {
		t.Errorf("opened %d times, want it to open, test once the cooldown ends, and open again", step.Opened)
	}
	if step.Rejected == 0 {
		t.Error("no call was rejected while the breaker was open")
	}
	if step.OpenSeconds <= 0 || step.OpenSeconds > cfg.step.Seconds() {
		t.Errorf("open for %.3fs in a step of %.3fs", step.OpenSeconds, cfg.step.Seconds())
	}

	if len(step.Transitions) < 3 {
		t.Fatalf("%d transitions, want at least open, half-open and open again", len(step.Transitions))
	}
	var got []string
	for _, tr := range step.Transitions[:3] {
		got = append(got, tr.From+">"+tr.To)
	}
	want := []string{"closed>open", "open>half-open", "half-open>open"}
	if !slices.Equal(got, want) {
		t.Errorf("first transitions = %v, want %v", got, want)
	}
	if gap := step.Transitions[1].AtMs - step.Transitions[0].AtMs; gap < ms(cfg.cooldown) {
		t.Errorf("half-open came %.1f ms after opening, sooner than the %s cooldown", gap, cfg.cooldown)
	}
}

func TestBreakerStepStopsOnAFailureThatIsNotUnavailability(t *testing.T) {
	srv := newCountingServer(t, func(w http.ResponseWriter, _ <-chan struct{}) {
		w.WriteHeader(http.StatusNotFound)
	})
	cfg := breakerTestConfig(10 * time.Second)

	started := time.Now()
	_, err := breakerStep(context.Background(), breakerTestClient(t, srv.URL, cfg), cfg, 4)

	var invErr *inventory.Error
	if !errors.As(err, &invErr) || invErr.Kind != inventory.KindProtocol {
		t.Fatalf("breakerStep error = %v, want an inventory error of kind %s", err, inventory.KindProtocol)
	}
	if elapsed := time.Since(started); elapsed > 5*time.Second {
		t.Fatalf("breakerStep took %s to stop, want it to stop at the first such failure", elapsed)
	}
}
