package main

import (
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/ErnestoCM-13/proyecto-sistema-resiliente-con-backpressure/internal/inventory"
)

func TestPercentileUsesNearestRank(t *testing.T) {
	hundred := make([]time.Duration, 100)
	for i := range hundred {
		hundred[i] = time.Duration(i+1) * time.Millisecond
	}

	cases := []struct {
		name   string
		sorted []time.Duration
		p      float64
		want   time.Duration
	}{
		{name: "p50 of 1..100", sorted: hundred, p: 50, want: 50 * time.Millisecond},
		{name: "p95 of 1..100", sorted: hundred, p: 95, want: 95 * time.Millisecond},
		{name: "p99 of 1..100", sorted: hundred, p: 99, want: 99 * time.Millisecond},
		{name: "p100 of 1..100", sorted: hundred, p: 100, want: 100 * time.Millisecond},
		{name: "p95 of a single value", sorted: hundred[:1], p: 95, want: time.Millisecond},
		{name: "p50 of two values is the lower", sorted: hundred[:2], p: 50, want: time.Millisecond},
		{name: "p95 of ten values is the highest", sorted: hundred[:10], p: 95, want: 10 * time.Millisecond},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := percentile(tc.sorted, tc.p); got != tc.want {
				t.Fatalf("percentile = %s, want %s", got, tc.want)
			}
		})
	}
}

func TestDist(t *testing.T) {
	if got := dist(nil); got != (Dist{}) {
		t.Fatalf("dist of nothing = %+v, want the zero value", got)
	}

	unsorted := []time.Duration{30 * time.Millisecond, 10 * time.Millisecond, 20 * time.Millisecond}
	got := dist(unsorted)
	want := Dist{N: 3, Min: 10, P50: 20, P95: 30, P99: 30, Max: 30}
	if got != want {
		t.Fatalf("dist = %+v, want %+v", got, want)
	}
	if unsorted[0] != 30*time.Millisecond {
		t.Fatal("dist reordered the caller's slice")
	}
}

func TestSpread(t *testing.T) {
	cases := []struct {
		name   string
		values []float64
		want   Spread
	}{
		{name: "no runs", values: nil, want: Spread{}},
		{name: "one run", values: []float64{7}, want: Spread{Runs: 1, Median: 7, Min: 7, Max: 7}},
		{name: "odd count takes the middle", values: []float64{9, 1, 5}, want: Spread{Runs: 3, Median: 5, Min: 1, Max: 9}},
		{name: "even count averages the middle two", values: []float64{4, 1, 3, 2}, want: Spread{Runs: 4, Median: 2.5, Min: 1, Max: 4}},
		{name: "an outlier moves the range, not the median", values: []float64{1, 2, 3, 4, 1000}, want: Spread{Runs: 5, Median: 3, Min: 1, Max: 1000}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := spread(tc.values); got != tc.want {
				t.Fatalf("spread = %+v, want %+v", got, tc.want)
			}
		})
	}
}

func TestMaxInFlight(t *testing.T) {
	at := func(offsetMs, latencyMs int) sample {
		return sample{offset: time.Duration(offsetMs) * time.Millisecond, latency: time.Duration(latencyMs) * time.Millisecond}
	}

	cases := []struct {
		name    string
		samples []sample
		want    int
	}{
		{name: "nothing sent", samples: nil, want: 0},
		{name: "one request", samples: []sample{at(0, 10)}, want: 1},
		{name: "one after another", samples: []sample{at(0, 10), at(20, 10), at(40, 10)}, want: 1},
		{name: "ending exactly when the next starts", samples: []sample{at(0, 10), at(10, 10)}, want: 1},
		{name: "three overlapping", samples: []sample{at(0, 100), at(10, 100), at(20, 100)}, want: 3},
		{name: "a slow one under two fast ones", samples: []sample{at(0, 100), at(10, 10), at(30, 10)}, want: 2},
		{name: "order of the slice does not matter", samples: []sample{at(20, 100), at(0, 100), at(10, 100)}, want: 3},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := maxInFlight(tc.samples); got != tc.want {
				t.Fatalf("maxInFlight = %d, want %d", got, tc.want)
			}
		})
	}
}

func TestMeasureCountsEachOutcomeAndSkipsTheSettlingPeriod(t *testing.T) {
	fail := func(kind inventory.Kind) error { return &inventory.Error{Kind: kind} }
	at := func(offsetMs, latencyMs int, err error) sample {
		return sample{
			offset:  time.Duration(offsetMs) * time.Millisecond,
			latency: time.Duration(latencyMs) * time.Millisecond,
			err:     err,
		}
	}

	samples := []sample{
		at(0, 500, fail(inventory.KindOverloaded)),
		at(999, 500, nil),
		at(1000, 10, nil),
		at(1100, 20, nil),
		at(1200, 30, nil),
		at(1300, 40, nil),
		at(1400, 200, fail(inventory.KindOverloaded)),
		at(1500, 400, fail(inventory.KindOverloaded)),
		at(1600, 5000, fail(inventory.KindTimeout)),
		at(1700, 1, fail(inventory.KindUnreachable)),
		at(1800, 15, fail(inventory.KindProtocol)),
		at(1900, 50, nil),
	}

	got, err := measure(samples, time.Second, 2*time.Second)
	if err != nil {
		t.Fatalf("measure: %v", err)
	}

	want := Step{
		Sent:                12,
		Measured:            10,
		OK:                  5,
		Overloaded:          2,
		Timeout:             1,
		Unreachable:         1,
		Protocol:            1,
		FailurePct:          50,
		OKPerSecond:         2.5,
		MaxInFlight:         3,
		OKLatencyMs:         Dist{N: 5, Min: 10, P50: 30, P95: 50, P99: 50, Max: 50},
		OverloadedLatencyMs: Dist{N: 2, Min: 200, P50: 200, P95: 400, P99: 400, Max: 400},
	}
	if got != want {
		t.Fatalf("measure =\n%+v\nwant\n%+v", got, want)
	}
}

func TestMeasureRefusesAFailureItCannotClassify(t *testing.T) {
	cases := []struct {
		name string
		err  error
	}{
		{name: "not an inventory error", err: errors.New("something else")},
		{name: "an inventory kind added later", err: &inventory.Error{Kind: inventory.Kind(99)}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := measure([]sample{{err: tc.err}}, 0, time.Second)
			if !errors.Is(err, tc.err) {
				t.Fatalf("measure error = %v, want it to wrap %v", err, tc.err)
			}
		})
	}
}

func TestParseRates(t *testing.T) {
	cases := []struct {
		name         string
		in           string
		want         []int
		wantProblems int
	}{
		{name: "one", in: "60", want: []int{60}},
		{name: "several with spaces", in: "12, 30,60", want: []int{12, 30, 60}},
		{name: "the highest allowed", in: "2000", want: []int{2000}},
		{name: "empty", in: "", wantProblems: 1},
		{name: "not a number", in: "fast", wantProblems: 1},
		{name: "a fraction", in: "12.5", wantProblems: 1},
		{name: "zero", in: "0", wantProblems: 1},
		{name: "negative", in: "-5", wantProblems: 1},
		{name: "above the highest allowed", in: "2001", wantProblems: 1},
		{name: "two bad among good", in: "12,x,30,0", wantProblems: 2},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parseRates(tc.in)

			if tc.wantProblems == 0 {
				if err != nil {
					t.Fatalf("parseRates(%q): %v", tc.in, err)
				}
				if !slices.Equal(got, tc.want) {
					t.Fatalf("parseRates(%q) = %v, want %v", tc.in, got, tc.want)
				}
				return
			}

			if err == nil {
				t.Fatalf("parseRates(%q) = %v, want an error", tc.in, got)
			}
			joined, ok := err.(interface{ Unwrap() []error })
			if !ok {
				t.Fatalf("parseRates error is %T, want one built by errors.Join", err)
			}
			if n := len(joined.Unwrap()); n != tc.wantProblems {
				t.Fatalf("parseRates(%q) reported %d problems, want %d: %v", tc.in, n, tc.wantProblems, err)
			}
		})
	}
}
