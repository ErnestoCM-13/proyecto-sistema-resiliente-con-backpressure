package main

import (
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/ErnestoCM-13/proyecto-sistema-resiliente-con-backpressure/internal/inventory"
)

func TestDueOffsets(t *testing.T) {
	msOffsets := func(values ...int) []time.Duration {
		out := make([]time.Duration, len(values))
		for i, v := range values {
			out[i] = time.Duration(v) * time.Millisecond
		}
		return out
	}

	cases := []struct {
		name     string
		segments []segment
		want     []time.Duration
	}{
		{name: "no segments", segments: nil, want: nil},
		{
			name:     "one segment starts at zero and stops before its end",
			segments: []segment{{rate: 4, d: time.Second}},
			want:     msOffsets(0, 250, 500, 750),
		},
		{
			name:     "the second segment starts where the first one ends",
			segments: []segment{{rate: 4, d: time.Second}, {rate: 2, d: time.Second}},
			want:     msOffsets(0, 250, 500, 750, 1000, 1500),
		},
		{
			name:     "a segment too short for its rate sends nothing but still takes its time",
			segments: []segment{{rate: 1, d: 500 * time.Millisecond}, {rate: 2, d: time.Second}},
			want:     msOffsets(500, 1000),
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := dueOffsets(tc.segments); !slices.Equal(got, tc.want) {
				t.Fatalf("dueOffsets = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestDueOffsetsSendsWhatTheLadderSentBeforeSegmentsExisted(t *testing.T) {
	cases := []struct {
		rate int
		want int
	}{
		{rate: 12, want: 204},
		{rate: 30, want: 510},
		{rate: 60, want: 1020},
		{rate: 90, want: 1530},
		{rate: 116, want: 1972},
		{rate: 180, want: 3060},
		{rate: 250, want: 4250},
		{rate: 400, want: 6800},
	}

	for _, tc := range cases {
		got := dueOffsets([]segment{{rate: tc.rate, d: 17 * time.Second}})
		if len(got) != tc.want {
			t.Errorf("%d/s for 17s schedules %d requests, want %d", tc.rate, len(got), tc.want)
		}
	}
}

func TestAnalyzeRecovery(t *testing.T) {
	const (
		loadEnd  = 5 * time.Second
		interval = 50 * time.Millisecond
		healthy  = 100 * time.Millisecond
	)
	fast := time.Duration(20) * time.Millisecond
	fail := func(kind inventory.Kind) error { return &inventory.Error{Kind: kind} }
	probes := func(results ...sample) []sample {
		for i := range results {
			results[i].offset = loadEnd + time.Duration(i)*interval
		}
		return results
	}
	ok := sample{latency: fast}
	overloaded := sample{latency: fast, err: fail(inventory.KindOverloaded)}

	cases := []struct {
		name          string
		probes        []sample
		wantUnhealthy int
		wantRecovered bool
		wantFromMs    float64
		wantOutcomes  []string
	}{
		{
			name:          "healthy from the first probe",
			probes:        probes(ok, ok, ok),
			wantRecovered: true,
			wantFromMs:    0,
			wantOutcomes:  []string{"ok", "ok", "ok"},
		},
		{
			name:          "three unhealthy probes, then healthy",
			probes:        probes(overloaded, overloaded, overloaded, ok, ok),
			wantUnhealthy: 3,
			wantRecovered: true,
			wantFromMs:    150,
			wantOutcomes:  []string{"503", "503", "503", "ok", "ok"},
		},
		{
			name:          "a late unhealthy probe moves the recovery after it",
			probes:        probes(overloaded, ok, ok, overloaded, ok),
			wantUnhealthy: 2,
			wantRecovered: true,
			wantFromMs:    200,
			wantOutcomes:  []string{"503", "ok", "ok", "503", "ok"},
		},
		{
			name:          "unhealthy at the last probe is not a recovery",
			probes:        probes(ok, ok, overloaded),
			wantUnhealthy: 1,
			wantRecovered: false,
			wantOutcomes:  []string{"ok", "ok", "503"},
		},
		{
			name:          "an answer exactly at the limit is healthy",
			probes:        probes(sample{latency: healthy}, ok),
			wantRecovered: true,
			wantFromMs:    0,
			wantOutcomes:  []string{"ok", "ok"},
		},
		{
			name:          "an answer just past the limit is slow, and slow is unhealthy",
			probes:        probes(sample{latency: healthy + time.Nanosecond}, ok),
			wantUnhealthy: 1,
			wantRecovered: true,
			wantFromMs:    50,
			wantOutcomes:  []string{"slow", "ok"},
		},
		{
			name: "every kind of failure is unhealthy and keeps its name",
			probes: probes(
				sample{err: fail(inventory.KindOverloaded)},
				sample{err: fail(inventory.KindTimeout)},
				sample{err: fail(inventory.KindUnreachable)},
				sample{err: fail(inventory.KindProtocol)},
				ok,
			),
			wantUnhealthy: 4,
			wantRecovered: true,
			wantFromMs:    200,
			wantOutcomes:  []string{"503", "timeout", "unreachable", "protocol", "ok"},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := analyzeRecovery(tc.probes, loadEnd, interval, healthy)
			if err != nil {
				t.Fatalf("analyzeRecovery: %v", err)
			}
			if got.Unhealthy != tc.wantUnhealthy {
				t.Errorf("unhealthy = %d, want %d", got.Unhealthy, tc.wantUnhealthy)
			}
			if got.Recovered != tc.wantRecovered {
				t.Errorf("recovered = %t, want %t", got.Recovered, tc.wantRecovered)
			}
			if got.HealthyFromMs != tc.wantFromMs {
				t.Errorf("healthy from = %.0f ms, want %.0f ms", got.HealthyFromMs, tc.wantFromMs)
			}

			outcomes := make([]string, len(got.Probes))
			for i, p := range got.Probes {
				outcomes[i] = p.Outcome
				if want := float64(i) * 50; p.AtMs != want {
					t.Errorf("probe %d is at %.0f ms, want %.0f ms", i, p.AtMs, want)
				}
			}
			if !slices.Equal(outcomes, tc.wantOutcomes) {
				t.Errorf("outcomes = %v, want %v", outcomes, tc.wantOutcomes)
			}
		})
	}
}

func TestAnalyzeRecoveryRefusesAFailureItCannotClassify(t *testing.T) {
	cases := []struct {
		name string
		err  error
	}{
		{name: "not an inventory error", err: errors.New("something else")},
		{name: "an inventory kind added later", err: &inventory.Error{Kind: inventory.Kind(99)}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := analyzeRecovery([]sample{{err: tc.err}}, 0, time.Second, time.Second)
			if !errors.Is(err, tc.err) {
				t.Fatalf("analyzeRecovery error = %v, want it to wrap %v", err, tc.err)
			}
		})
	}
}

func TestParseMode(t *testing.T) {
	cases := []struct {
		in      string
		want    mode
		wantErr bool
	}{
		{in: "ladder", want: modeLadder},
		{in: "recovery", want: modeRecovery},
		{in: "", wantErr: true},
		{in: "Ladder", wantErr: true},
		{in: "sweep", wantErr: true},
	}

	for _, tc := range cases {
		t.Run(tc.in, func(t *testing.T) {
			got, err := parseMode(tc.in)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("parseMode(%q) = %q, want an error", tc.in, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("parseMode(%q): %v", tc.in, err)
			}
			if got != tc.want {
				t.Fatalf("parseMode(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}
