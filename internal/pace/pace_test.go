package pace_test

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/ErnestoCM-13/proyecto-sistema-resiliente-con-backpressure/internal/pace"
)

func mustSchedule(t *testing.T, segments ...pace.Segment) pace.Schedule {
	t.Helper()
	s, err := pace.NewSchedule(segments)
	if err != nil {
		t.Fatalf("NewSchedule(%v): %v", segments, err)
	}
	return s
}

func TestPlan(t *testing.T) {
	ms := time.Millisecond
	cases := []struct {
		name     string
		segments []pace.Segment
		want     []pace.Due
	}{
		{
			name:     "one segment starts at zero and stops before its end",
			segments: []pace.Segment{{Rate: 4, For: time.Second}},
			want:     []pace.Due{{0, 0}, {250 * ms, 0}, {500 * ms, 0}, {750 * ms, 0}},
		},
		{
			name:     "the next segment starts where the one before ends",
			segments: []pace.Segment{{Rate: 4, For: time.Second}, {Rate: 2, For: time.Second}, {Rate: 1, For: 2 * time.Second}},
			want: []pace.Due{
				{0, 0}, {250 * ms, 0}, {500 * ms, 0}, {750 * ms, 0},
				{1000 * ms, 1}, {1500 * ms, 1},
				{2000 * ms, 2}, {3000 * ms, 2},
			},
		},
		{
			name:     "a rate that does not divide the second does not drift",
			segments: []pace.Segment{{Rate: 3, For: 2 * time.Second}},
			want: []pace.Due{
				{0, 0}, {333333333, 0}, {666666666, 0},
				{1000 * ms, 0}, {1333333333, 0}, {1666666666, 0},
			},
		},
		{
			name:     "a duration that ends between two events keeps only the events inside it",
			segments: []pace.Segment{{Rate: 10, For: 250 * ms}, {Rate: 10, For: 100 * ms}},
			want:     []pace.Due{{0, 0}, {100 * ms, 0}, {250 * ms, 1}},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := mustSchedule(t, tc.segments...)
			got := s.Plan()
			if !slices.Equal(got, tc.want) {
				t.Fatalf("Plan = %v, want %v", got, tc.want)
			}
			if s.Events() != len(tc.want) {
				t.Fatalf("Events = %d, want %d", s.Events(), len(tc.want))
			}
		})
	}
}

func TestTheHotSaleScheduleHasTheEventsItsRatesPromise(t *testing.T) {
	s := mustSchedule(t,
		pace.Segment{Rate: 43, For: time.Minute},
		pace.Segment{Rate: 430, For: 2 * time.Minute},
		pace.Segment{Rate: 43, For: 5 * time.Minute},
	)

	if got, want := s.Events(), 43*60+430*120+43*300; got != want {
		t.Fatalf("Events = %d, want %d", got, want)
	}
	if got, want := s.Duration(), 8*time.Minute; got != want {
		t.Fatalf("Duration = %s, want %s", got, want)
	}

	plan := s.Plan()
	perSegment := make([]int, 3)
	for i, due := range plan {
		perSegment[due.Segment]++
		if i > 0 && due.Offset <= plan[i-1].Offset {
			t.Fatalf("event %d is due at %s, not after event %d at %s", i, due.Offset, i-1, plan[i-1].Offset)
		}
	}
	if want := []int{43 * 60, 430 * 120, 43 * 300}; !slices.Equal(perSegment, want) {
		t.Fatalf("events per segment = %v, want %v", perSegment, want)
	}
	if first := plan[43*60].Offset; first != time.Minute {
		t.Fatalf("the spike starts at %s, want exactly 1m0s", first)
	}
	if last := plan[len(plan)-1].Offset; last >= 8*time.Minute {
		t.Fatalf("the last event is due at %s, want it before the schedule ends at 8m0s", last)
	}
}

func TestParseSchedule(t *testing.T) {
	cases := []struct {
		name         string
		in           string
		want         []pace.Segment
		wantProblems int
	}{
		{name: "one segment", in: "43:60s", want: []pace.Segment{{Rate: 43, For: time.Minute}}},
		{
			name: "three segments with spaces",
			in:   "43:1m, 430:2m ,43:5m",
			want: []pace.Segment{{Rate: 43, For: time.Minute}, {Rate: 430, For: 2 * time.Minute}, {Rate: 43, For: 5 * time.Minute}},
		},
		{name: "the highest rate and the longest segment", in: "100000:24h", want: []pace.Segment{{Rate: pace.MaxRate, For: pace.MaxSegment}}},
		{name: "empty", in: "", wantProblems: 1},
		{name: "no separator", in: "43", wantProblems: 1},
		{name: "rate is not a number", in: "fast:60s", wantProblems: 1},
		{name: "rate is a fraction", in: "4.5:60s", wantProblems: 1},
		{name: "duration without a unit", in: "43:60", wantProblems: 1},
		{name: "zero rate", in: "0:60s", wantProblems: 1},
		{name: "negative rate", in: "-5:60s", wantProblems: 1},
		{name: "rate above the highest", in: "100001:60s", wantProblems: 1},
		{name: "zero duration", in: "43:0s", wantProblems: 1},
		{name: "negative duration", in: "43:-60s", wantProblems: 1},
		{name: "segment above the longest", in: "43:24h1s", wantProblems: 1},
		{name: "too short for one event", in: "2:499ms", wantProblems: 1},
		{name: "just long enough for one event", in: "2:500ms", want: []pace.Segment{{Rate: 2, For: 500 * time.Millisecond}}},
		{name: "two unreadable segments are both reported", in: "43,fast:60s,43:60s", wantProblems: 2},
		{name: "two invalid segments are both reported", in: "0:60s,43:60s,43:0s", wantProblems: 2},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := pace.ParseSchedule(tc.in)

			if tc.wantProblems == 0 {
				if err != nil {
					t.Fatalf("ParseSchedule(%q): %v", tc.in, err)
				}
				if !slices.Equal(got.Segments(), tc.want) {
					t.Fatalf("ParseSchedule(%q) = %v, want %v", tc.in, got.Segments(), tc.want)
				}
				return
			}

			if err == nil {
				t.Fatalf("ParseSchedule(%q) = %v, want an error", tc.in, got.Segments())
			}
			problems := 1
			if joined, ok := err.(interface{ Unwrap() []error }); ok {
				problems = len(joined.Unwrap())
			}
			if problems != tc.wantProblems {
				t.Fatalf("ParseSchedule(%q) reported %d problems, want %d: %v", tc.in, problems, tc.wantProblems, err)
			}
		})
	}
}

func TestNewScheduleRejectsNoSegments(t *testing.T) {
	if _, err := pace.NewSchedule(nil); err == nil {
		t.Fatal("NewSchedule(nil) succeeded, want an error")
	}
}

func TestScheduleDoesNotShareItsSegmentsWithTheCaller(t *testing.T) {
	segments := []pace.Segment{{Rate: 4, For: time.Second}}
	s := mustSchedule(t, segments...)

	segments[0].Rate = 400
	s.Segments()[0].Rate = 400

	if got := s.Events(); got != 4 {
		t.Fatalf("Events = %d after the caller changed its own slices, want 4", got)
	}
}

func TestScheduleString(t *testing.T) {
	s := mustSchedule(t, pace.Segment{Rate: 43, For: time.Minute}, pace.Segment{Rate: 430, For: 2 * time.Minute})
	if got, want := s.String(), "43/s for 1m0s, then 430/s for 2m0s"; got != want {
		t.Fatalf("String = %q, want %q", got, want)
	}
}

func TestWait(t *testing.T) {
	t.Run("returns at once for an instant already past", func(t *testing.T) {
		started := time.Now()
		if err := pace.Wait(context.Background(), started.Add(-time.Hour)); err != nil {
			t.Fatalf("Wait: %v", err)
		}
		if elapsed := time.Since(started); elapsed > time.Second {
			t.Fatalf("Wait took %s for an instant in the past", elapsed)
		}
	})

	t.Run("does not return before the instant", func(t *testing.T) {
		until := time.Now().Add(50 * time.Millisecond)
		if err := pace.Wait(context.Background(), until); err != nil {
			t.Fatalf("Wait: %v", err)
		}
		if early := time.Until(until); early > 0 {
			t.Fatalf("Wait returned %s early", early)
		}
	})

	t.Run("gives up when the context is cancelled", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		time.AfterFunc(20*time.Millisecond, cancel)

		started := time.Now()
		err := pace.Wait(ctx, started.Add(time.Hour))
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Wait error = %v, want context.Canceled", err)
		}
		if elapsed := time.Since(started); elapsed > 5*time.Second {
			t.Fatalf("Wait took %s to notice the cancellation", elapsed)
		}
	})

	t.Run("reports a context already cancelled even for an instant already past", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if err := pace.Wait(ctx, time.Now().Add(-time.Hour)); !errors.Is(err, context.Canceled) {
			t.Fatalf("Wait error = %v, want context.Canceled", err)
		}
	})
}
