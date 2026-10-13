// Turns a schedule of rates into the instant at which each event is due.
package pace

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

const (
	MaxRate    = 100_000
	MaxSegment = 24 * time.Hour
)

type Segment struct {
	Rate int
	For  time.Duration
}

func (s Segment) events() int {
	return int(int64(s.For) * int64(s.Rate) / int64(time.Second))
}

type Due struct {
	Offset  time.Duration
	Segment int
}

type Schedule struct {
	segments []Segment
}

func NewSchedule(segments []Segment) (Schedule, error) {
	if len(segments) == 0 {
		return Schedule{}, errors.New("schedule: no segments")
	}

	var problems []error
	for i, seg := range segments {
		switch {
		case seg.Rate < 1 || seg.Rate > MaxRate:
			problems = append(problems, fmt.Errorf("segment %d: rate %d is outside 1..%d", i+1, seg.Rate, MaxRate))
		case seg.For <= 0 || seg.For > MaxSegment:
			problems = append(problems, fmt.Errorf("segment %d: %s is outside 0..%s", i+1, seg.For, MaxSegment))
		case seg.events() == 0:
			problems = append(problems, fmt.Errorf("segment %d: %s is too short for a single event at %d/s", i+1, seg.For, seg.Rate))
		}
	}
	if len(problems) > 0 {
		return Schedule{}, errors.Join(problems...)
	}
	return Schedule{segments: append([]Segment(nil), segments...)}, nil
}

func ParseSchedule(s string) (Schedule, error) {
	var segments []Segment
	var problems []error
	for i, part := range strings.Split(s, ",") {
		rateText, forText, found := strings.Cut(strings.TrimSpace(part), ":")
		if !found {
			problems = append(problems, fmt.Errorf("segment %d: %q is not rate:duration", i+1, part))
			continue
		}
		rate, err := strconv.Atoi(rateText)
		if err != nil {
			problems = append(problems, fmt.Errorf("segment %d: rate %q is not a whole number", i+1, rateText))
			continue
		}
		d, err := time.ParseDuration(forText)
		if err != nil {
			problems = append(problems, fmt.Errorf("segment %d: duration %q: %w", i+1, forText, err))
			continue
		}
		segments = append(segments, Segment{Rate: rate, For: d})
	}
	if len(problems) > 0 {
		return Schedule{}, errors.Join(problems...)
	}
	return NewSchedule(segments)
}

func (s Schedule) Segments() []Segment {
	return append([]Segment(nil), s.segments...)
}

func (s Schedule) Events() int {
	total := 0
	for _, seg := range s.segments {
		total += seg.events()
	}
	return total
}

func (s Schedule) Duration() time.Duration {
	var total time.Duration
	for _, seg := range s.segments {
		total += seg.For
	}
	return total
}

func (s Schedule) Plan() []Due {
	plan := make([]Due, 0, s.Events())
	var base time.Duration
	for i, seg := range s.segments {
		for k := range seg.events() {
			offset := time.Duration(int64(k) * int64(time.Second) / int64(seg.Rate))
			plan = append(plan, Due{Offset: base + offset, Segment: i})
		}
		base += seg.For
	}
	return plan
}

func (s Schedule) String() string {
	parts := make([]string, len(s.segments))
	for i, seg := range s.segments {
		parts[i] = fmt.Sprintf("%d/s for %s", seg.Rate, seg.For)
	}
	return strings.Join(parts, ", then ")
}

func Wait(ctx context.Context, until time.Time) error {
	wait := time.Until(until)
	if wait <= 0 {
		return ctx.Err()
	}
	timer := time.NewTimer(wait)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
