package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/ErnestoCM-13/proyecto-sistema-resiliente-con-backpressure/internal/clock"
	"github.com/ErnestoCM-13/proyecto-sistema-resiliente-con-backpressure/internal/pace"
	"github.com/ErnestoCM-13/proyecto-sistema-resiliente-con-backpressure/internal/stream"
)

var errFileEndedEarly = errors.New("the file ended before the schedule did")

type PaceReport struct {
	File            string          `json:"file"`
	Stream          string          `json:"stream"`
	Schedule        string          `json:"schedule"`
	PlannedEvents   int             `json:"planned_events"`
	Published       int             `json:"published_events"`
	AlreadyInStream int64           `json:"refused_as_already_in_the_stream"`
	DurationSeconds float64         `json:"duration_seconds"`
	Segments        []SegmentReport `json:"segments"`
}

type SegmentReport struct {
	Segment        int     `json:"segment"`
	TargetRate     int     `json:"target_rate_per_second"`
	PlannedSeconds float64 `json:"planned_seconds"`
	PlannedEvents  int     `json:"planned_events"`
	Published      int     `json:"published_events"`
	FirstAtSeconds float64 `json:"first_event_at_seconds"`
	LastAtSeconds  float64 `json:"last_event_at_seconds"`
	AchievedRate   float64 `json:"achieved_rate_per_second"`
	MaxLatenessMs  float64 `json:"max_lateness_ms"`
}

type segmentStats struct {
	published   int
	first, last time.Duration
	maxLate     time.Duration
}

func (s *segmentStats) observe(at, late time.Duration) {
	if s.published == 0 {
		s.first = at
	}
	s.last = at
	s.maxLate = max(s.maxLate, late)
	s.published++
}

func (s *segmentStats) report(index int, seg pace.Segment, planned int) SegmentReport {
	out := SegmentReport{
		Segment:        index + 1,
		TargetRate:     seg.Rate,
		PlannedSeconds: seg.For.Seconds(),
		PlannedEvents:  planned,
		Published:      s.published,
		FirstAtSeconds: s.first.Seconds(),
		LastAtSeconds:  s.last.Seconds(),
		MaxLatenessMs:  float64(s.maxLate) / float64(time.Millisecond),
	}
	if span := s.last - s.first; s.published > 1 && span > 0 {
		out.AchievedRate = float64(s.published-1) / span.Seconds()
	}
	return out
}

func publishOnSchedule(ctx context.Context, f *os.File, publisher *stream.Publisher, cfg config) error {
	segments := cfg.schedule.Segments()
	plan := cfg.schedule.Plan()
	stats := make([]segmentStats, len(segments))
	planned := make([]int, len(segments))
	for _, due := range plan {
		planned[due.Segment]++
	}

	fmt.Printf("publishing %s to stream %s on a schedule: %s (%d events in %s)\n",
		cfg.file, cfg.streamKey, cfg.schedule, len(plan), cfg.schedule.Duration())

	var stopped error
	published := 0
	current := -1
	sc := bufio.NewScanner(f)
	start := time.Now()
	for published < len(plan) {
		due := plan[published]
		if due.Segment != current {
			current = due.Segment
			fmt.Printf("  +%6.1fs  segment %d: %d ev/s for %s\n",
				time.Since(start).Seconds(), current+1, segments[current].Rate, segments[current].For)
		}

		dueAt := start.Add(due.Offset)
		if err := pace.Wait(ctx, dueAt); err != nil {
			stopped = err
			break
		}
		if !sc.Scan() {
			break
		}

		launched := time.Now()
		pubNS, err := clock.NowNS()
		if err != nil {
			return err
		}
		line := int64(published + 1)
		if err := publisher.Publish(ctx, stream.EntryID(line), sc.Bytes(), pubNS); err != nil {
			if ctx.Err() != nil {
				stopped = ctx.Err()
				break
			}
			return fmt.Errorf("publishing line %d: %w", line, err)
		}
		stats[current].observe(launched.Sub(start), launched.Sub(dueAt))
		published++
	}
	elapsed := time.Since(start)
	if err := sc.Err(); err != nil {
		return fmt.Errorf("reading %s at line %d: %w", cfg.file, published+1, err)
	}

	rep := PaceReport{
		File:            cfg.file,
		Stream:          cfg.streamKey,
		Schedule:        cfg.schedule.String(),
		PlannedEvents:   len(plan),
		Published:       published,
		AlreadyInStream: publisher.Rejected(),
		DurationSeconds: elapsed.Seconds(),
	}
	for i, seg := range segments {
		rep.Segments = append(rep.Segments, stats[i].report(i, seg, planned[i]))
	}
	printPaceReport(rep)

	if cfg.out != "" {
		b, err := json.MarshalIndent(rep, "", "  ")
		if err != nil {
			return fmt.Errorf("encoding report: %w", err)
		}
		if err := os.WriteFile(cfg.out, append(b, '\n'), 0o644); err != nil {
			return fmt.Errorf("writing %s: %w", cfg.out, err)
		}
		fmt.Printf("\nreport %s\n", cfg.out)
	}

	switch {
	case stopped != nil:
		return fmt.Errorf("stopped after %d of %d events: %w", published, len(plan), stopped)
	case published < len(plan):
		return fmt.Errorf("%s has %d events and the schedule asks for %d: %w", cfg.file, published, len(plan), errFileEndedEarly)
	}
	return nil
}

func printPaceReport(rep PaceReport) {
	fmt.Printf("\npublished %d of %d events in %.1fs\n", rep.Published, rep.PlannedEvents, rep.DurationSeconds)
	if rep.AlreadyInStream > 0 {
		fmt.Printf("%d publishes were refused as already in the stream, which is a client retry made visible\n",
			rep.AlreadyInStream)
	}
	fmt.Printf("\n  %-7s %10s %10s %10s %12s %12s %14s %16s\n",
		"segment", "target/s", "planned", "published", "first at s", "last at s", "achieved/s", "max lateness ms")
	for _, s := range rep.Segments {
		fmt.Printf("  %-7d %10d %10d %10d %12.3f %12.3f %14.2f %16.2f\n",
			s.Segment, s.TargetRate, s.PlannedEvents, s.Published,
			s.FirstAtSeconds, s.LastAtSeconds, s.AchievedRate, s.MaxLatenessMs)
	}
}
