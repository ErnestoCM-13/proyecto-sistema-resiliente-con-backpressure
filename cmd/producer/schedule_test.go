package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/ErnestoCM-13/proyecto-sistema-resiliente-con-backpressure/internal/pace"
	"github.com/ErnestoCM-13/proyecto-sistema-resiliente-con-backpressure/internal/stream"
	"github.com/redis/go-redis/v9"
)

func TestSegmentStats(t *testing.T) {
	ms := time.Millisecond
	seg := pace.Segment{Rate: 10, For: time.Second}

	cases := []struct {
		name string
		at   []time.Duration
		late []time.Duration
		want SegmentReport
	}{
		{
			name: "nothing published",
			want: SegmentReport{Segment: 2, TargetRate: 10, PlannedSeconds: 1, PlannedEvents: 10},
		},
		{
			name: "one event has no rate to speak of",
			at:   []time.Duration{2000 * ms},
			late: []time.Duration{3 * ms},
			want: SegmentReport{
				Segment: 2, TargetRate: 10, PlannedSeconds: 1, PlannedEvents: 10,
				Published: 1, FirstAtSeconds: 2, LastAtSeconds: 2, MaxLatenessMs: 3,
			},
		},
		{
			name: "events on time give the target rate",
			at:   []time.Duration{2000 * ms, 2100 * ms, 2200 * ms, 2300 * ms, 2400 * ms},
			late: []time.Duration{0, 0, 0, 0, 0},
			want: SegmentReport{
				Segment: 2, TargetRate: 10, PlannedSeconds: 1, PlannedEvents: 10,
				Published: 5, FirstAtSeconds: 2, LastAtSeconds: 2.4, AchievedRate: 10,
			},
		},
		{
			name: "events twice as far apart give half the rate, and the worst lateness is kept",
			at:   []time.Duration{2000 * ms, 2200 * ms, 2400 * ms},
			late: []time.Duration{0, 100 * ms, 50 * ms},
			want: SegmentReport{
				Segment: 2, TargetRate: 10, PlannedSeconds: 1, PlannedEvents: 10,
				Published: 3, FirstAtSeconds: 2, LastAtSeconds: 2.4, AchievedRate: 5, MaxLatenessMs: 100,
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var stats segmentStats
			for i := range tc.at {
				stats.observe(tc.at[i], tc.late[i])
			}
			if got := stats.report(1, seg, 10); got != tc.want {
				t.Fatalf("report =\n%+v\nwant\n%+v", got, tc.want)
			}
		})
	}
}

type scheduleFixture struct {
	client    *redis.Client
	publisher *stream.Publisher
	cfg       config
}

func newScheduleFixture(t *testing.T, lines int, schedule string) *scheduleFixture {
	t.Helper()

	if testing.Short() {
		t.Skip("skipping the Redis integration test in short mode")
	}
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		addr = "localhost:6379"
	}
	ctx := context.Background()
	client, err := stream.Connect(ctx, addr)
	if err != nil {
		t.Fatalf("this test needs Redis: %v", err)
	}
	streamKey := fmt.Sprintf("test:%s:%d", t.Name(), time.Now().UnixNano())
	t.Cleanup(func() {
		if err := client.Del(ctx, streamKey).Err(); err != nil {
			t.Errorf("removing %s: %v", streamKey, err)
		}
		client.Close()
	})

	dir := t.TempDir()
	var content strings.Builder
	for i := 1; i <= lines; i++ {
		fmt.Fprintf(&content, "{\"line\": %d}\n", i)
	}
	file := filepath.Join(dir, "events.jsonl")
	if err := os.WriteFile(file, []byte(content.String()), 0o644); err != nil {
		t.Fatalf("writing %s: %v", file, err)
	}

	parsed, err := pace.ParseSchedule(schedule)
	if err != nil {
		t.Fatalf("ParseSchedule(%q): %v", schedule, err)
	}
	return &scheduleFixture{
		client:    client,
		publisher: stream.NewPublisher(client, streamKey, 1),
		cfg: config{
			file:      file,
			streamKey: streamKey,
			schedule:  &parsed,
			out:       filepath.Join(dir, "report.json"),
		},
	}
}

func (f *scheduleFixture) publish(t *testing.T, ctx context.Context) (PaceReport, error) {
	t.Helper()
	file, err := os.Open(f.cfg.file)
	if err != nil {
		t.Fatalf("opening %s: %v", f.cfg.file, err)
	}
	defer file.Close()

	runErr := publishOnSchedule(ctx, file, f.publisher, f.cfg)

	var rep PaceReport
	b, err := os.ReadFile(f.cfg.out)
	if err != nil {
		t.Fatalf("the report was not written: %v", err)
	}
	if err := json.Unmarshal(b, &rep); err != nil {
		t.Fatalf("the report is not JSON: %v", err)
	}
	return rep, runErr
}

func (f *scheduleFixture) entries(t *testing.T) []redis.XMessage {
	t.Helper()
	entries, err := f.client.XRange(context.Background(), f.cfg.streamKey, "-", "+").Result()
	if err != nil {
		t.Fatalf("xrange: %v", err)
	}
	return entries
}

func TestPublishOnScheduleFollowsThePlan(t *testing.T) {
	f := newScheduleFixture(t, 500, "100:200ms,1000:200ms,100:200ms")

	rep, err := f.publish(t, context.Background())
	if err != nil {
		t.Fatalf("publishOnSchedule: %v", err)
	}

	entries := f.entries(t)
	if len(entries) != 240 {
		t.Fatalf("the stream holds %d entries, want 240", len(entries))
	}
	for i, entry := range entries {
		wantID := stream.EntryID(int64(i + 1))
		wantPayload := fmt.Sprintf("{\"line\": %d}", i+1)
		if entry.ID != wantID || entry.Values[stream.FieldPayload] != wantPayload {
			t.Fatalf("entry %d is %s with payload %v, want %s with %s",
				i, entry.ID, entry.Values[stream.FieldPayload], wantID, wantPayload)
		}
	}

	if rep.PlannedEvents != 240 || rep.Published != 240 || rep.AlreadyInStream != 0 {
		t.Fatalf("report: planned %d, published %d, refused %d, want 240, 240 and 0",
			rep.PlannedEvents, rep.Published, rep.AlreadyInStream)
	}
	wantPublished := []int{20, 200, 20}
	wantNotBefore := []float64{0, 0.2, 0.4}
	for i, seg := range rep.Segments {
		if seg.Published != wantPublished[i] || seg.PlannedEvents != wantPublished[i] {
			t.Errorf("segment %d: planned %d, published %d, want %d of each",
				i+1, seg.PlannedEvents, seg.Published, wantPublished[i])
		}
		if seg.FirstAtSeconds < wantNotBefore[i] {
			t.Errorf("segment %d started at %.4fs, ahead of its schedule at %.1fs", i+1, seg.FirstAtSeconds, wantNotBefore[i])
		}
	}
	if rep.DurationSeconds < 0.59 {
		t.Errorf("the run took %.4fs, but the last event was not due until 0.59s", rep.DurationSeconds)
	}
}

func TestPublishOnScheduleReportsAFileThatEndsEarly(t *testing.T) {
	f := newScheduleFixture(t, 10, "100:200ms")

	rep, err := f.publish(t, context.Background())
	if !errors.Is(err, errFileEndedEarly) {
		t.Fatalf("publishOnSchedule error = %v, want it to wrap errFileEndedEarly", err)
	}
	if got := len(f.entries(t)); got != 10 {
		t.Fatalf("the stream holds %d entries, want the 10 the file has", got)
	}
	if rep.PlannedEvents != 20 || rep.Published != 10 {
		t.Fatalf("report: planned %d, published %d, want 20 and 10", rep.PlannedEvents, rep.Published)
	}
}

func TestPublishOnScheduleStopsWhenCancelled(t *testing.T) {
	f := newScheduleFixture(t, 500, "10:30s")

	ctx, cancel := context.WithCancel(context.Background())
	time.AfterFunc(250*time.Millisecond, cancel)

	started := time.Now()
	rep, err := f.publish(t, ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("publishOnSchedule error = %v, want it to wrap context.Canceled", err)
	}
	if elapsed := time.Since(started); elapsed > 5*time.Second {
		t.Fatalf("publishOnSchedule took %s to stop", elapsed)
	}
	if got := len(f.entries(t)); got != rep.Published {
		t.Fatalf("the stream holds %d entries, the report says %d were published", got, rep.Published)
	}
	if rep.Published != 3 {
		t.Fatalf("published %d events in 250ms at 10/s, want the 3 due at 0, 100 and 200ms", rep.Published)
	}
}
