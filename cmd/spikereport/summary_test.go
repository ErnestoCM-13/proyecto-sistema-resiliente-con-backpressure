package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

const (
	testLines        = 67080
	testUnique       = 65131
	testReservations = 17549
)

func goodRun(scenario string, run int) runFiles {
	var r runFiles
	r.Checks = checks{
		Scenario: scenario, Run: run, Machine: "test machine", Schedule: "43:1m,430:2m,43:5m",
		LinesSent: testLines, ExpectedUnique: testUnique, ExpectedReservations: testReservations,
		StreamLength: testLines,
	}
	r.Checks.Service.TotalRequests = testReservations

	r.Producer.PlannedEvents, r.Producer.Published = testLines, testLines
	for _, seg := range []struct {
		rate    int
		seconds float64
	}{{43, 60}, {430, 120}, {43, 300}} {
		r.Producer.Segments = append(r.Producer.Segments, struct {
			TargetRate     int     `json:"target_rate_per_second"`
			PlannedSeconds float64 `json:"planned_seconds"`
			AchievedRate   float64 `json:"achieved_rate_per_second"`
			MaxLatenessMs  float64 `json:"max_lateness_ms"`
		}{TargetRate: seg.rate, PlannedSeconds: seg.seconds, AchievedRate: float64(seg.rate), MaxLatenessMs: 2})
	}

	c := &r.Consumer
	c.Consumed, c.Duplicates, c.DurationSeconds = testLines, testLines-testUnique, 480
	c.LatencyMs = latency{P50: 60000, P95: 200000, Max: 230000}
	c.Count.Aggregated = testUnique
	c.Inventory.Verified, c.Inventory.Calls = testReservations, testReservations
	c.Backpressure.MaxDepth, c.Backpressure.TimesEngaged, c.Backpressure.EngagedSeconds, c.Backpressure.Capacity = 226, 100, 350, 300

	if scenario == scenarioOutsideLoad {
		r.Load = &loadReport{Sent: 3000, Rate: 150, Seconds: 20, FailurePct: 27}
		r.Checks.Service.TotalRequests = testReservations + 9 + 3000
		c.Inventory.Calls, c.Inventory.Timeouts, c.Inventory.Rejected = testReservations+9, 9, 10
		c.Breaker.TimesOpened, c.Breaker.OpenSeconds = 10, 20
		for _, what := range []string{"breaker closed -> open", "breaker open -> half-open", "breaker half-open -> closed"} {
			c.Timeline = append(c.Timeline, struct {
				What string `json:"what"`
			}{What: what})
		}
	}
	return r
}

func failed(checks []Check) []string {
	var names []string
	for _, c := range checks {
		if !c.OK {
			names = append(names, c.Name)
		}
	}
	return names
}

func TestVerifyPassesARunWithNothingWrong(t *testing.T) {
	cases := []struct {
		scenario   string
		wantChecks int
	}{
		{scenario: scenarioSpike, wantChecks: 12},
		{scenario: scenarioOutsideLoad, wantChecks: 13},
	}
	for _, tc := range cases {
		t.Run(tc.scenario, func(t *testing.T) {
			got := verify(goodRun(tc.scenario, 1))
			if len(got) != tc.wantChecks {
				t.Fatalf("%d checks were made, want %d", len(got), tc.wantChecks)
			}
			if names := failed(got); len(names) != 0 {
				t.Fatalf("checks failed on a run with nothing wrong: %v", names)
			}
		})
	}
}

func TestVerifyCatchesEachProblemAndOnlyThatOne(t *testing.T) {
	const (
		producer     = "the producer published every line of the schedule"
		spike        = "the spike held ten times the normal rate for at least two minutes"
		rates        = "every segment kept its rate within 0.5%"
		clean        = "the consumer finished cleanly with nothing left in its hands"
		consumed     = "every line sent was consumed"
		unique       = "the unique events aggregated equal the count taken outside the system"
		duplicates   = "the duplicates discarded account for every other line"
		reservations = "every reservation was verified against the inventory service"
		redis        = "redis holds every line and none is unacknowledged"
		service      = "the service counted exactly the calls that were made to it"
		bound        = "the wait buffer stayed inside its bound"
		stayedClosed = "the breaker stayed closed, because nothing outside saturated the service"
		saturated    = "the outside client did saturate the service"
		reopened     = "the breaker opened, and ended the run closed again"
	)

	cases := []struct {
		name     string
		scenario string
		spoil    func(r *runFiles)
		want     []string
	}{
		{name: "the producer stopped one line short", scenario: scenarioSpike,
			spoil: func(r *runFiles) { r.Producer.Published-- }, want: []string{producer}},
		{name: "the producer exited with an error", scenario: scenarioSpike,
			spoil: func(r *runFiles) { r.Checks.ProducerExit = 1 }, want: []string{producer}},
		{name: "the spike lasted one second less than two minutes", scenario: scenarioSpike,
			spoil: func(r *runFiles) { r.Producer.Segments[1].PlannedSeconds = 119 }, want: []string{spike}},
		{name: "the spike was nine times the normal rate", scenario: scenarioSpike,
			spoil: func(r *runFiles) {
				r.Producer.Segments[1].TargetRate, r.Producer.Segments[1].AchievedRate = 387, 387
			}, want: []string{spike}},
		{name: "a segment ran 0.6% slow", scenario: scenarioSpike,
			spoil: func(r *runFiles) { r.Producer.Segments[1].AchievedRate = 430 * 0.994 }, want: []string{rates}},
		{name: "a segment exactly 0.5% slow is still within", scenario: scenarioSpike,
			spoil: func(r *runFiles) { r.Producer.Segments[1].AchievedRate = 430 - 2.15 }, want: nil},
		{name: "the consumer exited with an error", scenario: scenarioSpike,
			spoil: func(r *runFiles) { r.Checks.ConsumerExit = 1 }, want: []string{clean}},
		{name: "one event was read and never aggregated", scenario: scenarioSpike,
			spoil: func(r *runFiles) { r.Consumer.Count.ReadNotFinished = 1 }, want: []string{clean}},
		{name: "one reservation was left in the wait buffer", scenario: scenarioSpike,
			spoil: func(r *runFiles) { r.Consumer.Count.StillInTheBuffer = 1 }, want: []string{clean}},
		{name: "one line was never consumed", scenario: scenarioSpike,
			spoil: func(r *runFiles) { r.Consumer.Consumed-- }, want: []string{consumed}},
		{name: "one event is missing from the aggregate", scenario: scenarioSpike,
			spoil: func(r *runFiles) { r.Consumer.Count.Aggregated-- }, want: []string{unique}},
		{name: "one event too many in the aggregate", scenario: scenarioSpike,
			spoil: func(r *runFiles) { r.Consumer.Count.Aggregated++ }, want: []string{unique}},
		{name: "one duplicate too many was discarded", scenario: scenarioSpike,
			spoil: func(r *runFiles) { r.Consumer.Duplicates++ }, want: []string{duplicates}},
		{name: "one reservation was not verified", scenario: scenarioSpike,
			spoil: func(r *runFiles) { r.Consumer.Inventory.Verified-- }, want: []string{reservations}},
		{name: "one message is still unacknowledged", scenario: scenarioSpike,
			spoil: func(r *runFiles) { r.Checks.StreamPending = 1 }, want: []string{redis}},
		{name: "the stream is one entry short", scenario: scenarioSpike,
			spoil: func(r *runFiles) { r.Checks.StreamLength-- }, want: []string{redis}},
		{name: "the service counted one call more than was made", scenario: scenarioSpike,
			spoil: func(r *runFiles) { r.Checks.Service.TotalRequests++ }, want: []string{service}},
		{name: "the service never saw one of the calls", scenario: scenarioSpike,
			spoil: func(r *runFiles) { r.Checks.Service.TotalRequests-- }, want: []string{service}},
		{name: "a call that never reached the service is not expected in its count", scenario: scenarioSpike,
			spoil: func(r *runFiles) { r.Consumer.Inventory.Calls++; r.Consumer.Inventory.Unreachable++ }, want: nil},
		{name: "the wait buffer reached its capacity", scenario: scenarioSpike,
			spoil: func(r *runFiles) { r.Consumer.Backpressure.MaxDepth = 300 }, want: []string{bound}},
		{name: "the wait buffer one below its capacity is within the bound", scenario: scenarioSpike,
			spoil: func(r *runFiles) { r.Consumer.Backpressure.MaxDepth = 299 }, want: nil},
		{name: "the breaker opened with nobody saturating the service", scenario: scenarioSpike,
			spoil: func(r *runFiles) { r.Consumer.Breaker.TimesOpened = 1 }, want: []string{stayedClosed}},
		{name: "the outside client failed no request", scenario: scenarioOutsideLoad,
			spoil: func(r *runFiles) { r.Load.FailurePct = 0 }, want: []string{saturated}},
		{name: "the outside client exited with an error", scenario: scenarioOutsideLoad,
			spoil: func(r *runFiles) { r.Checks.LoadExit = 1 }, want: []string{saturated}},
		{name: "the outside client left no report", scenario: scenarioOutsideLoad,
			spoil: func(r *runFiles) { r.Load = nil }, want: []string{service, saturated}},
		{name: "the breaker never opened under outside load", scenario: scenarioOutsideLoad,
			spoil: func(r *runFiles) { r.Consumer.Breaker.TimesOpened = 0 }, want: []string{reopened}},
		{name: "the run ended with the breaker open", scenario: scenarioOutsideLoad,
			spoil: func(r *runFiles) {
				r.Consumer.Timeline[len(r.Consumer.Timeline)-1].What = "breaker half-open -> open"
			}, want: []string{reopened}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := goodRun(tc.scenario, 1)
			tc.spoil(&r)
			if got := failed(verify(r)); !slices.Equal(got, tc.want) {
				t.Fatalf("failed checks = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestSpread(t *testing.T) {
	cases := []struct {
		name   string
		values []float64
		want   Spread
	}{
		{name: "no runs", values: nil, want: Spread{}},
		{name: "one run", values: []float64{7}, want: Spread{Median: 7, Min: 7, Max: 7}},
		{name: "odd count takes the middle", values: []float64{9, 1, 5}, want: Spread{Median: 5, Min: 1, Max: 9}},
		{name: "even count averages the middle two", values: []float64{4, 1, 3, 2}, want: Spread{Median: 2.5, Min: 1, Max: 4}},
		{name: "an outlier moves the range, not the median", values: []float64{1, 2, 3, 4, 1000}, want: Spread{Median: 3, Min: 1, Max: 1000}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := spread(tc.values); got != tc.want {
				t.Fatalf("spread = %+v, want %+v", got, tc.want)
			}
		})
	}
}

func metric(t *testing.T, sc ScenarioSummary, name string) Spread {
	t.Helper()
	for _, m := range sc.Metrics {
		if m.Name == name {
			return m.Spread
		}
	}
	t.Fatalf("scenario %s has no metric %q", sc.Scenario, name)
	return Spread{}
}

func TestSummarizeGroupsRunsByScenarioAndCountsFailures(t *testing.T) {
	var runs []runFiles
	for run := 1; run <= 3; run++ {
		runs = append(runs, goodRun(scenarioSpike, run))
	}
	for run, opened := range []int{10, 9, 11} {
		r := goodRun(scenarioOutsideLoad, run+1)
		r.Consumer.Breaker.TimesOpened = opened
		runs = append(runs, r)
	}
	runs[4].Consumer.Count.Aggregated--

	rep := summarize(runs)

	if rep.Runs != 6 || rep.RunsFailed != 1 {
		t.Fatalf("%d runs with %d failed, want 6 with 1", rep.Runs, rep.RunsFailed)
	}
	if !slices.Equal(rep.Machines, []string{"test machine"}) {
		t.Fatalf("machines = %q, want the single machine every run names", rep.Machines)
	}
	if len(rep.Scenarios) != 2 || rep.Scenarios[0].Scenario != scenarioSpike || rep.Scenarios[1].Scenario != scenarioOutsideLoad {
		t.Fatalf("scenarios = %+v, want spike then spike-with-outside-load", rep.Scenarios)
	}

	spike, loaded := rep.Scenarios[0], rep.Scenarios[1]
	if spike.Runs != 3 || spike.RunsPassed != 3 || loaded.Runs != 3 || loaded.RunsPassed != 2 {
		t.Fatalf("passed %d of %d and %d of %d, want 3 of 3 and 2 of 3",
			spike.RunsPassed, spike.Runs, loaded.RunsPassed, loaded.Runs)
	}
	if loaded.PerRun[0].Passed != true || loaded.PerRun[1].Passed != false || loaded.PerRun[2].Passed != true {
		t.Fatalf("verdicts = %+v, want only the second run to fail", loaded.PerRun)
	}
	if spike.LinesSent != testLines || spike.ExpectedUnique != testUnique || spike.ExpectedReservations != testReservations {
		t.Fatalf("expectations = %d, %d, %d, want %d, %d, %d",
			spike.LinesSent, spike.ExpectedUnique, spike.ExpectedReservations, testLines, testUnique, testReservations)
	}

	if got, want := metric(t, loaded, "times the breaker opened"), (Spread{Median: 10, Min: 9, Max: 11}); got != want {
		t.Fatalf("times the breaker opened = %+v, want %+v", got, want)
	}
	if got, want := metric(t, spike, "times the breaker opened"), (Spread{}); got != want {
		t.Fatalf("times the breaker opened without outside load = %+v, want all zero", got)
	}
	if got, want := metric(t, loaded, "unique events aggregated"), (Spread{Median: testUnique, Min: testUnique - 1, Max: testUnique}); got != want {
		t.Fatalf("unique events aggregated = %+v, want %+v", got, want)
	}
	if got, want := metric(t, spike, "end to end latency p95, seconds"), (Spread{Median: 200, Min: 200, Max: 200}); got != want {
		t.Fatalf("latency p95 = %+v, want %+v", got, want)
	}
	if got, want := metric(t, spike, "segment 2 achieved rate, target 430/s"), (Spread{Median: 430, Min: 430, Max: 430}); got != want {
		t.Fatalf("segment 2 rate = %+v, want %+v", got, want)
	}
	if got, want := metric(t, loaded, "outside client requests that failed, %"), (Spread{Median: 27, Min: 27, Max: 27}); got != want {
		t.Fatalf("outside failures = %+v, want %+v", got, want)
	}
	for _, m := range spike.Metrics {
		if strings.HasPrefix(m.Name, "outside client") {
			t.Fatalf("the scenario without outside load reports %q", m.Name)
		}
	}
}

func TestRequireKeys(t *testing.T) {
	doc := []byte(`{"a": 1, "b": {"c": 0, "d": {"e": null}}, "f": [1, 2]}`)

	cases := []struct {
		name         string
		doc          []byte
		paths        []string
		wantProblems int
	}{
		{name: "everything present, even zero and null", doc: doc, paths: []string{"a", "b.c", "b.d.e", "f"}},
		{name: "a missing top-level key", doc: doc, paths: []string{"a", "z"}, wantProblems: 1},
		{name: "a missing nested key", doc: doc, paths: []string{"b.z"}, wantProblems: 1},
		{name: "a path through something that is not an object", doc: doc, paths: []string{"a.x", "f.x"}, wantProblems: 2},
		{name: "every missing key is reported", doc: doc, paths: []string{"x", "b.y", "b.d.z"}, wantProblems: 3},
		{name: "not JSON", doc: []byte(`nope`), paths: []string{"a"}, wantProblems: 1},
		{name: "a JSON array instead of an object", doc: []byte(`[1]`), paths: []string{"a"}, wantProblems: 1},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := requireKeys(tc.doc, tc.paths)
			if tc.wantProblems == 0 {
				if err != nil {
					t.Fatalf("requireKeys: %v", err)
				}
				return
			}
			if err == nil {
				t.Fatal("requireKeys found nothing wrong, want an error")
			}
			problems := 1
			if joined, ok := err.(interface{ Unwrap() []error }); ok {
				problems = len(joined.Unwrap())
			}
			if problems != tc.wantProblems {
				t.Fatalf("requireKeys reported %d problems, want %d: %v", problems, tc.wantProblems, err)
			}
		})
	}
}

func writeRun(t *testing.T, dir string, r runFiles) string {
	t.Helper()
	base := filepath.Join(dir, r.Checks.Scenario+"-run"+string(rune('0'+r.Checks.Run)))
	write := func(suffix string, v any) {
		b, err := json.Marshal(v)
		if err != nil {
			t.Fatalf("encoding %s: %v", suffix, err)
		}
		if err := os.WriteFile(base+suffix, b, 0o644); err != nil {
			t.Fatalf("writing %s: %v", base+suffix, err)
		}
	}
	write("-checks.json", r.Checks)
	write("-producer.json", r.Producer)
	write("-consumer.json", r.Consumer)
	if r.Load != nil {
		write("-load.json", r.Load)
	}
	return base
}

func TestLoadRunsReadsBackWhatTheScriptWrites(t *testing.T) {
	dir := t.TempDir()
	want := []runFiles{goodRun(scenarioSpike, 1), goodRun(scenarioSpike, 2), goodRun(scenarioOutsideLoad, 1)}
	for _, r := range want {
		writeRun(t, dir, r)
	}

	got, err := loadRuns(dir)
	if err != nil {
		t.Fatalf("loadRuns: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("loaded %d runs, want 3", len(got))
	}
	for i, r := range got {
		if r.Checks != want[i].Checks {
			t.Errorf("run %d checks = %+v, want %+v", i, r.Checks, want[i].Checks)
		}
		if (r.Load == nil) != (want[i].Load == nil) {
			t.Errorf("run %d has an outside load report: %t, want %t", i, r.Load != nil, want[i].Load != nil)
		}
		if names := failed(verify(r)); len(names) != 0 {
			t.Errorf("run %d fails %v after a round trip through its files", i, names)
		}
	}
}

func TestLoadRunsRefusesWhatItCannotTrust(t *testing.T) {
	cases := []struct {
		name    string
		prepare func(t *testing.T, dir string)
		wantIn  string
	}{
		{
			name:    "an empty directory",
			prepare: func(*testing.T, string) {},
			wantIn:  "no *-checks.json files",
		},
		{
			name: "a run without its consumer report",
			prepare: func(t *testing.T, dir string) {
				base := writeRun(t, dir, goodRun(scenarioSpike, 1))
				if err := os.Remove(base + "-consumer.json"); err != nil {
					t.Fatalf("removing the consumer report: %v", err)
				}
			},
			wantIn: "-consumer.json",
		},
		{
			name: "a consumer report without the count of unique events",
			prepare: func(t *testing.T, dir string) {
				base := writeRun(t, dir, goodRun(scenarioSpike, 1))
				doc := `{"consumed_messages": 1, "count": {"read_but_not_aggregated": 0}}`
				if err := os.WriteFile(base+"-consumer.json", []byte(doc), 0o644); err != nil {
					t.Fatalf("rewriting the consumer report: %v", err)
				}
			},
			wantIn: `key "count.unique_events_aggregated" is missing`,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			tc.prepare(t, dir)
			_, err := loadRuns(dir)
			if err == nil {
				t.Fatal("loadRuns succeeded, want an error")
			}
			if !strings.Contains(err.Error(), tc.wantIn) {
				t.Fatalf("loadRuns error = %v, want it to mention %q", err, tc.wantIn)
			}
		})
	}
}
