package main

import (
	"fmt"
	"math"
	"slices"
	"strings"
)

const (
	scenarioSpike       = "spike"
	scenarioOutsideLoad = "spike-with-outside-load"

	rateTolerance   = 0.005
	spikeFactor     = 10
	minSpikeSeconds = 120
)

type runFiles struct {
	Checks   checks
	Producer producerReport
	Consumer consumerReport
	Load     *loadReport
}

type checks struct {
	Scenario             string `json:"scenario"`
	Run                  int    `json:"run"`
	Machine              string `json:"machine"`
	Schedule             string `json:"schedule"`
	LinesSent            int64  `json:"lines_sent"`
	ExpectedUnique       int64  `json:"expected_unique_events"`
	ExpectedReservations int64  `json:"expected_reservations"`
	ProducerExit         int    `json:"producer_exit_status"`
	ConsumerExit         int    `json:"consumer_exit_status"`
	LoadExit             int    `json:"load_exit_status"`
	StreamLength         int64  `json:"stream_length"`
	StreamPending        int64  `json:"stream_pending"`
	Service              struct {
		TotalRequests int64 `json:"total_requests"`
	} `json:"service"`
}

var checksKeys = []string{
	"scenario", "run", "machine", "schedule", "lines_sent", "expected_unique_events", "expected_reservations",
	"producer_exit_status", "consumer_exit_status", "load_exit_status", "stream_length", "stream_pending",
	"service.total_requests",
}

type producerReport struct {
	PlannedEvents int64 `json:"planned_events"`
	Published     int64 `json:"published_events"`
	Segments      []struct {
		TargetRate     int     `json:"target_rate_per_second"`
		PlannedSeconds float64 `json:"planned_seconds"`
		AchievedRate   float64 `json:"achieved_rate_per_second"`
		MaxLatenessMs  float64 `json:"max_lateness_ms"`
	} `json:"segments"`
}

var producerKeys = []string{"planned_events", "published_events", "segments"}

type latency struct {
	P50 float64 `json:"p50"`
	P95 float64 `json:"p95"`
	Max float64 `json:"max"`
}

type consumerReport struct {
	Consumed        int64   `json:"consumed_messages"`
	Duplicates      int64   `json:"duplicates_discarded"`
	ReadOverruns    int64   `json:"stream_reads_redis_left_unanswered"`
	DurationSeconds float64 `json:"duration_seconds"`
	LatencyMs       latency `json:"end_to_end_latency_ms"`
	Count           struct {
		Aggregated       int64 `json:"unique_events_aggregated"`
		ReadNotFinished  int64 `json:"read_but_not_aggregated"`
		StillInTheBuffer int64 `json:"still_in_the_wait_buffer"`
	} `json:"count"`
	Inventory struct {
		Verified    int64   `json:"reservations_verified"`
		Calls       int64   `json:"calls_let_through"`
		Overloaded  int64   `json:"calls_failed_overloaded_503"`
		Timeouts    int64   `json:"calls_failed_timeout"`
		Unreachable int64   `json:"calls_failed_unreachable"`
		Rejected    int64   `json:"calls_rejected_by_the_breaker"`
		LatencyMs   latency `json:"publish_to_verified_latency_ms"`
	} `json:"inventory"`
	Breaker struct {
		TimesOpened int     `json:"times_opened"`
		OpenSeconds float64 `json:"seconds_open"`
	} `json:"breaker"`
	Backpressure struct {
		MaxDepth       int     `json:"deepest_wait_buffer"`
		TimesEngaged   int     `json:"times_engaged"`
		EngagedSeconds float64 `json:"seconds_engaged"`
		Capacity       int     `json:"capacity"`
	} `json:"backpressure"`
	Timeline []struct {
		What string `json:"what"`
	} `json:"timeline"`
}

var consumerKeys = []string{
	"consumed_messages", "duplicates_discarded", "stream_reads_redis_left_unanswered", "duration_seconds",
	"end_to_end_latency_ms.p50", "end_to_end_latency_ms.p95", "end_to_end_latency_ms.max",
	"count.unique_events_aggregated", "count.read_but_not_aggregated", "count.still_in_the_wait_buffer",
	"inventory.reservations_verified", "inventory.calls_let_through", "inventory.calls_failed_overloaded_503",
	"inventory.calls_failed_timeout", "inventory.calls_failed_unreachable", "inventory.calls_rejected_by_the_breaker",
	"inventory.publish_to_verified_latency_ms.p95", "inventory.publish_to_verified_latency_ms.max",
	"breaker.times_opened", "breaker.seconds_open",
	"backpressure.deepest_wait_buffer", "backpressure.times_engaged", "backpressure.seconds_engaged",
	"backpressure.capacity", "timeline",
}

type loadReport struct {
	Sent       int64   `json:"sent"`
	Rate       int     `json:"rate_per_second"`
	Seconds    float64 `json:"seconds"`
	FailurePct float64 `json:"failure_pct"`
}

var loadKeys = []string{"sent", "rate_per_second", "seconds", "failure_pct"}

type Check struct {
	Name   string `json:"check"`
	OK     bool   `json:"ok"`
	Detail string `json:"detail"`
}

func verify(r runFiles) []Check {
	c, p, k := r.Consumer, r.Producer, r.Checks
	var out []Check
	add := func(name string, ok bool, format string, args ...any) {
		out = append(out, Check{Name: name, OK: ok, Detail: fmt.Sprintf(format, args...)})
	}

	add("the producer published every line of the schedule",
		k.ProducerExit == 0 && p.Published == p.PlannedEvents && p.Published == k.LinesSent,
		"published %d of %d planned, the script counted %d lines, exit status %d",
		p.Published, p.PlannedEvents, k.LinesSent, k.ProducerExit)

	lowest, highest, spikeSeconds := math.MaxInt, 0, 0.0
	worstRate := 0.0
	for _, seg := range p.Segments {
		lowest, highest = min(lowest, seg.TargetRate), max(highest, seg.TargetRate)
		worstRate = max(worstRate, math.Abs(seg.AchievedRate-float64(seg.TargetRate))/float64(seg.TargetRate))
	}
	for _, seg := range p.Segments {
		if seg.TargetRate == highest {
			spikeSeconds += seg.PlannedSeconds
		}
	}
	add("the spike held ten times the normal rate for at least two minutes",
		len(p.Segments) > 0 && highest == spikeFactor*lowest && spikeSeconds >= minSpikeSeconds,
		"normal %d/s, spike %d/s for %.0fs", lowest, highest, spikeSeconds)
	add("every segment kept its rate within 0.5%",
		len(p.Segments) > 0 && worstRate <= rateTolerance,
		"the worst segment was %.3f%% off its rate", 100*worstRate)

	add("the consumer finished cleanly with nothing left in its hands",
		k.ConsumerExit == 0 && c.Count.ReadNotFinished == 0 && c.Count.StillInTheBuffer == 0,
		"exit status %d, %d read but not aggregated, %d still in the wait buffer",
		k.ConsumerExit, c.Count.ReadNotFinished, c.Count.StillInTheBuffer)
	add("every line sent was consumed",
		c.Consumed == k.LinesSent, "consumed %d of %d", c.Consumed, k.LinesSent)
	add("the unique events aggregated equal the count taken outside the system",
		c.Count.Aggregated == k.ExpectedUnique, "aggregated %d, expected %d", c.Count.Aggregated, k.ExpectedUnique)
	add("the duplicates discarded account for every other line",
		c.Duplicates == k.LinesSent-k.ExpectedUnique,
		"discarded %d, the lines hold %d", c.Duplicates, k.LinesSent-k.ExpectedUnique)
	add("every reservation was verified against the inventory service",
		c.Inventory.Verified == k.ExpectedReservations,
		"verified %d, expected %d", c.Inventory.Verified, k.ExpectedReservations)
	add("redis holds every line and none is unacknowledged",
		k.StreamLength == k.LinesSent && k.StreamPending == 0,
		"stream length %d, pending %d", k.StreamLength, k.StreamPending)

	var outside int64
	if r.Load != nil {
		outside = r.Load.Sent
	}
	ours := c.Inventory.Calls - c.Inventory.Unreachable
	add("the service counted exactly the calls that were made to it",
		k.Service.TotalRequests == ours+outside,
		"the service counted %d: %d from the consumer plus %d from the outside client make %d",
		k.Service.TotalRequests, ours, outside, ours+outside)
	add("the wait buffer stayed inside its bound",
		c.Backpressure.MaxDepth <= c.Backpressure.Capacity-1,
		"deepest %d, bound %d", c.Backpressure.MaxDepth, c.Backpressure.Capacity-1)

	lastBreaker := ""
	for _, entry := range c.Timeline {
		if strings.HasPrefix(entry.What, "breaker ") {
			lastBreaker = entry.What
		}
	}
	switch k.Scenario {
	case scenarioSpike:
		add("the breaker stayed closed, because nothing outside saturated the service",
			c.Breaker.TimesOpened == 0, "opened %d times", c.Breaker.TimesOpened)
	case scenarioOutsideLoad:
		add("the outside client did saturate the service",
			k.LoadExit == 0 && r.Load != nil && r.Load.FailurePct > 0,
			"exit status %d, %s", k.LoadExit, describeLoad(r.Load))
		add("the breaker opened, and ended the run closed again",
			c.Breaker.TimesOpened >= 1 && strings.HasSuffix(lastBreaker, "-> closed"),
			"opened %d times, last transition %q", c.Breaker.TimesOpened, lastBreaker)
	}
	return out
}

func describeLoad(l *loadReport) string {
	if l == nil {
		return "no report from the outside client"
	}
	return fmt.Sprintf("%d requests at %d/s, %.1f%% of them failed", l.Sent, l.Rate, l.FailurePct)
}

type Spread struct {
	Median float64 `json:"median"`
	Min    float64 `json:"min"`
	Max    float64 `json:"max"`
}

func spread(values []float64) Spread {
	if len(values) == 0 {
		return Spread{}
	}
	sorted := slices.Clone(values)
	slices.Sort(sorted)
	mid := len(sorted) / 2
	median := sorted[mid]
	if len(sorted)%2 == 0 {
		median = (sorted[mid-1] + sorted[mid]) / 2
	}
	return Spread{Median: median, Min: sorted[0], Max: sorted[len(sorted)-1]}
}

func (s Spread) format(decimals int) string {
	return fmt.Sprintf("%.*f [%.*f-%.*f]", decimals, s.Median, decimals, s.Min, decimals, s.Max)
}

type Summary struct {
	Machines   []string          `json:"machines"`
	Runs       int               `json:"runs"`
	RunsFailed int               `json:"runs_that_failed_a_check"`
	Scenarios  []ScenarioSummary `json:"scenarios"`
}

type ScenarioSummary struct {
	Scenario             string       `json:"scenario"`
	Schedule             string       `json:"schedule"`
	Runs                 int          `json:"runs"`
	RunsPassed           int          `json:"runs_that_passed_every_check"`
	LinesSent            int64        `json:"lines_sent"`
	ExpectedUnique       int64        `json:"expected_unique_events"`
	ExpectedReservations int64        `json:"expected_reservations"`
	Metrics              []Metric     `json:"median_and_range_across_runs"`
	PerRun               []RunVerdict `json:"runs_detail"`
}

type Metric struct {
	Name     string `json:"metric"`
	Decimals int    `json:"-"`
	Spread
}

type RunVerdict struct {
	Run    int     `json:"run"`
	Passed bool    `json:"passed_every_check"`
	Checks []Check `json:"checks"`
}

func summarize(runs []runFiles) *Summary {
	rep := &Summary{Runs: len(runs)}

	var order []string
	byScenario := map[string][]runFiles{}
	for _, r := range runs {
		if !slices.Contains(rep.Machines, r.Checks.Machine) {
			rep.Machines = append(rep.Machines, r.Checks.Machine)
		}
		if _, seen := byScenario[r.Checks.Scenario]; !seen {
			order = append(order, r.Checks.Scenario)
		}
		byScenario[r.Checks.Scenario] = append(byScenario[r.Checks.Scenario], r)
	}

	for _, name := range order {
		group := byScenario[name]
		first := group[0].Checks
		sc := ScenarioSummary{
			Scenario:             name,
			Schedule:             first.Schedule,
			Runs:                 len(group),
			LinesSent:            first.LinesSent,
			ExpectedUnique:       first.ExpectedUnique,
			ExpectedReservations: first.ExpectedReservations,
		}

		for _, r := range group {
			verdict := RunVerdict{Run: r.Checks.Run, Checks: verify(r), Passed: true}
			for _, check := range verdict.Checks {
				verdict.Passed = verdict.Passed && check.OK
			}
			if verdict.Passed {
				sc.RunsPassed++
			} else {
				rep.RunsFailed++
			}
			sc.PerRun = append(sc.PerRun, verdict)
		}

		collectWhere := func(name string, decimals int, value func(runFiles) (float64, bool)) {
			var values []float64
			for _, r := range group {
				if v, ok := value(r); ok {
					values = append(values, v)
				}
			}
			sc.Metrics = append(sc.Metrics, Metric{Name: name, Decimals: decimals, Spread: spread(values)})
		}
		collect := func(name string, decimals int, value func(runFiles) float64) {
			collectWhere(name, decimals, func(r runFiles) (float64, bool) { return value(r), true })
		}
		collect("unique events aggregated", 0, func(r runFiles) float64 { return float64(r.Consumer.Count.Aggregated) })
		collect("duplicates discarded", 0, func(r runFiles) float64 { return float64(r.Consumer.Duplicates) })
		collect("reservations verified", 0, func(r runFiles) float64 { return float64(r.Consumer.Inventory.Verified) })
		collect("inventory calls that failed", 0, func(r runFiles) float64 {
			inv := r.Consumer.Inventory
			return float64(inv.Overloaded + inv.Timeouts + inv.Unreachable)
		})
		collect("inventory calls the breaker rejected", 0, func(r runFiles) float64 { return float64(r.Consumer.Inventory.Rejected) })
		collect("times the breaker opened", 0, func(r runFiles) float64 { return float64(r.Consumer.Breaker.TimesOpened) })
		collect("seconds the breaker was open", 1, func(r runFiles) float64 { return r.Consumer.Breaker.OpenSeconds })
		collect("times backpressure engaged", 0, func(r runFiles) float64 { return float64(r.Consumer.Backpressure.TimesEngaged) })
		collect("seconds backpressure was engaged", 1, func(r runFiles) float64 { return r.Consumer.Backpressure.EngagedSeconds })
		collect("deepest wait buffer", 0, func(r runFiles) float64 { return float64(r.Consumer.Backpressure.MaxDepth) })
		collect("end to end latency p50, seconds", 1, func(r runFiles) float64 { return r.Consumer.LatencyMs.P50 / 1000 })
		collect("end to end latency p95, seconds", 1, func(r runFiles) float64 { return r.Consumer.LatencyMs.P95 / 1000 })
		collect("end to end latency max, seconds", 1, func(r runFiles) float64 { return r.Consumer.LatencyMs.Max / 1000 })
		collect("publish to verified p95, seconds", 1, func(r runFiles) float64 { return r.Consumer.Inventory.LatencyMs.P95 / 1000 })
		collect("seconds from first to last event processed", 1, func(r runFiles) float64 { return r.Consumer.DurationSeconds })
		collect("stream reads redis left unanswered", 0, func(r runFiles) float64 { return float64(r.Consumer.ReadOverruns) })
		for i, seg := range group[0].Producer.Segments {
			collectWhere(fmt.Sprintf("segment %d achieved rate, target %d/s", i+1, seg.TargetRate), 2, func(r runFiles) (float64, bool) {
				if i >= len(r.Producer.Segments) {
					return 0, false
				}
				return r.Producer.Segments[i].AchievedRate, true
			})
			collectWhere(fmt.Sprintf("segment %d worst lateness, ms", i+1), 1, func(r runFiles) (float64, bool) {
				if i >= len(r.Producer.Segments) {
					return 0, false
				}
				return r.Producer.Segments[i].MaxLatenessMs, true
			})
		}
		if name == scenarioOutsideLoad {
			collectWhere("outside client requests that failed, %", 1, func(r runFiles) (float64, bool) {
				if r.Load == nil {
					return 0, false
				}
				return r.Load.FailurePct, true
			})
		}

		rep.Scenarios = append(rep.Scenarios, sc)
	}
	return rep
}

func printSummary(rep *Summary) {
	fmt.Printf("measured on: %s\n", strings.Join(rep.Machines, " | "))
	for _, sc := range rep.Scenarios {
		fmt.Printf("\n%s: %d runs of %s, %d lines, %d unique events and %d reservations expected\n",
			sc.Scenario, sc.Runs, sc.Schedule, sc.LinesSent, sc.ExpectedUnique, sc.ExpectedReservations)
		fmt.Printf("  %d of %d runs passed every check\n\n", sc.RunsPassed, sc.Runs)
		fmt.Printf("  %-46s %s\n", "", "median [min-max]")
		for _, m := range sc.Metrics {
			fmt.Printf("  %-46s %s\n", m.Name, m.format(m.Decimals))
		}
		for _, run := range sc.PerRun {
			for _, check := range run.Checks {
				if !check.OK {
					fmt.Printf("\n  run %d FAILED: %s (%s)", run.Run, check.Name, check.Detail)
				}
			}
		}
		if sc.RunsPassed != sc.Runs {
			fmt.Println()
		}
	}

	if len(rep.Scenarios) > 0 && len(rep.Scenarios[0].PerRun) > 0 {
		fmt.Printf("\nwhat every run was checked for:\n")
		seen := map[string]bool{}
		for _, sc := range rep.Scenarios {
			for _, check := range sc.PerRun[0].Checks {
				if !seen[check.Name] {
					seen[check.Name] = true
					fmt.Printf("  - %s\n", check.Name)
				}
			}
		}
	}
}
