// Reads the Hot Sale stream, discards event ids it has already processed, has every inventory reservation verified by the
// inventory service, and keeps the per country and per type aggregate the operations team watches.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"math"
	"os"
	"os/signal"
	"runtime"
	"slices"
	"sync"
	"syscall"
	"time"

	"github.com/ErnestoCM-13/proyecto-sistema-resiliente-con-backpressure/internal/aggregate"
	"github.com/ErnestoCM-13/proyecto-sistema-resiliente-con-backpressure/internal/clock"
	"github.com/ErnestoCM-13/proyecto-sistema-resiliente-con-backpressure/internal/dedup"
	"github.com/ErnestoCM-13/proyecto-sistema-resiliente-con-backpressure/internal/event"
	"github.com/ErnestoCM-13/proyecto-sistema-resiliente-con-backpressure/internal/stream"
)

const (
	progressEvery = 250_000
	readBlock     = 2 * time.Second
)

type config struct {
	redisAddr    string
	streamKey    string
	group        string
	consumerName string
	readCount    int64
	idle         time.Duration
	claimMinIdle time.Duration
	dedupTTL     time.Duration
	dedupMax     int
	out          string

	inventoryURL string
	checkTimeout time.Duration
	failures     int
	cooldown     time.Duration
	trials       int
	bufferHigh   int
	bufferLow    int
	workers      int
	expectUnique int64
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "consumer: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := parseFlags()
	if err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	client, err := stream.Connect(ctx, cfg.redisAddr)
	if err != nil {
		return err
	}
	defer client.Close()

	reader := stream.NewReader(client, cfg.streamKey, cfg.group, cfg.consumerName, cfg.readCount, readBlock)
	if err := reader.EnsureGroup(ctx); err != nil {
		return err
	}

	p, err := newPipeline(reader, cfg, newTimeline(os.Stdout))
	if err != nil {
		return err
	}

	fmt.Printf("consuming stream %s as %s/%s, verifying inventory at %s, stopping after %s idle\n",
		cfg.streamKey, cfg.group, cfg.consumerName, cfg.inventoryURL, cfg.idle)

	rep, err := consume(ctx, p, cfg)
	if err != nil && !errors.Is(err, context.Canceled) {
		return err
	}

	printReport(rep)
	if cfg.out != "" {
		if err := writeReport(cfg.out, rep); err != nil {
			return err
		}
		fmt.Printf("\nreport %s\n", cfg.out)
	}
	if rep.Count.MatchesExpected != nil && !*rep.Count.MatchesExpected {
		return fmt.Errorf("%d unique events were aggregated, %d were expected",
			rep.Count.Aggregated, rep.Count.ExpectedUnique)
	}
	return nil
}

type Report struct {
	Consumed            int64              `json:"consumed_messages"`
	Claimed             int64              `json:"claimed_from_a_dead_consumer"`
	DuplicatesDiscarded int64              `json:"duplicates_discarded"`
	ParseErrors         int64              `json:"parse_errors"`
	ParseErrorSamples   []string           `json:"parse_error_samples"`
	DurationSeconds     float64            `json:"duration_seconds"`
	EventsPerSecond     float64            `json:"events_per_second"`
	LatencyMs           Dist               `json:"end_to_end_latency_ms"`
	Count               CountReport        `json:"count"`
	Inventory           InventoryReport    `json:"inventory"`
	Breaker             BreakerReport      `json:"breaker"`
	Backpressure        BackpressureReport `json:"backpressure"`
	Dedup               DedupReport        `json:"dedup"`
	Timeline            []TimelineEntry    `json:"timeline"`
	Aggregate           aggregate.Snapshot `json:"aggregate"`
}

type CountReport struct {
	Aggregated       int64 `json:"unique_events_aggregated"`
	ReadNotFinished  int64 `json:"read_but_not_aggregated"`
	ExpectedUnique   int64 `json:"expected_unique_events,omitempty"`
	MatchesExpected  *bool `json:"matches_expected,omitempty"`
	StillInTheBuffer int   `json:"still_in_the_wait_buffer"`
}

type InventoryReport struct {
	Verified    int64 `json:"reservations_verified"`
	Available   int64 `json:"answered_available"`
	Unavailable int64 `json:"answered_not_available"`
	Calls       int64 `json:"calls_let_through"`
	Overloaded  int64 `json:"calls_failed_overloaded_503"`
	Timeouts    int64 `json:"calls_failed_timeout"`
	Unreachable int64 `json:"calls_failed_unreachable"`
	Rejected    int64 `json:"calls_rejected_by_the_breaker"`
	LatencyMs   Dist  `json:"publish_to_verified_latency_ms"`
}

type BreakerReport struct {
	TimesOpened      int     `json:"times_opened"`
	OpenSeconds      float64 `json:"seconds_open"`
	CheckTimeoutMs   float64 `json:"check_timeout_ms"`
	FailureThreshold int     `json:"consecutive_failures_that_open"`
	CooldownSeconds  float64 `json:"cooldown_seconds"`
	HalfOpenTrials   int     `json:"half_open_trials"`
}

type BackpressureReport struct {
	MaxDepth       int     `json:"deepest_wait_buffer"`
	TimesEngaged   int     `json:"times_engaged"`
	EngagedSeconds float64 `json:"seconds_engaged"`
	High           int     `json:"engages_at_depth"`
	Low            int     `json:"releases_at_depth"`
	Capacity       int     `json:"capacity"`
	Workers        int     `json:"workers"`
}

type DedupReport struct {
	TTLSeconds        float64 `json:"window_ttl_seconds"`
	MaxEntries        int     `json:"window_max_entries"`
	Entries           int     `json:"entries_held_at_the_end"`
	EvictedByTime     int64   `json:"evicted_by_time"`
	EvictedByCapacity int64   `json:"evicted_by_capacity"`
	HeapAllocMiB      float64 `json:"consumer_heap_alloc_mib"`
}

type Dist struct {
	Min float64 `json:"min"`
	P50 float64 `json:"p50"`
	P95 float64 `json:"p95"`
	P99 float64 `json:"p99"`
	Max float64 `json:"max"`
}

func consume(ctx context.Context, p *pipeline, cfg config) (*Report, error) {
	window, err := dedup.NewWindow(cfg.dedupTTL, cfg.dedupMax)
	if err != nil {
		return nil, err
	}

	state := &processor{agg: aggregate.New()}
	rep := &Report{}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	workerErrs := make([]error, cfg.workers)
	var wg sync.WaitGroup
	for w := range cfg.workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := work(ctx, p, state); err != nil {
				workerErrs[w] = err
				cancel()
			}
		}()
	}

	readErr := read(ctx, p, cfg, window, state, rep)
	cancel()
	wg.Wait()

	fill(rep, p, cfg, window, state)

	for _, err := range append(workerErrs, readErr) {
		if err != nil && !errors.Is(err, context.Canceled) {
			return rep, err
		}
	}
	return rep, readErr
}

func read(ctx context.Context, p *pipeline, cfg config, window *dedup.Window, state *processor, rep *Report) error {
	ids := make([]string, 0, cfg.readCount)
	var lastMessage time.Time

	for {
		if err := p.buffer.WaitForRoom(ctx); err != nil {
			return err
		}

		messages, err := p.reader.Read(ctx)
		if err != nil {
			if !errors.Is(err, stream.ErrNoMessages) {
				return err
			}
			messages = nil
			if state.idle() {
				messages, err = p.reader.Claim(ctx, cfg.claimMinIdle, cfg.readCount)
				if err != nil {
					return err
				}
				rep.Claimed += int64(len(messages))
			}
		}

		if len(messages) == 0 {
			if lastMessage.IsZero() || time.Since(lastMessage) < cfg.idle {
				continue
			}
			pending, err := p.reader.Pending(ctx)
			if err != nil {
				return err
			}
			if pending == 0 {
				return nil
			}
			continue
		}

		nowNS, err := clock.NowNS()
		if err != nil {
			return err
		}
		now := time.Now()
		lastMessage = now

		ids = ids[:0]
		for _, msg := range messages {
			var ev event.Event
			if err := json.Unmarshal(msg.Payload, &ev); err != nil {
				rep.ParseErrors++
				if len(rep.ParseErrorSamples) < 10 {
					rep.ParseErrorSamples = append(rep.ParseErrorSamples,
						fmt.Sprintf("message %s: %v", msg.ID, err))
				}
				ids = append(ids, msg.ID)
				continue
			}
			rep.Consumed++
			if rep.Consumed%progressEvery == 0 {
				fmt.Printf("  %d events\n", rep.Consumed)
			}

			if window.Seen(ev.EventID, nowNS) {
				rep.DuplicatesDiscarded++
				ids = append(ids, msg.ID)
				continue
			}

			if ev.Type == event.InventoryReserved {
				state.hold()
				if err := p.buffer.Put(pendingCheck{id: msg.ID, ev: ev, pubNS: msg.PubNS}); err != nil {
					return fmt.Errorf("message %s: %w", msg.ID, err)
				}
				continue
			}

			state.apply(&ev, nowNS-msg.PubNS, now)
			ids = append(ids, msg.ID)
		}

		if err := ack(ctx, p.reader, ids); err != nil {
			return err
		}
	}
}

func fill(rep *Report, p *pipeline, cfg config, window *dedup.Window, state *processor) {
	state.mu.Lock()
	defer state.mu.Unlock()

	if !state.first.IsZero() {
		rep.DurationSeconds = state.last.Sub(state.first).Seconds()
	}
	snap := state.agg.Snapshot()
	if rep.DurationSeconds > 0 {
		rep.EventsPerSecond = float64(snap.Total) / rep.DurationSeconds
	}
	rep.LatencyMs = dist(state.latencies)
	rep.Aggregate = snap
	rep.Timeline = p.log.snapshot()

	buffer := p.buffer.Stats()
	rep.Count = CountReport{
		Aggregated:       snap.Total,
		ReadNotFinished:  rep.Consumed - rep.DuplicatesDiscarded - snap.Total,
		StillInTheBuffer: buffer.Depth,
	}
	if cfg.expectUnique > 0 {
		matches := snap.Total == cfg.expectUnique
		rep.Count.ExpectedUnique = cfg.expectUnique
		rep.Count.MatchesExpected = &matches
	}

	verified := state.available + state.unavailable
	rep.Inventory = InventoryReport{
		Verified:    verified,
		Available:   state.available,
		Unavailable: state.unavailable,
		Calls:       verified + state.overloaded + state.timeouts + state.unreachable,
		Overloaded:  state.overloaded,
		Timeouts:    state.timeouts,
		Unreachable: state.unreachable,
		Rejected:    state.rejected,
		LatencyMs:   dist(state.verifiedNS),
	}

	guard := p.checker.Stats()
	rep.Breaker = BreakerReport{
		TimesOpened:      guard.Opened,
		OpenSeconds:      guard.OpenFor.Seconds(),
		CheckTimeoutMs:   float64(cfg.checkTimeout) / float64(time.Millisecond),
		FailureThreshold: cfg.failures,
		CooldownSeconds:  cfg.cooldown.Seconds(),
		HalfOpenTrials:   cfg.trials,
	}
	rep.Backpressure = BackpressureReport{
		MaxDepth:       buffer.MaxDepth,
		TimesEngaged:   buffer.TimesEngaged,
		EngagedSeconds: buffer.EngagedFor.Seconds(),
		High:           cfg.bufferHigh,
		Low:            cfg.bufferLow,
		Capacity:       cfg.bufferHigh + int(cfg.readCount),
		Workers:        cfg.workers,
	}

	var ms runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&ms)
	rep.Dedup = DedupReport{
		TTLSeconds:        cfg.dedupTTL.Seconds(),
		MaxEntries:        cfg.dedupMax,
		Entries:           window.Len(),
		EvictedByTime:     window.EvictedByTime(),
		EvictedByCapacity: window.EvictedByCapacity(),
		HeapAllocMiB:      float64(ms.HeapAlloc) / (1 << 20),
	}
	runtime.KeepAlive(window)
}

func printReport(rep *Report) {
	fmt.Printf("\nconsumed        %d messages, %d unique events aggregated in %.1fs (%.0f ev/s)\n",
		rep.Consumed, rep.Count.Aggregated, rep.DurationSeconds, rep.EventsPerSecond)
	if rep.Claimed > 0 {
		fmt.Printf("claimed         %d messages another consumer had left unacknowledged\n", rep.Claimed)
	}
	fmt.Printf("duplicates      %d discarded\n", rep.DuplicatesDiscarded)
	fmt.Printf("dedup window    %.0fs ttl, %d max entries, %d held at the end\n",
		rep.Dedup.TTLSeconds, rep.Dedup.MaxEntries, rep.Dedup.Entries)
	fmt.Printf("                evicted %d by time, %d by capacity, consumer heap %.1f MiB\n",
		rep.Dedup.EvictedByTime, rep.Dedup.EvictedByCapacity, rep.Dedup.HeapAllocMiB)
	fmt.Printf("parse errors    %d\n", rep.ParseErrors)
	for _, s := range rep.ParseErrorSamples {
		fmt.Printf("                %s\n", s)
	}
	fmt.Printf("latency ms      min %.2f  p50 %.2f  p95 %.2f  p99 %.2f  max %.2f\n",
		rep.LatencyMs.Min, rep.LatencyMs.P50, rep.LatencyMs.P95, rep.LatencyMs.P99, rep.LatencyMs.Max)

	inv := rep.Inventory
	fmt.Printf("\ninventory       %d reservations verified (%d available, %d not available)\n",
		inv.Verified, inv.Available, inv.Unavailable)
	fmt.Printf("                %d calls let through: %d overloaded, %d timeouts, %d unreachable; %d more rejected by the breaker\n",
		inv.Calls, inv.Overloaded, inv.Timeouts, inv.Unreachable, inv.Rejected)
	fmt.Printf("                publish to verified ms  p50 %.2f  p95 %.2f  p99 %.2f  max %.2f\n",
		inv.LatencyMs.P50, inv.LatencyMs.P95, inv.LatencyMs.P99, inv.LatencyMs.Max)
	fmt.Printf("breaker         opened %d times, open for %.1fs (%.0f ms timeout, %d failures, %.0fs cooldown, %d trials)\n",
		rep.Breaker.TimesOpened, rep.Breaker.OpenSeconds, rep.Breaker.CheckTimeoutMs,
		rep.Breaker.FailureThreshold, rep.Breaker.CooldownSeconds, rep.Breaker.HalfOpenTrials)
	fmt.Printf("backpressure    on %d times for %.1fs, deepest wait buffer %d (on at %d, off at %d, capacity %d, %d workers)\n",
		rep.Backpressure.TimesEngaged, rep.Backpressure.EngagedSeconds, rep.Backpressure.MaxDepth,
		rep.Backpressure.High, rep.Backpressure.Low, rep.Backpressure.Capacity, rep.Backpressure.Workers)

	fmt.Printf("count           %d unique events aggregated", rep.Count.Aggregated)
	switch {
	case rep.Count.MatchesExpected == nil:
		fmt.Println()
	case *rep.Count.MatchesExpected:
		fmt.Printf(", %d expected: match\n", rep.Count.ExpectedUnique)
	default:
		fmt.Printf(", %d expected: MISMATCH of %d\n",
			rep.Count.ExpectedUnique, rep.Count.Aggregated-rep.Count.ExpectedUnique)
	}
	if rep.Count.ReadNotFinished != 0 {
		fmt.Printf("                %d events were read but not aggregated; they stay unacknowledged in the stream\n",
			rep.Count.ReadNotFinished)
	}

	snap := rep.Aggregate
	fmt.Printf("\naggregate, %d events\n", snap.Total)
	fmt.Printf("  %-20s %12s %16s\n", "type", "events", "amount USD")
	for _, t := range event.Types {
		fmt.Printf("  %-20s %12d %16.2f\n",
			t, snap.ByType[string(t)], snap.AmountUSDByType[string(t)])
	}
	fmt.Printf("\n  %-4s %12s %20s %20s %20s\n",
		"", "total", string(event.OrderPlaced), string(event.PaymentConfirmed), string(event.InventoryReserved))
	for _, c := range event.Countries {
		perType := snap.ByCountryType[string(c)]
		fmt.Printf("  %-4s %12d %20d %20d %20d\n", c, snap.ByCountry[string(c)],
			perType[string(event.OrderPlaced)],
			perType[string(event.PaymentConfirmed)],
			perType[string(event.InventoryReserved)])
	}
}

func writeReport(path string, rep *Report) error {
	b, err := json.MarshalIndent(rep, "", "  ")
	if err != nil {
		return fmt.Errorf("encoding report: %w", err)
	}
	if err := os.WriteFile(path, append(b, '\n'), 0o644); err != nil {
		return fmt.Errorf("writing %s: %w", path, err)
	}
	return nil
}

func dist(values []int64) Dist {
	if len(values) == 0 {
		return Dist{}
	}
	slices.Sort(values)
	ms := func(ns int64) float64 { return float64(ns) / 1e6 }
	return Dist{
		Min: ms(values[0]),
		P50: ms(percentile(values, 50)),
		P95: ms(percentile(values, 95)),
		P99: ms(percentile(values, 99)),
		Max: ms(values[len(values)-1]),
	}
}

func percentile(sorted []int64, p float64) int64 {
	i := int(math.Ceil(p/100*float64(len(sorted)))) - 1
	if i < 0 {
		i = 0
	}
	if i >= len(sorted) {
		i = len(sorted) - 1
	}
	return sorted[i]
}

func parseFlags() (config, error) {
	var cfg config
	flag.StringVar(&cfg.redisAddr, "redis-addr", "localhost:6379", "host:port of Redis")
	flag.StringVar(&cfg.streamKey, "stream", "hotsale:events", "stream key to consume")
	flag.StringVar(&cfg.group, "group", "aggregators", "consumer group name")
	flag.StringVar(&cfg.consumerName, "name", "consumer-1", "consumer name inside the group")
	flag.Int64Var(&cfg.readCount, "read-count", 100, "messages per XREADGROUP call")
	flag.DurationVar(&cfg.idle, "idle", 5*time.Second, "stop after this long without new messages")
	flag.DurationVar(&cfg.claimMinIdle, "claim-min-idle", 30*time.Second,
		"take over messages another consumer has left unacknowledged for this long")
	flag.DurationVar(&cfg.dedupTTL, "dedup-ttl", 5*time.Minute, "how far back the deduplication window remembers")
	flag.IntVar(&cfg.dedupMax, "dedup-max", 3_200_000, "how many event ids the window may hold")
	flag.StringVar(&cfg.out, "out", "data/consumer-report.json", "path for the JSON report; empty writes none")

	flag.StringVar(&cfg.inventoryURL, "inventory-url", "http://localhost:8090", "base address of the inventory service")
	flag.DurationVar(&cfg.checkTimeout, "check-timeout", 100*time.Millisecond, "how long one inventory check may take")
	flag.IntVar(&cfg.failures, "breaker-failures", 5, "consecutive failed checks that open the circuit breaker")
	flag.DurationVar(&cfg.cooldown, "breaker-cooldown", 2*time.Second, "how long the circuit breaker stays open")
	flag.IntVar(&cfg.trials, "breaker-trials", 3, "successful trial checks that close the circuit breaker again")
	flag.IntVar(&cfg.bufferHigh, "buffer-high", 200, "reservations waiting for a check at which reading from the stream stops")
	flag.IntVar(&cfg.bufferLow, "buffer-low", 50, "reservations waiting for a check at which reading resumes")
	flag.IntVar(&cfg.workers, "workers", 1, "inventory checks in flight at once")
	flag.Int64Var(&cfg.expectUnique, "expect-unique", 0, "unique events the run must aggregate; 0 does not check")
	flag.Parse()

	var problems []error
	if cfg.streamKey == "" {
		problems = append(problems, errors.New("-stream: empty"))
	}
	if cfg.group == "" {
		problems = append(problems, errors.New("-group: empty"))
	}
	if cfg.consumerName == "" {
		problems = append(problems, errors.New("-name: empty"))
	}
	if cfg.readCount < 1 {
		problems = append(problems, fmt.Errorf("-read-count: %d is not positive", cfg.readCount))
	}
	if cfg.idle < readBlock {
		problems = append(problems, fmt.Errorf("-idle: %s is shorter than the %s read block", cfg.idle, readBlock))
	}
	if cfg.claimMinIdle <= 0 {
		problems = append(problems, fmt.Errorf("-claim-min-idle: %s is not positive", cfg.claimMinIdle))
	}
	if cfg.dedupTTL <= 0 {
		problems = append(problems, fmt.Errorf("-dedup-ttl: %s is not positive", cfg.dedupTTL))
	}
	if cfg.dedupMax <= 0 {
		problems = append(problems, fmt.Errorf("-dedup-max: %d is not positive", cfg.dedupMax))
	}
	if cfg.checkTimeout <= 0 {
		problems = append(problems, fmt.Errorf("-check-timeout: %s is not positive", cfg.checkTimeout))
	}
	if cfg.failures < 1 {
		problems = append(problems, fmt.Errorf("-breaker-failures: %d is not positive", cfg.failures))
	}
	if cfg.cooldown <= 0 {
		problems = append(problems, fmt.Errorf("-breaker-cooldown: %s is not positive", cfg.cooldown))
	}
	if cfg.trials < 1 {
		problems = append(problems, fmt.Errorf("-breaker-trials: %d is not positive", cfg.trials))
	}
	if cfg.bufferHigh < 1 {
		problems = append(problems, fmt.Errorf("-buffer-high: %d is not positive", cfg.bufferHigh))
	}
	if cfg.bufferLow < 0 || cfg.bufferLow >= cfg.bufferHigh {
		problems = append(problems, fmt.Errorf("-buffer-low: %d is outside 0..%d", cfg.bufferLow, cfg.bufferHigh-1))
	}
	if cfg.workers < 1 {
		problems = append(problems, fmt.Errorf("-workers: %d is not positive", cfg.workers))
	}
	if cfg.expectUnique < 0 {
		problems = append(problems, fmt.Errorf("-expect-unique: %d is negative", cfg.expectUnique))
	}
	if len(problems) > 0 {
		return config{}, errors.Join(problems...)
	}
	return cfg, nil
}
