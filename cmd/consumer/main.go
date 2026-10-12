// Reads the Hot Sale stream, discards event ids it has already processed, and keeps the per country and per type aggregate the
// operations team watches.
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

	fmt.Printf("consuming stream %s as %s/%s, stopping after %s idle\n",
		cfg.streamKey, cfg.group, cfg.consumerName, cfg.idle)

	rep, err := consume(ctx, reader, cfg)
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
	Dedup               DedupReport        `json:"dedup"`
	Aggregate           aggregate.Snapshot `json:"aggregate"`
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

func consume(ctx context.Context, reader *stream.Reader, cfg config) (*Report, error) {
	window, err := dedup.NewWindow(cfg.dedupTTL, cfg.dedupMax)
	if err != nil {
		return nil, err
	}

	agg := aggregate.New()
	rep := &Report{Aggregate: agg.Snapshot()}

	var latencies []int64
	ids := make([]string, 0, cfg.readCount)
	var lastMessage time.Time
	var start time.Time

	for {
		messages, err := reader.Read(ctx)
		if err != nil {
			if !errors.Is(err, stream.ErrNoMessages) {
				return rep, err
			}
			messages, err = reader.Claim(ctx, cfg.claimMinIdle, cfg.readCount)
			if err != nil {
				return rep, err
			}
			rep.Claimed += int64(len(messages))
		}

		if len(messages) == 0 {
			if lastMessage.IsZero() || time.Since(lastMessage) < cfg.idle {
				continue
			}
			pending, err := reader.Pending(ctx)
			if err != nil {
				return rep, err
			}
			if pending == 0 {
				break
			}
			continue
		}

		nowNS, err := clock.NowNS()
		if err != nil {
			return rep, err
		}
		now := time.Now()
		if start.IsZero() {
			start = now
		}
		lastMessage = now

		ids = ids[:0]
		for _, msg := range messages {
			latencies = append(latencies, nowNS-msg.PubNS)
			ids = append(ids, msg.ID)

			var ev event.Event
			if err := json.Unmarshal(msg.Payload, &ev); err != nil {
				rep.ParseErrors++
				if len(rep.ParseErrorSamples) < 10 {
					rep.ParseErrorSamples = append(rep.ParseErrorSamples,
						fmt.Sprintf("message %s: %v", msg.ID, err))
				}
				continue
			}
			rep.Consumed++

			if window.Seen(ev.EventID, nowNS) {
				rep.DuplicatesDiscarded++
				continue
			}
			agg.Add(&ev)
			if rep.Consumed%progressEvery == 0 {
				fmt.Printf("  %d events\n", rep.Consumed)
			}
		}

		if err := reader.Ack(ctx, ids); err != nil {
			return rep, err
		}
	}

	if !start.IsZero() {
		rep.DurationSeconds = lastMessage.Sub(start).Seconds()
	}
	if rep.DurationSeconds > 0 {
		rep.EventsPerSecond = float64(rep.Consumed) / rep.DurationSeconds
	}
	rep.LatencyMs = dist(latencies)
	rep.Aggregate = agg.Snapshot()

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
	return rep, nil
}

func printReport(rep *Report) {
	fmt.Printf("\nconsumed        %d messages in %.1fs (%.0f ev/s)\n",
		rep.Consumed, rep.DurationSeconds, rep.EventsPerSecond)
	if rep.Claimed > 0 {
		fmt.Printf("claimed         %d messages another consumer had left unacknowledged\n", rep.Claimed)
	}
	fmt.Printf("duplicates      %d discarded, %d unique events aggregated\n",
		rep.DuplicatesDiscarded, rep.Consumed-rep.DuplicatesDiscarded)
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
	flag.Int64Var(&cfg.readCount, "read-count", 1000, "messages per XREADGROUP call")
	flag.DurationVar(&cfg.idle, "idle", 5*time.Second, "stop after this long without new messages")
	flag.DurationVar(&cfg.claimMinIdle, "claim-min-idle", 30*time.Second,
		"take over messages another consumer has left unacknowledged for this long")
	flag.DurationVar(&cfg.dedupTTL, "dedup-ttl", 5*time.Minute, "how far back the deduplication window remembers")
	flag.IntVar(&cfg.dedupMax, "dedup-max", 3_200_000, "how many event ids the window may hold")
	flag.StringVar(&cfg.out, "out", "data/consumer-report.json", "path for the JSON report; empty writes none")
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
	if len(problems) > 0 {
		return config{}, errors.Join(problems...)
	}
	return cfg, nil
}
