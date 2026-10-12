package main

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/ErnestoCM-13/proyecto-sistema-resiliente-con-backpressure/internal/inventory"
)

const (
	outcomeOK   = "ok"
	outcomeSlow = "slow"
)

type RecoveryReport struct {
	Machine         string        `json:"machine"`
	BaseURL         string        `json:"base_url"`
	OverloadRate    int           `json:"overload_rate_per_second"`
	OverloadSeconds float64       `json:"overload_seconds"`
	ProbeRate       int           `json:"probe_rate_per_second"`
	ObserveSeconds  float64       `json:"observe_seconds"`
	HealthyMs       float64       `json:"healthy_means_ok_within_ms"`
	TimeoutSeconds  float64       `json:"request_timeout_seconds"`
	Runs            []RecoveryRun `json:"runs"`
	NotRecovered    int           `json:"runs_that_did_not_recover_while_observed"`
	HealthyFromMs   Spread        `json:"healthy_from_ms_median_and_range_across_recovered_runs"`
}

type RecoveryRun struct {
	Run            int     `json:"run"`
	Sent           int     `json:"sent"`
	ServiceCounted int     `json:"service_counted"`
	MaxLatenessMs  float64 `json:"max_launch_lateness_ms"`
	Unhealthy      int     `json:"unhealthy_probes"`
	Recovered      bool    `json:"recovered_while_observed"`
	HealthyFromMs  float64 `json:"healthy_from_ms"`
	Probes         []Probe `json:"probes"`
}

type Probe struct {
	AtMs      float64 `json:"at_ms_after_the_load_stopped"`
	Outcome   string  `json:"outcome"`
	LatencyMs float64 `json:"latency_ms"`
}

func recovery(ctx context.Context, client *inventory.Client, svc *service, cfg config) (*RecoveryReport, error) {
	rep := &RecoveryReport{
		Machine:         cfg.machine,
		BaseURL:         cfg.baseURL,
		OverloadRate:    cfg.overloadRate,
		OverloadSeconds: cfg.overload.Seconds(),
		ProbeRate:       cfg.probeRate,
		ObserveSeconds:  cfg.observe.Seconds(),
		HealthyMs:       ms(cfg.healthy),
		TimeoutSeconds:  cfg.timeout.Seconds(),
	}

	load := []segment{{rate: cfg.overloadRate, d: cfg.overload}}
	loadCount := len(dueOffsets(load))
	offsets := dueOffsets(append(load, segment{rate: cfg.probeRate, d: cfg.observe}))
	probeInterval := time.Second / time.Duration(cfg.probeRate)

	var healthyFrom []float64
	for run := 1; run <= cfg.runs; run++ {
		if err := svc.reset(ctx); err != nil {
			return nil, err
		}
		samples, err := fire(ctx, client, offsets)
		if err != nil {
			return nil, err
		}
		counted, err := svc.totalRequests(ctx)
		if err != nil {
			return nil, err
		}

		result, err := analyzeRecovery(samples[loadCount:], cfg.overload, probeInterval, cfg.healthy)
		if err != nil {
			return nil, fmt.Errorf("run %d: %w", run, err)
		}
		result.Run, result.Sent, result.ServiceCounted = run, len(samples), counted
		for _, s := range samples {
			result.MaxLatenessMs = max(result.MaxLatenessMs, ms(s.late))
		}
		rep.Runs = append(rep.Runs, result)

		if result.Recovered {
			healthyFrom = append(healthyFrom, result.HealthyFromMs)
			fmt.Printf("run %d  sent %5d (service counted %5d)  %2d of %d probes unhealthy  healthy from %4.0f ms\n",
				run, result.Sent, result.ServiceCounted, result.Unhealthy, len(result.Probes), result.HealthyFromMs)
		} else {
			rep.NotRecovered++
			fmt.Printf("run %d  sent %5d (service counted %5d)  %2d of %d probes unhealthy  still unhealthy when the probes ended\n",
				run, result.Sent, result.ServiceCounted, result.Unhealthy, len(result.Probes))
		}
	}

	rep.HealthyFromMs = spread(healthyFrom)
	return rep, nil
}

func analyzeRecovery(probes []sample, loadEnd, probeInterval, healthy time.Duration) (RecoveryRun, error) {
	out := RecoveryRun{Probes: make([]Probe, 0, len(probes))}

	lastUnhealthy := -1
	for i, s := range probes {
		outcome, err := outcomeOf(s, healthy)
		if err != nil {
			return RecoveryRun{}, err
		}
		if outcome != outcomeOK {
			out.Unhealthy++
			lastUnhealthy = i
		}
		out.Probes = append(out.Probes, Probe{AtMs: ms(s.offset - loadEnd), Outcome: outcome, LatencyMs: ms(s.latency)})
	}

	out.Recovered = lastUnhealthy < len(probes)-1
	if out.Recovered {
		out.HealthyFromMs = ms(time.Duration(lastUnhealthy+1) * probeInterval)
	}
	return out, nil
}

func outcomeOf(s sample, healthy time.Duration) (string, error) {
	if s.err == nil {
		if s.latency <= healthy {
			return outcomeOK, nil
		}
		return outcomeSlow, nil
	}
	var invErr *inventory.Error
	if !errors.As(s.err, &invErr) {
		return "", fmt.Errorf("a failure the inventory client did not classify: %w", s.err)
	}
	switch invErr.Kind {
	case inventory.KindOverloaded:
		return "503", nil
	case inventory.KindTimeout, inventory.KindUnreachable, inventory.KindProtocol:
		return invErr.Kind.String(), nil
	default:
		return "", fmt.Errorf("an inventory failure of a kind this tool does not name: %w", s.err)
	}
}

func printRecovery(rep *RecoveryReport) {
	fmt.Printf("\n%d/s for %.0fs, then probes at %d/s for %.0fs; healthy means ok within %.0f ms\n",
		rep.OverloadRate, rep.OverloadSeconds, rep.ProbeRate, rep.ObserveSeconds, rep.HealthyMs)
	if rep.NotRecovered > 0 {
		fmt.Printf("%d of %d runs were still unhealthy when the probes ended\n", rep.NotRecovered, len(rep.Runs))
	}
	fmt.Printf("healthy from %s ms after the load stopped, median [min-max] across %d runs\n\n",
		rep.HealthyFromMs.format(0), rep.HealthyFromMs.Runs)

	probeIntervalMs := 1000 / float64(rep.ProbeRate)
	rows := 0
	for _, r := range rep.Runs {
		rows = max(rows, len(r.Probes))
	}
	if rep.NotRecovered == 0 {
		rows = min(rows, int(rep.HealthyFromMs.Max/probeIntervalMs)+3)
	}

	fmt.Printf("%7s", "t ms")
	for _, r := range rep.Runs {
		fmt.Printf("  %-11s", fmt.Sprintf("run %d", r.Run))
	}
	fmt.Println()
	for i := range rows {
		fmt.Printf("%7.0f", float64(i)*probeIntervalMs)
		for _, r := range rep.Runs {
			cell := ""
			if i < len(r.Probes) {
				cell = r.Probes[i].Outcome
			}
			fmt.Printf("  %-11s", cell)
		}
		fmt.Println()
	}

	var agreed int
	var latest float64
	for _, r := range rep.Runs {
		if r.ServiceCounted == r.Sent {
			agreed++
		}
		latest = max(latest, r.MaxLatenessMs)
	}
	fmt.Printf("\ninstrument: the service counted exactly what was sent in %d of %d runs; the latest launch ran %.1f ms behind schedule\n",
		agreed, len(rep.Runs), latest)
}
