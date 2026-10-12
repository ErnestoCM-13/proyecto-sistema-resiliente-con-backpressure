package main

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/ErnestoCM-13/proyecto-sistema-resiliente-con-backpressure/internal/inventory"
)

type Report struct {
	Machine        string    `json:"machine"`
	BaseURL        string    `json:"base_url"`
	StepSeconds    float64   `json:"measured_seconds_per_step"`
	SettleSeconds  float64   `json:"settle_seconds_per_step"`
	Runs           int       `json:"runs"`
	TimeoutSeconds float64   `json:"request_timeout_seconds"`
	Steps          []Step    `json:"steps"`
	Summary        []Summary `json:"summary_median_and_range_across_runs"`
}

type Step struct {
	Run                 int     `json:"run"`
	Rate                int     `json:"target_rate_per_second"`
	Sent                int     `json:"sent"`
	ServiceCounted      int     `json:"service_counted"`
	MaxLatenessMs       float64 `json:"max_launch_lateness_ms"`
	Measured            int     `json:"measured_after_settling"`
	OK                  int     `json:"ok"`
	Overloaded          int     `json:"overloaded_503"`
	Timeout             int     `json:"timeout"`
	Unreachable         int     `json:"unreachable"`
	Protocol            int     `json:"protocol"`
	FailurePct          float64 `json:"failure_pct"`
	OKPerSecond         float64 `json:"ok_per_second"`
	MaxInFlight         int     `json:"max_in_flight"`
	OKLatencyMs         Dist    `json:"ok_latency_ms"`
	OverloadedLatencyMs Dist    `json:"overloaded_503_latency_ms"`
}

type Summary struct {
	Rate          int    `json:"target_rate_per_second"`
	FailurePct    Spread `json:"failure_pct"`
	OKP50Ms       Spread `json:"ok_p50_ms"`
	OKP95Ms       Spread `json:"ok_p95_ms"`
	OverloadP50Ms Spread `json:"overloaded_503_p50_ms"`
	OKPerSecond   Spread `json:"ok_per_second"`
	MaxInFlight   Spread `json:"max_in_flight"`
}

func ladder(ctx context.Context, client *inventory.Client, svc *service, cfg config) (*Report, error) {
	rep := &Report{
		Machine:        cfg.machine,
		BaseURL:        cfg.baseURL,
		StepSeconds:    cfg.step.Seconds(),
		SettleSeconds:  cfg.settle.Seconds(),
		Runs:           cfg.runs,
		TimeoutSeconds: cfg.timeout.Seconds(),
	}

	for run := 1; run <= cfg.runs; run++ {
		for _, rate := range cfg.rates {
			if err := svc.reset(ctx); err != nil {
				return nil, err
			}
			samples, err := fire(ctx, client, dueOffsets([]segment{{rate: rate, d: cfg.settle + cfg.step}}))
			if err != nil {
				return nil, err
			}
			counted, err := svc.totalRequests(ctx)
			if err != nil {
				return nil, err
			}

			step, err := measure(samples, cfg.settle, cfg.step)
			if err != nil {
				return nil, fmt.Errorf("run %d at %d/s: %w", run, rate, err)
			}
			step.Run, step.Rate, step.ServiceCounted = run, rate, counted
			rep.Steps = append(rep.Steps, step)

			fmt.Printf("run %d  %4d/s  sent %5d (service counted %5d)  failed %5.1f%%  ok p50 %7.1f ms  ok p95 %7.1f ms\n",
				run, rate, step.Sent, step.ServiceCounted, step.FailurePct, step.OKLatencyMs.P50, step.OKLatencyMs.P95)
		}
	}

	for _, rate := range cfg.rates {
		rep.Summary = append(rep.Summary, summarize(rate, rep.Steps))
	}
	return rep, nil
}

func measure(samples []sample, settle, step time.Duration) (Step, error) {
	out := Step{Sent: len(samples), MaxInFlight: maxInFlight(samples)}

	var okLatencies, overloadedLatencies []time.Duration
	for _, s := range samples {
		out.MaxLatenessMs = max(out.MaxLatenessMs, ms(s.late))
		if s.offset < settle {
			continue
		}
		out.Measured++

		if s.err == nil {
			out.OK++
			okLatencies = append(okLatencies, s.latency)
			continue
		}
		var invErr *inventory.Error
		if !errors.As(s.err, &invErr) {
			return Step{}, fmt.Errorf("a failure the inventory client did not classify: %w", s.err)
		}
		switch invErr.Kind {
		case inventory.KindOverloaded:
			out.Overloaded++
			overloadedLatencies = append(overloadedLatencies, s.latency)
		case inventory.KindTimeout:
			out.Timeout++
		case inventory.KindUnreachable:
			out.Unreachable++
		case inventory.KindProtocol:
			out.Protocol++
		default:
			return Step{}, fmt.Errorf("an inventory failure of a kind this tool does not count: %w", s.err)
		}
	}

	if out.Measured > 0 {
		out.FailurePct = 100 * float64(out.Measured-out.OK) / float64(out.Measured)
	}
	out.OKPerSecond = float64(out.OK) / step.Seconds()
	out.OKLatencyMs = dist(okLatencies)
	out.OverloadedLatencyMs = dist(overloadedLatencies)
	return out, nil
}

func summarize(rate int, steps []Step) Summary {
	var failure, okP50, okP95, overloadP50, okPerSecond, inFlight []float64
	for _, s := range steps {
		if s.Rate != rate {
			continue
		}
		failure = append(failure, s.FailurePct)
		okPerSecond = append(okPerSecond, s.OKPerSecond)
		inFlight = append(inFlight, float64(s.MaxInFlight))
		if s.OKLatencyMs.N > 0 {
			okP50 = append(okP50, s.OKLatencyMs.P50)
			okP95 = append(okP95, s.OKLatencyMs.P95)
		}
		if s.OverloadedLatencyMs.N > 0 {
			overloadP50 = append(overloadP50, s.OverloadedLatencyMs.P50)
		}
	}
	return Summary{
		Rate:          rate,
		FailurePct:    spread(failure),
		OKP50Ms:       spread(okP50),
		OKP95Ms:       spread(okP95),
		OverloadP50Ms: spread(overloadP50),
		OKPerSecond:   spread(okPerSecond),
		MaxInFlight:   spread(inFlight),
	}
}

func printLadder(rep *Report) {
	fmt.Printf("\nmedian [min-max] across %d runs, %.0fs measured per step after %.0fs of settling\n\n",
		rep.Runs, rep.StepSeconds, rep.SettleSeconds)
	fmt.Printf("%6s  %-19s %-22s %-22s %-22s %-19s %-14s\n",
		"rate/s", "failed %", "ok p50 ms", "ok p95 ms", "503 p50 ms", "ok/s", "max in flight")
	for _, s := range rep.Summary {
		fmt.Printf("%6d  %-19s %-22s %-22s %-22s %-19s %-14s\n", s.Rate,
			s.FailurePct.format(1), s.OKP50Ms.format(1), s.OKP95Ms.format(1),
			s.OverloadP50Ms.format(1), s.OKPerSecond.format(1), s.MaxInFlight.format(0))
	}

	var agreed, timeouts, unreachable, protocol int
	var latest float64
	for _, s := range rep.Steps {
		if s.ServiceCounted == s.Sent {
			agreed++
		}
		timeouts += s.Timeout
		unreachable += s.Unreachable
		protocol += s.Protocol
		latest = max(latest, s.MaxLatenessMs)
	}
	fmt.Printf("\nfailures that were not a 503: %d timeouts, %d unreachable, %d protocol\n", timeouts, unreachable, protocol)
	fmt.Printf("instrument: the service counted exactly what was sent in %d of %d steps; the latest launch ran %.1f ms behind schedule\n",
		agreed, len(rep.Steps), latest)
}
