package main

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/ErnestoCM-13/proyecto-sistema-resiliente-con-backpressure/internal/breaker"
	"github.com/ErnestoCM-13/proyecto-sistema-resiliente-con-backpressure/internal/inventory"
)

const halfOpenRetry = 10 * time.Millisecond

type BreakerReport struct {
	Machine          string           `json:"machine"`
	BaseURL          string           `json:"base_url"`
	CheckTimeoutMs   float64          `json:"check_timeout_ms"`
	FailureThreshold int              `json:"consecutive_failures_that_open"`
	CooldownSeconds  float64          `json:"cooldown_seconds"`
	HalfOpenTrials   int              `json:"half_open_trials"`
	StepSeconds      float64          `json:"seconds_per_step"`
	Runs             int              `json:"runs"`
	Steps            []BreakerStep    `json:"steps"`
	Summary          []BreakerSummary `json:"summary_median_and_range_across_runs"`
}

type BreakerStep struct {
	Run            int             `json:"run"`
	Workers        int             `json:"workers"`
	Calls          int             `json:"calls_let_through"`
	ServiceCounted int             `json:"service_counted"`
	NeverCounted   int             `json:"let_through_but_never_counted_by_the_service"`
	OK             int             `json:"ok"`
	Overloaded     int             `json:"overloaded_503"`
	Timeout        int             `json:"timeout"`
	Unreachable    int             `json:"unreachable"`
	Rejected       int             `json:"rejected_by_the_breaker"`
	FailurePct     float64         `json:"failure_pct_of_calls_let_through"`
	CallsPerSecond float64         `json:"calls_per_second"`
	OKPerSecond    float64         `json:"ok_per_second"`
	Opened         int             `json:"times_opened"`
	OpenSeconds    float64         `json:"seconds_open"`
	OKLatencyMs    Dist            `json:"ok_latency_ms"`
	Transitions    []TransitionLog `json:"transitions"`
}

type TransitionLog struct {
	AtMs float64 `json:"at_ms"`
	From string  `json:"from"`
	To   string  `json:"to"`
}

type BreakerSummary struct {
	Workers        int    `json:"workers"`
	CallsPerSecond Spread `json:"calls_per_second"`
	OKPerSecond    Spread `json:"ok_per_second"`
	FailurePct     Spread `json:"failure_pct_of_calls_let_through"`
	Opened         Spread `json:"times_opened"`
	OpenSeconds    Spread `json:"seconds_open"`
}

type tally struct {
	ok, overloaded, timeout, unreachable, rejected int
	okLatencies                                    []time.Duration
}

func breakerLadder(ctx context.Context, client *inventory.Client, svc *service, cfg config) (*BreakerReport, error) {
	rep := &BreakerReport{
		Machine:          cfg.machine,
		BaseURL:          cfg.baseURL,
		CheckTimeoutMs:   ms(cfg.checkTimeout),
		FailureThreshold: cfg.failures,
		CooldownSeconds:  cfg.cooldown.Seconds(),
		HalfOpenTrials:   cfg.trials,
		StepSeconds:      cfg.step.Seconds(),
		Runs:             cfg.runs,
	}

	for run := 1; run <= cfg.runs; run++ {
		for _, workers := range cfg.workers {
			if err := svc.reset(ctx); err != nil {
				return nil, err
			}
			step, err := breakerStep(ctx, client, cfg, workers)
			if err != nil {
				return nil, fmt.Errorf("run %d with %d workers: %w", run, workers, err)
			}
			counted, err := svc.totalRequests(ctx)
			if err != nil {
				return nil, err
			}
			step.Run, step.ServiceCounted = run, counted
			step.NeverCounted = step.Calls - step.Unreachable - counted
			rep.Steps = append(rep.Steps, step)

			fmt.Printf("run %d  %2d workers  %5d calls (service counted %5d)  ok %5.1f/s  failed %5.1f%%  opened %2d times, open %4.1fs\n",
				run, workers, step.Calls, step.ServiceCounted, step.OKPerSecond, step.FailurePct, step.Opened, step.OpenSeconds)
			if run == 1 {
				for _, tr := range step.Transitions {
					fmt.Printf("         +%7.3fs  %s -> %s\n", tr.AtMs/1000, tr.From, tr.To)
				}
			}
		}
	}

	for _, workers := range cfg.workers {
		rep.Summary = append(rep.Summary, summarizeBreaker(workers, rep.Steps))
	}
	return rep, nil
}

func breakerStep(ctx context.Context, client *inventory.Client, cfg config, workers int) (BreakerStep, error) {
	start := time.Now()
	deadline := start.Add(cfg.step)

	var transitions []TransitionLog
	guarded, err := inventory.NewGuarded(client, breaker.Options{
		FailureThreshold: cfg.failures,
		Cooldown:         cfg.cooldown,
		HalfOpenTrials:   cfg.trials,
		OnTransition: func(tr breaker.Transition) {
			transitions = append(transitions, TransitionLog{AtMs: ms(tr.At.Sub(start)), From: tr.From.String(), To: tr.To.String()})
		},
	})
	if err != nil {
		return BreakerStep{}, err
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	tallies := make([]tally, workers)
	failed := make([]error, workers)
	var wg sync.WaitGroup
	for w := range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := callUntil(ctx, guarded, deadline, &tallies[w]); err != nil {
				failed[w] = err
				cancel()
			}
		}()
	}
	wg.Wait()

	var firstErr error
	for _, err := range failed {
		if err != nil && (firstErr == nil || errors.Is(firstErr, context.Canceled)) {
			firstErr = err
		}
	}
	if firstErr != nil {
		return BreakerStep{}, firstErr
	}

	var total tally
	for _, t := range tallies {
		total.ok += t.ok
		total.overloaded += t.overloaded
		total.timeout += t.timeout
		total.unreachable += t.unreachable
		total.rejected += t.rejected
		total.okLatencies = append(total.okLatencies, t.okLatencies...)
	}

	stats := guarded.Stats()
	out := BreakerStep{
		Workers:     workers,
		Calls:       total.ok + total.overloaded + total.timeout + total.unreachable,
		OK:          total.ok,
		Overloaded:  total.overloaded,
		Timeout:     total.timeout,
		Unreachable: total.unreachable,
		Rejected:    total.rejected,
		Opened:      stats.Opened,
		OpenSeconds: stats.OpenFor.Seconds(),
		OKLatencyMs: dist(total.okLatencies),
		Transitions: transitions,
	}
	if out.Calls > 0 {
		out.FailurePct = 100 * float64(out.Calls-out.OK) / float64(out.Calls)
	}
	out.CallsPerSecond = float64(out.Calls) / cfg.step.Seconds()
	out.OKPerSecond = float64(out.OK) / cfg.step.Seconds()
	return out, nil
}

func callUntil(ctx context.Context, guarded *inventory.Guarded, deadline time.Time, t *tally) error {
	for time.Now().Before(deadline) {
		launched := time.Now()
		_, err := guarded.Check(ctx, "probe", "MX")
		if err == nil {
			t.ok++
			t.okLatencies = append(t.okLatencies, time.Since(launched))
			continue
		}

		var rejected *breaker.RejectedError
		if errors.As(err, &rejected) {
			t.rejected++
			retryAt := time.Now().Add(max(rejected.RetryAfter, halfOpenRetry))
			if retryAt.After(deadline) {
				retryAt = deadline
			}
			if err := sleepUntil(ctx, retryAt); err != nil {
				return err
			}
			continue
		}

		var invErr *inventory.Error
		if !errors.As(err, &invErr) {
			return err
		}
		switch invErr.Kind {
		case inventory.KindOverloaded:
			t.overloaded++
		case inventory.KindTimeout:
			t.timeout++
		case inventory.KindUnreachable:
			t.unreachable++
		default:
			return err
		}
	}
	return nil
}

func summarizeBreaker(workers int, steps []BreakerStep) BreakerSummary {
	var calls, ok, failure, opened, open []float64
	for _, s := range steps {
		if s.Workers != workers {
			continue
		}
		calls = append(calls, s.CallsPerSecond)
		ok = append(ok, s.OKPerSecond)
		failure = append(failure, s.FailurePct)
		opened = append(opened, float64(s.Opened))
		open = append(open, s.OpenSeconds)
	}
	return BreakerSummary{
		Workers:        workers,
		CallsPerSecond: spread(calls),
		OKPerSecond:    spread(ok),
		FailurePct:     spread(failure),
		Opened:         spread(opened),
		OpenSeconds:    spread(open),
	}
}

func printBreaker(rep *BreakerReport) {
	fmt.Printf("\nbreaker: %.0f ms timeout, opens after %d failures in a row, %.0fs cooldown, %d trials\n",
		rep.CheckTimeoutMs, rep.FailureThreshold, rep.CooldownSeconds, rep.HalfOpenTrials)
	fmt.Printf("median [min-max] across %d runs, %.0fs per step\n\n", rep.Runs, rep.StepSeconds)
	fmt.Printf("%7s  %-22s %-19s %-19s %-12s %-16s\n",
		"workers", "calls/s", "ok/s", "failed %", "opened", "seconds open")
	for _, s := range rep.Summary {
		fmt.Printf("%7d  %-22s %-19s %-19s %-12s %-16s\n", s.Workers,
			s.CallsPerSecond.format(1), s.OKPerSecond.format(1), s.FailurePct.format(1),
			s.Opened.format(0), s.OpenSeconds.format(1))
	}

	var agreed, neverCounted, timeouts, overloaded, unreachable int
	for _, s := range rep.Steps {
		if s.NeverCounted == 0 {
			agreed++
		}
		neverCounted += s.NeverCounted
		timeouts += s.Timeout
		overloaded += s.Overloaded
		unreachable += s.Unreachable
	}
	fmt.Printf("\nfailures by kind: %d timeouts, %d overloaded, %d unreachable\n", timeouts, overloaded, unreachable)
	fmt.Printf("instrument: the service counted exactly the calls the breaker let through in %d of %d steps; it never counted %d of them\n",
		agreed, len(rep.Steps), neverCounted)
}
