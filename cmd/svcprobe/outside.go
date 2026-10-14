package main

import (
	"context"
	"fmt"

	"github.com/ErnestoCM-13/proyecto-sistema-resiliente-con-backpressure/internal/inventory"
)

type OutsideLoadReport struct {
	Machine             string  `json:"machine"`
	BaseURL             string  `json:"base_url"`
	Rate                int     `json:"rate_per_second"`
	Seconds             float64 `json:"seconds"`
	TimeoutSeconds      float64 `json:"request_timeout_seconds"`
	Sent                int     `json:"sent"`
	OK                  int     `json:"ok"`
	Overloaded          int     `json:"overloaded_503"`
	Timeout             int     `json:"timeout"`
	Unreachable         int     `json:"unreachable"`
	Protocol            int     `json:"protocol"`
	FailurePct          float64 `json:"failure_pct"`
	MaxLatenessMs       float64 `json:"max_launch_lateness_ms"`
	OKLatencyMs         Dist    `json:"ok_latency_ms"`
	OverloadedLatencyMs Dist    `json:"overloaded_503_latency_ms"`
}

func outsideLoad(ctx context.Context, client *inventory.Client, cfg config) (*OutsideLoadReport, error) {
	samples, err := fire(ctx, client, dueOffsets([]segment{{rate: cfg.loadRate, d: cfg.loadFor}}))
	if err != nil {
		return nil, err
	}
	step, err := measure(samples, 0, cfg.loadFor)
	if err != nil {
		return nil, err
	}
	return &OutsideLoadReport{
		Machine:             cfg.machine,
		BaseURL:             cfg.baseURL,
		Rate:                cfg.loadRate,
		Seconds:             cfg.loadFor.Seconds(),
		TimeoutSeconds:      cfg.timeout.Seconds(),
		Sent:                step.Sent,
		OK:                  step.OK,
		Overloaded:          step.Overloaded,
		Timeout:             step.Timeout,
		Unreachable:         step.Unreachable,
		Protocol:            step.Protocol,
		FailurePct:          step.FailurePct,
		MaxLatenessMs:       step.MaxLatenessMs,
		OKLatencyMs:         step.OKLatencyMs,
		OverloadedLatencyMs: step.OverloadedLatencyMs,
	}, nil
}

func printOutsideLoad(rep *OutsideLoadReport) {
	fmt.Printf("outside load: %d requests at %d/s for %.0fs, %.1f%% failed (%d overloaded, %d timeouts, %d unreachable, %d protocol)\n",
		rep.Sent, rep.Rate, rep.Seconds, rep.FailurePct, rep.Overloaded, rep.Timeout, rep.Unreachable, rep.Protocol)
	fmt.Printf("              ok p50 %.1f ms, ok p95 %.1f ms; the service's counters were left alone\n",
		rep.OKLatencyMs.P50, rep.OKLatencyMs.P95)
}
