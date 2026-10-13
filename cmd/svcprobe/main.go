// Measures the inventory service as a black box.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/ErnestoCM-13/proyecto-sistema-resiliente-con-backpressure/internal/inventory"
)

const (
	maxRate    = 2000
	maxWorkers = 256
)

type mode string

const (
	modeLadder   mode = "ladder"
	modeRecovery mode = "recovery"
	modeBreaker  mode = "breaker"
)

var modes = [3]mode{modeLadder, modeRecovery, modeBreaker}

func parseMode(s string) (mode, error) {
	for _, m := range modes {
		if mode(s) == m {
			return m, nil
		}
	}
	return "", fmt.Errorf("unknown mode %q", s)
}

type config struct {
	mode    mode
	baseURL string
	runs    int
	timeout time.Duration
	machine string
	out     string

	rates  []int
	step   time.Duration
	settle time.Duration

	overloadRate int
	overload     time.Duration
	probeRate    int
	observe      time.Duration
	healthy      time.Duration

	workers      []int
	checkTimeout time.Duration
	failures     int
	cooldown     time.Duration
	trials       int
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "svcprobe: %v\n", err)
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

	timeout := cfg.timeout
	if cfg.mode == modeBreaker {
		timeout = cfg.checkTimeout
	}
	client, err := inventory.NewClient(cfg.baseURL, timeout)
	if err != nil {
		return err
	}
	svc := newService(cfg.baseURL)
	if err := svc.reset(ctx); err != nil {
		return fmt.Errorf("is inventory_service.py running at %s? %w", cfg.baseURL, err)
	}

	var report any
	switch cfg.mode {
	case modeLadder:
		rep, err := ladder(ctx, client, svc, cfg)
		if err != nil {
			return err
		}
		printLadder(rep)
		report = rep
	case modeRecovery:
		rep, err := recovery(ctx, client, svc, cfg)
		if err != nil {
			return err
		}
		printRecovery(rep)
		report = rep
	case modeBreaker:
		rep, err := breakerLadder(ctx, client, svc, cfg)
		if err != nil {
			return err
		}
		printBreaker(rep)
		report = rep
	default:
		return fmt.Errorf("mode %q has no experiment", cfg.mode)
	}

	if cfg.out != "" {
		b, err := json.MarshalIndent(report, "", "  ")
		if err != nil {
			return fmt.Errorf("encoding report: %w", err)
		}
		if err := os.WriteFile(cfg.out, append(b, '\n'), 0o644); err != nil {
			return fmt.Errorf("writing %s: %w", cfg.out, err)
		}
		fmt.Printf("\nreport %s\n", cfg.out)
	}
	return nil
}

func parseRates(s string) ([]int, error) {
	return parseList("-rates", s, maxRate)
}

func parseWorkers(s string) ([]int, error) {
	return parseList("-workers", s, maxWorkers)
}

func parseList(name, s string, highest int) ([]int, error) {
	var values []int
	var problems []error
	for _, part := range strings.Split(s, ",") {
		value, err := strconv.Atoi(strings.TrimSpace(part))
		switch {
		case err != nil:
			problems = append(problems, fmt.Errorf("%s: %q is not a whole number", name, part))
		case value < 1 || value > highest:
			problems = append(problems, fmt.Errorf("%s: %d is outside 1..%d", name, value, highest))
		default:
			values = append(values, value)
		}
	}
	if len(problems) > 0 {
		return nil, errors.Join(problems...)
	}
	return values, nil
}

func parseFlags() (config, error) {
	var cfg config
	var modeName, rates, workers string
	flag.StringVar(&modeName, "mode", string(modeLadder), "experiment to run: ladder, recovery or breaker")
	flag.StringVar(&cfg.baseURL, "url", "http://localhost:8090", "base address of the inventory service")
	flag.IntVar(&cfg.runs, "runs", 5, "how many times the experiment is repeated")
	flag.DurationVar(&cfg.timeout, "timeout", 5*time.Second, "ladder and recovery: per request; long on purpose, so slow answers are seen instead of cut")
	flag.StringVar(&cfg.machine, "machine", "", "where this was measured, which is part of the result")
	flag.StringVar(&cfg.out, "out", "data/svcprobe-{mode}.json", "path for the JSON report, {mode} is replaced; empty writes none")

	flag.StringVar(&rates, "rates", "12,30,60,90,116,180,250,400", "ladder: arrival rates to hold, in requests per second")
	flag.DurationVar(&cfg.step, "step", 15*time.Second, "ladder and breaker: how long each rate or worker count is measured")
	flag.DurationVar(&cfg.settle, "settle", 2*time.Second, "ladder: time at each rate before measuring starts; its requests are sent but not counted")

	flag.IntVar(&cfg.overloadRate, "overload-rate", 116, "recovery: arrival rate that puts the service under load")
	flag.DurationVar(&cfg.overload, "overload", 5*time.Second, "recovery: how long that load is held")
	flag.IntVar(&cfg.probeRate, "probe-rate", 20, "recovery: arrival rate of the probes sent once the load stops")
	flag.DurationVar(&cfg.observe, "observe", 4*time.Second, "recovery: how long the probes go on")
	flag.DurationVar(&cfg.healthy, "healthy", 100*time.Millisecond, "recovery: a probe is healthy when it succeeds within this long")

	flag.StringVar(&workers, "workers", "1,2,4,8,16", "breaker: how many callers run at once, one step per value")
	flag.DurationVar(&cfg.checkTimeout, "check-timeout", 100*time.Millisecond, "breaker: per request, the timeout the real system will use")
	flag.IntVar(&cfg.failures, "failures", 5, "breaker: consecutive failures that open it")
	flag.DurationVar(&cfg.cooldown, "cooldown", 2*time.Second, "breaker: how long it stays open")
	flag.IntVar(&cfg.trials, "trials", 3, "breaker: successful trial calls that close it again")
	flag.Parse()

	var problems []error
	parsedMode, err := parseMode(modeName)
	if err != nil {
		problems = append(problems, fmt.Errorf("-mode: %w", err))
	}
	cfg.mode = parsedMode
	cfg.out = strings.ReplaceAll(cfg.out, "{mode}", string(cfg.mode))

	if cfg.runs < 1 {
		problems = append(problems, fmt.Errorf("-runs: %d is not positive", cfg.runs))
	}
	if cfg.timeout <= 0 {
		problems = append(problems, fmt.Errorf("-timeout: %s is not positive", cfg.timeout))
	}
	if cfg.machine == "" {
		problems = append(problems, errors.New("-machine: empty"))
	}

	parsedRates, err := parseRates(rates)
	if err != nil {
		problems = append(problems, err)
	}
	cfg.rates = parsedRates
	if cfg.step < time.Second {
		problems = append(problems, fmt.Errorf("-step: %s is shorter than one second", cfg.step))
	}
	if cfg.settle < 0 {
		problems = append(problems, fmt.Errorf("-settle: %s is negative", cfg.settle))
	}

	if cfg.overloadRate < 1 || cfg.overloadRate > maxRate {
		problems = append(problems, fmt.Errorf("-overload-rate: %d is outside 1..%d", cfg.overloadRate, maxRate))
	}
	if cfg.overload < time.Second {
		problems = append(problems, fmt.Errorf("-overload: %s is shorter than one second", cfg.overload))
	}
	if cfg.probeRate < 1 || cfg.probeRate > maxRate {
		problems = append(problems, fmt.Errorf("-probe-rate: %d is outside 1..%d", cfg.probeRate, maxRate))
	}
	if cfg.observe < time.Second {
		problems = append(problems, fmt.Errorf("-observe: %s is shorter than one second", cfg.observe))
	}
	if cfg.healthy <= 0 {
		problems = append(problems, fmt.Errorf("-healthy: %s is not positive", cfg.healthy))
	}
	if cfg.healthy >= cfg.timeout && cfg.timeout > 0 {
		problems = append(problems, fmt.Errorf("-healthy: %s is not below the %s request timeout", cfg.healthy, cfg.timeout))
	}

	parsedWorkers, err := parseWorkers(workers)
	if err != nil {
		problems = append(problems, err)
	}
	cfg.workers = parsedWorkers
	if cfg.checkTimeout <= 0 {
		problems = append(problems, fmt.Errorf("-check-timeout: %s is not positive", cfg.checkTimeout))
	}
	if cfg.failures < 1 {
		problems = append(problems, fmt.Errorf("-failures: %d is not positive", cfg.failures))
	}
	if cfg.cooldown <= 0 {
		problems = append(problems, fmt.Errorf("-cooldown: %s is not positive", cfg.cooldown))
	}
	if cfg.trials < 1 {
		problems = append(problems, fmt.Errorf("-trials: %d is not positive", cfg.trials))
	}

	if len(problems) > 0 {
		return config{}, errors.Join(problems...)
	}
	return cfg, nil
}
