package main

import (
	"cmp"
	"fmt"
	"math"
	"slices"
	"time"
)

type Dist struct {
	N   int     `json:"n"`
	Min float64 `json:"min"`
	P50 float64 `json:"p50"`
	P95 float64 `json:"p95"`
	P99 float64 `json:"p99"`
	Max float64 `json:"max"`
}

type Spread struct {
	Runs   int     `json:"runs_with_data"`
	Median float64 `json:"median"`
	Min    float64 `json:"min"`
	Max    float64 `json:"max"`
}

func (s Spread) format(decimals int) string {
	if s.Runs == 0 {
		return "-"
	}
	return fmt.Sprintf("%.*f [%.*f-%.*f]", decimals, s.Median, decimals, s.Min, decimals, s.Max)
}

func ms(d time.Duration) float64 {
	return float64(d) / float64(time.Millisecond)
}

func dist(latencies []time.Duration) Dist {
	if len(latencies) == 0 {
		return Dist{}
	}
	sorted := slices.Clone(latencies)
	slices.Sort(sorted)
	return Dist{
		N:   len(sorted),
		Min: ms(sorted[0]),
		P50: ms(percentile(sorted, 50)),
		P95: ms(percentile(sorted, 95)),
		P99: ms(percentile(sorted, 99)),
		Max: ms(sorted[len(sorted)-1]),
	}
}

func percentile(sorted []time.Duration, p float64) time.Duration {
	i := int(math.Ceil(p/100*float64(len(sorted)))) - 1
	i = min(max(i, 0), len(sorted)-1)
	return sorted[i]
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
	return Spread{Runs: len(sorted), Median: median, Min: sorted[0], Max: sorted[len(sorted)-1]}
}

func maxInFlight(samples []sample) int {
	type edge struct {
		at    time.Duration
		delta int
	}
	edges := make([]edge, 0, 2*len(samples))
	for _, s := range samples {
		edges = append(edges, edge{at: s.offset, delta: 1}, edge{at: s.offset + s.latency, delta: -1})
	}
	slices.SortFunc(edges, func(a, b edge) int {
		if c := cmp.Compare(a.at, b.at); c != 0 {
			return c
		}
		return cmp.Compare(a.delta, b.delta)
	})

	current, most := 0, 0
	for _, e := range edges {
		current += e.delta
		most = max(most, current)
	}
	return most
}
