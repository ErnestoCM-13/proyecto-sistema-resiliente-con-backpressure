package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/ErnestoCM-13/proyecto-sistema-resiliente-con-backpressure/internal/inventory"
)

const serviceCallTimeout = 5 * time.Second

type sample struct {
	offset  time.Duration
	late    time.Duration
	latency time.Duration
	err     error
}

type segment struct {
	rate int
	d    time.Duration
}

func dueOffsets(segments []segment) []time.Duration {
	var offsets []time.Duration
	var base time.Duration
	for _, seg := range segments {
		interval := time.Second / time.Duration(seg.rate)
		n := int(seg.d / interval)
		for i := range n {
			offsets = append(offsets, base+time.Duration(i)*interval)
		}
		base += seg.d
	}
	return offsets
}

func fire(ctx context.Context, client *inventory.Client, offsets []time.Duration) ([]sample, error) {
	samples := make([]sample, len(offsets))

	var wg sync.WaitGroup
	start := time.Now()
	for i, offset := range offsets {
		due := start.Add(offset)
		if err := sleepUntil(ctx, due); err != nil {
			wg.Wait()
			return nil, err
		}

		wg.Add(1)
		go func() {
			defer wg.Done()
			launched := time.Now()
			_, err := client.Check(ctx, fmt.Sprintf("probe-%06d", i), "MX")
			samples[i] = sample{
				offset:  launched.Sub(start),
				late:    launched.Sub(due),
				latency: time.Since(launched),
				err:     err,
			}
		}()
	}
	wg.Wait()

	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return samples, nil
}

func sleepUntil(ctx context.Context, t time.Time) error {
	wait := time.Until(t)
	if wait <= 0 {
		return ctx.Err()
	}
	timer := time.NewTimer(wait)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

type service struct {
	base string
	http *http.Client
}

func newService(base string) *service {
	return &service{base: base, http: &http.Client{}}
}

func (s *service) do(ctx context.Context, method, path string, into any) error {
	callCtx, cancel := context.WithTimeout(ctx, serviceCallTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(callCtx, method, s.base+path, nil)
	if err != nil {
		return fmt.Errorf("building %s %s: %w", method, path, err)
	}
	resp, err := s.http.Do(req)
	if err != nil {
		return fmt.Errorf("%s %s: %w", method, path, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("%s %s: status %d", method, path, resp.StatusCode)
	}
	if err := json.NewDecoder(resp.Body).Decode(into); err != nil {
		return fmt.Errorf("%s %s: decoding the answer: %w", method, path, err)
	}
	return nil
}

func (s *service) reset(ctx context.Context) error {
	var answer struct {
		Reset *bool `json:"reset"`
	}
	if err := s.do(ctx, http.MethodPost, "/reset", &answer); err != nil {
		return err
	}
	if answer.Reset == nil || !*answer.Reset {
		return fmt.Errorf("POST /reset: the service did not confirm the reset")
	}
	return nil
}

func (s *service) totalRequests(ctx context.Context) (int, error) {
	var answer struct {
		Total *int `json:"total_requests"`
	}
	if err := s.do(ctx, http.MethodGet, "/stats", &answer); err != nil {
		return 0, err
	}
	if answer.Total == nil {
		return 0, fmt.Errorf("GET /stats: no total_requests field")
	}
	return *answer.Total, nil
}
