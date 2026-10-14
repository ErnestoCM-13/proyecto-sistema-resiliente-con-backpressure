package main

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/ErnestoCM-13/proyecto-sistema-resiliente-con-backpressure/internal/inventory"
)

func TestParseModeKnowsEveryMode(t *testing.T) {
	want := []mode{"ladder", "recovery", "breaker", "load"}
	if len(modes) != len(want) {
		t.Fatalf("there are %d modes, this test knows %d", len(modes), len(want))
	}
	for i, m := range want {
		if modes[i] != m {
			t.Errorf("mode %d is %q, want %q", i, modes[i], m)
		}
		got, err := parseMode(string(m))
		if err != nil || got != m {
			t.Errorf("parseMode(%q) = %q, %v, want it accepted", m, got, err)
		}
	}
}

func TestOutsideLoadSendsWhatItSaysAndNeverTouchesTheCounters(t *testing.T) {
	var mu sync.Mutex
	paths := map[string]int{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		paths[r.Method+" "+r.URL.Path]++
		n := paths["POST /check-inventory"]
		mu.Unlock()

		if r.URL.Path == "/check-inventory" && n%4 == 0 {
			w.WriteHeader(http.StatusServiceUnavailable)
			io.WriteString(w, `{"error": "overloaded"}`)
			return
		}
		io.WriteString(w, `{"available": true}`)
	}))
	t.Cleanup(srv.Close)

	cfg := config{
		baseURL:  srv.URL,
		machine:  "test",
		timeout:  2 * time.Second,
		loadRate: 100,
		loadFor:  time.Second,
	}
	client, err := inventory.NewClient(cfg.baseURL, cfg.timeout)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}

	rep, err := outsideLoad(context.Background(), client, cfg)
	if err != nil {
		t.Fatalf("outsideLoad: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(paths) != 1 || paths["POST /check-inventory"] != 100 {
		t.Fatalf("the service saw %v, want exactly 100 checks and nothing else", paths)
	}
	want := OutsideLoadReport{
		Machine: "test", BaseURL: srv.URL, Rate: 100, Seconds: 1, TimeoutSeconds: 2,
		Sent: 100, OK: 75, Overloaded: 25, FailurePct: 25,
	}
	got := *rep
	got.MaxLatenessMs, got.OKLatencyMs, got.OverloadedLatencyMs = 0, Dist{}, Dist{}
	if got != want {
		t.Fatalf("report =\n%+v\nwant\n%+v", got, want)
	}
	if rep.OKLatencyMs.N != 75 || rep.OverloadedLatencyMs.N != 25 {
		t.Fatalf("%d ok and %d overloaded latencies, want 75 and 25", rep.OKLatencyMs.N, rep.OverloadedLatencyMs.N)
	}
}
