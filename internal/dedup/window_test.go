package dedup_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/ErnestoCM-13/proyecto-sistema-resiliente-con-backpressure/internal/dedup"
)

func TestNewWindowRejectsBadLimits(t *testing.T) {
	cases := []struct {
		name       string
		ttl        time.Duration
		maxEntries int
		wantErr    bool
	}{
		{name: "valid", ttl: time.Minute, maxEntries: 10},
		{name: "zero ttl", ttl: 0, maxEntries: 10, wantErr: true},
		{name: "negative ttl", ttl: -time.Second, maxEntries: 10, wantErr: true},
		{name: "zero entries", ttl: time.Minute, maxEntries: 0, wantErr: true},
		{name: "negative entries", ttl: time.Minute, maxEntries: -1, wantErr: true},
		{name: "both bad", ttl: 0, maxEntries: 0, wantErr: true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			w, err := dedup.NewWindow(tc.ttl, tc.maxEntries)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("NewWindow(%s, %d) succeeded, want an error", tc.ttl, tc.maxEntries)
				}
				return
			}
			if err != nil {
				t.Fatalf("NewWindow(%s, %d): %v", tc.ttl, tc.maxEntries, err)
			}
			if w.Len() != 0 {
				t.Fatalf("a new window holds %d ids, want 0", w.Len())
			}
		})
	}
}

func TestSeenDetectsRepeats(t *testing.T) {
	w := newWindow(t, time.Minute, 8)

	if w.Seen("a", 0) {
		t.Fatal(`first sighting of "a" reported as already seen`)
	}
	if !w.Seen("a", 0) {
		t.Fatal(`second sighting of "a" reported as new`)
	}
	if w.Seen("b", 0) {
		t.Fatal(`first sighting of "b" reported as already seen`)
	}
	if !w.Seen("b", 0) {
		t.Fatal(`second sighting of "b" at the same instant reported as new`)
	}
	if w.Len() != 2 {
		t.Fatalf("window holds %d ids, want 2", w.Len())
	}
}

func TestTimeBoundary(t *testing.T) {
	ttl := 5 * time.Minute
	cases := []struct {
		name          string
		age           time.Duration
		wantDuplicate bool
	}{
		{name: "just inside", age: ttl - time.Nanosecond, wantDuplicate: true},
		{name: "exactly at the ttl", age: ttl, wantDuplicate: true},
		{name: "one nanosecond past", age: ttl + time.Nanosecond, wantDuplicate: false},
		{name: "long past", age: 2 * ttl, wantDuplicate: false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			w := newWindow(t, ttl, 8)
			if w.Seen("a", 0) {
				t.Fatal("first sighting reported as already seen")
			}
			if got := w.Seen("a", tc.age.Nanoseconds()); got != tc.wantDuplicate {
				t.Fatalf("after %s, Seen = %v, want %v", tc.age, got, tc.wantDuplicate)
			}
		})
	}
}

func TestCapacityBoundary(t *testing.T) {
	const maxEntries = 4
	w := newWindow(t, time.Hour, maxEntries)

	for i := range maxEntries {
		if w.Seen(id(i), int64(i)) {
			t.Fatalf("first sighting of %s reported as already seen", id(i))
		}
	}
	if w.Len() != maxEntries {
		t.Fatalf("window holds %d ids, want %d", w.Len(), maxEntries)
	}

	if !w.Seen(id(0), int64(maxEntries)) {
		t.Fatalf("%s fell out of a window that is only just full", id(0))
	}

	if w.Seen(id(maxEntries), int64(maxEntries+1)) {
		t.Fatalf("first sighting of %s reported as already seen", id(maxEntries))
	}
	if w.Seen(id(0), int64(maxEntries+2)) {
		t.Fatalf("%s was still remembered after capacity eviction", id(0))
	}
	if w.Len() != maxEntries {
		t.Fatalf("window holds %d ids after eviction, want %d", w.Len(), maxEntries)
	}
	if w.EvictedByCapacity() == 0 {
		t.Fatal("capacity eviction happened but was not counted")
	}
	if w.EvictedByTime() != 0 {
		t.Fatalf("time evicted %d ids with an hour long ttl, want 0", w.EvictedByTime())
	}
}

func TestNeverExceedsCapacity(t *testing.T) {
	const maxEntries = 16
	w := newWindow(t, time.Hour, maxEntries)

	for i := range 10 * maxEntries {
		w.Seen(id(i), int64(i))
		if w.Len() > maxEntries {
			t.Fatalf("after %d ids the window holds %d, above the limit of %d", i+1, w.Len(), maxEntries)
		}
	}
	if got, want := w.EvictedByCapacity(), int64(10*maxEntries-maxEntries); got != want {
		t.Fatalf("evicted %d ids by capacity, want %d", got, want)
	}
}

func TestExpiryIsDrivenByArrivals(t *testing.T) {
	ttl := time.Minute
	w := newWindow(t, ttl, 8)

	for i := range 4 {
		w.Seen(id(i), int64(i))
	}
	if w.Len() != 4 {
		t.Fatalf("window holds %d ids, want 4", w.Len())
	}

	w.Seen("late", (2 * ttl).Nanoseconds())
	if w.Len() != 1 {
		t.Fatalf("window holds %d ids after a late arrival, want only the late one", w.Len())
	}
	if got, want := w.EvictedByTime(), int64(4); got != want {
		t.Fatalf("time evicted %d ids, want %d", got, want)
	}
}

func newWindow(t *testing.T, ttl time.Duration, maxEntries int) *dedup.Window {
	t.Helper()
	w, err := dedup.NewWindow(ttl, maxEntries)
	if err != nil {
		t.Fatalf("NewWindow(%s, %d): %v", ttl, maxEntries, err)
	}
	return w
}

func id(i int) string {
	return fmt.Sprintf("event-%04d", i)
}
