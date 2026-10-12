// Remembers which event ids have already been processed.
package dedup

import (
	"errors"
	"fmt"
	"time"
)

type Window struct {
	ttlNS      int64
	maxEntries int

	seen map[string]struct{}

	ring  []entry
	head  int
	count int

	evictedByTime     int64
	evictedByCapacity int64
}

type entry struct {
	id       string
	insertNS int64
}

func NewWindow(ttl time.Duration, maxEntries int) (*Window, error) {
	var problems []error
	if ttl <= 0 {
		problems = append(problems, fmt.Errorf("ttl: %s is not positive", ttl))
	}
	if maxEntries <= 0 {
		problems = append(problems, fmt.Errorf("max entries: %d is not positive", maxEntries))
	}
	if len(problems) > 0 {
		return nil, errors.Join(problems...)
	}
	return &Window{
		ttlNS:      ttl.Nanoseconds(),
		maxEntries: maxEntries,
		seen:       make(map[string]struct{}, maxEntries),
		ring:       make([]entry, maxEntries),
	}, nil
}

func (w *Window) Seen(id string, nowNS int64) bool {
	w.expire(nowNS)

	if _, ok := w.seen[id]; ok {
		return true
	}

	if w.count == w.maxEntries {
		w.dropOldest()
		w.evictedByCapacity++
	}
	w.ring[(w.head+w.count)%w.maxEntries] = entry{id: id, insertNS: nowNS}
	w.count++
	w.seen[id] = struct{}{}
	return false
}

func (w *Window) expire(nowNS int64) {
	for w.count > 0 && nowNS-w.ring[w.head].insertNS > w.ttlNS {
		w.dropOldest()
		w.evictedByTime++
	}
}

func (w *Window) dropOldest() {
	delete(w.seen, w.ring[w.head].id)
	w.ring[w.head] = entry{}
	w.head = (w.head + 1) % w.maxEntries
	w.count--
}

func (w *Window) Len() int {
	return w.count
}

func (w *Window) EvictedByTime() int64 {
	return w.evictedByTime
}

func (w *Window) EvictedByCapacity() int64 {
	return w.evictedByCapacity
}
