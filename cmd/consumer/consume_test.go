package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/ErnestoCM-13/proyecto-sistema-resiliente-con-backpressure/internal/event"
	"github.com/ErnestoCM-13/proyecto-sistema-resiliente-con-backpressure/internal/inventory"
	"github.com/ErnestoCM-13/proyecto-sistema-resiliente-con-backpressure/internal/stream"
	"github.com/redis/go-redis/v9"
)

const (
	testEvents      = 300
	perCombination  = testEvents / (len(event.Countries) * len(event.Types))
	testReserved    = testEvents / len(event.Types)
	testAmountCents = 1234
	testGroup       = "test-aggregators"
	testBlock       = 50 * time.Millisecond
)

type answer struct {
	status int
	body   string
	delay  time.Duration
}

var available = answer{status: http.StatusOK, body: `{"available": true}`}

type inventoryStub struct {
	*httptest.Server
	mu       sync.Mutex
	calls    int
	perOrder map[string]int
}

func newInventoryStub(t *testing.T, decide func(call int, orderID string) answer) *inventoryStub {
	t.Helper()
	s := &inventoryStub{perOrder: map[string]int{}}
	release := make(chan struct{})
	s.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			OrderID string `json:"order_id"`
		}
		json.NewDecoder(r.Body).Decode(&body)

		s.mu.Lock()
		s.calls++
		call := s.calls
		s.perOrder[body.OrderID]++
		s.mu.Unlock()

		a := decide(call, body.OrderID)
		if a.delay > 0 {
			select {
			case <-time.After(a.delay):
			case <-release:
				return
			}
		}
		w.WriteHeader(a.status)
		io.WriteString(w, a.body)
	}))
	t.Cleanup(func() {
		close(release)
		s.Server.Close()
	})
	return s
}

func (s *inventoryStub) totals() (calls int, perOrder map[string]int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	perOrder = make(map[string]int, len(s.perOrder))
	for k, v := range s.perOrder {
		perOrder[k] = v
	}
	return s.calls, perOrder
}

type consumerFixture struct {
	client    *redis.Client
	streamKey string
	cfg       config
}

func newConsumerFixture(t *testing.T, inventoryURL string) *consumerFixture {
	t.Helper()

	if testing.Short() {
		t.Skip("skipping the Redis integration test in short mode")
	}
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		addr = "localhost:6379"
	}
	ctx := context.Background()
	client, err := stream.Connect(ctx, addr)
	if err != nil {
		t.Fatalf("this test needs Redis: %v", err)
	}
	streamKey := fmt.Sprintf("test:%s:%d", t.Name(), time.Now().UnixNano())
	t.Cleanup(func() {
		if err := client.Del(ctx, streamKey).Err(); err != nil {
			t.Errorf("removing %s: %v", streamKey, err)
		}
		client.Close()
	})

	return &consumerFixture{
		client:    client,
		streamKey: streamKey,
		cfg: config{
			streamKey:    streamKey,
			group:        testGroup,
			consumerName: "consumer-under-test",
			readCount:    10,
			idle:         150 * time.Millisecond,
			claimMinIdle: time.Hour,
			dedupTTL:     5 * time.Minute,
			dedupMax:     4 * testEvents,
			inventoryURL: inventoryURL,
			checkTimeout: 100 * time.Millisecond,
			failures:     5,
			cooldown:     60 * time.Millisecond,
			trials:       3,
			bufferHigh:   20,
			bufferLow:    5,
			workers:      1,
		},
	}
}

func testPayload(i int) []byte {
	country := event.Countries[i%len(event.Countries)]
	evType := event.Types[i%len(event.Types)]

	amount := fmt.Sprintf("%.2f", float64(testAmountCents)/100)
	if evType == event.InventoryReserved {
		amount = "null"
	}
	return []byte(fmt.Sprintf(
		`{"event_id": "event-%04d", "event_type": %q, "order_id": "order-%04d", `+
			`"country": %q, "amount_usd": %s, "timestamp": "2026-11-14T00:00:00"}`,
		i, evType, i, country, amount))
}

func (f *consumerFixture) publish(t *testing.T, events []int) {
	t.Helper()
	ctx := context.Background()
	publisher := stream.NewPublisher(f.client, f.streamKey, 1)
	for line, i := range events {
		if err := publisher.Publish(ctx, stream.EntryID(int64(line+1)), testPayload(i), 0); err != nil {
			t.Fatalf("publishing event %d: %v", i, err)
		}
	}
}

func oneOfEach() []int {
	events := make([]int, testEvents)
	for i := range events {
		events[i] = i
	}
	return events
}

func eachTwiceInARow() []int {
	events := make([]int, 0, 2*testEvents)
	for i := range testEvents {
		events = append(events, i, i)
	}
	return events
}

func (f *consumerFixture) reader(name string) *stream.Reader {
	return stream.NewReader(f.client, f.streamKey, f.cfg.group, name, f.cfg.readCount, testBlock)
}

func (f *consumerFixture) consume(t *testing.T, ctx context.Context) (*Report, error) {
	t.Helper()
	reader := f.reader(f.cfg.consumerName)
	if err := reader.EnsureGroup(ctx); err != nil {
		t.Fatalf("ensure group: %v", err)
	}
	p, err := newPipeline(reader, f.cfg, newTimeline(io.Discard))
	if err != nil {
		t.Fatalf("newPipeline: %v", err)
	}
	return consume(ctx, p, f.cfg)
}

func (f *consumerFixture) pending(t *testing.T) int64 {
	t.Helper()
	pending, err := f.reader("inspector").Pending(context.Background())
	if err != nil {
		t.Fatalf("pending: %v", err)
	}
	return pending
}

func assertEveryEventOnce(t *testing.T, rep *Report) {
	t.Helper()
	snap := rep.Aggregate
	if snap.Total != testEvents {
		t.Fatalf("the aggregate holds %d events, want %d", snap.Total, testEvents)
	}
	for _, country := range event.Countries {
		for _, evType := range event.Types {
			if got := snap.ByCountryType[string(country)][string(evType)]; got != int64(perCombination) {
				t.Fatalf("%s/%s holds %d events, want %d", country, evType, got, perCombination)
			}
		}
		cents := snap.CentsByCountry[string(country)][string(event.OrderPlaced)]
		if want := int64(perCombination * testAmountCents); cents != want {
			t.Fatalf("%s/order_placed holds %d cents, want %d", country, cents, want)
		}
	}
	if rep.Count.Aggregated != testEvents || rep.Count.ReadNotFinished != 0 || rep.Count.StillInTheBuffer != 0 {
		t.Fatalf("count = %+v, want %d aggregated and nothing left behind", rep.Count, testEvents)
	}
}

func assertEveryReservationCheckedOnce(t *testing.T, stub *inventoryStub, wantCalls int) {
	t.Helper()
	calls, perOrder := stub.totals()
	if calls != wantCalls {
		t.Errorf("the inventory service received %d calls, want %d", calls, wantCalls)
	}
	if len(perOrder) != testReserved {
		t.Fatalf("the inventory service was asked about %d orders, want %d", len(perOrder), testReserved)
	}
	for i := range testEvents {
		if event.Types[i%len(event.Types)] != event.InventoryReserved {
			continue
		}
		order := fmt.Sprintf("order-%04d", i)
		if _, asked := perOrder[order]; !asked {
			t.Fatalf("the inventory service was never asked about %s", order)
		}
	}
}

func TestConsumeVerifiesEveryReservationAndAcknowledgesEverything(t *testing.T) {
	stub := newInventoryStub(t, func(int, string) answer { return available })
	f := newConsumerFixture(t, stub.URL)
	f.publish(t, oneOfEach())

	rep, err := f.consume(t, context.Background())
	if err != nil {
		t.Fatalf("consume: %v", err)
	}

	assertEveryEventOnce(t, rep)
	assertEveryReservationCheckedOnce(t, stub, testReserved)
	if rep.Consumed != testEvents || rep.DuplicatesDiscarded != 0 || rep.Claimed != 0 {
		t.Fatalf("consumed %d, duplicates %d, claimed %d, want %d, 0 and 0",
			rep.Consumed, rep.DuplicatesDiscarded, rep.Claimed, testEvents)
	}
	want := InventoryReport{Verified: int64(testReserved), Available: int64(testReserved), Calls: int64(testReserved)}
	got := rep.Inventory
	got.LatencyMs = Dist{}
	if got != want {
		t.Fatalf("inventory = %+v, want %+v", got, want)
	}
	if rep.Breaker.TimesOpened != 0 {
		t.Fatalf("the breaker opened %d times against a healthy service, want 0", rep.Breaker.TimesOpened)
	}
	if pending := f.pending(t); pending != 0 {
		t.Fatalf("%d messages are still unacknowledged, want 0", pending)
	}
}

func TestConsumeNeverAsksTheServiceAboutADuplicate(t *testing.T) {
	stub := newInventoryStub(t, func(int, string) answer { return available })
	f := newConsumerFixture(t, stub.URL)
	f.publish(t, eachTwiceInARow())

	rep, err := f.consume(t, context.Background())
	if err != nil {
		t.Fatalf("consume: %v", err)
	}

	assertEveryEventOnce(t, rep)
	assertEveryReservationCheckedOnce(t, stub, testReserved)
	if rep.Consumed != 2*testEvents || rep.DuplicatesDiscarded != testEvents {
		t.Fatalf("consumed %d and discarded %d duplicates, want %d and %d",
			rep.Consumed, rep.DuplicatesDiscarded, 2*testEvents, testEvents)
	}
	if pending := f.pending(t); pending != 0 {
		t.Fatalf("%d messages are still unacknowledged, want 0", pending)
	}
}

func TestConsumeLosesNothingWhileTheServiceFails(t *testing.T) {
	overloaded := answer{status: http.StatusServiceUnavailable, body: `{"error": "overloaded"}`}
	slow := answer{status: http.StatusOK, body: `{"available": true}`, delay: time.Second}

	cases := []struct {
		name            string
		failing         answer
		wantOverloaded  int64
		wantTimeouts    int64
		wantFailedCalls int
	}{
		{name: "it answers 503", failing: overloaded, wantOverloaded: 12, wantFailedCalls: 12},
		{name: "it answers too slowly", failing: slow, wantTimeouts: 12, wantFailedCalls: 12},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			stub := newInventoryStub(t, func(call int, _ string) answer {
				if call > 10 && call <= 10+tc.wantFailedCalls {
					return tc.failing
				}
				return available
			})
			f := newConsumerFixture(t, stub.URL)
			f.publish(t, oneOfEach())

			rep, err := f.consume(t, context.Background())
			if err != nil {
				t.Fatalf("consume: %v", err)
			}

			assertEveryEventOnce(t, rep)
			assertEveryReservationCheckedOnce(t, stub, testReserved+tc.wantFailedCalls)
			if rep.Inventory.Overloaded != tc.wantOverloaded || rep.Inventory.Timeouts != tc.wantTimeouts {
				t.Fatalf("failed calls: %d overloaded and %d timeouts, want %d and %d",
					rep.Inventory.Overloaded, rep.Inventory.Timeouts, tc.wantOverloaded, tc.wantTimeouts)
			}
			if rep.Inventory.Verified != int64(testReserved) || rep.Inventory.Calls != int64(testReserved+tc.wantFailedCalls) {
				t.Fatalf("verified %d with %d calls, want %d with %d",
					rep.Inventory.Verified, rep.Inventory.Calls, testReserved, testReserved+tc.wantFailedCalls)
			}
			if rep.Breaker.TimesOpened != 8 {
				t.Fatalf("the breaker opened %d times, want 8: once after five failures and once for each of the seven failed trials",
					rep.Breaker.TimesOpened)
			}
			if rep.Inventory.Rejected == 0 {
				t.Fatal("no call was rejected while the breaker was open")
			}
			if pending := f.pending(t); pending != 0 {
				t.Fatalf("%d messages are still unacknowledged, want 0", pending)
			}
		})
	}
}

func TestConsumeStopsReadingWhenTheWaitBufferFills(t *testing.T) {
	stub := newInventoryStub(t, func(int, string) answer {
		return answer{status: http.StatusOK, body: `{"available": true}`, delay: 2 * time.Millisecond}
	})
	f := newConsumerFixture(t, stub.URL)
	f.cfg.bufferHigh, f.cfg.bufferLow, f.cfg.readCount = 8, 2, 12
	f.publish(t, oneOfEach())

	rep, err := f.consume(t, context.Background())
	if err != nil {
		t.Fatalf("consume: %v", err)
	}

	assertEveryEventOnce(t, rep)
	assertEveryReservationCheckedOnce(t, stub, testReserved)

	bp := rep.Backpressure
	if bp.TimesEngaged == 0 {
		t.Fatal("backpressure never engaged although the service is slower than the stream")
	}
	if most := bp.High - 1 + int(f.cfg.readCount); bp.MaxDepth > most {
		t.Fatalf("the wait buffer reached %d, above the %d that one read past the high mark allows", bp.MaxDepth, most)
	}
	if bp.MaxDepth < bp.High {
		t.Fatalf("the wait buffer reached %d, yet backpressure engages at %d", bp.MaxDepth, bp.High)
	}
	if bp.High != 8 || bp.Low != 2 || bp.Capacity != 20 {
		t.Fatalf("backpressure settings in the report = %+v, want 8, 2 and a capacity of 20", bp)
	}

	on, off := 0, 0
	for _, entry := range rep.Timeline {
		switch {
		case len(entry.What) >= 15 && entry.What[:15] == "backpressure on":
			on++
		case len(entry.What) >= 16 && entry.What[:16] == "backpressure off":
			off++
		}
	}
	if on != bp.TimesEngaged || off != on {
		t.Fatalf("the timeline shows backpressure on %d times and off %d times, the buffer counted %d", on, off, bp.TimesEngaged)
	}
}

func TestConsumeDoesNotTakeItsOwnWaitingWorkForAbandoned(t *testing.T) {
	stub := newInventoryStub(t, func(int, string) answer {
		return answer{status: http.StatusOK, body: `{"available": true}`, delay: 3 * time.Millisecond}
	})
	f := newConsumerFixture(t, stub.URL)
	f.cfg.claimMinIdle = time.Millisecond
	f.cfg.bufferHigh, f.cfg.bufferLow, f.cfg.readCount = 200, 50, 100
	f.publish(t, oneOfEach())

	rep, err := f.consume(t, context.Background())
	if err != nil {
		t.Fatalf("consume: %v", err)
	}

	assertEveryEventOnce(t, rep)
	assertEveryReservationCheckedOnce(t, stub, testReserved)
	if rep.Claimed != 0 || rep.DuplicatesDiscarded != 0 {
		t.Fatalf("claimed %d of its own messages and discarded %d as duplicates, want 0 and 0",
			rep.Claimed, rep.DuplicatesDiscarded)
	}
}

func TestConsumeTakesOverWhatADeadConsumerLeft(t *testing.T) {
	stub := newInventoryStub(t, func(int, string) answer { return available })
	f := newConsumerFixture(t, stub.URL)
	f.cfg.claimMinIdle = time.Millisecond
	f.publish(t, oneOfEach())

	ctx := context.Background()
	dead := f.reader("consumer-that-died")
	if err := dead.EnsureGroup(ctx); err != nil {
		t.Fatalf("ensure group: %v", err)
	}
	taken := 0
	for taken < 40 {
		messages, err := dead.Read(ctx)
		if err != nil {
			t.Fatalf("the consumer that dies: %v", err)
		}
		taken += len(messages)
	}

	rep, err := f.consume(t, ctx)
	if err != nil {
		t.Fatalf("consume: %v", err)
	}

	assertEveryEventOnce(t, rep)
	assertEveryReservationCheckedOnce(t, stub, testReserved)
	if rep.Claimed != int64(taken) {
		t.Fatalf("claimed %d messages, want the %d the dead consumer had read", rep.Claimed, taken)
	}
	if pending := f.pending(t); pending != 0 {
		t.Fatalf("%d messages are still unacknowledged, want 0", pending)
	}
}

func TestConsumeCountsAReservationTheServiceSaysIsNotAvailable(t *testing.T) {
	stub := newInventoryStub(t, func(_ int, orderID string) answer {
		if orderID == "order-0002" || orderID == "order-0005" {
			return answer{status: http.StatusOK, body: `{"available": false}`}
		}
		return available
	})
	f := newConsumerFixture(t, stub.URL)
	f.publish(t, oneOfEach())

	rep, err := f.consume(t, context.Background())
	if err != nil {
		t.Fatalf("consume: %v", err)
	}

	assertEveryEventOnce(t, rep)
	if rep.Inventory.Available != int64(testReserved-2) || rep.Inventory.Unavailable != 2 {
		t.Fatalf("%d available and %d not available, want %d and 2",
			rep.Inventory.Available, rep.Inventory.Unavailable, testReserved-2)
	}
}

func TestConsumeStopsOnAnAnswerItDoesNotUnderstandAndKeepsTheEventUnacknowledged(t *testing.T) {
	stub := newInventoryStub(t, func(call int, _ string) answer {
		if call == 7 {
			return answer{status: http.StatusNotFound, body: `{"error": "not found"}`}
		}
		return available
	})
	f := newConsumerFixture(t, stub.URL)
	f.publish(t, oneOfEach())

	rep, err := f.consume(t, context.Background())

	var invErr *inventory.Error
	if !errors.As(err, &invErr) || invErr.Kind != inventory.KindProtocol {
		t.Fatalf("consume error = %v, want an inventory error of kind %s", err, inventory.KindProtocol)
	}
	if rep.Inventory.Verified != 6 {
		t.Fatalf("%d reservations were verified before the stop, want the 6 that were answered", rep.Inventory.Verified)
	}
	if calls, _ := stub.totals(); calls != 7 {
		t.Fatalf("the inventory service received %d calls, want 7: nothing is retried after such an answer", calls)
	}

	pending := f.pending(t)
	if pending != rep.Count.ReadNotFinished || pending == 0 {
		t.Fatalf("%d messages are unacknowledged and %d events were read but not aggregated, want them equal and above zero",
			pending, rep.Count.ReadNotFinished)
	}
}

func TestConsumeInterruptedLeavesUnfinishedWorkUnacknowledged(t *testing.T) {
	stub := newInventoryStub(t, func(int, string) answer {
		return answer{status: http.StatusOK, body: `{"available": true}`, delay: 5 * time.Millisecond}
	})
	f := newConsumerFixture(t, stub.URL)
	f.publish(t, oneOfEach())

	ctx, cancel := context.WithCancel(context.Background())
	time.AfterFunc(120*time.Millisecond, cancel)

	rep, err := f.consume(t, ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("consume error = %v, want context.Canceled", err)
	}
	if rep.Count.Aggregated == 0 || rep.Count.Aggregated == testEvents {
		t.Fatalf("%d events were aggregated, want the run to be cut part of the way", rep.Count.Aggregated)
	}

	calls, _ := stub.totals()
	if verified := rep.Inventory.Verified; int64(calls) != verified && int64(calls) != verified+1 {
		t.Fatalf("%d calls reached the service and %d reservations were verified, want at most one call cut short", calls, verified)
	}

	second, err := f.consumeAgain(t)
	if err != nil {
		t.Fatalf("the consumer that takes over: %v", err)
	}
	if total := rep.Count.Aggregated + second.Count.Aggregated; total != testEvents {
		t.Fatalf("the two runs aggregated %d + %d = %d events, want %d",
			rep.Count.Aggregated, second.Count.Aggregated, total, testEvents)
	}
	if pending := f.pending(t); pending != 0 {
		t.Fatalf("%d messages are still unacknowledged after the second run, want 0", pending)
	}
}

func (f *consumerFixture) consumeAgain(t *testing.T) (*Report, error) {
	t.Helper()
	f.cfg.consumerName = "consumer-that-takes-over"
	f.cfg.claimMinIdle = time.Millisecond
	return f.consume(t, context.Background())
}
