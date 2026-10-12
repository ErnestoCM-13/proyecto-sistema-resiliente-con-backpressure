package stream_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/ErnestoCM-13/proyecto-sistema-resiliente-con-backpressure/internal/aggregate"
	"github.com/ErnestoCM-13/proyecto-sistema-resiliente-con-backpressure/internal/dedup"
	"github.com/ErnestoCM-13/proyecto-sistema-resiliente-con-backpressure/internal/event"
	"github.com/ErnestoCM-13/proyecto-sistema-resiliente-con-backpressure/internal/stream"
	"github.com/redis/go-redis/v9"
)

const (
	testEvents      = 300
	perCombination  = testEvents / (len(event.Countries) * len(event.Types))
	testAmountCents = 1234
)

func TestRedeliveredMessagesDoNotMoveTheAggregate(t *testing.T) {
	ctx := context.Background()
	client, streamKey := newTestStream(t)

	publish(ctx, t, client, streamKey, testEvents)

	window := newWindow(t)
	agg := aggregate.New()

	dying := stream.NewReader(client, streamKey, testGroup, "worker-dies", testEvents, time.Second)
	if err := dying.EnsureGroup(ctx); err != nil {
		t.Fatalf("ensure group: %v", err)
	}
	firstPass := drain(ctx, t, dying, window, agg)
	if firstPass.applied != testEvents {
		t.Fatalf("first pass applied %d events, want %d", firstPass.applied, testEvents)
	}
	if firstPass.duplicates != 0 {
		t.Fatalf("first pass discarded %d duplicates, want 0", firstPass.duplicates)
	}

	before := agg.Snapshot()
	assertEvenlySpread(t, "after the first pass", before)

	pending, err := dying.Pending(ctx)
	if err != nil {
		t.Fatalf("pending: %v", err)
	}
	if pending != testEvents {
		t.Fatalf("%d messages are pending, want all %d", pending, testEvents)
	}

	taking := stream.NewReader(client, streamKey, testGroup, "worker-takes-over", testEvents, time.Second)
	claimed, err := taking.Claim(ctx, 0, testEvents)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if len(claimed) != testEvents {
		t.Fatalf("claimed %d messages, want all %d", len(claimed), testEvents)
	}

	second := apply(t, claimed, window, agg)
	if second.duplicates != testEvents {
		t.Fatalf("the redelivery produced %d duplicates, want all %d", second.duplicates, testEvents)
	}
	if second.applied != 0 {
		t.Fatalf("the redelivery applied %d events again, want 0", second.applied)
	}

	after := agg.Snapshot()
	assertSameAggregate(t, before, after)

	if err := taking.Ack(ctx, idsOf(claimed)); err != nil {
		t.Fatalf("ack: %v", err)
	}
	pending, err = taking.Pending(ctx)
	if err != nil {
		t.Fatalf("pending after ack: %v", err)
	}
	if pending != 0 {
		t.Fatalf("%d messages are still pending after the ack, want 0", pending)
	}
}

func TestAckedMessagesAreNotRedelivered(t *testing.T) {
	ctx := context.Background()
	client, streamKey := newTestStream(t)

	publish(ctx, t, client, streamKey, testEvents)

	window := newWindow(t)
	agg := aggregate.New()

	reader := stream.NewReader(client, streamKey, testGroup, "worker-acks", testEvents, time.Second)
	if err := reader.EnsureGroup(ctx); err != nil {
		t.Fatalf("ensure group: %v", err)
	}

	pass := drain(ctx, t, reader, window, agg)
	if pass.applied != testEvents {
		t.Fatalf("applied %d events, want %d", pass.applied, testEvents)
	}
	if err := reader.Ack(ctx, pass.ids); err != nil {
		t.Fatalf("ack: %v", err)
	}

	pending, err := reader.Pending(ctx)
	if err != nil {
		t.Fatalf("pending: %v", err)
	}
	if pending != 0 {
		t.Fatalf("%d messages are pending after acking everything, want 0", pending)
	}

	claimed, err := reader.Claim(ctx, 0, testEvents)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if len(claimed) != 0 {
		t.Fatalf("claimed %d messages from a fully acked group, want 0", len(claimed))
	}

	assertEvenlySpread(t, "after a clean run", agg.Snapshot())
}

const testGroup = "test-aggregators"

type outcome struct {
	applied    int
	duplicates int
	ids        []string
}

func drain(ctx context.Context, t *testing.T, reader *stream.Reader, window *dedup.Window, agg *aggregate.Aggregate) outcome {
	t.Helper()

	var result outcome
	for result.applied+result.duplicates < testEvents {
		messages, err := reader.Read(ctx)
		if err != nil {
			t.Fatalf("read: %v", err)
		}
		batch := apply(t, messages, window, agg)
		result.applied += batch.applied
		result.duplicates += batch.duplicates
		result.ids = append(result.ids, batch.ids...)
	}
	return result
}

func apply(t *testing.T, messages []stream.Message, window *dedup.Window, agg *aggregate.Aggregate) outcome {
	t.Helper()

	var result outcome
	nowNS := time.Now().UnixNano()
	for _, msg := range messages {
		var ev event.Event
		if err := json.Unmarshal(msg.Payload, &ev); err != nil {
			t.Fatalf("decoding %s: %v", msg.ID, err)
		}
		result.ids = append(result.ids, msg.ID)
		if window.Seen(ev.EventID, nowNS) {
			result.duplicates++
			continue
		}
		agg.Add(&ev)
		result.applied++
	}
	return result
}

func assertEvenlySpread(t *testing.T, when string, snap aggregate.Snapshot) {
	t.Helper()

	if snap.Total != testEvents {
		t.Fatalf("%s the aggregate holds %d events, want %d", when, snap.Total, testEvents)
	}
	for _, country := range event.Countries {
		for _, evType := range event.Types {
			got := snap.ByCountryType[string(country)][string(evType)]
			if got != int64(perCombination) {
				t.Fatalf("%s %s/%s holds %d events, want %d",
					when, country, evType, got, perCombination)
			}
		}
		cents := snap.CentsByCountry[string(country)][string(event.OrderPlaced)]
		if want := int64(perCombination * testAmountCents); cents != want {
			t.Fatalf("%s %s/order_placed holds %d cents, want %d", when, country, cents, want)
		}
		if cents := snap.CentsByCountry[string(country)][string(event.InventoryReserved)]; cents != 0 {
			t.Fatalf("%s %s/inventory_reserved holds %d cents, want 0", when, country, cents)
		}
	}
}

func assertSameAggregate(t *testing.T, before, after aggregate.Snapshot) {
	t.Helper()

	if before.Total != after.Total {
		t.Fatalf("the aggregate moved from %d to %d events", before.Total, after.Total)
	}
	for _, country := range event.Countries {
		for _, evType := range event.Types {
			b := before.ByCountryType[string(country)][string(evType)]
			a := after.ByCountryType[string(country)][string(evType)]
			if a != b {
				t.Fatalf("%s/%s moved from %d to %d events", country, evType, b, a)
			}
			bc := before.CentsByCountry[string(country)][string(evType)]
			ac := after.CentsByCountry[string(country)][string(evType)]
			if ac != bc {
				t.Fatalf("%s/%s moved from %d to %d cents", country, evType, bc, ac)
			}
		}
	}
}

func idsOf(messages []stream.Message) []string {
	ids := make([]string, 0, len(messages))
	for _, msg := range messages {
		ids = append(ids, msg.ID)
	}
	return ids
}

func newWindow(t *testing.T) *dedup.Window {
	t.Helper()
	window, err := dedup.NewWindow(5*time.Minute, testEvents*4)
	if err != nil {
		t.Fatalf("new window: %v", err)
	}
	return window
}

func newTestStream(t *testing.T) (*redis.Client, string) {
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
	return client, streamKey
}

func publish(ctx context.Context, t *testing.T, client *redis.Client, streamKey string, n int) {
	t.Helper()

	publisher := stream.NewPublisher(client, streamKey, 1)
	for i := range n {
		payload := testPayload(i)
		if err := publisher.Publish(ctx, stream.EntryID(int64(i+1)), payload, int64(i)); err != nil {
			t.Fatalf("publishing event %d: %v", i, err)
		}
	}
	if err := publisher.Flush(ctx); err != nil {
		t.Fatalf("flush: %v", err)
	}
	if rejected := publisher.Rejected(); rejected != 0 {
		t.Fatalf("%d publishes were rejected on a fresh stream, want 0", rejected)
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
