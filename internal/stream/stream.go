// Carries Hot Sale events over a Redis stream.
package stream

import (
	"context"
	"errors"
	"fmt"
	"net"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	FieldPayload = "p"
	FieldPubNS   = "t"
)

var (
	ErrNoMessages   = errors.New("no messages within the block timeout")
	ErrBlockOverran = errors.New("redis did not answer the blocking read, not even to say the block had ended")
)

func EntryID(line int64) string {
	return "1-" + strconv.FormatInt(line, 10)
}

func isAlreadyPublished(err error) bool {
	return err != nil &&
		strings.Contains(err.Error(), "equal or smaller than the target stream top item")
}

func Connect(ctx context.Context, addr string) (*redis.Client, error) {
	client := redis.NewClient(&redis.Options{Addr: addr})
	pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := client.Ping(pingCtx).Err(); err != nil {
		client.Close()
		return nil, fmt.Errorf("redis at %s: %w", addr, err)
	}
	return client, nil
}

type Publisher struct {
	client    *redis.Client
	stream    string
	pipe      redis.Pipeliner
	batchSize int
	pending   int
	rejected  int64
}

func NewPublisher(client *redis.Client, stream string, batchSize int) *Publisher {
	p := &Publisher{client: client, stream: stream, batchSize: batchSize}
	if batchSize > 1 {
		p.pipe = client.Pipeline()
	}
	return p
}

func (p *Publisher) Publish(ctx context.Context, id string, payload []byte, pubNS int64) error {
	if p.batchSize <= 1 {
		err := p.client.XAdd(ctx, p.args(id, payload, pubNS)).Err()
		if isAlreadyPublished(err) {
			p.rejected++
			return nil
		}
		return err
	}

	p.pipe.XAdd(ctx, p.args(id, slices.Clone(payload), pubNS))
	p.pending++
	if p.pending >= p.batchSize {
		return p.Flush(ctx)
	}
	return nil
}

func (p *Publisher) args(id string, payload []byte, pubNS int64) *redis.XAddArgs {
	return &redis.XAddArgs{
		Stream: p.stream,
		ID:     id,
		Values: []any{FieldPayload, payload, FieldPubNS, pubNS},
	}
}

func (p *Publisher) Flush(ctx context.Context) error {
	if p.pending == 0 {
		return nil
	}
	cmds, err := p.pipe.Exec(ctx)
	p.pending = 0
	if err == nil {
		return nil
	}

	for _, cmd := range cmds {
		cmdErr := cmd.Err()
		switch {
		case cmdErr == nil:
		case isAlreadyPublished(cmdErr):
			p.rejected++
		default:
			return fmt.Errorf("pipeline exec: %w", cmdErr)
		}
	}
	return nil
}

func (p *Publisher) Rejected() int64 {
	return p.rejected
}

type Message struct {
	ID      string
	Payload []byte
	PubNS   int64
}

type Reader struct {
	client   *redis.Client
	stream   string
	group    string
	consumer string
	count    int64
	block    time.Duration
}

func NewReader(client *redis.Client, stream, group, consumer string, count int64, block time.Duration) *Reader {
	return &Reader{
		client:   client,
		stream:   stream,
		group:    group,
		consumer: consumer,
		count:    count,
		block:    block,
	}
}

func (r *Reader) EnsureGroup(ctx context.Context) error {
	err := r.client.XGroupCreateMkStream(ctx, r.stream, r.group, "0").Err()
	if err == nil || strings.HasPrefix(err.Error(), "BUSYGROUP") {
		return nil
	}
	return fmt.Errorf("creating group %s on %s: %w", r.group, r.stream, err)
}

func (r *Reader) Read(ctx context.Context) ([]Message, error) {
	streams, err := r.client.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    r.group,
		Consumer: r.consumer,
		Streams:  []string{r.stream, ">"},
		Count:    r.count,
		Block:    r.block,
	}).Result()
	if err != nil {
		return nil, classifyReadError(ctx, err)
	}

	var messages []Message
	for _, s := range streams {
		for _, raw := range s.Messages {
			msg, err := decode(raw)
			if err != nil {
				return nil, err
			}
			messages = append(messages, msg)
		}
	}
	return messages, nil
}

func classifyReadError(ctx context.Context, err error) error {
	if errors.Is(err, redis.Nil) {
		return ErrNoMessages
	}
	var netErr net.Error
	if ctx.Err() == nil && errors.As(err, &netErr) && netErr.Timeout() {
		return fmt.Errorf("xreadgroup: %w: %w", ErrBlockOverran, err)
	}
	return fmt.Errorf("xreadgroup: %w", err)
}

func (r *Reader) Claim(ctx context.Context, minIdle time.Duration, count int64) ([]Message, error) {
	raws, _, err := r.client.XAutoClaim(ctx, &redis.XAutoClaimArgs{
		Stream:   r.stream,
		Group:    r.group,
		Consumer: r.consumer,
		MinIdle:  minIdle,
		Start:    "0",
		Count:    count,
	}).Result()
	if err != nil {
		return nil, fmt.Errorf("xautoclaim: %w", err)
	}

	messages := make([]Message, 0, len(raws))
	for _, raw := range raws {
		msg, err := decode(raw)
		if err != nil {
			return nil, err
		}
		messages = append(messages, msg)
	}
	return messages, nil
}

func (r *Reader) Pending(ctx context.Context) (int64, error) {
	res, err := r.client.XPending(ctx, r.stream, r.group).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return 0, nil
		}
		return 0, fmt.Errorf("xpending: %w", err)
	}
	return res.Count, nil
}

func (r *Reader) Ack(ctx context.Context, ids []string) error {
	if len(ids) == 0 {
		return nil
	}
	if err := r.client.XAck(ctx, r.stream, r.group, ids...).Err(); err != nil {
		return fmt.Errorf("xack: %w", err)
	}
	return nil
}

func decode(raw redis.XMessage) (Message, error) {
	payload, err := field(raw, FieldPayload)
	if err != nil {
		return Message{}, err
	}
	pubNSText, err := field(raw, FieldPubNS)
	if err != nil {
		return Message{}, err
	}
	pubNS, err := strconv.ParseInt(pubNSText, 10, 64)
	if err != nil {
		return Message{}, fmt.Errorf("message %s has unparseable %s %q: %w",
			raw.ID, FieldPubNS, pubNSText, err)
	}
	return Message{ID: raw.ID, Payload: []byte(payload), PubNS: pubNS}, nil
}

func field(raw redis.XMessage, name string) (string, error) {
	value, ok := raw.Values[name]
	if !ok {
		return "", fmt.Errorf("message %s has no %s field", raw.ID, name)
	}
	text, ok := value.(string)
	if !ok {
		return "", fmt.Errorf("message %s has %s of type %T", raw.ID, name, value)
	}
	return text, nil
}
