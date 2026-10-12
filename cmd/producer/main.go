// Publishes the Hot Sale event log to a Redis stream, in file order, one raw JSONL line per message.
package main

import (
	"bufio"
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/ErnestoCM-13/proyecto-sistema-resiliente-con-backpressure/internal/clock"
	"github.com/ErnestoCM-13/proyecto-sistema-resiliente-con-backpressure/internal/stream"
)

const progressEvery = 250_000

type config struct {
	file      string
	redisAddr string
	streamKey string
	rate      int
	batchSize int
	limit     int
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "producer: %v\n", err)
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

	client, err := stream.Connect(ctx, cfg.redisAddr)
	if err != nil {
		return err
	}
	defer client.Close()

	f, err := os.Open(cfg.file)
	if err != nil {
		return fmt.Errorf("opening %s: %w", cfg.file, err)
	}
	defer f.Close()

	batchSize := cfg.batchSize
	if cfg.rate > 0 {
		batchSize = 1
	}
	publisher := stream.NewPublisher(client, cfg.streamKey, batchSize)

	var tick <-chan time.Time
	if cfg.rate > 0 {
		ticker := time.NewTicker(time.Second / time.Duration(cfg.rate))
		defer ticker.Stop()
		tick = ticker.C
	}

	fmt.Printf("publishing %s to stream %s at %s (batch %d, rate %s)\n",
		cfg.file, cfg.streamKey, cfg.redisAddr, batchSize, describeRate(cfg.rate))

	var published int64
	start := time.Now()
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		if cfg.limit > 0 && published >= int64(cfg.limit) {
			break
		}
		if tick != nil {
			select {
			case <-tick:
			case <-ctx.Done():
				return ctx.Err()
			}
		}

		pubNS, err := clock.NowNS()
		if err != nil {
			return err
		}
		line := published + 1
		if err := publisher.Publish(ctx, stream.EntryID(line), sc.Bytes(), pubNS); err != nil {
			return fmt.Errorf("publishing line %d: %w", line, err)
		}
		published++
		if published%progressEvery == 0 {
			fmt.Printf("  %d events, %.0f ev/s\n", published, float64(published)/time.Since(start).Seconds())
		}
	}
	if err := sc.Err(); err != nil {
		return fmt.Errorf("reading %s at line %d: %w", cfg.file, published+1, err)
	}
	if err := publisher.Flush(ctx); err != nil {
		return err
	}
	elapsed := time.Since(start)

	fmt.Printf("\npublished %d events in %s (%.0f ev/s)\n",
		published, elapsed.Round(time.Millisecond), float64(published)/elapsed.Seconds())
	if rejected := publisher.Rejected(); rejected > 0 {
		fmt.Printf("%d publishes were refused as already in the stream, which is a client retry made visible\n",
			rejected)
	}
	return nil
}

func describeRate(rate int) string {
	if rate <= 0 {
		return "unlimited"
	}
	return fmt.Sprintf("%d ev/s", rate)
}

func parseFlags() (config, error) {
	var cfg config
	flag.StringVar(&cfg.file, "file", "data/eventos_hotsale.jsonl", "path to the JSONL event log")
	flag.StringVar(&cfg.redisAddr, "redis-addr", "localhost:6379", "host:port of Redis")
	flag.StringVar(&cfg.streamKey, "stream", "hotsale:events", "stream key to publish to")
	flag.IntVar(&cfg.rate, "rate", 0, "events per second; 0 publishes as fast as possible")
	flag.IntVar(&cfg.batchSize, "batch", 1000, "events per pipeline batch when the rate is unlimited")
	flag.IntVar(&cfg.limit, "limit", 0, "stop after this many events; 0 publishes the whole file")
	flag.Parse()

	var problems []error
	if _, err := os.Stat(cfg.file); err != nil {
		problems = append(problems, fmt.Errorf("-file: %w", err))
	}
	if cfg.streamKey == "" {
		problems = append(problems, errors.New("-stream: empty"))
	}
	if cfg.rate < 0 {
		problems = append(problems, fmt.Errorf("-rate: %d is negative", cfg.rate))
	}
	if cfg.rate > 1_000_000 {
		problems = append(problems, fmt.Errorf("-rate: %d is beyond what a ticker can pace", cfg.rate))
	}
	if cfg.batchSize < 1 {
		problems = append(problems, fmt.Errorf("-batch: %d is not positive", cfg.batchSize))
	}
	if cfg.limit < 0 {
		problems = append(problems, fmt.Errorf("-limit: %d is negative", cfg.limit))
	}
	if len(problems) > 0 {
		return config{}, errors.Join(problems...)
	}
	return cfg, nil
}
