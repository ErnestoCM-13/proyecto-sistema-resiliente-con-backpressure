package stream

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"testing"

	"github.com/redis/go-redis/v9"
)

func TestClassifyReadError(t *testing.T) {
	socketTimeout := &net.OpError{Op: "read", Net: "tcp", Err: os.ErrDeadlineExceeded}
	refused := &net.OpError{Op: "dial", Net: "tcp", Err: errors.New("connection refused")}

	cancelled, cancel := context.WithCancel(context.Background())
	cancel()

	cases := []struct {
		name        string
		ctx         context.Context
		err         error
		wantIs      error
		wantIsNot   []error
		wantWrapped error
	}{
		{
			name:      "the block ended with nothing to read",
			ctx:       context.Background(),
			err:       redis.Nil,
			wantIs:    ErrNoMessages,
			wantIsNot: []error{ErrBlockOverran},
		},
		{
			name:        "redis never answered and the socket deadline passed",
			ctx:         context.Background(),
			err:         socketTimeout,
			wantIs:      ErrBlockOverran,
			wantIsNot:   []error{ErrNoMessages},
			wantWrapped: socketTimeout,
		},
		{
			name:        "the same timeout wrapped by the client library",
			ctx:         context.Background(),
			err:         fmt.Errorf("redis: %w", socketTimeout),
			wantIs:      ErrBlockOverran,
			wantIsNot:   []error{ErrNoMessages},
			wantWrapped: socketTimeout,
		},
		{
			name:        "redis cannot be reached",
			ctx:         context.Background(),
			err:         refused,
			wantIs:      refused,
			wantIsNot:   []error{ErrNoMessages, ErrBlockOverran},
			wantWrapped: refused,
		},
		{
			name:      "a timeout while the caller is shutting down is not an overrun",
			ctx:       cancelled,
			err:       socketTimeout,
			wantIs:    socketTimeout,
			wantIsNot: []error{ErrNoMessages, ErrBlockOverran},
		},
		{
			name:      "the caller gave up",
			ctx:       cancelled,
			err:       context.Canceled,
			wantIs:    context.Canceled,
			wantIsNot: []error{ErrNoMessages, ErrBlockOverran},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := classifyReadError(tc.ctx, tc.err)

			if !errors.Is(got, tc.wantIs) {
				t.Fatalf("classifyReadError = %v, want it to be %v", got, tc.wantIs)
			}
			for _, not := range tc.wantIsNot {
				if errors.Is(got, not) {
					t.Fatalf("classifyReadError = %v, want it not to be %v", got, not)
				}
			}
			if tc.wantWrapped != nil && !errors.Is(got, tc.wantWrapped) {
				t.Fatalf("classifyReadError = %v, want it to keep the cause %v", got, tc.wantWrapped)
			}
		})
	}
}
