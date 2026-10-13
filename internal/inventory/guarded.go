package inventory

import (
	"context"
	"errors"

	"github.com/ErnestoCM-13/proyecto-sistema-resiliente-con-backpressure/internal/breaker"
)

type Guarded struct {
	client  *Client
	breaker *breaker.Breaker
}

func NewGuarded(client *Client, opts breaker.Options) (*Guarded, error) {
	var problems []error
	if client == nil {
		problems = append(problems, errors.New("client: missing"))
	}
	if opts.IsFailure != nil {
		problems = append(problems, errors.New("is failure: set by the caller, but which inventory errors open the breaker is decided here"))
	}
	if len(problems) > 0 {
		return nil, errors.Join(problems...)
	}

	opts.IsFailure = isUnavailable
	b, err := breaker.New(opts)
	if err != nil {
		return nil, err
	}
	return &Guarded{client: client, breaker: b}, nil
}

func isUnavailable(err error) bool {
	var invErr *Error
	return errors.As(err, &invErr) && invErr.Kind.Retryable()
}

func (g *Guarded) Check(ctx context.Context, orderID, country string) (bool, error) {
	var available bool
	err := g.breaker.Do(func() error {
		var err error
		available, err = g.client.Check(ctx, orderID, country)
		return err
	})
	return available, err
}

func (g *Guarded) Stats() breaker.Stats {
	return g.breaker.Stats()
}
