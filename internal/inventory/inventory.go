// Asks the external inventory service whether an order's stock is still available.
package inventory

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"
)

const (
	checkPath    = "/check-inventory"
	maxBodyBytes = 4 << 10
)

type Kind int

const (
	KindOverloaded Kind = iota + 1
	KindTimeout
	KindUnreachable
	KindProtocol
)

func (k Kind) String() string {
	switch k {
	case KindOverloaded:
		return "overloaded"
	case KindTimeout:
		return "timeout"
	case KindUnreachable:
		return "unreachable"
	case KindProtocol:
		return "protocol"
	default:
		return fmt.Sprintf("kind(%d)", int(k))
	}
}

func (k Kind) Retryable() bool {
	switch k {
	case KindOverloaded, KindTimeout, KindUnreachable:
		return true
	default:
		return false
	}
}

type Error struct {
	Kind   Kind
	Status int
	Err    error
}

func (e *Error) Error() string {
	msg := "inventory service " + e.Kind.String()
	if e.Status != 0 {
		msg += fmt.Sprintf(" (status %d)", e.Status)
	}
	if e.Err != nil {
		msg += ": " + e.Err.Error()
	}
	return msg
}

func (e *Error) Unwrap() error {
	return e.Err
}

type Client struct {
	checkURL string
	timeout  time.Duration
	http     *http.Client
}

func NewClient(baseURL string, timeout time.Duration) (*Client, error) {
	var problems []error

	u, err := url.Parse(baseURL)
	switch {
	case err != nil:
		problems = append(problems, fmt.Errorf("base url: %w", err))
	case u.Scheme != "http" && u.Scheme != "https":
		problems = append(problems, fmt.Errorf("base url: %q is not an http or https address", baseURL))
	case u.Host == "":
		problems = append(problems, fmt.Errorf("base url: %q has no host", baseURL))
	}
	if timeout <= 0 {
		problems = append(problems, fmt.Errorf("timeout: %s is not positive", timeout))
	}
	if len(problems) > 0 {
		return nil, errors.Join(problems...)
	}

	return &Client{
		checkURL: u.JoinPath(checkPath).String(),
		timeout:  timeout,
		http:     &http.Client{},
	}, nil
}

type checkRequest struct {
	OrderID string `json:"order_id"`
	Country string `json:"country"`
}

type checkResponse struct {
	Available *bool `json:"available"`
}

func (c *Client) Check(ctx context.Context, orderID, country string) (bool, error) {
	body, err := json.Marshal(checkRequest{OrderID: orderID, Country: country})
	if err != nil {
		return false, fmt.Errorf("encoding the inventory request: %w", err)
	}

	callCtx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(callCtx, http.MethodPost, c.checkURL, bytes.NewReader(body))
	if err != nil {
		return false, fmt.Errorf("building the inventory request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.http.Do(req)
	if err != nil {
		return false, classify(ctx, 0, err)
	}
	defer resp.Body.Close()

	raw, err := io.ReadAll(io.LimitReader(resp.Body, maxBodyBytes))
	if err != nil {
		return false, classify(ctx, resp.StatusCode, err)
	}

	switch resp.StatusCode {
	case http.StatusOK:
	case http.StatusServiceUnavailable:
		return false, &Error{Kind: KindOverloaded, Status: resp.StatusCode}
	default:
		return false, &Error{Kind: KindProtocol, Status: resp.StatusCode}
	}

	var decoded checkResponse
	if err := json.Unmarshal(raw, &decoded); err != nil {
		return false, &Error{Kind: KindProtocol, Status: resp.StatusCode, Err: err}
	}
	if decoded.Available == nil {
		return false, &Error{Kind: KindProtocol, Status: resp.StatusCode, Err: errors.New(`no "available" field`)}
	}
	return *decoded.Available, nil
}

func classify(caller context.Context, status int, err error) error {
	if callerErr := caller.Err(); callerErr != nil {
		return fmt.Errorf("inventory check abandoned: %w", callerErr)
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return &Error{Kind: KindTimeout, Status: status, Err: err}
	}
	return &Error{Kind: KindUnreachable, Status: status, Err: err}
}
