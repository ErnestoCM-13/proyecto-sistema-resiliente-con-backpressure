package inventory_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/ErnestoCM-13/proyecto-sistema-resiliente-con-backpressure/internal/inventory"
)

const testTimeout = 2 * time.Second

func newClient(t *testing.T, baseURL string, timeout time.Duration) *inventory.Client {
	t.Helper()
	client, err := inventory.NewClient(baseURL, timeout)
	if err != nil {
		t.Fatalf("NewClient(%q, %s): %v", baseURL, timeout, err)
	}
	return client
}

func newServer(t *testing.T, handler http.HandlerFunc) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)
	return srv
}

func respond(status int, body string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		io.WriteString(w, body)
	}
}

func newHangingServer(t *testing.T, before func(w http.ResponseWriter)) *httptest.Server {
	t.Helper()
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if before != nil {
			before(w)
		}
		<-release
	}))
	t.Cleanup(func() {
		close(release)
		srv.Close()
	})
	return srv
}

func TestCheckAnswers(t *testing.T) {
	cases := []struct {
		name string
		body string
		want bool
	}{
		{name: "available", body: `{"available": true}`, want: true},
		{name: "not available", body: `{"available": false}`, want: false},
		{name: "extra fields are ignored", body: `{"available": true, "warehouse": "MX-01"}`, want: true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv := newServer(t, respond(http.StatusOK, tc.body))
			client := newClient(t, srv.URL, testTimeout)

			got, err := client.Check(context.Background(), "order-0001", "MX")
			if err != nil {
				t.Fatalf("Check: %v", err)
			}
			if got != tc.want {
				t.Fatalf("Check = %t, want %t", got, tc.want)
			}
		})
	}
}

func TestCheckSendsTheDocumentedRequest(t *testing.T) {
	type seen struct {
		method      string
		path        string
		contentType string
		body        map[string]string
		decodeErr   error
	}
	got := make(chan seen, 1)

	srv := newServer(t, func(w http.ResponseWriter, r *http.Request) {
		s := seen{method: r.Method, path: r.URL.Path, contentType: r.Header.Get("Content-Type")}
		s.decodeErr = json.NewDecoder(r.Body).Decode(&s.body)
		got <- s
		respond(http.StatusOK, `{"available": true}`)(w, r)
	})
	client := newClient(t, srv.URL, testTimeout)

	if _, err := client.Check(context.Background(), "order-0042", "CL"); err != nil {
		t.Fatalf("Check: %v", err)
	}

	s := <-got
	if s.decodeErr != nil {
		t.Fatalf("the request body is not JSON: %v", s.decodeErr)
	}
	if s.method != http.MethodPost {
		t.Errorf("method = %q, want POST", s.method)
	}
	if s.path != "/check-inventory" {
		t.Errorf("path = %q, want /check-inventory", s.path)
	}
	if s.contentType != "application/json" {
		t.Errorf("content type = %q, want application/json", s.contentType)
	}
	if len(s.body) != 2 || s.body["order_id"] != "order-0042" || s.body["country"] != "CL" {
		t.Errorf("body = %v, want exactly order_id=order-0042 and country=CL", s.body)
	}
}

func TestCheckClassifiesEachFailure(t *testing.T) {
	cases := []struct {
		name       string
		serve      func(t *testing.T) *httptest.Server
		timeout    time.Duration
		wantKind   inventory.Kind
		wantStatus int
	}{
		{
			name: "503 is overload",
			serve: func(t *testing.T) *httptest.Server {
				return newServer(t, respond(http.StatusServiceUnavailable, `{"error": "overloaded"}`))
			},
			timeout:    testTimeout,
			wantKind:   inventory.KindOverloaded,
			wantStatus: http.StatusServiceUnavailable,
		},
		{
			name: "500 is not a status the contract names",
			serve: func(t *testing.T) *httptest.Server {
				return newServer(t, respond(http.StatusInternalServerError, `{}`))
			},
			timeout:    testTimeout,
			wantKind:   inventory.KindProtocol,
			wantStatus: http.StatusInternalServerError,
		},
		{
			name: "404 is not a status the contract names",
			serve: func(t *testing.T) *httptest.Server {
				return newServer(t, respond(http.StatusNotFound, `{"error": "not found"}`))
			},
			timeout:    testTimeout,
			wantKind:   inventory.KindProtocol,
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "200 without the available field",
			serve:      func(t *testing.T) *httptest.Server { return newServer(t, respond(http.StatusOK, `{}`)) },
			timeout:    testTimeout,
			wantKind:   inventory.KindProtocol,
			wantStatus: http.StatusOK,
		},
		{
			name: "200 with a null available field",
			serve: func(t *testing.T) *httptest.Server {
				return newServer(t, respond(http.StatusOK, `{"available": null}`))
			},
			timeout:    testTimeout,
			wantKind:   inventory.KindProtocol,
			wantStatus: http.StatusOK,
		},
		{
			name: "200 with a body that is not JSON",
			serve: func(t *testing.T) *httptest.Server {
				return newServer(t, respond(http.StatusOK, `<html>gateway</html>`))
			},
			timeout:    testTimeout,
			wantKind:   inventory.KindProtocol,
			wantStatus: http.StatusOK,
		},
		{
			name:     "no answer within the timeout",
			serve:    func(t *testing.T) *httptest.Server { return newHangingServer(t, nil) },
			timeout:  50 * time.Millisecond,
			wantKind: inventory.KindTimeout,
		},
		{
			name: "headers arrive but the body never does",
			serve: func(t *testing.T) *httptest.Server {
				return newHangingServer(t, func(w http.ResponseWriter) {
					w.Header().Set("Content-Length", "19")
					w.WriteHeader(http.StatusOK)
					w.(http.Flusher).Flush()
				})
			},
			timeout:    50 * time.Millisecond,
			wantKind:   inventory.KindTimeout,
			wantStatus: http.StatusOK,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv := tc.serve(t)
			client := newClient(t, srv.URL, tc.timeout)

			_, err := client.Check(context.Background(), "order-0001", "MX")

			var invErr *inventory.Error
			if !errors.As(err, &invErr) {
				t.Fatalf("Check error = %v (%T), want an *inventory.Error", err, err)
			}
			if invErr.Kind != tc.wantKind {
				t.Errorf("kind = %s, want %s (error: %v)", invErr.Kind, tc.wantKind, err)
			}
			if invErr.Status != tc.wantStatus {
				t.Errorf("status = %d, want %d", invErr.Status, tc.wantStatus)
			}
		})
	}
}

func TestCheckAgainstAClosedPortIsUnreachable(t *testing.T) {
	srv := httptest.NewServer(respond(http.StatusOK, `{"available": true}`))
	baseURL := srv.URL
	srv.Close()

	client := newClient(t, baseURL, testTimeout)
	_, err := client.Check(context.Background(), "order-0001", "MX")

	var invErr *inventory.Error
	if !errors.As(err, &invErr) {
		t.Fatalf("Check error = %v (%T), want an *inventory.Error", err, err)
	}
	if invErr.Kind != inventory.KindUnreachable {
		t.Errorf("kind = %s, want %s (error: %v)", invErr.Kind, inventory.KindUnreachable, err)
	}
	if invErr.Status != 0 {
		t.Errorf("status = %d, want 0 because no response arrived", invErr.Status)
	}
}

func TestCheckAbandonedByTheCallerIsNotAServiceFailure(t *testing.T) {
	cases := []struct {
		name    string
		ctx     func() (context.Context, context.CancelFunc)
		wantErr error
	}{
		{
			name: "cancelled",
			ctx: func() (context.Context, context.CancelFunc) {
				ctx, cancel := context.WithCancel(context.Background())
				time.AfterFunc(50*time.Millisecond, cancel)
				return ctx, cancel
			},
			wantErr: context.Canceled,
		},
		{
			name: "caller deadline shorter than the client timeout",
			ctx: func() (context.Context, context.CancelFunc) {
				return context.WithTimeout(context.Background(), 50*time.Millisecond)
			},
			wantErr: context.DeadlineExceeded,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv := newHangingServer(t, nil)
			client := newClient(t, srv.URL, testTimeout)

			ctx, cancel := tc.ctx()
			defer cancel()
			_, err := client.Check(ctx, "order-0001", "MX")

			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("Check error = %v, want it to wrap %v", err, tc.wantErr)
			}
			var invErr *inventory.Error
			if errors.As(err, &invErr) {
				t.Fatalf("Check error is an *inventory.Error of kind %s, want none", invErr.Kind)
			}
		})
	}
}

func TestKindRetryable(t *testing.T) {
	cases := []struct {
		kind inventory.Kind
		want bool
	}{
		{kind: inventory.KindOverloaded, want: true},
		{kind: inventory.KindTimeout, want: true},
		{kind: inventory.KindUnreachable, want: true},
		{kind: inventory.KindProtocol, want: false},
		{kind: inventory.Kind(0), want: false},
		{kind: inventory.Kind(99), want: false},
	}

	for _, tc := range cases {
		t.Run(tc.kind.String(), func(t *testing.T) {
			if got := tc.kind.Retryable(); got != tc.want {
				t.Fatalf("%s.Retryable() = %t, want %t", tc.kind, got, tc.want)
			}
		})
	}
}

func TestNewClientRejectsBadConfiguration(t *testing.T) {
	cases := []struct {
		name         string
		baseURL      string
		timeout      time.Duration
		wantProblems int
	}{
		{name: "valid", baseURL: "http://localhost:8090", timeout: time.Second},
		{name: "valid https", baseURL: "https://inventory.example", timeout: time.Second},
		{name: "empty url", baseURL: "", timeout: time.Second, wantProblems: 1},
		{name: "no scheme", baseURL: "localhost:8090", timeout: time.Second, wantProblems: 1},
		{name: "wrong scheme", baseURL: "ftp://localhost:8090", timeout: time.Second, wantProblems: 1},
		{name: "no host", baseURL: "http://", timeout: time.Second, wantProblems: 1},
		{name: "unparseable url", baseURL: "http://local host:8090", timeout: time.Second, wantProblems: 1},
		{name: "zero timeout", baseURL: "http://localhost:8090", timeout: 0, wantProblems: 1},
		{name: "negative timeout", baseURL: "http://localhost:8090", timeout: -time.Second, wantProblems: 1},
		{name: "both bad", baseURL: "localhost:8090", timeout: 0, wantProblems: 2},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			client, err := inventory.NewClient(tc.baseURL, tc.timeout)

			if tc.wantProblems == 0 {
				if err != nil {
					t.Fatalf("NewClient(%q, %s): %v", tc.baseURL, tc.timeout, err)
				}
				if client == nil {
					t.Fatal("NewClient returned no client and no error")
				}
				return
			}

			if err == nil {
				t.Fatalf("NewClient(%q, %s) succeeded, want an error", tc.baseURL, tc.timeout)
			}
			joined, ok := err.(interface{ Unwrap() []error })
			if !ok {
				t.Fatalf("NewClient error is %T, want one built by errors.Join", err)
			}
			if got := len(joined.Unwrap()); got != tc.wantProblems {
				t.Fatalf("NewClient reported %d problems, want %d: %v", got, tc.wantProblems, err)
			}
		})
	}
}
