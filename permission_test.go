package rcpx

import (
	"context"
	"io"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestRoundTrip_ExplicitPermissionControlsContinuation(t *testing.T) {
	tests := []struct {
		name       string
		context    func(context.Context) context.Context
		wantSecond bool
	}{
		{
			name: "allow",
			context: func(ctx context.Context) context.Context {
				return WithFailoverAllowed(ctx)
			},
			wantSecond: true,
		},
		{
			name: "deny",
			context: func(ctx context.Context) context.Context {
				return WithFailoverDenied(ctx)
			},
			wantSecond: false,
		},
		{
			name: "derived deny overrides inherited allow",
			context: func(ctx context.Context) context.Context {
				return WithFailoverDenied(WithFailoverAllowed(ctx))
			},
			wantSecond: false,
		},
		{
			name: "derived allow overrides inherited deny",
			context: func(ctx context.Context) context.Context {
				return WithFailoverAllowed(WithFailoverDenied(ctx))
			},
			wantSecond: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			u1 := "https://u1.test/rpc"
			u2 := "https://u2.test/rpc"

			base := &scriptRT{
				results: map[string][]rtResult{
					u1: {{err: io.EOF}},
					u2: {{resp: httpResp(http.StatusOK, "ok")}},
				},
			}
			rt := mustNewTransport(t, Config{
				Endpoints: testEndpoints(u1, u2),
				Base:      base,
			})

			req := newBodylessRequest(t, http.MethodPost, u1)
			req = req.WithContext(tt.context(req.Context()))
			resp, err := rt.RoundTrip(req)

			if tt.wantSecond {
				if err != nil {
					t.Fatalf("RoundTrip error: %v", err)
				}
				if resp == nil {
					t.Fatal("expected response")
				}
				resp.Body.Close()
				assertStatus(t, resp, http.StatusOK)
				assertCalls(t, base, u1, u2)
				return
			}

			if resp != nil {
				t.Fatalf("expected nil response, got %#v", resp)
			}
			fe := mustAsFailoverError(t, err)
			if len(fe.Attempts) != 1 {
				t.Fatalf("expected 1 no-response attempt, got %d", len(fe.Attempts))
			}
			assertCalls(t, base, u1)
		})
	}
}

func TestRoundTrip_ExplicitPermissionBypassesPolicy(t *testing.T) {
	tests := []struct {
		name       string
		context    func(context.Context) context.Context
		policy     Permission
		wantSecond bool
	}{
		{
			name: "explicit allow bypasses policy deny",
			context: func(ctx context.Context) context.Context {
				return WithFailoverAllowed(ctx)
			},
			policy:     PermissionDeny,
			wantSecond: true,
		},
		{
			name: "explicit deny bypasses policy allow",
			context: func(ctx context.Context) context.Context {
				return WithFailoverDenied(ctx)
			},
			policy:     PermissionAllow,
			wantSecond: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			u1 := "https://u1.test/rpc"
			u2 := "https://u2.test/rpc"

			base := &scriptRT{
				results: map[string][]rtResult{
					u1: {{err: io.EOF}},
					u2: {{resp: httpResp(http.StatusOK, "ok")}},
				},
			}
			policyCalls := 0
			rt := mustNewTransport(t, Config{
				Endpoints: testEndpoints(u1, u2),
				Base:      base,
				PermissionPolicy: func(*http.Request) Permission {
					policyCalls++
					return tt.policy
				},
			})

			req := newBodylessRequest(t, http.MethodPost, u1)
			req = req.WithContext(tt.context(req.Context()))
			resp, err := rt.RoundTrip(req)

			if policyCalls != 0 {
				t.Fatalf("expected PermissionPolicy not to be called, got %d calls", policyCalls)
			}
			if tt.wantSecond {
				if err != nil {
					t.Fatalf("RoundTrip error: %v", err)
				}
				resp.Body.Close()
				assertCalls(t, base, u1, u2)
				return
			}

			if resp != nil {
				t.Fatalf("expected nil response, got %#v", resp)
			}
			mustAsFailoverError(t, err)
			assertCalls(t, base, u1)
		})
	}
}

func TestRoundTrip_PermissionPolicyAuthority(t *testing.T) {
	tests := []struct {
		name       string
		method     string
		permission Permission
		wantSecond bool
	}{
		{
			name:       "allow permits post",
			method:     http.MethodPost,
			permission: PermissionAllow,
			wantSecond: true,
		},
		{
			name:       "deny overrides get inference",
			method:     http.MethodGet,
			permission: PermissionDeny,
			wantSecond: false,
		},
		{
			name:       "defer falls through to get inference",
			method:     http.MethodGet,
			permission: PermissionDefer,
			wantSecond: true,
		},
		{
			name:       "defer falls through to post deny",
			method:     http.MethodPost,
			permission: PermissionDefer,
			wantSecond: false,
		},
		{
			name:       "unknown permission denies conservatively",
			method:     http.MethodGet,
			permission: Permission(255),
			wantSecond: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			u1 := "https://u1.test/rpc"
			u2 := "https://u2.test/rpc"

			base := &scriptRT{
				results: map[string][]rtResult{
					u1: {{err: io.EOF}},
					u2: {{resp: httpResp(http.StatusOK, "ok")}},
				},
			}
			policyCalls := 0
			rt := mustNewTransport(t, Config{
				Endpoints: testEndpoints(u1, u2),
				Base:      base,
				PermissionPolicy: func(*http.Request) Permission {
					policyCalls++
					return tt.permission
				},
			})

			req := newBodylessRequest(t, tt.method, u1)
			resp, err := rt.RoundTrip(req)

			if policyCalls != 1 {
				t.Fatalf("expected PermissionPolicy calls=1, got %d", policyCalls)
			}
			if tt.wantSecond {
				if err != nil {
					t.Fatalf("RoundTrip error: %v", err)
				}
				resp.Body.Close()
				assertCalls(t, base, u1, u2)
				return
			}

			if resp != nil {
				t.Fatalf("expected nil response, got %#v", resp)
			}
			mustAsFailoverError(t, err)
			assertCalls(t, base, u1)
		})
	}
}

func TestRoundTrip_BuiltInPermissionInference(t *testing.T) {
	tests := []struct {
		name       string
		method     string
		wantSecond bool
	}{
		{name: "get allowed", method: http.MethodGet, wantSecond: true},
		{name: "head allowed", method: http.MethodHead, wantSecond: true},
		{name: "post denied", method: http.MethodPost, wantSecond: false},
		{name: "put denied", method: http.MethodPut, wantSecond: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			u1 := "https://u1.test/rpc"
			u2 := "https://u2.test/rpc"

			base := &scriptRT{
				results: map[string][]rtResult{
					u1: {{err: io.EOF}},
					u2: {{resp: httpResp(http.StatusOK, "ok")}},
				},
			}
			rt := mustNewTransport(t, Config{
				Endpoints: testEndpoints(u1, u2),
				Base:      base,
			})

			req := newBodylessRequest(t, tt.method, u1)
			resp, err := rt.RoundTrip(req)

			if tt.wantSecond {
				if err != nil {
					t.Fatalf("RoundTrip error: %v", err)
				}
				resp.Body.Close()
				assertCalls(t, base, u1, u2)
				return
			}

			if resp != nil {
				t.Fatalf("expected nil response, got %#v", resp)
			}
			mustAsFailoverError(t, err)
			assertCalls(t, base, u1)
		})
	}
}

func TestRoundTrip_PermissionPolicyCalledAtMostOncePerLogicalRequest(t *testing.T) {
	u1 := "https://u1.test/rpc"
	u2 := "https://u2.test/rpc"
	u3 := "https://u3.test/rpc"

	base := &scriptRT{
		results: map[string][]rtResult{
			u1: {{err: io.EOF}},
			u2: {{err: io.EOF}},
			u3: {{resp: httpResp(http.StatusOK, "ok")}},
		},
	}
	policyCalls := 0
	rt := mustNewTransport(t, Config{
		Endpoints: testEndpoints(u1, u2, u3),
		Base:      base,
		PermissionPolicy: func(*http.Request) Permission {
			policyCalls++
			return PermissionAllow
		},
	})

	req := newBodylessRequest(t, http.MethodPost, u1)
	resp, err := rt.RoundTrip(req)
	if err != nil {
		t.Fatalf("RoundTrip error: %v", err)
	}
	resp.Body.Close()

	if policyCalls != 1 {
		t.Fatalf("expected PermissionPolicy calls=1, got %d", policyCalls)
	}
	assertCalls(t, base, u1, u2, u3)
}

func TestRoundTrip_PermissionPolicyMayRunConcurrently(t *testing.T) {
	u1 := "https://u1.test/rpc"

	entered := make(chan struct{}, 2)
	release := make(chan struct{})
	var policyCalls atomic.Int32

	rt := mustNewTransport(t, Config{
		Endpoints: testEndpoints(u1),
		Base: roundTripperFunc(func(req *http.Request) (*http.Response, error) {
			return &http.Response{
				StatusCode: http.StatusOK,
				Body:       io.NopCloser(strings.NewReader("ok")),
				Header:     make(http.Header),
				Request:    req,
			}, nil
		}),
		PermissionPolicy: func(*http.Request) Permission {
			policyCalls.Add(1)
			entered <- struct{}{}
			<-release
			return PermissionAllow
		},
	})

	requests := []*http.Request{
		newBodylessRequest(t, http.MethodPost, u1),
		newBodylessRequest(t, http.MethodPost, u1),
	}
	errCh := make(chan error, len(requests))
	var wg sync.WaitGroup
	for _, req := range requests {
		wg.Add(1)
		go func() {
			defer wg.Done()
			resp, err := rt.RoundTrip(req)
			if resp != nil {
				resp.Body.Close()
			}
			errCh <- err
		}()
	}

	for i := 0; i < 2; i++ {
		select {
		case <-entered:
		case <-time.After(time.Second):
			close(release)
			wg.Wait()
			t.Fatal("PermissionPolicy calls were serialized; expected both logical requests to enter concurrently")
		}
	}
	close(release)
	wg.Wait()
	close(errCh)

	for err := range errCh {
		if err != nil {
			t.Fatalf("RoundTrip error: %v", err)
		}
	}
	if got := policyCalls.Load(); got != 2 {
		t.Fatalf("expected PermissionPolicy calls=2, got %d", got)
	}
}

func TestRoundTrip_JSONRPCMethodNamesDoNotAffectPermission(t *testing.T) {
	tests := []struct {
		name string
		body string
	}{
		{
			name: "read method",
			body: `{"jsonrpc":"2.0","id":1,"method":"eth_blockNumber","params":[]}`,
		},
		{
			name: "write method",
			body: `{"jsonrpc":"2.0","id":1,"method":"eth_sendRawTransaction","params":["0xdeadbeef"]}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			u1 := "https://u1.test/rpc"
			u2 := "https://u2.test/rpc"

			base := &scriptRT{
				results: map[string][]rtResult{
					u1: {{err: io.EOF}},
					u2: {{resp: httpResp(http.StatusOK, "ok")}},
				},
			}
			rt := mustNewTransport(t, Config{
				Endpoints: testEndpoints(u1, u2),
				Base:      base,
			})

			req := newJSONRequest(t, u1, tt.body)
			req = req.WithContext(WithFailoverAllowed(req.Context()))
			resp, err := rt.RoundTrip(req)
			if err != nil {
				t.Fatalf("RoundTrip error: %v", err)
			}
			resp.Body.Close()
			assertCalls(t, base, u1, u2)
		})
	}
}

func newBodylessRequest(t *testing.T, method, url string) *http.Request {
	t.Helper()

	req, err := http.NewRequest(method, url, nil)
	if err != nil {
		t.Fatalf("http.NewRequest: %v", err)
	}
	return req
}
