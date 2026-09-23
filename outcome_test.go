package failnext

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"testing"
	"time"
)

func TestRoundTrip_BuiltInTriggerStatusesFailOver(t *testing.T) {
	for _, status := range []int{
		http.StatusBadGateway,
		http.StatusServiceUnavailable,
		http.StatusGatewayTimeout,
	} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			u1 := "https://u1.test/rpc"
			u2 := "https://u2.test/rpc"

			triggerBody := newTrackingBody(http.StatusText(status))
			base := &scriptRT{
				results: map[string][]rtResult{
					u1: {{resp: &http.Response{StatusCode: status, Body: triggerBody}}},
					u2: {{resp: httpResp(http.StatusOK, "ok")}},
				},
			}
			rt := mustNewTransport(t, Config{
				Endpoints: testEndpoints(u1, u2),
				Base:      base,
			})

			req, err := http.NewRequest(http.MethodGet, u1, nil)
			if err != nil {
				t.Fatalf("http.NewRequest: %v", err)
			}
			resp := mustRoundTrip(t, rt, req)

			assertStatus(t, resp, http.StatusOK)
			if !triggerBody.Closed() {
				t.Fatalf("expected HTTP %d response body to be closed after failover", status)
			}
			assertCalls(t, base, u1, u2)
		})
	}
}

func TestRoundTrip_HTTP429IsNotBuiltInTrigger(t *testing.T) {
	u1 := "https://u1.test/rpc"
	u2 := "https://u2.test/rpc"

	body := newTrackingBody("rate limited")
	base := &scriptRT{
		results: map[string][]rtResult{
			u1: {{resp: &http.Response{StatusCode: http.StatusTooManyRequests, Body: body}}},
			u2: {{resp: httpResp(http.StatusOK, "unexpected")}},
		},
	}
	rt := mustNewTransport(t, Config{
		Endpoints: testEndpoints(u1, u2),
		Base:      base,
	})

	req, err := http.NewRequest(http.MethodGet, u1, nil)
	if err != nil {
		t.Fatalf("http.NewRequest: %v", err)
	}
	resp := mustRoundTrip(t, rt, req)

	assertStatus(t, resp, http.StatusTooManyRequests)
	if body.Closed() {
		t.Fatal("expected returned 429 response body to remain open")
	}
	assertCalls(t, base, u1)
}

func TestRoundTrip_AdditionalTriggerStatusesFailOver(t *testing.T) {
	for _, status := range []int{
		http.StatusTooManyRequests,
		http.StatusInternalServerError,
	} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			u1 := "https://u1.test/rpc"
			u2 := "https://u2.test/rpc"

			triggerBody := newTrackingBody(http.StatusText(status))
			base := &scriptRT{
				results: map[string][]rtResult{
					u1: {{resp: &http.Response{StatusCode: status, Body: triggerBody}}},
					u2: {{resp: httpResp(http.StatusOK, "ok")}},
				},
			}
			rt := mustNewTransport(t, Config{
				Endpoints:                    testEndpoints(u1, u2),
				Base:                         base,
				AdditionalTriggerStatusCodes: []int{status},
			})

			req, err := http.NewRequest(http.MethodGet, u1, nil)
			if err != nil {
				t.Fatalf("http.NewRequest: %v", err)
			}
			resp := mustRoundTrip(t, rt, req)

			assertStatus(t, resp, http.StatusOK)
			if !triggerBody.Closed() {
				t.Fatalf("expected configured HTTP %d trigger response body to be closed after failover", status)
			}
			assertCalls(t, base, u1, u2)
		})
	}
}

func TestRoundTrip_AdditionalTriggerStatusDoesNotReplaceBuiltIns(t *testing.T) {
	u1 := "https://u1.test/rpc"
	u2 := "https://u2.test/rpc"

	base := &scriptRT{
		results: map[string][]rtResult{
			u1: {{resp: httpResp(http.StatusServiceUnavailable, "unavailable")}},
			u2: {{resp: httpResp(http.StatusOK, "ok")}},
		},
	}
	rt := mustNewTransport(t, Config{
		Endpoints:                    testEndpoints(u1, u2),
		Base:                         base,
		AdditionalTriggerStatusCodes: []int{http.StatusInternalServerError},
	})

	req, err := http.NewRequest(http.MethodGet, u1, nil)
	if err != nil {
		t.Fatalf("http.NewRequest: %v", err)
	}
	resp := mustRoundTrip(t, rt, req)

	assertStatus(t, resp, http.StatusOK)
	assertCalls(t, base, u1, u2)
}

func TestCooldown_AdditionalTriggerResponseResetsFailureStreak(t *testing.T) {
	for _, status := range []int{
		http.StatusTooManyRequests,
		http.StatusInternalServerError,
	} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			u1 := "https://u1.test/rpc"
			u2 := "https://u2.test/rpc"
			fixedNow := time.Unix(1200, 0)

			base := &scriptRT{
				results: map[string][]rtResult{
					u1: {
						{resp: httpResp(http.StatusServiceUnavailable, "failure-1")},
						{resp: httpResp(status, "configured-trigger")},
						{resp: httpResp(http.StatusServiceUnavailable, "failure-2")},
						{resp: httpResp(http.StatusOK, "ok-u1")},
					},
					u2: {
						{resp: httpResp(http.StatusOK, "ok-1")},
						{resp: httpResp(http.StatusOK, "ok-2")},
						{resp: httpResp(http.StatusOK, "ok-3")},
					},
				},
			}
			tr := mustNewTransport(t, Config{
				Endpoints:                    testEndpoints(u1, u2),
				Base:                         base,
				AdditionalTriggerStatusCodes: []int{status},
				Cooldown: CooldownConfig{
					Threshold: 2,
					Duration:  time.Hour,
				},
			})
			tr.now = func() time.Time { return fixedNow }

			for i := 0; i < 4; i++ {
				req, err := http.NewRequest(http.MethodGet, u1, nil)
				if err != nil {
					t.Fatalf("http.NewRequest: %v", err)
				}
				mustRoundTripCode(t, tr, req, http.StatusOK)
			}

			assertCalls(t, base, u1, u2, u1, u2, u1, u2, u1)
		})
	}
}

func TestCooldown_BuiltInTriggerRecordedWhenPermissionDeniesContinuation(t *testing.T) {
	for _, status := range []int{
		http.StatusBadGateway,
		http.StatusServiceUnavailable,
		http.StatusGatewayTimeout,
	} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			u1 := "https://u1.test/rpc"
			u2 := "https://u2.test/rpc"
			fixedNow := time.Unix(1300, 0)

			base := &scriptRT{
				results: map[string][]rtResult{
					u1: {{resp: httpResp(status, "trigger")}},
					u2: {{resp: httpResp(http.StatusOK, "ok")}},
				},
			}
			tr := mustNewTransport(t, Config{
				Endpoints: testEndpoints(u1, u2),
				Base:      base,
				Cooldown: CooldownConfig{
					Threshold: 1,
					Duration:  time.Hour,
				},
			})
			tr.now = func() time.Time { return fixedNow }

			first, err := http.NewRequest(http.MethodPost, u1, nil)
			if err != nil {
				t.Fatalf("http.NewRequest first: %v", err)
			}
			mustRoundTripCode(t, tr, first, status)

			second, err := http.NewRequest(http.MethodGet, u1, nil)
			if err != nil {
				t.Fatalf("http.NewRequest second: %v", err)
			}
			mustRoundTripCode(t, tr, second, http.StatusOK)

			assertCalls(t, base, u1, u2)
		})
	}
}

func TestCooldown_BuiltInTriggerRecordedWithNoLaterCandidate(t *testing.T) {
	for _, status := range []int{
		http.StatusBadGateway,
		http.StatusServiceUnavailable,
		http.StatusGatewayTimeout,
	} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			u1 := "https://u1.test/rpc"
			fixedNow := time.Unix(1400, 0)

			base := &scriptRT{
				results: map[string][]rtResult{
					u1: {{resp: httpResp(status, "trigger")}},
				},
			}
			tr := mustNewTransport(t, Config{
				Endpoints: testEndpoints(u1),
				Base:      base,
				Cooldown: CooldownConfig{
					Threshold: 1,
					Duration:  time.Hour,
				},
			})
			tr.now = func() time.Time { return fixedNow }

			req, err := http.NewRequest(http.MethodGet, u1, nil)
			if err != nil {
				t.Fatalf("http.NewRequest: %v", err)
			}
			mustRoundTripCode(t, tr, req, status)

			if tr.cooldown.eligible(fixedNow, 0) {
				t.Fatalf("expected endpoint to be cooling after terminal HTTP %d", status)
			}
			assertCalls(t, base, u1)
		})
	}
}

func TestCooldown_LiveTransportFailureRecordedWhenPermissionDeniesContinuation(t *testing.T) {
	u1 := "https://u1.test/rpc"
	u2 := "https://u2.test/rpc"
	fixedNow := time.Unix(1500, 0)

	base := &scriptRT{
		results: map[string][]rtResult{
			u1: {{err: io.EOF}},
			u2: {{resp: httpResp(http.StatusOK, "ok")}},
		},
	}
	tr := mustNewTransport(t, Config{
		Endpoints: testEndpoints(u1, u2),
		Base:      base,
		Cooldown: CooldownConfig{
			Threshold: 1,
			Duration:  time.Hour,
		},
	})
	tr.now = func() time.Time { return fixedNow }

	first, err := http.NewRequest(http.MethodPost, u1, nil)
	if err != nil {
		t.Fatalf("http.NewRequest first: %v", err)
	}
	resp, err := tr.RoundTrip(first)
	if resp != nil {
		t.Fatalf("expected nil response, got %#v", resp)
	}
	var failoverErr *FailoverError
	if !errors.As(err, &failoverErr) {
		t.Fatalf("expected FailoverError, got %v", err)
	}

	second, err := http.NewRequest(http.MethodGet, u1, nil)
	if err != nil {
		t.Fatalf("http.NewRequest second: %v", err)
	}
	mustRoundTripCode(t, tr, second, http.StatusOK)

	assertCalls(t, base, u1, u2)
}

func TestCooldown_LogicalCancellationDoesNotRecordFailure(t *testing.T) {
	u1 := "https://u1.test/rpc"
	u2 := "https://u2.test/rpc"
	fixedNow := time.Unix(1600, 0)

	ctx, cancel := context.WithCancel(context.Background())
	calls := 0
	base := roundTripperFunc(func(req *http.Request) (*http.Response, error) {
		calls++
		switch calls {
		case 1:
			if req.URL.String() != u1 {
				t.Fatalf("unexpected first URL: %s", req.URL)
			}
			cancel()
			return nil, io.EOF
		case 2:
			if req.URL.String() != u1 {
				t.Fatalf("expected u1 to remain eligible after cancellation, got %s", req.URL)
			}
			return httpResp(http.StatusOK, "ok"), nil
		default:
			return nil, fmt.Errorf("unexpected base call %d to %s", calls, req.URL)
		}
	})
	tr := mustNewTransport(t, Config{
		Endpoints: testEndpoints(u1, u2),
		Base:      base,
		Cooldown: CooldownConfig{
			Threshold: 1,
			Duration:  time.Hour,
		},
	})
	tr.now = func() time.Time { return fixedNow }

	first, err := http.NewRequestWithContext(ctx, http.MethodGet, u1, nil)
	if err != nil {
		t.Fatalf("http.NewRequestWithContext: %v", err)
	}
	resp, err := tr.RoundTrip(first)
	if resp != nil {
		t.Fatalf("expected nil response, got %#v", resp)
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}

	second, err := http.NewRequest(http.MethodGet, u1, nil)
	if err != nil {
		t.Fatalf("http.NewRequest second: %v", err)
	}
	mustRoundTripCode(t, tr, second, http.StatusOK)

	if calls != 2 {
		t.Fatalf("unexpected base call count: got=%d want=2", calls)
	}
}

func TestRoundTrip_LiveContextContextShapedBaseErrorsFailOver(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
	}{
		{name: "canceled", err: context.Canceled},
		{name: "deadline exceeded", err: context.DeadlineExceeded},
	} {
		t.Run(tc.name, func(t *testing.T) {
			u1 := "https://u1.test/rpc"
			u2 := "https://u2.test/rpc"

			baseErr := fmt.Errorf("wrapped base error: %w", tc.err)
			base := &scriptRT{
				results: map[string][]rtResult{
					u1: {{err: baseErr}},
					u2: {{resp: httpResp(http.StatusOK, "ok")}},
				},
			}
			rt := mustNewTransport(t, Config{
				Endpoints: testEndpoints(u1, u2),
				Base:      base,
			})

			req, err := http.NewRequest(http.MethodGet, u1, nil)
			if err != nil {
				t.Fatalf("http.NewRequest: %v", err)
			}
			resp := mustRoundTrip(t, rt, req)

			assertStatus(t, resp, http.StatusOK)
			assertCalls(t, base, u1, u2)
		})
	}
}
