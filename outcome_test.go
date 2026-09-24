package failnext

import (
	"context"
	"fmt"
	"net/http"
	"testing"
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
