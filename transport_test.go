package rcpx

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
)

func TestRoundTrip_HTTP500_IsTreatedAsSuccess_NoFailover(t *testing.T) {
	u1 := "https://u1.test/rpc"
	u2 := "https://u2.test/rpc"

	respBody := newTrackingBody("internal error")
	base := &scriptRT{
		results: map[string][]rtResult{
			u1: {
				{resp: &http.Response{StatusCode: 500, Body: respBody}, err: nil},
			},
			// Intentionally omit u2. If rcpx fails over incorrectly, scriptRT will error on an unexpected call.
		},
	}

	rt := mustNewTransport(t, Config{
		Endpoints: testEndpoints(u1, u2),
		Base:      base,
	})

	req := newRPCRequest(t, u1, "eth_blockNumber")
	resp, err := rt.RoundTrip(req)
	if err != nil {
		t.Fatalf("RoundTrip returned error: %v", err)
	}
	t.Cleanup(func() { resp.Body.Close() })

	assertStatus(t, resp, 500)

	// For a non-retryable status, the response is returned unchanged and the caller owns the body.
	if respBody.Closed() {
		t.Fatalf("expected returned response body to remain open")
	}

	assertCalls(t, base, u1)
}

func TestRoundTrip_AdditionalTriggerStatus_FailsOver(t *testing.T) {
	u1 := "https://u1.test/rpc"
	u2 := "https://u2.test/rpc"

	respBody := newTrackingBody("internal error")
	base := &scriptRT{
		results: map[string][]rtResult{
			u1: {
				{resp: &http.Response{StatusCode: 500, Body: respBody}, err: nil},
			},
			u2: {
				{resp: httpResp(200, "ok"), err: nil},
			},
		},
	}

	rt := mustNewTransport(t, Config{
		Endpoints:                    testEndpoints(u1, u2),
		Base:                         base,
		AdditionalTriggerStatusCodes: []int{500},
	})

	req := newRPCRequest(t, u1, "eth_blockNumber")
	resp := mustRoundTrip(t, rt, req)
	assertStatus(t, resp, 200)
	if !respBody.Closed() {
		t.Fatalf("expected 500 response body to be closed before failover")
	}
	assertCalls(t, base, u1, u2)
}

func TestRoundTrip_AdditionalTriggerStatus_DoesNotReplaceDefaults(t *testing.T) {
	u1 := "https://u1.test/rpc"
	u2 := "https://u2.test/rpc"

	respBody := newTrackingBody("service unavailable")
	base := &scriptRT{
		results: map[string][]rtResult{
			u1: {
				{resp: &http.Response{StatusCode: 503, Body: respBody}, err: nil},
			},
			u2: {
				{resp: httpResp(200, "ok"), err: nil},
			},
		},
	}

	rt := mustNewTransport(t, Config{
		Endpoints:                    testEndpoints(u1, u2),
		Base:                         base,
		AdditionalTriggerStatusCodes: []int{500},
	})

	req := newRPCRequest(t, u1, "eth_blockNumber")
	resp := mustRoundTrip(t, rt, req)
	assertStatus(t, resp, 200)
	if !respBody.Closed() {
		t.Fatalf("expected 503 response body to be closed before failover")
	}
	assertCalls(t, base, u1, u2)
}

func TestRoundTrip_NonConfiguredStatus_IsTreatedAsSuccess_NoFailover(t *testing.T) {
	u1 := "https://u1.test/rpc"
	u2 := "https://u2.test/rpc"

	respBody := newTrackingBody("not implemented")
	base := &scriptRT{
		results: map[string][]rtResult{
			u1: {
				{resp: &http.Response{StatusCode: 501, Body: respBody}, err: nil},
			},
		},
	}

	rt := mustNewTransport(t, Config{
		Endpoints:                    testEndpoints(u1, u2),
		Base:                         base,
		AdditionalTriggerStatusCodes: []int{500},
	})

	req := newRPCRequest(t, u1, "eth_blockNumber")
	resp, err := rt.RoundTrip(req)
	if err != nil {
		t.Fatalf("RoundTrip returned error: %v", err)
	}
	t.Cleanup(func() { resp.Body.Close() })

	assertStatus(t, resp, 501)

	if respBody.Closed() {
		t.Fatalf("expected returned response body to remain open")
	}

	assertCalls(t, base, u1)
}

func TestRoundTrip_HTTP200_WithJSONRPCErrorPayload_IsTreatedAsSuccess_NoFailover(t *testing.T) {
	u1 := "https://u1.test/rpc"
	u2 := "https://u2.test/rpc"

	// rcpx must not interpret the JSON-RPC payload; HTTP 200 is treated as success.
	jsonrpcErrResp := `{"jsonrpc":"2.0","id":1,"error":{"code":-32000,"message":"boom"}}`

	base := &scriptRT{
		results: map[string][]rtResult{
			u1: {
				{resp: &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(jsonrpcErrResp))}, err: nil},
			},
			// Omit u2 results to ensure it is never called.
		},
	}

	rt := mustNewTransport(t, Config{
		Endpoints: testEndpoints(u1, u2),
		Base:      base,
	})

	req := newRPCRequest(t, u1, "eth_call")
	resp, err := rt.RoundTrip(req)
	if err != nil {
		t.Fatalf("RoundTrip returned error: %v", err)
	}
	t.Cleanup(func() { resp.Body.Close() })

	assertStatus(t, resp, 200)
	assertCalls(t, base, u1)
}

func TestRoundTrip_NormalizesNilNilAsErrorAndFailsOver(t *testing.T) {
	u1 := "https://u1.test/rpc"
	u2 := "https://u2.test/rpc"

	base := &scriptRT{
		results: map[string][]rtResult{
			u1: {
				{resp: nil, err: nil}, // (nil, nil) must be treated as an error and trigger failover.
			},
			u2: {
				{resp: httpResp(200, "ok"), err: nil},
			},
		},
	}

	rt := mustNewTransport(t, Config{
		Endpoints: testEndpoints(u1, u2),
		Base:      base,
	})

	req := newRPCRequest(t, u1, "eth_blockNumber")
	resp := mustRoundTrip(t, rt, req)

	assertStatus(t, resp, 200)
	assertCalls(t, base, u1, u2)
}

func TestRoundTrip_FailoverOnTransportErrorEOF(t *testing.T) {
	u1 := "https://u1.test/rpc"
	u2 := "https://u2.test/rpc"

	base := &scriptRT{
		results: map[string][]rtResult{
			u1: {
				{resp: nil, err: io.EOF}, // retryable transport error => failover
			},
			u2: {
				{resp: httpResp(200, "ok"), err: nil},
			},
		},
	}

	rt := mustNewTransport(t, Config{
		Endpoints: testEndpoints(u1, u2),
		Base:      base,
	})

	req := newRPCRequest(t, u1, "eth_blockNumber")
	resp := mustRoundTrip(t, rt, req)

	assertStatus(t, resp, 200)
	assertCalls(t, base, u1, u2)
}

func TestRoundTrip_FailoverOnRetryableHTTPStatus_ClosesBody(t *testing.T) {
	cases := []struct {
		name   string
		status int
		body   string
	}{
		{name: "503 service unavailable", status: 503, body: "service unavailable"},
		{name: "429 too many requests", status: 429, body: "rate limited"},
		{name: "502 bad gateway", status: 502, body: "bad gateway"},
		{name: "504 gateway timeout", status: 504, body: "gateway timeout"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			u1 := "https://u1.test/rpc"
			u2 := "https://u2.test/rpc"

			respBody := newTrackingBody(tc.body)
			base := &scriptRT{
				results: map[string][]rtResult{
					u1: {
						{resp: &http.Response{StatusCode: tc.status, Body: respBody}, err: nil},
					},
					u2: {
						{resp: httpResp(200, "ok"), err: nil},
					},
				},
			}

			rt := mustNewTransport(t, Config{
				Endpoints: testEndpoints(u1, u2),
				Base:      base,
			})

			req := newRPCRequest(t, u1, "eth_blockNumber")
			resp, err := rt.RoundTrip(req)
			if err != nil {
				t.Fatalf("RoundTrip returned error: %v", err)
			}
			t.Cleanup(func() { resp.Body.Close() })

			assertStatus(t, resp, 200)
			if !respBody.Closed() {
				t.Fatalf("expected %d response body to be closed before failover", tc.status)
			}
			assertCalls(t, base, u1, u2)
		})
	}
}

func TestRoundTrip_ClosesBodyWhenRespAndErrReturnedThenFailsOver(t *testing.T) {
	u1 := "https://u1.test/rpc"
	u2 := "https://u2.test/rpc"

	respBody := newTrackingBody("oops")
	base := &scriptRT{
		results: map[string][]rtResult{
			u1: {
				// Returning (resp, err) must be treated as an error only outcome,
				// and the response body must be closed before failing over.
				{resp: &http.Response{StatusCode: 503, Body: respBody}, err: errors.New("boom")},
			},
			u2: {
				{resp: httpResp(200, "ok"), err: nil},
			},
		},
	}

	rt := mustNewTransport(t, Config{
		Endpoints: testEndpoints(u1, u2),
		Base:      base,
	})

	req := newRPCRequest(t, u1, "eth_blockNumber")
	resp := mustRoundTrip(t, rt, req)

	assertStatus(t, resp, 200)
	if !respBody.Closed() {
		t.Fatalf("expected response body from (resp, err) attempt to be closed before failover")
	}
	assertCalls(t, base, u1, u2)
}

func TestRoundTrip_PolicyNotCalledOnSuccess(t *testing.T) {
	u1 := "https://u1.test/rpc"
	u2 := "https://u2.test/rpc"

	base := &scriptRT{
		results: map[string][]rtResult{
			u1: {
				{resp: httpResp(200, "ok"), err: nil},
			},
			// Not expected to be called.
			u2: {
				{resp: httpResp(200, "ok"), err: nil},
			},
		},
	}

	pol, policy := newPolicyRecorder(true)

	rt := mustNewTransport(t, Config{
		Endpoints:   testEndpoints(u1, u2),
		Base:        base,
		RetryPolicy: policy,
	})

	req := newRPCRequest(t, u1, "eth_blockNumber")
	resp, err := rt.RoundTrip(req)
	if err != nil {
		t.Fatalf("RoundTrip error: %v", err)
	}
	t.Cleanup(func() { resp.Body.Close() })

	assertStatus(t, resp, 200)
	assertCalls(t, base, u1)
	assertPolicyCalls(t, pol, 0)
}

func TestRoundTrip_PolicyCalledWhenConsideringContinuing(t *testing.T) {
	u1 := "https://u1.test/rpc"
	u2 := "https://u2.test/rpc"

	respBody := newTrackingBody("retry me")
	resp1 := &http.Response{StatusCode: 503, Body: respBody}

	base := &scriptRT{
		results: map[string][]rtResult{
			u1: {
				{resp: resp1, err: nil}, // retryable status => rcpx will consider continuing
			},
			u2: {
				{resp: httpResp(200, "ok"), err: nil},
			},
		},
	}

	pol, policy := newPolicyRecorder(true) // allow failover

	rt := mustNewTransport(t, Config{
		Endpoints:   testEndpoints(u1, u2),
		Base:        base,
		RetryPolicy: policy,
	})

	req := newRPCRequest(t, u1, "eth_blockNumber")
	resp, err := rt.RoundTrip(req)
	if err != nil {
		t.Fatalf("RoundTrip error: %v", err)
	}
	t.Cleanup(func() { resp.Body.Close() })

	assertStatus(t, resp, 200)
	assertPolicyCalls(t, pol, 1)

	if !respBody.Closed() {
		t.Fatalf("expected 503 response body closed before failover")
	}
	assertCalls(t, base, u1, u2)
}

func TestRoundTrip_PolicyNotCalledOnLastEligibleAttempt(t *testing.T) {
	u1 := "https://u1.test/rpc"
	u2 := "https://u2.test/rpc"

	respBody := newTrackingBody("retry me")
	resp1 := &http.Response{StatusCode: 503, Body: respBody}

	base := &scriptRT{
		results: map[string][]rtResult{
			u1: {
				{resp: resp1, err: nil},
			},
			// u2 would succeed if called, but we will force it ineligible via cooldown.
			u2: {
				{resp: httpResp(200, "ok"), err: nil},
			},
		},
	}

	pol, policy := newPolicyRecorder(true)

	tr := mustNewTransport(t, Config{
		Endpoints:   testEndpoints(u1, u2),
		Base:        base,
		RetryPolicy: policy,
	})

	fixedNow := time.Unix(1, 0)
	tr.now = func() time.Time { return fixedNow }

	// Force upstream2 to be cooling down so it is not eligible.
	if tr.cooldown == nil {
		t.Fatalf("expected cooldown tracker")
	}
	tr.cooldown.mu.Lock()
	tr.cooldown.enabled = true
	tr.cooldown.coolingTo[1] = fixedNow.Add(1 * time.Hour)
	tr.cooldown.mu.Unlock()

	req := newRPCRequest(t, u1, "eth_blockNumber")
	resp, err := tr.RoundTrip(req)
	if resp != nil {
		t.Fatalf("expected nil response, got %#v", resp)
	}

	ae := mustAsAllUpstreamsFailed(t, err)
	if ae.Attempted != 1 {
		t.Fatalf("expected Attempted=1, got %d", ae.Attempted)
	}
	if ae.SkippedCooldown != 1 {
		t.Fatalf("expected SkippedCooldown=1, got %d", ae.SkippedCooldown)
	}

	assertPolicyCalls(t, pol, 0)

	if !respBody.Closed() {
		t.Fatalf("expected 503 response body closed on terminal failure")
	}
	assertCalls(t, base, u1)
}

func TestRoundTrip_PolicyCanStopFailoverOnRetryableStatus(t *testing.T) {
	u1 := "https://u1.test/rpc"
	u2 := "https://u2.test/rpc"

	respBody := newTrackingBody("retry me")
	resp1 := &http.Response{StatusCode: 503, Body: respBody}

	base := &scriptRT{
		results: map[string][]rtResult{
			u1: {
				{resp: resp1, err: nil},
			},
			// If rcpx fails over anyway, this will be called and the test should fail.
			u2: {
				{resp: httpResp(200, "ok"), err: nil},
			},
		},
	}

	pol, policy := newPolicyRecorder(false) // stop failover

	rt := mustNewTransport(t, Config{
		Endpoints:   testEndpoints(u1, u2),
		Base:        base,
		RetryPolicy: policy,
	})

	req := newRPCRequest(t, u1, "eth_blockNumber")
	resp, err := rt.RoundTrip(req)
	if resp != nil {
		t.Fatalf("expected nil response, got %#v", resp)
	}

	ae := mustAsAllUpstreamsFailed(t, err)
	if ae.Attempted != 1 {
		t.Fatalf("expected Attempted=1, got %d", ae.Attempted)
	}

	assertPolicyCalls(t, pol, 1)

	if !respBody.Closed() {
		t.Fatalf("expected 503 response body closed when not failing over")
	}
	assertCalls(t, base, u1)
}

func TestRoundTrip_OnAttempt_SuccessFirstAttempt(t *testing.T) {
	u1 := "https://u1.test/rpc"

	base := &scriptRT{
		results: map[string][]rtResult{
			u1: {{resp: httpResp(200, "ok"), err: nil}},
		},
	}

	var attempts []AttemptInfo
	rt := mustNewTransport(t, Config{
		Endpoints: testEndpoints(u1),
		Base:      base,
		OnAttempt: func(info AttemptInfo) {
			attempts = append(attempts, info)
		},
	})

	req := newRPCRequest(t, u1, "eth_blockNumber")
	resp := mustRoundTrip(t, rt, req)
	assertStatus(t, resp, 200)

	if len(attempts) != 1 {
		t.Fatalf("expected 1 attempt observation, got %d: %#v", len(attempts), attempts)
	}

	got := attempts[0]
	if got.Attempt != 1 {
		t.Fatalf("expected Attempt=1, got %d", got.Attempt)
	}
	if got.Upstream != u1 {
		t.Fatalf("expected Upstream=%q, got %q", u1, got.Upstream)
	}
	if got.Method != "eth_blockNumber" {
		t.Fatalf("expected Method=eth_blockNumber, got %q", got.Method)
	}
	if got.Batch {
		t.Fatalf("expected Batch=false")
	}
	if got.StatusCode != 200 {
		t.Fatalf("expected StatusCode=200, got %d", got.StatusCode)
	}
	if got.Err != nil {
		t.Fatalf("expected Err=nil, got %v", got.Err)
	}
	if !got.Final {
		t.Fatalf("expected Final=true")
	}

	assertCalls(t, base, u1)
}

func TestRoundTrip_OnAttempt_Failover(t *testing.T) {
	u1 := "https://u1.test/rpc"
	u2 := "https://u2.test/rpc"

	respBody := newTrackingBody("service unavailable")
	base := &scriptRT{
		results: map[string][]rtResult{
			u1: {{resp: &http.Response{StatusCode: 503, Body: respBody}, err: nil}},
			u2: {{resp: httpResp(200, "ok"), err: nil}},
		},
	}

	var attempts []AttemptInfo
	rt := mustNewTransport(t, Config{
		Endpoints: testEndpoints(u1, u2),
		Base:      base,
		OnAttempt: func(info AttemptInfo) {
			attempts = append(attempts, info)
		},
	})

	req := newRPCRequest(t, u1, "eth_blockNumber")
	resp := mustRoundTrip(t, rt, req)
	assertStatus(t, resp, 200)

	if len(attempts) != 2 {
		t.Fatalf("expected 2 attempt observations, got %d: %#v", len(attempts), attempts)
	}

	first := attempts[0]
	if first.Attempt != 1 {
		t.Fatalf("expected first Attempt=1, got %d", first.Attempt)
	}
	if first.Upstream != u1 {
		t.Fatalf("expected first Upstream=%q, got %q", u1, first.Upstream)
	}
	if first.Method != "eth_blockNumber" {
		t.Fatalf("expected first Method=eth_blockNumber, got %q", first.Method)
	}
	if first.Batch {
		t.Fatalf("expected first Batch=false")
	}
	if first.StatusCode != 503 {
		t.Fatalf("expected first StatusCode=503, got %d", first.StatusCode)
	}
	if first.Err == nil {
		t.Fatalf("expected first Err to be non nil")
	}
	if first.Final {
		t.Fatalf("expected first Final=false")
	}

	second := attempts[1]
	if second.Attempt != 2 {
		t.Fatalf("expected second Attempt=2, got %d", second.Attempt)
	}
	if second.Upstream != u2 {
		t.Fatalf("expected second Upstream=%q, got %q", u2, second.Upstream)
	}
	if second.Method != "eth_blockNumber" {
		t.Fatalf("expected second Method=eth_blockNumber, got %q", second.Method)
	}
	if second.Batch {
		t.Fatalf("expected second Batch=false")
	}
	if second.StatusCode != 200 {
		t.Fatalf("expected second StatusCode=200, got %d", second.StatusCode)
	}
	if second.Err != nil {
		t.Fatalf("expected second Err=nil, got %v", second.Err)
	}
	if !second.Final {
		t.Fatalf("expected second Final=true")
	}

	if !respBody.Closed() {
		t.Fatalf("expected 503 response body to be closed before failover")
	}
	assertCalls(t, base, u1, u2)
}

func TestRoundTrip_OnAttempt_PolicyStopsFailover(t *testing.T) {
	u1 := "https://u1.test/rpc"
	u2 := "https://u2.test/rpc"

	respBody := newTrackingBody("service unavailable")
	base := &scriptRT{
		results: map[string][]rtResult{
			u1: {{resp: &http.Response{StatusCode: 503, Body: respBody}, err: nil}},
			u2: {{resp: httpResp(200, "ok"), err: nil}},
		},
	}

	var attempts []AttemptInfo
	pol, policy := newPolicyRecorder(false)
	rt := mustNewTransport(t, Config{
		Endpoints:   testEndpoints(u1, u2),
		Base:        base,
		RetryPolicy: policy,
		OnAttempt: func(info AttemptInfo) {
			attempts = append(attempts, info)
		},
	})

	req := newRPCRequest(t, u1, "eth_blockNumber")
	resp, err := rt.RoundTrip(req)
	if resp != nil {
		t.Fatalf("expected nil response, got %#v", resp)
	}

	ae := mustAsAllUpstreamsFailed(t, err)
	if ae.Attempted != 1 {
		t.Fatalf("expected Attempted=1, got %d", ae.Attempted)
	}

	if len(attempts) != 1 {
		t.Fatalf("expected 1 attempt observation, got %d: %#v", len(attempts), attempts)
	}

	got := attempts[0]
	if got.Attempt != 1 {
		t.Fatalf("expected Attempt=1, got %d", got.Attempt)
	}
	if got.Upstream != u1 {
		t.Fatalf("expected Upstream=%q, got %q", u1, got.Upstream)
	}
	if got.StatusCode != 503 {
		t.Fatalf("expected StatusCode=503, got %d", got.StatusCode)
	}
	if got.Err == nil {
		t.Fatalf("expected Err to be non nil")
	}
	if !got.Final {
		t.Fatalf("expected Final=true")
	}

	assertPolicyCalls(t, pol, 1)
	if !respBody.Closed() {
		t.Fatalf("expected 503 response body closed when not failing over")
	}
	assertCalls(t, base, u1)
}

func TestRoundTrip_OnAttempt_NonIdempotentBlocked(t *testing.T) {
	u1 := "https://u1.test/rpc"
	u2 := "https://u2.test/rpc"

	base := &scriptRT{
		results: map[string][]rtResult{
			u1: {{resp: nil, err: io.EOF}},
			u2: {{resp: httpResp(200, "ok"), err: nil}},
		},
	}

	var attempts []AttemptInfo
	rt := mustNewTransport(t, Config{
		Endpoints: testEndpoints(u1, u2),
		Base:      base,
		OnAttempt: func(info AttemptInfo) {
			attempts = append(attempts, info)
		},
	})

	req := newRPCRequest(t, u1, "eth_sendRawTransaction")
	resp, err := rt.RoundTrip(req)
	if resp != nil {
		t.Fatalf("expected nil response, got %#v", resp)
	}

	assertNonIdempotentBlocked(t, err, io.EOF)

	if len(attempts) != 1 {
		t.Fatalf("expected 1 attempt observation, got %d: %#v", len(attempts), attempts)
	}

	got := attempts[0]
	if got.Attempt != 1 {
		t.Fatalf("expected Attempt=1, got %d", got.Attempt)
	}
	if got.Upstream != u1 {
		t.Fatalf("expected Upstream=%q, got %q", u1, got.Upstream)
	}
	if got.Method != "eth_sendRawTransaction" {
		t.Fatalf("expected Method=eth_sendRawTransaction, got %q", got.Method)
	}
	if got.StatusCode != 0 {
		t.Fatalf("expected StatusCode=0, got %d", got.StatusCode)
	}
	if !errors.Is(got.Err, io.EOF) {
		t.Fatalf("expected Err to be io.EOF, got %v", got.Err)
	}
	if !got.Final {
		t.Fatalf("expected Final=true")
	}

	assertCalls(t, base, u1)
}

func TestRoundTrip_NonIdempotentBlockedByDefault(t *testing.T) {
	cases := []struct {
		name string
		body string
	}{
		{
			name: "single non idempotent method",
			body: `{"jsonrpc":"2.0","id":1,"method":"eth_sendRawTransaction","params":["0xdeadbeef"]}`,
		},
		{
			name: "batch containing non idempotent call",
			body: `[
				{"jsonrpc":"2.0","id":1,"method":"eth_blockNumber","params":[]},
				{"jsonrpc":"2.0","id":2,"method":"eth_sendTransaction","params":[{"from":"0x0"}]}
			]`,
		},
		{
			name: "invalid json treated non idempotent",
			body: `{not-json`,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			u1 := "https://u1.test/rpc"
			u2 := "https://u2.test/rpc"

			base := &scriptRT{
				results: map[string][]rtResult{
					u1: {{resp: nil, err: io.EOF}},
					u2: {{resp: httpResp(200, "ok"), err: nil}}, // should not be called when blocked
				},
			}

			rt := mustNewTransport(t, Config{
				Endpoints: testEndpoints(u1, u2),
				Base:      base,
				// AllowNonIdempotent defaults to false.
			})

			req := newJSONRequest(t, u1, tc.body)
			resp, err := rt.RoundTrip(req)
			if resp != nil {
				t.Fatalf("expected nil response, got %#v", resp)
			}

			assertNonIdempotentBlocked(t, err, io.EOF)
			assertCalls(t, base, u1)
		})
	}
}

func TestRoundTrip_NonIdempotentAllowed_FailoverSucceeds(t *testing.T) {
	u1 := "https://u1.test/rpc"
	u2 := "https://u2.test/rpc"

	base := &scriptRT{
		results: map[string][]rtResult{
			u1: {{resp: nil, err: io.EOF}},
			u2: {{resp: httpResp(200, "ok"), err: nil}},
		},
	}

	rt := mustNewTransport(t, Config{
		Endpoints:          testEndpoints(u1, u2),
		Base:               base,
		AllowNonIdempotent: true,
	})

	req := newJSONRequest(t, u1, `{"jsonrpc":"2.0","id":1,"method":"eth_sendRawTransaction","params":["0xdeadbeef"]}`)
	resp, err := rt.RoundTrip(req)
	if err != nil {
		t.Fatalf("RoundTrip error: %v", err)
	}
	t.Cleanup(func() { resp.Body.Close() })

	assertStatus(t, resp, 200)
	assertCalls(t, base, u1, u2)
}

func TestRoundTrip_AdditionalNonIdempotentMethod_BlockedByDefault(t *testing.T) {
	u1 := "https://u1.test/rpc"
	u2 := "https://u2.test/rpc"

	base := &scriptRT{
		results: map[string][]rtResult{
			u1: {{resp: nil, err: io.EOF}},
			u2: {{resp: httpResp(200, "ok")}}, // should not be called when blocked
		},
	}

	rt := mustNewTransport(t, Config{
		Endpoints:                      testEndpoints(u1, u2),
		Base:                           base,
		AdditionalNonIdempotentMethods: []string{"custom_send"},
	})

	req := newRPCRequest(t, u1, "custom_send")
	resp, err := rt.RoundTrip(req)
	if resp != nil {
		t.Fatalf("expected nil response, got %#v", resp)
	}

	var be *NonIdempotentBlockedError
	if !errors.As(err, &be) {
		t.Fatalf("expected NonIdempotentBlockedError, got %v", err)
	}
	if !errors.Is(err, io.EOF) {
		t.Fatalf("expected underlying cause io.EOF, got %v", err)
	}
	if be.Outcome.Method != "custom_send" {
		t.Fatalf("expected blocked method custom_send, got %q", be.Outcome.Method)
	}

	assertCalls(t, base, u1)
}

func TestRoundTrip_AdditionalNonIdempotentMethod_AllowNonIdempotentFailsOver(t *testing.T) {
	u1 := "https://u1.test/rpc"
	u2 := "https://u2.test/rpc"

	base := &scriptRT{
		results: map[string][]rtResult{
			u1: {{resp: nil, err: io.EOF}},
			u2: {{resp: httpResp(200, "ok")}},
		},
	}

	rt := mustNewTransport(t, Config{
		Endpoints:                      testEndpoints(u1, u2),
		Base:                           base,
		AllowNonIdempotent:             true,
		AdditionalNonIdempotentMethods: []string{"custom_send"},
	})

	req := newRPCRequest(t, u1, "custom_send")
	resp, err := rt.RoundTrip(req)
	if err != nil {
		t.Fatalf("RoundTrip error: %v", err)
	}
	t.Cleanup(func() { resp.Body.Close() })

	assertStatus(t, resp, 200)
	assertCalls(t, base, u1, u2)
}

func TestRoundTrip_BatchWithAdditionalNonIdempotentMethod_BlockedByDefault(t *testing.T) {
	u1 := "https://u1.test/rpc"
	u2 := "https://u2.test/rpc"

	base := &scriptRT{
		results: map[string][]rtResult{
			u1: {{resp: nil, err: io.EOF}},
			u2: {{resp: httpResp(200, "ok")}}, // should not be called when blocked
		},
	}

	rt := mustNewTransport(t, Config{
		Endpoints:                      testEndpoints(u1, u2),
		Base:                           base,
		AdditionalNonIdempotentMethods: []string{"custom_send"},
	})

	req := newJSONRequest(t, u1, `[
		{"jsonrpc":"2.0","id":1,"method":"eth_blockNumber","params":[]},
		{"jsonrpc":"2.0","id":2,"method":"custom_send","params":[]}
	]`)

	resp, err := rt.RoundTrip(req)
	if resp != nil {
		t.Fatalf("expected nil response, got %#v", resp)
	}

	var be *NonIdempotentBlockedError
	if !errors.As(err, &be) {
		t.Fatalf("expected NonIdempotentBlockedError, got %v", err)
	}
	if !errors.Is(err, io.EOF) {
		t.Fatalf("expected underlying cause io.EOF, got %v", err)
	}
	if !be.Outcome.Batch {
		t.Fatalf("expected batch outcome")
	}

	assertCalls(t, base, u1)
}

func TestRoundTrip_ContextDoneBeforeCall_BaseNotCalled(t *testing.T) {
	u1 := "https://u1.test/rpc"

	t.Run("canceled", func(t *testing.T) {
		base := &scriptRT{
			results: map[string][]rtResult{
				// No entries; if base is called, scriptRT will error and the test will fail.
			},
		}

		rt := mustNewTransport(t, Config{
			Endpoints: testEndpoints(u1),
			Base:      base,
		})

		req := newRPCRequest(t, u1, "eth_blockNumber")
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		req = req.WithContext(ctx)

		resp, err := rt.RoundTrip(req)
		if resp != nil {
			t.Fatalf("expected nil response, got %#v", resp)
		}
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("expected context.Canceled, got %v", err)
		}
		assertCalls(t, base)
	})

	t.Run("deadline exceeded", func(t *testing.T) {
		base := &scriptRT{
			results: map[string][]rtResult{
				// No entries; if base is called, scriptRT will error and the test will fail.
			},
		}

		rt := mustNewTransport(t, Config{
			Endpoints: testEndpoints(u1),
			Base:      base,
		})

		req := newRPCRequest(t, u1, "eth_blockNumber")
		ctx, cancel := context.WithDeadline(context.Background(), time.Unix(1, 0))
		t.Cleanup(cancel)
		req = req.WithContext(ctx)

		resp, err := rt.RoundTrip(req)
		if resp != nil {
			t.Fatalf("expected nil response, got %#v", resp)
		}
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("expected context.DeadlineExceeded, got %v", err)
		}
		assertCalls(t, base)
	})
}

func TestRoundTrip_CanceledByBase_ReturnsImmediately(t *testing.T) {
	u1 := "https://u1.test/rpc"
	u2 := "https://u2.test/rpc"

	base := &scriptRT{
		results: map[string][]rtResult{
			u1: {
				{resp: nil, err: fmt.Errorf("wrapped: %w", context.Canceled)},
			},
			// If rcpx incorrectly fails over, we'd hit u2.
			u2: {
				{resp: httpResp(200, "ok"), err: nil},
			},
		},
	}

	pol, policy := newPolicyRecorder(true)

	rt := mustNewTransport(t, Config{
		Endpoints:   testEndpoints(u1, u2),
		Base:        base,
		RetryPolicy: policy,
	})

	req := newRPCRequest(t, u1, "eth_blockNumber")
	resp, err := rt.RoundTrip(req)
	if resp != nil {
		t.Fatalf("expected nil response, got %#v", resp)
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}

	assertCalls(t, base, u1)
	assertPolicyCalls(t, pol, 0)
}

func TestRoundTrip_CanceledByBaseWithResponse_ClosesBodyAndReturnsImmediately(t *testing.T) {
	u1 := "https://u1.test/rpc"
	u2 := "https://u2.test/rpc"

	respBody := newTrackingBody("should be closed")
	resp1 := &http.Response{StatusCode: 200, Body: respBody}

	base := &scriptRT{
		results: map[string][]rtResult{
			u1: {
				// resp+err: must close body; cancellation rail => return immediately (no failover)
				{resp: resp1, err: context.Canceled},
			},
			// Should not be called.
			u2: {
				{resp: httpResp(200, "ok"), err: nil},
			},
		},
	}

	pol, policy := newPolicyRecorder(true)

	rt := mustNewTransport(t, Config{
		Endpoints:   testEndpoints(u1, u2),
		Base:        base,
		RetryPolicy: policy,
	})

	req := newRPCRequest(t, u1, "eth_blockNumber")
	resp, err := rt.RoundTrip(req)
	if resp != nil {
		t.Fatalf("expected nil response, got %#v", resp)
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}

	assertPolicyCalls(t, pol, 0)

	if !respBody.Closed() {
		t.Fatalf("expected response body to be closed when resp+err returned from base")
	}
	assertCalls(t, base, u1)
}

func TestRoundTrip_DeadlineExceededDoesNotCountForCooldownByDefault(t *testing.T) {
	u1 := "https://u1.test/rpc"
	u2 := "https://u2.test/rpc"

	base := &scriptRT{
		results: map[string][]rtResult{
			u1: {
				{resp: nil, err: context.DeadlineExceeded},
				{resp: httpResp(200, "ok"), err: nil},
			},
			u2: {
				{resp: httpResp(200, "unexpected"), err: nil},
			},
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
	fixedNow := time.Unix(500, 0)
	tr.now = func() time.Time { return fixedNow }

	req1 := newRPCRequest(t, u1, "eth_blockNumber")
	resp, err := tr.RoundTrip(req1)
	if resp != nil {
		t.Fatalf("expected nil response, got %#v", resp)
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected context.DeadlineExceeded, got %v", err)
	}

	req2 := newRPCRequest(t, u1, "eth_blockNumber")
	resp = mustRoundTrip(t, tr, req2)
	assertStatus(t, resp, 200)

	assertCalls(t, base, u1, u1)
}

func TestCooldown_TripsAfterNConsecutiveFailoverFailures_SkipsCooledUpstream(t *testing.T) {
	u1 := "https://u1.test/rpc"
	u2 := "https://u2.test/rpc"

	// Request #1: u1 => 503 (retryable), u2 => 200
	// Request #2: u1 => 503 (retryable), u2 => 200 (this second failover trips cooldown for u1)
	// Request #3: u1 skipped (cooling), u2 => 200
	tb1 := newTrackingBody("503-1")
	tb2 := newTrackingBody("503-2")
	base := &scriptRT{
		results: map[string][]rtResult{
			u1: {
				{resp: &http.Response{StatusCode: 503, Body: tb1}, err: nil},
				{resp: &http.Response{StatusCode: 503, Body: tb2}, err: nil},
			},
			u2: {
				{resp: httpResp(200, "ok1"), err: nil},
				{resp: httpResp(200, "ok2"), err: nil},
				{resp: httpResp(200, "ok3"), err: nil},
			},
		},
	}

	tr := mustNewTransport(t, Config{
		Endpoints: testEndpoints(u1, u2),
		Base:      base,
		Cooldown: CooldownConfig{
			Threshold: 2,
			Duration:  time.Minute,
		},
	})
	fixedNow := time.Unix(100, 0)
	tr.now = func() time.Time { return fixedNow }

	makeReq := func() *http.Request { return newRPCRequest(t, u1, "eth_blockNumber") }

	mustRoundTripCode(t, tr, makeReq(), 200)
	mustRoundTripCode(t, tr, makeReq(), 200)
	mustRoundTripCode(t, tr, makeReq(), 200)

	if !tb1.Closed() || !tb2.Closed() {
		t.Fatalf("expected retryable 503 bodies to be closed on failover")
	}
	assertCalls(t, base, u1, u2, u1, u2, u2)
}

func TestCooldown_ResetsOnSuccess(t *testing.T) {
	u1 := "https://u1.test/rpc"
	u2 := "https://u2.test/rpc"

	// Sequence across 3 requests:
	// Req1: u1 503 => failover to u2 200 (consec=1)
	// Req2: u1 200 => success resets (consec=0)
	// Req3: u1 503 => should NOT be skipped; failover to u2 200 (consec=1 again)
	tbFail1 := newTrackingBody("503-1")
	tbFail2 := newTrackingBody("503-2")
	base := &scriptRT{
		results: map[string][]rtResult{
			u1: {
				{resp: &http.Response{StatusCode: 503, Body: tbFail1}, err: nil},
				{resp: httpResp(200, "ok-u1"), err: nil},
				{resp: &http.Response{StatusCode: 503, Body: tbFail2}, err: nil},
			},
			u2: {
				{resp: httpResp(200, "ok1"), err: nil},
				{resp: httpResp(200, "ok3"), err: nil},
			},
		},
	}

	tr := mustNewTransport(t, Config{
		Endpoints: testEndpoints(u1, u2),
		Base:      base,
		Cooldown: CooldownConfig{
			Threshold: 2,
			Duration:  time.Minute,
		},
	})
	fixedNow := time.Unix(200, 0)
	tr.now = func() time.Time { return fixedNow }

	makeReq := func() *http.Request { return newRPCRequest(t, u1, "eth_blockNumber") }

	mustRoundTripCode(t, tr, makeReq(), 200)
	mustRoundTripCode(t, tr, makeReq(), 200)
	mustRoundTripCode(t, tr, makeReq(), 200)

	if !tbFail1.Closed() || !tbFail2.Closed() {
		t.Fatalf("expected retryable 503 bodies to be closed on failover")
	}
	assertCalls(t, base, u1, u2, u1, u1, u2)
}

func TestCooldown_NoEligibleUpstreams_ReturnsAggregateError_UnwrapsSentinel(t *testing.T) {
	u1 := "https://u1.test/rpc"
	u2 := "https://u2.test/rpc"

	base := &scriptRT{results: map[string][]rtResult{}}

	tr := mustNewTransport(t, Config{
		Endpoints: testEndpoints(u1, u2),
		Base:      base,
		Cooldown: CooldownConfig{
			Threshold: 1,
			Duration:  time.Hour,
		},
	})
	fixedNow := time.Unix(300, 0)
	tr.now = func() time.Time { return fixedNow }

	// Force both upstreams into cooldown.
	if tr.cooldown == nil {
		t.Fatalf("expected cooldown tracker")
	}
	tr.cooldown.mu.Lock()
	tr.cooldown.enabled = true
	tr.cooldown.coolingTo[0] = fixedNow.Add(1 * time.Hour)
	tr.cooldown.coolingTo[1] = fixedNow.Add(1 * time.Hour)
	tr.cooldown.mu.Unlock()

	req := newRPCRequest(t, u1, "eth_blockNumber")
	resp, err := tr.RoundTrip(req)
	if resp != nil {
		t.Fatalf("expected nil response, got %#v", resp)
	}

	ae := mustAsAllUpstreamsFailed(t, err)
	if ae.Attempted != 0 {
		t.Fatalf("expected Attempted=0, got %d", ae.Attempted)
	}
	if ae.SkippedCooldown != 2 {
		t.Fatalf("expected SkippedCooldown=2, got %d", ae.SkippedCooldown)
	}
	if !errors.Is(err, ErrNoEligibleUpstreams) {
		t.Fatalf("expected errors.Is(err, ErrNoEligibleUpstreams)=true, got %v", err)
	}

	assertCalls(t, base)
}

func TestRoundTrip_DoesNotMutateOriginalRequestURLOrHost(t *testing.T) {
	u1 := "https://u1.test/rpc"
	u2 := "https://u2.test/rpc"

	base := &scriptRT{
		results: map[string][]rtResult{
			u1: {{resp: httpResp(200, "ok"), err: nil}},
		},
	}

	tr := mustNewTransport(t, Config{
		Endpoints: testEndpoints(u1, u2),
		Base:      base,
	})

	req := newRPCRequest(t, u1, "eth_blockNumber")
	req.Host = "signed.example"
	origURL := req.URL.String()
	origHost := req.Host
	origRequestURI := req.RequestURI

	resp, err := tr.RoundTrip(req)
	if err != nil {
		t.Fatalf("RoundTrip error: %v", err)
	}
	t.Cleanup(func() { resp.Body.Close() })

	assertStatus(t, resp, 200)

	if req.URL.String() != origURL {
		t.Fatalf("original req.URL mutated: got=%q want=%q", req.URL.String(), origURL)
	}
	if req.Host != origHost {
		t.Fatalf("original req.Host mutated: got=%q want=%q", req.Host, origHost)
	}
	if req.RequestURI != origRequestURI {
		t.Fatalf("original req.RequestURI mutated: got=%q want=%q", req.RequestURI, origRequestURI)
	}

	assertCalls(t, base, u1)
}

func TestRoundTrip_UsesConfiguredEndpointOrder(t *testing.T) {
	firstURL := "https://z-priority.test/rpc"
	secondURL := "https://a-priority.test/rpc"

	base := &scriptRT{
		results: map[string][]rtResult{
			firstURL:  {{resp: nil, err: io.EOF}},
			secondURL: {{resp: httpResp(200, "ok"), err: nil}},
		},
	}

	tr := mustNewTransport(t, Config{
		Endpoints: []Endpoint{
			{ID: "z-id", URL: firstURL},
			{ID: "a-id", URL: secondURL},
		},
		Base: base,
	})

	req := newRPCRequest(t, secondURL, "eth_blockNumber")
	resp := mustRoundTrip(t, tr, req)
	assertStatus(t, resp, 200)
	assertCalls(t, base, firstURL, secondURL)
}

func TestRoundTrip_UsesCompleteEndpointDestinationAndPreservesHost(t *testing.T) {
	endpointURL := "https://endpoint.test/fixed/rpc?key=abc"
	originalURL := "https://original.test/original/path?old=1"

	inspector := roundTripperFunc(func(req *http.Request) (*http.Response, error) {
		if req.URL == nil {
			t.Fatal("outbound req.URL is nil")
		}
		if got := req.URL.Scheme; got != "https" {
			t.Fatalf("expected scheme https, got %q", got)
		}
		if got := req.URL.Host; got != "endpoint.test" {
			t.Fatalf("expected URL host endpoint.test, got %q", got)
		}
		if got := req.URL.Path; got != "/fixed/rpc" {
			t.Fatalf("expected endpoint path /fixed/rpc, got %q", got)
		}
		if got := req.URL.RawQuery; got != "key=abc" {
			t.Fatalf("expected endpoint query key=abc, got %q", got)
		}
		if got := req.Host; got != "signed.example" {
			t.Fatalf("expected caller Host preserved, got %q", got)
		}
		if req.RequestURI != "" {
			t.Fatalf("expected RequestURI cleared on client request, got %q", req.RequestURI)
		}
		return &http.Response{
			StatusCode: 200,
			Body:       io.NopCloser(strings.NewReader("ok")),
			Header:     make(http.Header),
			Request:    req,
		}, nil
	})

	tr := mustNewTransport(t, Config{
		Endpoints: []Endpoint{{ID: "primary", URL: endpointURL}},
		Base:      inspector,
	})

	req := newRPCRequest(t, originalURL, "eth_blockNumber")
	req.Host = "signed.example"
	resp, err := tr.RoundTrip(req)
	if err != nil {
		t.Fatalf("RoundTrip error: %v", err)
	}
	t.Cleanup(func() { resp.Body.Close() })

	assertStatus(t, resp, 200)
}

func TestRoundTrip_AllUpstreamsFailedError_CollectsFailuresAndUnwrapsCause(t *testing.T) {
	u1 := "https://u1.test/rpc"
	u2 := "https://u2.test/rpc"

	tb1 := newTrackingBody("503")
	base := &scriptRT{
		results: map[string][]rtResult{
			u1: {
				{resp: &http.Response{StatusCode: 503, Body: tb1}, err: nil},
			},
			u2: {
				{resp: nil, err: io.EOF},
			},
		},
	}

	tr := mustNewTransport(t, Config{
		Endpoints: testEndpoints(u1, u2),
		Base:      base,
	})

	req := newRPCRequest(t, u1, "eth_blockNumber")
	resp, err := tr.RoundTrip(req)
	if resp != nil {
		t.Fatalf("expected nil response, got %#v", resp)
	}

	ae := mustAsAllUpstreamsFailed(t, err)
	if ae.Attempted != 2 {
		t.Fatalf("expected Attempted=2, got %d", ae.Attempted)
	}
	if len(ae.Failures) != 2 {
		t.Fatalf("expected 2 failures, got %d: %#v", len(ae.Failures), ae.Failures)
	}

	f0 := ae.Failures[0]
	if f0.StatusCode != 503 {
		t.Fatalf("expected first failure status 503, got %d", f0.StatusCode)
	}
	if f0.Err == nil {
		t.Fatalf("expected first failure Err to be non nil (synthesized status cause)")
	}
	if !f0.Retryable {
		t.Fatalf("expected first failure Retryable=true (continued after 503)")
	}
	if !tb1.Closed() {
		t.Fatalf("expected 503 response body closed on failover")
	}

	f1 := ae.Failures[1]
	if !errors.Is(f1.Err, io.EOF) {
		t.Fatalf("expected second failure Err to be or unwrap to io.EOF, got %v", f1.Err)
	}
	if f1.Retryable {
		t.Fatalf("expected second failure Retryable=false (no further upstream)")
	}

	if !errors.Is(err, io.EOF) {
		t.Fatalf("expected aggregate error to unwrap to io.EOF, got %v", err)
	}

	assertCalls(t, base, u1, u2)
}

func TestRoundTrip_BodyBufferedOnce_ReplayedAcrossAttempts(t *testing.T) {
	u1 := "https://u1.test/rpc"
	u2 := "https://u2.test/rpc"

	payload := `{"jsonrpc":"2.0","id":1,"method":"eth_blockNumber","params":[]}`

	var gotURLs []string
	var gotBodies []string

	readBody := func(req *http.Request) string {
		t.Helper()

		b, err := io.ReadAll(req.Body)
		if err != nil {
			t.Fatalf("read req body: %v", err)
		}
		req.Body.Close()
		return string(b)
	}

	dispatch := roundTripperFunc(func(req *http.Request) (*http.Response, error) {
		gotURLs = append(gotURLs, req.URL.String())
		gotBodies = append(gotBodies, readBody(req))

		switch req.URL.String() {
		case u1:
			// Fail in a retryable way so rcpx will fail over.
			return nil, io.EOF
		case u2:
			return &http.Response{
				StatusCode: 200,
				Body:       io.NopCloser(strings.NewReader("ok")),
				Header:     make(http.Header),
				Request:    req,
			}, nil
		default:
			return nil, errors.New("unexpected url: " + req.URL.String())
		}
	})

	rt := mustNewTransport(t, Config{
		Endpoints: testEndpoints(u1, u2),
		Base:      dispatch,
	})

	req := newJSONRequest(t, u1, payload)
	resp, err := rt.RoundTrip(req)
	if err != nil {
		t.Fatalf("RoundTrip error: %v", err)
	}
	t.Cleanup(func() { resp.Body.Close() })

	assertStatus(t, resp, 200)

	if len(gotURLs) != 2 || gotURLs[0] != u1 || gotURLs[1] != u2 {
		t.Fatalf("unexpected attempt order: %v", gotURLs)
	}
	if len(gotBodies) != 2 || gotBodies[0] != payload || gotBodies[1] != payload {
		t.Fatalf("unexpected bodies: %q", gotBodies)
	}
}

func TestRoundTrip_PreservesNilBodyWhenOriginalBodyNilAndEmpty(t *testing.T) {
	u1 := "https://u1.test/rpc"

	inspector := roundTripperFunc(func(req *http.Request) (*http.Response, error) {
		if req.Body != nil {
			t.Fatalf("expected outbound req.Body == nil, got %T", req.Body)
		}
		if req.ContentLength != 0 {
			t.Fatalf("expected ContentLength=0, got %d", req.ContentLength)
		}
		if req.GetBody != nil {
			t.Fatalf("expected GetBody == nil for nil body request")
		}
		return &http.Response{
			StatusCode: 200,
			Body:       io.NopCloser(strings.NewReader("ok")),
			Header:     make(http.Header),
			Request:    req,
		}, nil
	})

	rt := mustNewTransport(t, Config{
		Endpoints: testEndpoints(u1),
		Base:      inspector,
	})

	// Construct a request and force Body to nil (NewRequest usually sets http.NoBody).
	req, err := http.NewRequest("POST", u1, nil)
	if err != nil {
		t.Fatalf("http.NewRequest: %v", err)
	}
	req.Body = nil
	req.ContentLength = 0
	req.GetBody = nil

	resp, err := rt.RoundTrip(req)
	if err != nil {
		t.Fatalf("RoundTrip error: %v", err)
	}
	t.Cleanup(func() { resp.Body.Close() })

	assertStatus(t, resp, 200)
}

func TestRoundTrip_BodyTooLarge_ReturnsErrBodyTooLarge_BaseNotCalled(t *testing.T) {
	u1 := "https://u1.test/rpc"

	base := &scriptRT{results: map[string][]rtResult{}}

	rt := mustNewTransport(t, Config{
		Endpoints:       testEndpoints(u1),
		Base:            base,
		BodyBufferBytes: 8, // tiny cap
	})

	// 9 bytes > cap
	req := newJSONRequest(t, u1, "123456789")
	resp, err := rt.RoundTrip(req)
	if resp != nil {
		t.Fatalf("expected nil response, got %#v", resp)
	}
	if !errors.Is(err, ErrBodyTooLarge) {
		t.Fatalf("expected ErrBodyTooLarge, got %v", err)
	}

	assertCalls(t, base)
}

func TestRoundTrip_BodyUnreadable_ReturnsErrBodyUnreadable_BaseNotCalled(t *testing.T) {
	u1 := "https://u1.test/rpc"

	base := &scriptRT{results: map[string][]rtResult{}}

	rt := mustNewTransport(t, Config{
		Endpoints: testEndpoints(u1),
		Base:      base,
	})

	req, err := http.NewRequest("POST", u1, io.NopCloser(unreadableReader{}))
	if err != nil {
		t.Fatalf("http.NewRequest: %v", err)
	}

	resp, rerr := rt.RoundTrip(req)
	if resp != nil {
		t.Fatalf("expected nil response, got %#v", resp)
	}
	if !errors.Is(rerr, ErrBodyUnreadable) {
		t.Fatalf("expected errors.Is(err, ErrBodyUnreadable)=true, got %v", rerr)
	}

	assertCalls(t, base)
}

type closeIdleTrackingRT struct {
	closeCalls     int
	roundTripCalls int
}

func (r *closeIdleTrackingRT) RoundTrip(*http.Request) (*http.Response, error) {
	r.roundTripCalls++
	return nil, errors.New("unexpected RoundTrip")
}

func (r *closeIdleTrackingRT) CloseIdleConnections() {
	r.closeCalls++
}

func TestTransport_CloseIdleConnections_ForwardsWhenSupported(t *testing.T) {
	base := &closeIdleTrackingRT{}
	tr := mustNewTransport(t, Config{
		Endpoints: testEndpoints("https://u1.test/rpc"),
		Base:      base,
	})

	tr.CloseIdleConnections()

	if base.closeCalls != 1 {
		t.Fatalf("expected CloseIdleConnections calls=1, got %d", base.closeCalls)
	}
	if base.roundTripCalls != 0 {
		t.Fatalf("expected RoundTrip calls=0, got %d", base.roundTripCalls)
	}
}

func TestTransport_CloseIdleConnections_NoOpWhenUnsupported(t *testing.T) {
	roundTripCalls := 0
	base := roundTripperFunc(func(*http.Request) (*http.Response, error) {
		roundTripCalls++
		return nil, errors.New("unexpected RoundTrip")
	})
	tr := mustNewTransport(t, Config{
		Endpoints: testEndpoints("https://u1.test/rpc"),
		Base:      base,
	})

	tr.CloseIdleConnections()

	if roundTripCalls != 0 {
		t.Fatalf("expected RoundTrip calls=0, got %d", roundTripCalls)
	}
}
