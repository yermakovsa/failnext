package failnext

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestRoundTrip_HTTP500_IsTerminal_NoFailover(t *testing.T) {
	u1 := "https://u1.test/rpc"
	u2 := "https://u2.test/rpc"

	respBody := newTrackingBody("internal error")
	base := &scriptRT{
		results: map[string][]rtResult{
			u1: {
				{resp: &http.Response{StatusCode: 500, Body: respBody}, err: nil},
			},
			// Intentionally omit u2. If failnext fails over incorrectly, scriptRT will error on an unexpected call.
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

	// For a non-trigger status, the response is returned unchanged and the caller owns the body.
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
	req = req.WithContext(WithFailoverAllowed(req.Context()))
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
	req = req.WithContext(WithFailoverAllowed(req.Context()))
	resp := mustRoundTrip(t, rt, req)
	assertStatus(t, resp, 200)
	if !respBody.Closed() {
		t.Fatalf("expected 503 response body to be closed before failover")
	}
	assertCalls(t, base, u1, u2)
}

func TestRoundTrip_NonConfiguredStatus_IsTerminal_NoFailover(t *testing.T) {
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

func TestRoundTrip_HTTP200_WithJSONRPCErrorPayload_IsTerminal_NoFailover(t *testing.T) {
	u1 := "https://u1.test/rpc"
	u2 := "https://u2.test/rpc"

	// failnext must not interpret the JSON-RPC payload; HTTP 200 is non-triggering and terminal.
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
	req = req.WithContext(WithFailoverAllowed(req.Context()))
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
				{resp: nil, err: io.EOF}, // live-context transport error => failover
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
	req = req.WithContext(WithFailoverAllowed(req.Context()))
	resp := mustRoundTrip(t, rt, req)

	assertStatus(t, resp, 200)
	assertCalls(t, base, u1, u2)
}

func TestRoundTrip_FailoverOnBuiltInTriggerStatus_ClosesBody(t *testing.T) {
	cases := []struct {
		name   string
		status int
		body   string
	}{
		{name: "503 service unavailable", status: 503, body: "service unavailable"},
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
			req = req.WithContext(WithFailoverAllowed(req.Context()))
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
	req = req.WithContext(WithFailoverAllowed(req.Context()))
	resp := mustRoundTrip(t, rt, req)

	assertStatus(t, resp, 200)
	if !respBody.Closed() {
		t.Fatalf("expected response body from (resp, err) attempt to be closed before failover")
	}
	assertCalls(t, base, u1, u2)
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

func TestRoundTrip_LiveCooldownSkipsCandidateThatBeginsCoolingBeforeTurn(t *testing.T) {
	u1 := "https://u1.test/rpc"
	u2 := "https://u2.test/rpc"
	u3 := "https://u3.test/rpc"
	fixedNow := time.Unix(600, 0)

	var tr *Transport
	var calls []string
	base := roundTripperFunc(func(req *http.Request) (*http.Response, error) {
		calls = append(calls, req.URL.String())

		switch req.URL.String() {
		case u1:
			tr.cooldown.mu.Lock()
			tr.cooldown.coolingTo[1] = fixedNow.Add(time.Hour)
			tr.cooldown.mu.Unlock()
			return nil, io.EOF
		case u2:
			return nil, errors.New("cooling candidate was physically attempted")
		case u3:
			return httpResp(http.StatusOK, "ok"), nil
		default:
			return nil, fmt.Errorf("unexpected URL: %s", req.URL)
		}
	})

	tr = mustNewTransport(t, Config{
		Endpoints: testEndpoints(u1, u2, u3),
		Base:      base,
	})
	tr.now = func() time.Time { return fixedNow }

	req := newRPCRequest(t, u1, "eth_blockNumber")
	req = req.WithContext(WithFailoverAllowed(req.Context()))
	resp := mustRoundTrip(t, tr, req)
	assertStatus(t, resp, http.StatusOK)

	want := []string{u1, u3}
	if len(calls) != len(want) {
		t.Fatalf("unexpected physical attempts: got=%v want=%v", calls, want)
	}
	for i := range want {
		if calls[i] != want[i] {
			t.Fatalf("unexpected physical attempts: got=%v want=%v", calls, want)
		}
	}
}

func TestRoundTrip_LiveCooldownAdmitsCandidateThatExpiresBeforeTurn(t *testing.T) {
	u1 := "https://u1.test/rpc"
	u2 := "https://u2.test/rpc"

	now := time.Unix(700, 0)
	base := roundTripperFunc(func(req *http.Request) (*http.Response, error) {
		switch req.URL.String() {
		case u1:
			now = now.Add(2 * time.Hour)
			return nil, io.EOF
		case u2:
			return httpResp(http.StatusOK, "ok"), nil
		default:
			return nil, fmt.Errorf("unexpected URL: %s", req.URL)
		}
	})

	tr := mustNewTransport(t, Config{
		Endpoints: testEndpoints(u1, u2),
		Base:      base,
	})
	tr.now = func() time.Time { return now }

	tr.cooldown.mu.Lock()
	tr.cooldown.coolingTo[1] = now.Add(time.Hour)
	tr.cooldown.mu.Unlock()

	req := newRPCRequest(t, u1, "eth_blockNumber")
	req = req.WithContext(WithFailoverAllowed(req.Context()))
	resp := mustRoundTrip(t, tr, req)
	assertStatus(t, resp, http.StatusOK)
}

func TestRoundTrip_LiveCooldownNeverRevisitsPassedEndpoint(t *testing.T) {
	u1 := "https://u1.test/rpc"
	u2 := "https://u2.test/rpc"
	u3 := "https://u3.test/rpc"

	now := time.Unix(800, 0)
	var calls []string
	base := roundTripperFunc(func(req *http.Request) (*http.Response, error) {
		calls = append(calls, req.URL.String())

		switch req.URL.String() {
		case u1:
			return nil, io.EOF
		case u2:
			return nil, errors.New("cooling candidate was physically attempted")
		case u3:
			now = now.Add(2 * time.Hour)
			return nil, io.ErrUnexpectedEOF
		default:
			return nil, fmt.Errorf("unexpected URL: %s", req.URL)
		}
	})

	tr := mustNewTransport(t, Config{
		Endpoints: testEndpoints(u1, u2, u3),
		Base:      base,
		Cooldown: CooldownConfig{
			Threshold: 1,
			Duration:  time.Hour,
		},
	})
	tr.now = func() time.Time { return now }

	tr.cooldown.mu.Lock()
	tr.cooldown.coolingTo[1] = now.Add(time.Hour)
	tr.cooldown.mu.Unlock()

	req := newRPCRequest(t, u1, "eth_blockNumber")
	req = req.WithContext(WithFailoverAllowed(req.Context()))
	resp, err := tr.RoundTrip(req)
	if resp != nil {
		t.Fatalf("expected nil response, got %#v", resp)
	}

	fe := mustAsFailoverError(t, err)
	if len(fe.Attempts) != 2 {
		t.Fatalf("expected 2 no-response attempts, got %d", len(fe.Attempts))
	}

	want := []string{u1, u3}
	if len(calls) != len(want) {
		t.Fatalf("unexpected physical attempts: got=%v want=%v", calls, want)
	}
	for i := range want {
		if calls[i] != want[i] {
			t.Fatalf("unexpected physical attempts: got=%v want=%v", calls, want)
		}
	}
}

func TestRoundTrip_MixedEligibilityAndLiveCooldownAdmission(t *testing.T) {
	u1 := "https://u1.test/rpc"
	u2 := "https://u2.test/rpc"
	u3 := "https://u3.test/rpc"
	fixedNow := time.Unix(900, 0)

	base := &scriptRT{
		results: map[string][]rtResult{
			u3: {{resp: httpResp(http.StatusOK, "ok")}},
		},
	}
	tr := mustNewTransport(t, Config{
		Endpoints: testEndpoints(u1, u2, u3),
		Base:      base,
		Eligible: func(id EndpointID) bool {
			return id != "endpoint-1"
		},
	})
	tr.now = func() time.Time { return fixedNow }

	tr.cooldown.mu.Lock()
	tr.cooldown.coolingTo[1] = fixedNow.Add(time.Hour)
	tr.cooldown.mu.Unlock()

	req := newRPCRequest(t, u1, "eth_blockNumber")
	resp := mustRoundTrip(t, tr, req)
	assertStatus(t, resp, http.StatusOK)
	assertCalls(t, base, u3)
}

func TestCooldown_TripsAfterNConsecutiveAvailabilityFailures_SkipsCooledUpstream(t *testing.T) {
	u1 := "https://u1.test/rpc"
	u2 := "https://u2.test/rpc"

	// Request #1: u1 => 503, u2 => 200
	// Request #2: u1 => 503, u2 => 200 (this second availability failure trips cooldown for u1)
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

	makeReq := func() *http.Request {
		req := newRPCRequest(t, u1, "eth_blockNumber")
		return req.WithContext(WithFailoverAllowed(req.Context()))
	}

	mustRoundTripCode(t, tr, makeReq(), 200)
	mustRoundTripCode(t, tr, makeReq(), 200)
	mustRoundTripCode(t, tr, makeReq(), 200)

	if !tb1.Closed() || !tb2.Closed() {
		t.Fatalf("expected triggering 503 bodies to be closed on failover")
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

	makeReq := func() *http.Request {
		req := newRPCRequest(t, u1, "eth_blockNumber")
		return req.WithContext(WithFailoverAllowed(req.Context()))
	}

	mustRoundTripCode(t, tr, makeReq(), 200)
	mustRoundTripCode(t, tr, makeReq(), 200)
	mustRoundTripCode(t, tr, makeReq(), 200)

	if !tbFail1.Closed() || !tbFail2.Closed() {
		t.Fatalf("expected triggering 503 bodies to be closed on failover")
	}
	assertCalls(t, base, u1, u2, u1, u1, u2)
}

func TestCooldown_AllCandidatesCooling_ReturnsErrNoUsableEndpoint(t *testing.T) {
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

	tr.cooldown.mu.Lock()
	tr.cooldown.coolingTo[0] = fixedNow.Add(time.Hour)
	tr.cooldown.coolingTo[1] = fixedNow.Add(time.Hour)
	tr.cooldown.mu.Unlock()

	req := newRPCRequest(t, u1, "eth_blockNumber")
	resp, err := tr.RoundTrip(req)
	if resp != nil {
		t.Fatalf("expected nil response, got %#v", resp)
	}
	if !errors.Is(err, ErrNoUsableEndpoint) {
		t.Fatalf("expected ErrNoUsableEndpoint, got %v", err)
	}
	var fe *FailoverError
	if errors.As(err, &fe) {
		t.Fatalf("expected direct zero-attempt error, got FailoverError: %#v", fe)
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
	req = req.WithContext(WithFailoverAllowed(req.Context()))
	resp := mustRoundTrip(t, tr, req)
	assertStatus(t, resp, 200)
	assertCalls(t, base, firstURL, secondURL)
}

func TestRoundTrip_UsesCompleteEndpointDestinationAndPreservesCustomHost(t *testing.T) {
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

func TestRoundTrip_ConcurrentReuseWithFailover(t *testing.T) {
	const requestCount = 32

	u1 := "https://u1.test/rpc"
	u2 := "https://u2.test/rpc"

	var primaryCalls atomic.Int32
	var backupCalls atomic.Int32
	base := roundTripperFunc(func(req *http.Request) (*http.Response, error) {
		switch req.URL.String() {
		case u1:
			primaryCalls.Add(1)
			return nil, io.EOF
		case u2:
			backupCalls.Add(1)
			return &http.Response{
				StatusCode: http.StatusOK,
				Body:       io.NopCloser(strings.NewReader("ok")),
				Header:     make(http.Header),
				Request:    req,
			}, nil
		default:
			return nil, fmt.Errorf("unexpected URL: %s", req.URL)
		}
	})

	rt := mustNewTransport(t, Config{
		Endpoints: testEndpoints(u1, u2),
		Base:      base,
		Cooldown:  CooldownConfig{Disabled: true},
	})

	requests := make([]*http.Request, requestCount)
	for i := range requests {
		req, err := http.NewRequest(http.MethodGet, "https://logical.test/request", nil)
		if err != nil {
			t.Fatalf("http.NewRequest: %v", err)
		}
		requests[i] = req.WithContext(WithFailoverAllowed(req.Context()))
	}

	start := make(chan struct{})
	errCh := make(chan error, requestCount)
	var wg sync.WaitGroup
	for _, req := range requests {
		wg.Add(1)
		go func(req *http.Request) {
			defer wg.Done()
			<-start

			resp, err := rt.RoundTrip(req)
			if err != nil {
				errCh <- err
				return
			}
			if resp == nil {
				errCh <- errors.New("RoundTrip returned nil response")
				return
			}
			defer resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				errCh <- fmt.Errorf("unexpected status: got=%d want=%d", resp.StatusCode, http.StatusOK)
				return
			}
			errCh <- nil
		}(req)
	}

	close(start)
	wg.Wait()
	close(errCh)

	for err := range errCh {
		if err != nil {
			t.Fatalf("concurrent RoundTrip error: %v", err)
		}
	}
	if got := primaryCalls.Load(); got != requestCount {
		t.Fatalf("expected primary calls=%d, got %d", requestCount, got)
	}
	if got := backupCalls.Load(); got != requestCount {
		t.Fatalf("expected backup calls=%d, got %d", requestCount, got)
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
