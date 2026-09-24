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
