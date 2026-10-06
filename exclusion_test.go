package failnext

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
)

func TestRoundTrip_ExcludedEndpointIsNotAttempted(t *testing.T) {
	u1 := "https://u1.test/rpc"
	u2 := "https://u2.test/rpc"

	base := &scriptRT{
		results: map[string][]rtResult{
			u2: {{resp: newHTTPResp(http.StatusOK, "ok")}},
		},
	}
	rt := mustNewTransport(t, Config{
		Endpoints: testEndpoints(u1, u2),
		Base:      base,
	})

	req := newTestGETRequest(t, u1)
	req = req.WithContext(
		WithExcludedEndpoints(req.Context(), "endpoint-1"),
	)

	mustRoundTripCode(t, rt, req, http.StatusOK)
	assertCalls(t, base, u2)
}

func TestRoundTrip_MultipleExcludedEndpointsPreserveConfiguredOrder(t *testing.T) {
	u1 := "https://u1.test/rpc"
	u2 := "https://u2.test/rpc"
	u3 := "https://u3.test/rpc"
	u4 := "https://u4.test/rpc"

	base := &scriptRT{
		results: map[string][]rtResult{
			u2: {{err: io.EOF}},
			u4: {{resp: newHTTPResp(http.StatusOK, "ok")}},
		},
	}
	rt := mustNewTransport(t, Config{
		Endpoints: testEndpoints(u1, u2, u3, u4),
		Base:      base,
	})

	req := newTestGETRequest(t, u1)
	req = req.WithContext(
		WithExcludedEndpoints(
			req.Context(),
			"endpoint-1",
			"endpoint-3",
		),
	)

	mustRoundTripCode(t, rt, req, http.StatusOK)
	assertCalls(t, base, u2, u4)
}

func TestRoundTrip_ExcludedEndpointsAccumulateAcrossDerivedContexts(t *testing.T) {
	u1 := "https://u1.test/rpc"
	u2 := "https://u2.test/rpc"
	u3 := "https://u3.test/rpc"

	base := &scriptRT{
		results: map[string][]rtResult{
			u3: {{resp: newHTTPResp(http.StatusOK, "ok")}},
		},
	}
	rt := mustNewTransport(t, Config{
		Endpoints: testEndpoints(u1, u2, u3),
		Base:      base,
	})

	ctx := WithExcludedEndpoints(
		context.Background(),
		"endpoint-1",
	)
	ctx = WithExcludedEndpoints(
		ctx,
		"endpoint-1",
		"endpoint-2",
	)

	req := newTestGETRequest(t, u1).WithContext(ctx)

	mustRoundTripCode(t, rt, req, http.StatusOK)
	assertCalls(t, base, u3)
}

func TestRoundTrip_ExcludedEndpointsAreRequestScoped(t *testing.T) {
	u1 := "https://u1.test/rpc"
	u2 := "https://u2.test/rpc"

	base := &scriptRT{
		results: map[string][]rtResult{
			u1: {{resp: newHTTPResp(http.StatusOK, "from u1")}},
			u2: {{resp: newHTTPResp(http.StatusOK, "from u2")}},
		},
	}
	rt := mustNewTransport(t, Config{
		Endpoints: testEndpoints(u1, u2),
		Base:      base,
	})

	parent := context.Background()

	excludeFirst := WithExcludedEndpoints(
		parent,
		"endpoint-1",
	)
	excludeSecond := WithExcludedEndpoints(
		parent,
		"endpoint-2",
	)

	mustRoundTripCode(
		t,
		rt,
		newTestGETRequest(t, u1).WithContext(excludeFirst),
		http.StatusOK,
	)
	mustRoundTripCode(
		t,
		rt,
		newTestGETRequest(t, u1).WithContext(excludeSecond),
		http.StatusOK,
	)

	assertCalls(t, base, u2, u1)
}

func TestRoundTrip_UnknownExcludedEndpointFailsBeforeEligibilityOrAttempt(t *testing.T) {
	u1 := "https://u1.test/rpc"

	eligibilityCalls := 0
	base := &scriptRT{results: map[string][]rtResult{}}

	rt := mustNewTransport(t, Config{
		Endpoints: testEndpoints(u1),
		Base:      base,
		Eligible: func(EndpointID) bool {
			eligibilityCalls++
			return true
		},
	})

	req := newTestGETRequest(t, u1)
	req = req.WithContext(
		WithExcludedEndpoints(req.Context(), "missing"),
	)

	resp, err := rt.RoundTrip(req)

	if resp != nil {
		t.Fatalf("expected nil response, got %#v", resp)
	}
	if !errors.Is(err, ErrUnknownEndpoint) {
		t.Fatalf("expected ErrUnknownEndpoint, got %v", err)
	}
	if err == nil || !strings.Contains(err.Error(), "missing") {
		t.Fatalf("expected error to include unknown endpoint id, got %v", err)
	}
	if eligibilityCalls != 0 {
		t.Fatalf(
			"expected no eligibility calls before unknown exclusion error, got %d",
			eligibilityCalls,
		)
	}

	assertCalls(t, base)
}

func TestRoundTrip_AllEndpointsExcludedReturnsErrNoUsableEndpoint(t *testing.T) {
	u1 := "https://u1.test/rpc"
	u2 := "https://u2.test/rpc"

	base := &scriptRT{results: map[string][]rtResult{}}
	rt := mustNewTransport(t, Config{
		Endpoints: testEndpoints(u1, u2),
		Base:      base,
	})

	req := newTestGETRequest(t, u1)
	req = req.WithContext(
		WithExcludedEndpoints(
			req.Context(),
			"endpoint-1",
			"endpoint-2",
		),
	)

	resp, err := rt.RoundTrip(req)

	if resp != nil {
		t.Fatalf("expected nil response, got %#v", resp)
	}
	if !errors.Is(err, ErrNoUsableEndpoint) {
		t.Fatalf("expected ErrNoUsableEndpoint, got %v", err)
	}

	var failoverErr *FailoverError
	if errors.As(err, &failoverErr) {
		t.Fatalf(
			"expected direct ErrNoUsableEndpoint, got FailoverError: %v",
			err,
		)
	}

	assertCalls(t, base)
}

func TestRoundTrip_ExclusionAndEligibilityFilterIndependently(t *testing.T) {
	u1 := "https://u1.test/rpc"
	u2 := "https://u2.test/rpc"
	u3 := "https://u3.test/rpc"
	u4 := "https://u4.test/rpc"

	eligibilityCalls := map[EndpointID]int{}

	base := &scriptRT{
		results: map[string][]rtResult{
			u2: {{err: io.EOF}},
			u4: {{resp: newHTTPResp(http.StatusOK, "ok")}},
		},
	}
	rt := mustNewTransport(t, Config{
		Endpoints: testEndpoints(u1, u2, u3, u4),
		Base:      base,
		Eligible: func(id EndpointID) bool {
			eligibilityCalls[id]++
			return id == "endpoint-2" || id == "endpoint-4"
		},
	})

	req := newTestGETRequest(t, u1)
	req = req.WithContext(
		WithExcludedEndpoints(req.Context(), "endpoint-1"),
	)

	mustRoundTripCode(t, rt, req, http.StatusOK)
	assertCalls(t, base, u2, u4)

	if got := eligibilityCalls["endpoint-1"]; got != 0 {
		t.Fatalf(
			"expected excluded endpoint eligibility calls=0, got %d",
			got,
		)
	}

	for _, id := range []EndpointID{
		"endpoint-2",
		"endpoint-3",
		"endpoint-4",
	} {
		if got := eligibilityCalls[id]; got != 1 {
			t.Fatalf(
				"expected eligibility for %s to be captured once, got %d calls",
				id,
				got,
			)
		}
	}
}

func TestRoundTrip_ExcludedPreferredEndpointRemainsExcluded(t *testing.T) {
	u1 := "https://u1.test/rpc"
	u2 := "https://u2.test/rpc"
	u3 := "https://u3.test/rpc"

	base := &scriptRT{
		results: map[string][]rtResult{
			u1: {{err: io.EOF}},
			u3: {{resp: newHTTPResp(http.StatusOK, "ok")}},
		},
	}
	rt := mustNewTransport(t, Config{
		Endpoints: testEndpoints(u1, u2, u3),
		Base:      base,
	})

	ctx := WithPreferredEndpoint(
		context.Background(),
		"endpoint-2",
	)
	ctx = WithExcludedEndpoints(
		ctx,
		"endpoint-2",
	)

	req := newTestGETRequest(t, u1).WithContext(ctx)

	mustRoundTripCode(t, rt, req, http.StatusOK)
	assertCalls(t, base, u1, u3)
}

func TestRoundTrip_ExclusionDoesNotChangeFailoverPermission(t *testing.T) {
	tests := []struct {
		name       string
		context    func(context.Context) context.Context
		wantSecond bool
	}{
		{
			name: "inherited allow is preserved",
			context: func(ctx context.Context) context.Context {
				return WithFailoverAllowed(ctx)
			},
			wantSecond: true,
		},
		{
			name: "exclusion does not grant permission",
			context: func(ctx context.Context) context.Context {
				return ctx
			},
			wantSecond: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			u1 := "https://u1.test/rpc"
			u2 := "https://u2.test/rpc"
			u3 := "https://u3.test/rpc"

			base := &scriptRT{
				results: map[string][]rtResult{
					u2: {{err: io.EOF}},
					u3: {{resp: newHTTPResp(http.StatusOK, "ok")}},
				},
			}
			rt := mustNewTransport(t, Config{
				Endpoints: testEndpoints(u1, u2, u3),
				Base:      base,
			})

			req, err := http.NewRequest(
				http.MethodPost,
				u1,
				nil,
			)
			if err != nil {
				t.Fatalf("http.NewRequest: %v", err)
			}

			ctx := tt.context(req.Context())
			ctx = WithExcludedEndpoints(ctx, "endpoint-1")
			req = req.WithContext(ctx)

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
				assertCalls(t, base, u2, u3)
				return
			}

			if resp != nil {
				t.Fatalf("expected nil response, got %#v", resp)
			}

			fe := mustAsFailoverError(t, err)
			if len(fe.Attempts) != 1 {
				t.Fatalf(
					"expected 1 attempt, got %d",
					len(fe.Attempts),
				)
			}
			if fe.Attempts[0].Endpoint != "endpoint-2" {
				t.Fatalf(
					"unexpected attempt endpoint: %s",
					fe.Attempts[0].Endpoint,
				)
			}

			assertCalls(t, base, u2)
		})
	}
}

func TestRoundTrip_ExclusionDoesNotGrantReplayability(t *testing.T) {
	u1 := "https://u1.test/rpc"
	u2 := "https://u2.test/rpc"
	u3 := "https://u3.test/rpc"

	body := newTrackingBody("one-shot")

	base := &scriptRT{
		results: map[string][]rtResult{
			u2: {{err: io.EOF}},
			u3: {{resp: newHTTPResp(http.StatusOK, "unexpected")}},
		},
	}
	rt := mustNewTransport(t, Config{
		Endpoints: testEndpoints(u1, u2, u3),
		Base:      base,
	})

	req, err := http.NewRequest(
		http.MethodPost,
		u1,
		body,
	)
	if err != nil {
		t.Fatalf("http.NewRequest: %v", err)
	}
	if req.GetBody != nil {
		t.Fatal("expected one-shot request GetBody to be nil")
	}

	ctx := WithFailoverAllowed(req.Context())
	ctx = WithExcludedEndpoints(ctx, "endpoint-1")
	req = req.WithContext(ctx)

	resp, err := rt.RoundTrip(req)

	if resp != nil {
		t.Fatalf("expected nil response, got %#v", resp)
	}

	fe := mustAsFailoverError(t, err)
	if len(fe.Attempts) != 1 {
		t.Fatalf(
			"expected 1 attempt, got %d",
			len(fe.Attempts),
		)
	}
	if fe.Attempts[0].Endpoint != "endpoint-2" {
		t.Fatalf(
			"unexpected attempt endpoint: %s",
			fe.Attempts[0].Endpoint,
		)
	}

	assertCalls(t, base, u2)
}

func TestRoundTrip_ExclusionDoesNotChangeCooldownAdmission(t *testing.T) {
	u1 := "https://u1.test/rpc"
	u2 := "https://u2.test/rpc"
	u3 := "https://u3.test/rpc"

	fixedNow := time.Unix(1500, 0)

	base := &scriptRT{
		results: map[string][]rtResult{
			u3: {{resp: newHTTPResp(http.StatusOK, "ok")}},
		},
	}

	var events []Event
	rt := mustNewTransport(t, Config{
		Endpoints: testEndpoints(u1, u2, u3),
		Base:      base,
		OnEvent: func(_ context.Context, event Event) {
			events = append(events, event)
		},
	})
	rt.now = func() time.Time {
		return fixedNow
	}

	rt.cooldown.mu.Lock()
	rt.cooldown.coolingTo[0] = fixedNow.Add(time.Hour)
	rt.cooldown.coolingTo[1] = fixedNow.Add(time.Hour)
	rt.cooldown.mu.Unlock()

	req := newTestGETRequest(t, u1)
	req = req.WithContext(
		WithExcludedEndpoints(req.Context(), "endpoint-1"),
	)

	mustRoundTripCode(t, rt, req, http.StatusOK)
	assertCalls(t, base, u3)

	if len(events) != 3 {
		t.Fatalf("unexpected events: %#v", events)
	}

	assertEvent(t, events[0], Event{
		Kind:     EventCooldownSkip,
		Endpoint: "endpoint-2",
	})
	assertEvent(t, events[1], Event{
		Kind:       EventAttempt,
		Endpoint:   "endpoint-3",
		Attempt:    1,
		StatusCode: http.StatusOK,
	})
	assertEvent(t, events[2], Event{
		Kind:       EventResult,
		Endpoint:   "endpoint-3",
		StatusCode: http.StatusOK,
	})
	assertSingleFinalResult(t, events)
}
