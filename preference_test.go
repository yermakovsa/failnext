package rcpx

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
)

func TestRoundTrip_PreferredEndpointOrdering(t *testing.T) {
	u1 := "https://u1.test/rpc"
	u2 := "https://u2.test/rpc"
	u3 := "https://u3.test/rpc"

	tests := []struct {
		name      string
		preferred EndpointID
		eligible  func(EndpointID) bool
		wantCalls []string
	}{
		{
			name:      "promotes eligible endpoint",
			preferred: "endpoint-3",
			wantCalls: []string{u3, u1, u2},
		},
		{
			name:      "already first",
			preferred: "endpoint-1",
			wantCalls: []string{u1, u2, u3},
		},
		{
			name:      "externally ineligible preferred endpoint remains excluded",
			preferred: "endpoint-3",
			eligible: func(id EndpointID) bool {
				return id != "endpoint-3"
			},
			wantCalls: []string{u1, u2},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			results := make(map[string][]rtResult, len(tt.wantCalls))
			for i, url := range tt.wantCalls {
				if i == len(tt.wantCalls)-1 {
					results[url] = []rtResult{{resp: httpResp(http.StatusOK, "ok")}}
					continue
				}
				results[url] = []rtResult{{err: io.EOF}}
			}

			base := &scriptRT{results: results}
			rt := mustNewTransport(t, Config{
				Endpoints: testEndpoints(u1, u2, u3),
				Base:      base,
				Eligible:  tt.eligible,
			})

			req := newTestGETRequest(t, u1)
			req = req.WithContext(WithPreferredEndpoint(req.Context(), tt.preferred))
			mustRoundTripCode(t, rt, req, http.StatusOK)

			assertCalls(t, base, tt.wantCalls...)
		})
	}
}

func TestRoundTrip_UnknownPreferredEndpointFailsBeforeAttempt(t *testing.T) {
	u1 := "https://u1.test/rpc"
	base := &scriptRT{results: map[string][]rtResult{}}
	rt := mustNewTransport(t, Config{
		Endpoints: testEndpoints(u1),
		Base:      base,
	})

	req := newTestGETRequest(t, u1)
	req = req.WithContext(WithPreferredEndpoint(req.Context(), "missing"))
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
	assertCalls(t, base)
}

func TestRoundTrip_PreferredEndpointDoesNotGrantFailoverPermission(t *testing.T) {
	u1 := "https://u1.test/rpc"
	u2 := "https://u2.test/rpc"
	u3 := "https://u3.test/rpc"

	base := &scriptRT{
		results: map[string][]rtResult{
			u3: {{err: io.EOF}},
			u1: {{resp: httpResp(http.StatusOK, "unexpected")}},
		},
	}
	rt := mustNewTransport(t, Config{
		Endpoints: testEndpoints(u1, u2, u3),
		Base:      base,
	})

	req, err := http.NewRequest(http.MethodPost, u1, nil)
	if err != nil {
		t.Fatalf("http.NewRequest: %v", err)
	}
	req = req.WithContext(WithPreferredEndpoint(req.Context(), "endpoint-3"))

	resp, err := rt.RoundTrip(req)
	if resp != nil {
		t.Fatalf("expected nil response, got %#v", resp)
	}
	if err == nil {
		t.Fatal("expected terminal error when permission denies continuation")
	}
	fe := mustAsFailoverError(t, err)
	if len(fe.Attempts) != 1 {
		t.Fatalf("expected 1 no-response attempt, got %d", len(fe.Attempts))
	}
	assertCalls(t, base, u3)
}

func TestRoundTrip_PreferenceAndEligibilityDoNotGrantReplayability(t *testing.T) {
	u1 := "https://u1.test/rpc"
	u2 := "https://u2.test/rpc"
	u3 := "https://u3.test/rpc"
	body := newTrackingBody("one-shot")

	base := &scriptRT{
		results: map[string][]rtResult{
			u3: {{err: io.EOF}},
			u2: {{resp: httpResp(http.StatusOK, "unexpected")}},
		},
	}
	rt := mustNewTransport(t, Config{
		Endpoints: testEndpoints(u1, u2, u3),
		Base:      base,
		Eligible: func(id EndpointID) bool {
			return id == "endpoint-2" || id == "endpoint-3"
		},
	})

	req, err := http.NewRequest(http.MethodPost, u1, body)
	if err != nil {
		t.Fatalf("http.NewRequest: %v", err)
	}
	if req.GetBody != nil {
		t.Fatal("expected one-shot request GetBody to be nil")
	}
	ctx := WithPreferredEndpoint(req.Context(), "endpoint-3")
	ctx = WithFailoverAllowed(ctx)
	resp, err := rt.RoundTrip(req.WithContext(ctx))

	if resp != nil {
		t.Fatalf("expected nil response, got %#v", resp)
	}
	fe := mustAsFailoverError(t, err)
	if len(fe.Attempts) != 1 {
		t.Fatalf("expected 1 no-response attempt, got %d", len(fe.Attempts))
	}
	assertCalls(t, base, u3)
}

func TestRoundTrip_PreferenceUsesCapturedEligibilityAfterStateChanges(t *testing.T) {
	u1 := "https://u1.test/rpc"
	u2 := "https://u2.test/rpc"
	u3 := "https://u3.test/rpc"

	var calls []string
	disableEndpoint3 := false
	eligibilityCalls := map[EndpointID]int{}

	base := roundTripperFunc(func(req *http.Request) (*http.Response, error) {
		calls = append(calls, req.URL.String())
		switch req.URL.String() {
		case u2:
			// External application state changes only after rcpx has captured
			// eligibility for this logical request.
			disableEndpoint3 = true
			return nil, io.EOF
		case u1:
			return nil, io.EOF
		case u3:
			return httpResp(http.StatusOK, "ok"), nil
		default:
			return nil, errors.New("unexpected destination")
		}
	})

	rt := mustNewTransport(t, Config{
		Endpoints: testEndpoints(u1, u2, u3),
		Base:      base,
		Eligible: func(id EndpointID) bool {
			eligibilityCalls[id]++
			if id == "endpoint-3" {
				return !disableEndpoint3
			}
			return true
		},
	})

	req := newTestGETRequest(t, u1)
	req = req.WithContext(WithPreferredEndpoint(req.Context(), "endpoint-2"))
	mustRoundTripCode(t, rt, req, http.StatusOK)

	wantCalls := []string{u2, u1, u3}
	if len(calls) != len(wantCalls) {
		t.Fatalf("unexpected base calls: got=%v want=%v", calls, wantCalls)
	}
	for i := range calls {
		if calls[i] != wantCalls[i] {
			t.Fatalf("unexpected base calls: got=%v want=%v", calls, wantCalls)
		}
	}
	for _, id := range []EndpointID{"endpoint-1", "endpoint-2", "endpoint-3"} {
		if got := eligibilityCalls[id]; got != 1 {
			t.Fatalf("expected eligibility for %s to be captured once, got %d calls", id, got)
		}
	}
	if !disableEndpoint3 {
		t.Fatal("expected application eligibility state to change after the preferred attempt")
	}
}

func TestRoundTrip_PreferredEndpointDoesNotBypassLiveCooldown(t *testing.T) {
	u1 := "https://u1.test/rpc"
	u2 := "https://u2.test/rpc"
	u3 := "https://u3.test/rpc"

	base := &scriptRT{results: map[string][]rtResult{
		u3: {{err: io.EOF}},
		u1: {
			{resp: httpResp(http.StatusOK, "first")},
			{resp: httpResp(http.StatusOK, "second")},
		},
	}}
	var events []Event
	rt := mustNewTransport(t, Config{
		Endpoints: testEndpoints(u1, u2, u3),
		Base:      base,
		Cooldown: CooldownConfig{
			Threshold: 1,
			Duration:  time.Hour,
		},
		OnEvent: func(_ context.Context, event Event) {
			events = append(events, event)
		},
	})

	first := newTestGETRequest(t, u1)
	first = first.WithContext(WithPreferredEndpoint(first.Context(), "endpoint-3"))
	mustRoundTripCode(t, rt, first, http.StatusOK)

	// The first request recorded a qualifying failure for endpoint-3, so with a
	// threshold of one it is now cooling. Ignore the first request's events and
	// inspect only the next logical request.
	events = nil

	second := newTestGETRequest(t, u1)
	second = second.WithContext(WithPreferredEndpoint(second.Context(), "endpoint-3"))
	mustRoundTripCode(t, rt, second, http.StatusOK)

	assertCalls(t, base, u3, u1, u1)
	if len(events) != 3 {
		t.Fatalf("unexpected second-request events: %#v", events)
	}
	if got := events[0]; got.Kind != EventCooldownSkip || got.Endpoint != "endpoint-3" || got.Attempt != 0 {
		t.Fatalf("unexpected cooldown skip event: %#v", got)
	}
	if got := events[1]; got.Kind != EventAttempt || got.Endpoint != "endpoint-1" || got.Attempt != 1 || got.StatusCode != http.StatusOK || got.Err != nil {
		t.Fatalf("unexpected physical attempt event: %#v", got)
	}
	if got := events[2]; got.Kind != EventResult || got.Endpoint != "endpoint-1" || got.StatusCode != http.StatusOK || got.Err != nil {
		t.Fatalf("unexpected result event: %#v", got)
	}
}
