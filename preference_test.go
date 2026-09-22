package rcpx

import (
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
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
