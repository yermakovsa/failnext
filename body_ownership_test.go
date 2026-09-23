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

func TestRoundTrip_ClosesOriginalBodyWhenContextDoneBeforeHandoff(t *testing.T) {
	u1 := "https://u1.test/rpc"
	body := newTrackingBody("payload")
	base := &scriptRT{results: map[string][]rtResult{}}

	rt := mustNewTransport(t, Config{
		Endpoints: testEndpoints(u1),
		Base:      base,
	})

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u1, body)
	if err != nil {
		t.Fatalf("http.NewRequestWithContext: %v", err)
	}

	resp, err := rt.RoundTrip(req)
	if resp != nil {
		t.Fatalf("expected nil response, got %#v", resp)
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
	if !body.Closed() {
		t.Fatal("expected failnext to close original body before any base handoff")
	}
	assertCalls(t, base)
}

func TestRoundTrip_ClosesOriginalBodyWhenAllCandidatesCooling(t *testing.T) {
	u1 := "https://u1.test/rpc"
	body := newTrackingBody("payload")
	base := &scriptRT{results: map[string][]rtResult{}}

	rt := mustNewTransport(t, Config{
		Endpoints: testEndpoints(u1),
		Base:      base,
		Cooldown: CooldownConfig{
			Threshold: 1,
			Duration:  time.Hour,
		},
	})
	fixedNow := time.Unix(300, 0)
	rt.now = func() time.Time { return fixedNow }

	rt.cooldown.mu.Lock()
	rt.cooldown.coolingTo[0] = fixedNow.Add(time.Hour)
	rt.cooldown.mu.Unlock()

	req, err := http.NewRequest(http.MethodPost, u1, body)
	if err != nil {
		t.Fatalf("http.NewRequest: %v", err)
	}

	resp, err := rt.RoundTrip(req)
	if resp != nil {
		t.Fatalf("expected nil response, got %#v", resp)
	}
	if !errors.Is(err, ErrNoUsableEndpoint) {
		t.Fatalf("expected ErrNoUsableEndpoint, got %v", err)
	}
	if !body.Closed() {
		t.Fatal("expected failnext to close original body when all candidates are cooling")
	}
	assertCalls(t, base)
}

func TestRoundTrip_ClosesOriginalBodyWhenAllEndpointsExternallyIneligible(t *testing.T) {
	u1 := "https://u1.test/rpc"
	u2 := "https://u2.test/rpc"
	body := newTrackingBody("payload")
	base := &scriptRT{results: map[string][]rtResult{}}

	rt := mustNewTransport(t, Config{
		Endpoints: testEndpoints(u1, u2),
		Base:      base,
		Eligible: func(EndpointID) bool {
			return false
		},
	})

	req, err := http.NewRequest(http.MethodPost, u1, body)
	if err != nil {
		t.Fatalf("http.NewRequest: %v", err)
	}

	resp, err := rt.RoundTrip(req)
	if resp != nil {
		t.Fatalf("expected nil response, got %#v", resp)
	}
	if !errors.Is(err, ErrNoUsableEndpoint) {
		t.Fatalf("expected ErrNoUsableEndpoint, got %v", err)
	}
	if !body.Closed() {
		t.Fatal("expected failnext to close original body when all endpoints are externally ineligible")
	}
	assertCalls(t, base)
}

func TestRoundTrip_ClosesOriginalBodyWhenPreferredEndpointIsUnknown(t *testing.T) {
	u1 := "https://u1.test/rpc"
	body := newTrackingBody("payload")
	base := &scriptRT{results: map[string][]rtResult{}}

	rt := mustNewTransport(t, Config{
		Endpoints: testEndpoints(u1),
		Base:      base,
	})

	req, err := http.NewRequest(http.MethodPost, u1, body)
	if err != nil {
		t.Fatalf("http.NewRequest: %v", err)
	}
	req = req.WithContext(WithPreferredEndpoint(req.Context(), "missing"))

	resp, err := rt.RoundTrip(req)
	if resp != nil {
		t.Fatalf("expected nil response, got %#v", resp)
	}
	if !errors.Is(err, ErrUnknownEndpoint) {
		t.Fatalf("expected ErrUnknownEndpoint, got %v", err)
	}
	if !body.Closed() {
		t.Fatal("expected failnext to close original body when preferred endpoint is unknown")
	}
	assertCalls(t, base)
}

func TestRoundTrip_ClosesBodyReturnedWithGetBodyError(t *testing.T) {
	u1 := "https://u1.test/rpc"
	u2 := "https://u2.test/rpc"
	replayErr := errors.New("replay failed")
	original := newTrackingBody("original")
	fresh := newTrackingBody("fresh")
	var calls []string

	base := roundTripperFunc(func(req *http.Request) (*http.Response, error) {
		calls = append(calls, req.URL.String())
		if req.URL.String() != u1 {
			t.Fatalf("unexpected physical attempt: %s", req.URL)
		}
		if err := req.Body.Close(); err != nil {
			t.Fatalf("close original body: %v", err)
		}
		return nil, io.EOF
	})
	rt := mustNewTransport(t, Config{
		Endpoints: testEndpoints(u1, u2),
		Base:      base,
	})

	req, err := http.NewRequest(http.MethodPost, u1, original)
	if err != nil {
		t.Fatalf("http.NewRequest: %v", err)
	}
	req.GetBody = func() (io.ReadCloser, error) {
		return fresh, replayErr
	}
	req = req.WithContext(WithFailoverAllowed(req.Context()))

	resp, err := rt.RoundTrip(req)
	if resp != nil {
		t.Fatalf("expected nil response, got %#v", resp)
	}
	if !errors.Is(err, replayErr) {
		t.Fatalf("expected replay error, got %v", err)
	}
	if !fresh.Closed() {
		t.Fatal("expected failnext to close fresh body returned with GetBody error")
	}
	if len(calls) != 1 || calls[0] != u1 {
		t.Fatalf("unexpected physical attempts: %v", calls)
	}
}

func TestRoundTrip_ClosesFreshReplayBodyWhenCanceledBeforeHandoff(t *testing.T) {
	u1 := "https://u1.test/rpc"
	u2 := "https://u2.test/rpc"
	original := newTrackingBody("original")
	var fresh *trackingBody
	var calls []string

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	base := roundTripperFunc(func(req *http.Request) (*http.Response, error) {
		calls = append(calls, req.URL.String())
		if req.URL.String() != u1 {
			t.Fatalf("unexpected physical attempt: %s", req.URL)
		}
		if err := req.Body.Close(); err != nil {
			t.Fatalf("close original body: %v", err)
		}
		return nil, io.EOF
	})
	rt := mustNewTransport(t, Config{
		Endpoints: testEndpoints(u1, u2),
		Base:      base,
	})

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u1, original)
	if err != nil {
		t.Fatalf("http.NewRequestWithContext: %v", err)
	}
	req.GetBody = func() (io.ReadCloser, error) {
		fresh = newTrackingBody("fresh")
		cancel()
		return fresh, nil
	}
	req = req.WithContext(WithFailoverAllowed(req.Context()))

	resp, err := rt.RoundTrip(req)
	if resp != nil {
		t.Fatalf("expected nil response, got %#v", resp)
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
	if fresh == nil {
		t.Fatal("expected GetBody to create a fresh body")
	}
	if !fresh.Closed() {
		t.Fatal("expected failnext to close fresh replay body before base handoff")
	}
	if len(calls) != 1 || calls[0] != u1 {
		t.Fatalf("unexpected physical attempts: %v", calls)
	}
}

func TestRoundTrip_TransfersRequestBodyOwnershipToBaseAtHandoff(t *testing.T) {
	u1 := "https://u1.test/rpc"
	u2 := "https://u2.test/rpc"
	original := newTrackingBody("original")
	replay := newTrackingBody("replay")

	base := roundTripperFunc(func(req *http.Request) (*http.Response, error) {
		switch req.URL.String() {
		case u1:
			if original.Closed() {
				t.Fatal("original body was closed before base handoff")
			}
			if req.Body != original {
				t.Fatalf("expected original body on first attempt, got %T", req.Body)
			}
			if err := req.Body.Close(); err != nil {
				t.Fatalf("base close original body: %v", err)
			}
			return nil, io.EOF
		case u2:
			if replay.Closed() {
				t.Fatal("replay body was closed before base handoff")
			}
			if req.Body != replay {
				t.Fatalf("expected replay body on second attempt, got %T", req.Body)
			}
			if err := req.Body.Close(); err != nil {
				t.Fatalf("base close replay body: %v", err)
			}
			return &http.Response{
				StatusCode: http.StatusOK,
				Body:       io.NopCloser(strings.NewReader("ok")),
				Header:     make(http.Header),
				Request:    req,
			}, nil
		default:
			return nil, errors.New("unexpected URL: " + req.URL.String())
		}
	})
	rt := mustNewTransport(t, Config{
		Endpoints: testEndpoints(u1, u2),
		Base:      base,
	})

	req, err := http.NewRequest(http.MethodPost, u1, original)
	if err != nil {
		t.Fatalf("http.NewRequest: %v", err)
	}
	req.GetBody = func() (io.ReadCloser, error) {
		return replay, nil
	}
	req = req.WithContext(WithFailoverAllowed(req.Context()))

	resp, err := rt.RoundTrip(req)
	if err != nil {
		t.Fatalf("RoundTrip error: %v", err)
	}
	resp.Body.Close()

	if !original.Closed() {
		t.Fatal("expected base transport to close original body after handoff")
	}
	if !replay.Closed() {
		t.Fatal("expected base transport to close replay body after handoff")
	}
}
