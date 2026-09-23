package failnext

import (
	"context"
	"errors"
	"io"
	"net/http"
	"testing"
)

func TestRoundTrip_FailoverErrorRecordsNoResponseAttemptsInOrder(t *testing.T) {
	u1 := "https://u1.test/rpc"
	u2 := "https://u2.test/rpc"
	u3 := "https://u3.test/rpc"

	err1 := errors.New("dial failed")
	err2 := errors.New("tls failed")
	err3 := &typedTransportError{message: "connection reset"}
	base := &scriptRT{
		results: map[string][]rtResult{
			u1: {{err: err1}},
			u2: {{err: err2}},
			u3: {{err: err3}},
		},
	}
	rt := mustNewTransport(t, Config{
		Endpoints: testEndpoints(u1, u2, u3),
		Base:      base,
	})

	req, err := http.NewRequest(http.MethodGet, u1, nil)
	if err != nil {
		t.Fatalf("http.NewRequest: %v", err)
	}
	resp, err := rt.RoundTrip(req)
	if resp != nil {
		t.Fatalf("expected nil response, got %#v", resp)
	}

	var failoverErr *FailoverError
	if !errors.As(err, &failoverErr) {
		t.Fatalf("expected FailoverError, got %v", err)
	}
	if len(failoverErr.Attempts) != 3 {
		t.Fatalf("expected 3 no-response attempts, got %d: %#v", len(failoverErr.Attempts), failoverErr.Attempts)
	}

	want := []struct {
		endpoint EndpointID
		err      error
	}{
		{endpoint: "endpoint-1", err: err1},
		{endpoint: "endpoint-2", err: err2},
		{endpoint: "endpoint-3", err: err3},
	}
	for i, wantAttempt := range want {
		got := failoverErr.Attempts[i]
		if got.Endpoint != wantAttempt.endpoint {
			t.Fatalf("attempt %d endpoint: got=%q want=%q", i+1, got.Endpoint, wantAttempt.endpoint)
		}
		if got.Err != wantAttempt.err {
			t.Fatalf("attempt %d error: got=%v want=%v", i+1, got.Err, wantAttempt.err)
		}
	}
	if !errors.Is(err, err3) {
		t.Fatalf("expected FailoverError to unwrap to final transport error, got %v", err)
	}
	var typedErr *typedTransportError
	if !errors.As(err, &typedErr) {
		t.Fatalf("expected errors.As to reach terminal transport error, got %v", err)
	}
	if typedErr != err3 {
		t.Fatalf("unexpected terminal typed error: got=%v want=%v", typedErr, err3)
	}
	assertCalls(t, base, u1, u2, u3)
}

func TestRoundTrip_TransportErrorThenGetBodyErrorUsesReplayErrorAsCause(t *testing.T) {
	u1 := "https://u1.test/rpc"
	u2 := "https://u2.test/rpc"

	transportErr := errors.New("transport failed")
	replayErr := errors.New("replay failed")
	original := newTrackingBody("original")
	calls := make([]string, 0, 1)
	base := roundTripperFunc(func(req *http.Request) (*http.Response, error) {
		calls = append(calls, req.URL.String())
		if req.URL.String() != u1 {
			t.Fatalf("unexpected physical attempt: %s", req.URL)
		}
		if err := req.Body.Close(); err != nil {
			t.Fatalf("close original body: %v", err)
		}
		return nil, transportErr
	})
	rt := mustNewTransport(t, Config{
		Endpoints: testEndpoints(u1, u2),
		Base:      base,
	})

	req, err := http.NewRequest(http.MethodPost, u1, original)
	if err != nil {
		t.Fatalf("http.NewRequest: %v", err)
	}
	getBodyCalls := 0
	req.GetBody = func() (io.ReadCloser, error) {
		getBodyCalls++
		return nil, replayErr
	}
	req = req.WithContext(WithFailoverAllowed(req.Context()))

	resp, err := rt.RoundTrip(req)
	if resp != nil {
		t.Fatalf("expected nil response, got %#v", resp)
	}

	var failoverErr *FailoverError
	if !errors.As(err, &failoverErr) {
		t.Fatalf("expected FailoverError, got %v", err)
	}
	if len(failoverErr.Attempts) != 1 {
		t.Fatalf("expected 1 physical no-response attempt, got %d: %#v", len(failoverErr.Attempts), failoverErr.Attempts)
	}
	if failoverErr.Attempts[0].Endpoint != "endpoint-1" {
		t.Fatalf("expected first endpoint ID, got %q", failoverErr.Attempts[0].Endpoint)
	}
	if failoverErr.Attempts[0].Err != transportErr {
		t.Fatalf("expected transport error in attempt history, got %v", failoverErr.Attempts[0].Err)
	}
	if !errors.Is(err, replayErr) {
		t.Fatalf("expected FailoverError to unwrap to replay error, got %v", err)
	}
	if errors.Is(err, transportErr) {
		t.Fatalf("expected terminal cause to be replay error, not transport error: %v", err)
	}
	if getBodyCalls != 1 {
		t.Fatalf("expected GetBody calls=1, got %d", getBodyCalls)
	}
	if len(calls) != 1 || calls[0] != u1 {
		t.Fatalf("unexpected physical attempts: %v", calls)
	}
}

func TestRoundTrip_RetainedResponseWinsOverGetBodyError(t *testing.T) {
	u1 := "https://u1.test/rpc"
	u2 := "https://u2.test/rpc"

	replayErr := errors.New("replay failed")
	original := newTrackingBody("original")
	retainedBody := newTrackingBody("unavailable")
	retainedResp := &http.Response{
		StatusCode: http.StatusServiceUnavailable,
		Body:       retainedBody,
	}
	calls := make([]string, 0, 1)
	base := roundTripperFunc(func(req *http.Request) (*http.Response, error) {
		calls = append(calls, req.URL.String())
		if req.URL.String() != u1 {
			t.Fatalf("unexpected physical attempt: %s", req.URL)
		}
		if err := req.Body.Close(); err != nil {
			t.Fatalf("close original body: %v", err)
		}
		return retainedResp, nil
	})
	rt := mustNewTransport(t, Config{
		Endpoints: testEndpoints(u1, u2),
		Base:      base,
	})

	req, err := http.NewRequest(http.MethodPost, u1, original)
	if err != nil {
		t.Fatalf("http.NewRequest: %v", err)
	}
	getBodyCalls := 0
	req.GetBody = func() (io.ReadCloser, error) {
		getBodyCalls++
		return nil, replayErr
	}
	req = req.WithContext(WithFailoverAllowed(req.Context()))

	resp, err := rt.RoundTrip(req)
	if err != nil {
		t.Fatalf("RoundTrip error: %v", err)
	}
	if resp != retainedResp {
		t.Fatalf("expected retained 503 response, got %#v", resp)
	}
	t.Cleanup(func() { resp.Body.Close() })

	if retainedBody.Closed() {
		t.Fatal("expected returned retained response body to remain open for caller")
	}
	if getBodyCalls != 1 {
		t.Fatalf("expected GetBody calls=1, got %d", getBodyCalls)
	}
	if len(calls) != 1 || calls[0] != u1 {
		t.Fatalf("unexpected physical attempts: %v", calls)
	}
}

func TestRoundTrip_OneShotBodyTransportErrorReturnsFailoverError(t *testing.T) {
	u1 := "https://u1.test/rpc"
	u2 := "https://u2.test/rpc"

	transportErr := errors.New("transport failed")
	body := newTrackingBody("one-shot")
	calls := make([]string, 0, 1)
	base := roundTripperFunc(func(req *http.Request) (*http.Response, error) {
		calls = append(calls, req.URL.String())
		if req.URL.String() != u1 {
			t.Fatalf("unexpected physical attempt: %s", req.URL)
		}
		if err := req.Body.Close(); err != nil {
			t.Fatalf("close one-shot body: %v", err)
		}
		return nil, transportErr
	})
	rt := mustNewTransport(t, Config{
		Endpoints: testEndpoints(u1, u2),
		Base:      base,
	})

	req, err := http.NewRequest(http.MethodPost, u1, body)
	if err != nil {
		t.Fatalf("http.NewRequest: %v", err)
	}
	req.GetBody = nil
	req = req.WithContext(WithFailoverAllowed(req.Context()))

	resp, err := rt.RoundTrip(req)
	if resp != nil {
		t.Fatalf("expected nil response, got %#v", resp)
	}
	var failoverErr *FailoverError
	if !errors.As(err, &failoverErr) {
		t.Fatalf("expected FailoverError, got %v", err)
	}
	if len(failoverErr.Attempts) != 1 {
		t.Fatalf("expected 1 physical attempt, got %d: %#v", len(failoverErr.Attempts), failoverErr.Attempts)
	}
	if failoverErr.Attempts[0].Endpoint != "endpoint-1" || failoverErr.Attempts[0].Err != transportErr {
		t.Fatalf("unexpected attempt history: %#v", failoverErr.Attempts)
	}
	if !errors.Is(err, transportErr) {
		t.Fatalf("expected FailoverError to unwrap to transport error, got %v", err)
	}
	if len(calls) != 1 || calls[0] != u1 {
		t.Fatalf("unexpected physical attempts: %v", calls)
	}
}

func TestRoundTrip_OneShotBodyRetainsTriggerResponse(t *testing.T) {
	u1 := "https://u1.test/rpc"
	u2 := "https://u2.test/rpc"

	body := newTrackingBody("one-shot")
	retainedBody := newTrackingBody("unavailable")
	retainedResp := &http.Response{
		StatusCode: http.StatusServiceUnavailable,
		Body:       retainedBody,
	}
	calls := make([]string, 0, 1)
	base := roundTripperFunc(func(req *http.Request) (*http.Response, error) {
		calls = append(calls, req.URL.String())
		if req.URL.String() != u1 {
			t.Fatalf("unexpected physical attempt: %s", req.URL)
		}
		if err := req.Body.Close(); err != nil {
			t.Fatalf("close one-shot body: %v", err)
		}
		return retainedResp, nil
	})
	rt := mustNewTransport(t, Config{
		Endpoints: testEndpoints(u1, u2),
		Base:      base,
	})

	req, err := http.NewRequest(http.MethodPost, u1, body)
	if err != nil {
		t.Fatalf("http.NewRequest: %v", err)
	}
	req.GetBody = nil
	req = req.WithContext(WithFailoverAllowed(req.Context()))

	resp, err := rt.RoundTrip(req)
	if err != nil {
		t.Fatalf("RoundTrip error: %v", err)
	}
	if resp != retainedResp {
		t.Fatalf("expected retained 503 response, got %#v", resp)
	}
	t.Cleanup(func() { resp.Body.Close() })

	if retainedBody.Closed() {
		t.Fatal("expected returned 503 response body to remain open for caller")
	}
	if len(calls) != 1 || calls[0] != u1 {
		t.Fatalf("unexpected physical attempts: %v", calls)
	}
}

func TestRoundTrip_GetBodyReturnedBodyIsClosedWhenGetBodyFails(t *testing.T) {
	u1 := "https://u1.test/rpc"
	u2 := "https://u2.test/rpc"

	transportErr := errors.New("transport failed")
	replayErr := errors.New("replay failed")
	original := newTrackingBody("original")
	fresh := newTrackingBody("fresh")
	calls := make([]string, 0, 1)
	base := roundTripperFunc(func(req *http.Request) (*http.Response, error) {
		calls = append(calls, req.URL.String())
		if err := req.Body.Close(); err != nil {
			t.Fatalf("close original body: %v", err)
		}
		return nil, transportErr
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
		t.Fatalf("expected replay error through FailoverError, got %v", err)
	}
	if !fresh.Closed() {
		t.Fatal("expected failnext to close body returned with GetBody error")
	}
	if len(calls) != 1 || calls[0] != u1 {
		t.Fatalf("unexpected physical attempts: %v", calls)
	}
}

func TestRoundTrip_CancellationAfterReplayConstructionClosesReplayAndRetainedResponse(t *testing.T) {
	u1 := "https://u1.test/rpc"
	u2 := "https://u2.test/rpc"

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	original := newTrackingBody("original")
	retainedBody := newTrackingBody("unavailable")
	fresh := newTrackingBody("fresh")
	calls := make([]string, 0, 1)
	base := roundTripperFunc(func(req *http.Request) (*http.Response, error) {
		calls = append(calls, req.URL.String())
		if req.URL.String() != u1 {
			t.Fatalf("unexpected physical attempt: %s", req.URL)
		}
		if err := req.Body.Close(); err != nil {
			t.Fatalf("close original body: %v", err)
		}
		return &http.Response{
			StatusCode: http.StatusServiceUnavailable,
			Body:       retainedBody,
		}, nil
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
	if !fresh.Closed() {
		t.Fatal("expected failnext to close fresh replay body before base handoff")
	}
	if !retainedBody.Closed() {
		t.Fatal("expected failnext to close retained response when cancellation wins")
	}
	if len(calls) != 1 || calls[0] != u1 {
		t.Fatalf("unexpected physical attempts: %v", calls)
	}
}

type typedTransportError struct {
	message string
}

func (e *typedTransportError) Error() string {
	return e.message
}
