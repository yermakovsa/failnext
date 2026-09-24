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

func TestEvents_FirstAttemptHTTPSuccess(t *testing.T) {
	u1 := "https://u1.test/rpc"

	base := &scriptRT{
		results: map[string][]rtResult{
			u1: {{resp: httpResp(http.StatusOK, "ok")}},
		},
	}
	var events []Event
	rt := mustNewTransport(t, Config{
		Endpoints: testEndpoints(u1),
		Base:      base,
		OnEvent: func(_ context.Context, event Event) {
			events = append(events, event)
		},
	})

	req := newRPCRequest(t, u1, "eth_blockNumber")
	resp := mustRoundTrip(t, rt, req)
	assertStatus(t, resp, http.StatusOK)
	assertCalls(t, base, u1)

	if len(events) != 2 {
		t.Fatalf("unexpected event count: got=%d events=%#v", len(events), events)
	}
	assertEvent(t, events[0], Event{
		Kind:       EventAttempt,
		Endpoint:   "endpoint-1",
		Attempt:    1,
		StatusCode: http.StatusOK,
	})
	assertEvent(t, events[1], Event{
		Kind:       EventResult,
		Endpoint:   "endpoint-1",
		StatusCode: http.StatusOK,
	})
}

func TestEvents_TransportFailureThenSuccess(t *testing.T) {
	u1 := "https://u1.test/rpc"
	u2 := "https://u2.test/rpc"
	transportErr := errors.New("dial failed")

	base := &scriptRT{
		results: map[string][]rtResult{
			u1: {{err: transportErr}},
			u2: {{resp: httpResp(http.StatusOK, "ok")}},
		},
	}
	var events []Event
	rt := mustNewTransport(t, Config{
		Endpoints: testEndpoints(u1, u2),
		Base:      base,
		OnEvent: func(_ context.Context, event Event) {
			events = append(events, event)
		},
	})

	req := newRPCRequest(t, u1, "eth_blockNumber")
	req = req.WithContext(WithFailoverAllowed(req.Context()))
	resp := mustRoundTrip(t, rt, req)
	assertStatus(t, resp, http.StatusOK)
	assertCalls(t, base, u1, u2)

	if len(events) != 3 {
		t.Fatalf("unexpected event count: got=%d events=%#v", len(events), events)
	}
	assertEvent(t, events[0], Event{
		Kind:     EventAttempt,
		Endpoint: "endpoint-1",
		Attempt:  1,
		Err:      transportErr,
	})
	assertEvent(t, events[1], Event{
		Kind:       EventAttempt,
		Endpoint:   "endpoint-2",
		Attempt:    2,
		StatusCode: http.StatusOK,
	})
	assertEvent(t, events[2], Event{
		Kind:       EventResult,
		Endpoint:   "endpoint-2",
		StatusCode: http.StatusOK,
	})
}

func TestEvents_TriggerHTTPResponseThenSuccess(t *testing.T) {
	u1 := "https://u1.test/rpc"
	u2 := "https://u2.test/rpc"
	triggerBody := newTrackingBody("service unavailable")

	base := &scriptRT{
		results: map[string][]rtResult{
			u1: {{resp: &http.Response{StatusCode: http.StatusServiceUnavailable, Body: triggerBody}}},
			u2: {{resp: httpResp(http.StatusOK, "ok")}},
		},
	}
	var events []Event
	rt := mustNewTransport(t, Config{
		Endpoints: testEndpoints(u1, u2),
		Base:      base,
		OnEvent: func(_ context.Context, event Event) {
			events = append(events, event)
		},
	})

	req := newRPCRequest(t, u1, "eth_blockNumber")
	req = req.WithContext(WithFailoverAllowed(req.Context()))
	resp := mustRoundTrip(t, rt, req)
	assertStatus(t, resp, http.StatusOK)
	assertCalls(t, base, u1, u2)

	if !triggerBody.Closed() {
		t.Fatal("expected superseded trigger response body to be closed")
	}
	if len(events) != 3 {
		t.Fatalf("unexpected event count: got=%d events=%#v", len(events), events)
	}
	assertEvent(t, events[0], Event{
		Kind:       EventAttempt,
		Endpoint:   "endpoint-1",
		Attempt:    1,
		StatusCode: http.StatusServiceUnavailable,
	})
	if events[0].Err != nil {
		t.Fatalf("trigger HTTP response must not synthesize EventAttempt.Err: %v", events[0].Err)
	}
	assertEvent(t, events[1], Event{
		Kind:       EventAttempt,
		Endpoint:   "endpoint-2",
		Attempt:    2,
		StatusCode: http.StatusOK,
	})
	assertEvent(t, events[2], Event{
		Kind:       EventResult,
		Endpoint:   "endpoint-2",
		StatusCode: http.StatusOK,
	})
}

func TestEvents_CooldownSkipDoesNotConsumeAttemptNumber(t *testing.T) {
	u1 := "https://u1.test/rpc"
	u2 := "https://u2.test/rpc"
	u3 := "https://u3.test/rpc"
	fixedNow := time.Unix(1000, 0)

	base := &scriptRT{
		results: map[string][]rtResult{
			u1: {{err: io.EOF}},
			u3: {{resp: httpResp(http.StatusOK, "ok")}},
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
	rt.now = func() time.Time { return fixedNow }

	rt.cooldown.mu.Lock()
	rt.cooldown.coolingTo[1] = fixedNow.Add(time.Hour)
	rt.cooldown.mu.Unlock()

	req := newRPCRequest(t, u1, "eth_blockNumber")
	req = req.WithContext(WithFailoverAllowed(req.Context()))
	resp := mustRoundTrip(t, rt, req)
	assertStatus(t, resp, http.StatusOK)
	assertCalls(t, base, u1, u3)

	if len(events) != 4 {
		t.Fatalf("unexpected event count: got=%d events=%#v", len(events), events)
	}
	assertEvent(t, events[0], Event{
		Kind:     EventAttempt,
		Endpoint: "endpoint-1",
		Attempt:  1,
		Err:      io.EOF,
	})
	assertEvent(t, events[1], Event{
		Kind:     EventCooldownSkip,
		Endpoint: "endpoint-2",
	})
	assertEvent(t, events[2], Event{
		Kind:       EventAttempt,
		Endpoint:   "endpoint-3",
		Attempt:    2,
		StatusCode: http.StatusOK,
	})
	assertEvent(t, events[3], Event{
		Kind:       EventResult,
		Endpoint:   "endpoint-3",
		StatusCode: http.StatusOK,
	})
}

func TestEvents_ReplayErrorAfterTransportFailure(t *testing.T) {
	u1 := "https://u1.test/rpc"
	u2 := "https://u2.test/rpc"
	transportErr := errors.New("dial failed")
	replayErr := errors.New("replay failed")

	base := &scriptRT{
		results: map[string][]rtResult{
			u1: {{err: transportErr}},
			u2: {{resp: httpResp(http.StatusOK, "unexpected")}},
		},
	}
	var events []Event
	rt := mustNewTransport(t, Config{
		Endpoints: testEndpoints(u1, u2),
		Base:      base,
		OnEvent: func(_ context.Context, event Event) {
			events = append(events, event)
		},
	})

	req, err := http.NewRequest(http.MethodPost, u1, strings.NewReader("payload"))
	if err != nil {
		t.Fatalf("http.NewRequest: %v", err)
	}
	req.GetBody = func() (io.ReadCloser, error) {
		return nil, replayErr
	}
	req = req.WithContext(WithFailoverAllowed(req.Context()))

	resp, err := rt.RoundTrip(req)
	if resp != nil {
		t.Fatalf("expected nil response, got %#v", resp)
	}
	fe := mustAsFailoverError(t, err)
	if !errors.Is(fe, replayErr) {
		t.Fatalf("expected replay error as FailoverError cause, got %v", fe)
	}
	assertCalls(t, base, u1)

	if len(events) != 3 {
		t.Fatalf("unexpected event count: got=%d events=%#v", len(events), events)
	}
	assertEvent(t, events[0], Event{
		Kind:     EventAttempt,
		Endpoint: "endpoint-1",
		Attempt:  1,
		Err:      transportErr,
	})
	assertEvent(t, events[1], Event{
		Kind:     EventReplayError,
		Endpoint: "endpoint-2",
		Err:      replayErr,
	})
	assertEvent(t, events[2], Event{
		Kind: EventResult,
		Err:  err,
	})
	if events[2].Err != err {
		t.Fatalf("expected EventResult.Err to be the returned error: event=%v returned=%v", events[2].Err, err)
	}
}

func TestEvents_ReplayErrorDoesNotHideRetainedResponse(t *testing.T) {
	u1 := "https://u1.test/rpc"
	u2 := "https://u2.test/rpc"
	replayErr := errors.New("replay failed")
	retainedBody := newTrackingBody("service unavailable")

	base := &scriptRT{
		results: map[string][]rtResult{
			u1: {{resp: &http.Response{StatusCode: http.StatusServiceUnavailable, Body: retainedBody}}},
			u2: {{resp: httpResp(http.StatusOK, "unexpected")}},
		},
	}
	var events []Event
	rt := mustNewTransport(t, Config{
		Endpoints: testEndpoints(u1, u2),
		Base:      base,
		OnEvent: func(_ context.Context, event Event) {
			events = append(events, event)
		},
	})

	req, err := http.NewRequest(http.MethodPost, u1, strings.NewReader("payload"))
	if err != nil {
		t.Fatalf("http.NewRequest: %v", err)
	}
	req.GetBody = func() (io.ReadCloser, error) {
		return nil, replayErr
	}
	req = req.WithContext(WithFailoverAllowed(req.Context()))

	resp, err := rt.RoundTrip(req)
	if err != nil {
		t.Fatalf("RoundTrip error: %v", err)
	}
	if resp == nil {
		t.Fatal("expected retained response")
	}
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("expected retained HTTP %d, got %d", http.StatusServiceUnavailable, resp.StatusCode)
	}
	if retainedBody.Closed() {
		t.Fatal("expected returned retained response body to remain caller-owned")
	}
	t.Cleanup(func() { resp.Body.Close() })
	assertCalls(t, base, u1)

	if len(events) != 3 {
		t.Fatalf("unexpected event count: got=%d events=%#v", len(events), events)
	}
	assertEvent(t, events[0], Event{
		Kind:       EventAttempt,
		Endpoint:   "endpoint-1",
		Attempt:    1,
		StatusCode: http.StatusServiceUnavailable,
	})
	assertEvent(t, events[1], Event{
		Kind:     EventReplayError,
		Endpoint: "endpoint-2",
		Err:      replayErr,
	})
	assertEvent(t, events[2], Event{
		Kind:       EventResult,
		Endpoint:   "endpoint-1",
		StatusCode: http.StatusServiceUnavailable,
	})
}

func TestEventResult_RetainedResponseAfterLaterTransportError(t *testing.T) {
	u1 := "https://u1.test/rpc"
	u2 := "https://u2.test/rpc"
	transportErr := errors.New("transport failed")
	retainedBody := newTrackingBody("service unavailable")

	base := &scriptRT{
		results: map[string][]rtResult{
			u1: {{resp: &http.Response{StatusCode: http.StatusServiceUnavailable, Body: retainedBody}}},
			u2: {{err: transportErr}},
		},
	}
	var events []Event
	rt := mustNewTransport(t, Config{
		Endpoints: testEndpoints(u1, u2),
		Base:      base,
		OnEvent: func(_ context.Context, event Event) {
			events = append(events, event)
		},
	})

	req := newRPCRequest(t, u1, "eth_blockNumber")
	req = req.WithContext(WithFailoverAllowed(req.Context()))
	resp, err := rt.RoundTrip(req)
	if err != nil {
		t.Fatalf("RoundTrip error: %v", err)
	}
	if resp == nil {
		t.Fatal("expected retained response")
	}
	t.Cleanup(func() { resp.Body.Close() })

	assertStatus(t, resp, http.StatusServiceUnavailable)
	if retainedBody.Closed() {
		t.Fatal("expected returned retained response body to remain caller-owned")
	}
	assertCalls(t, base, u1, u2)

	if len(events) != 3 {
		t.Fatalf("unexpected event count: got=%d events=%#v", len(events), events)
	}
	assertEvent(t, events[0], Event{
		Kind:       EventAttempt,
		Endpoint:   "endpoint-1",
		Attempt:    1,
		StatusCode: http.StatusServiceUnavailable,
	})
	assertEvent(t, events[1], Event{
		Kind:     EventAttempt,
		Endpoint: "endpoint-2",
		Attempt:  2,
		Err:      transportErr,
	})
	assertEvent(t, events[2], Event{
		Kind:       EventResult,
		Endpoint:   "endpoint-1",
		StatusCode: http.StatusServiceUnavailable,
	})
	assertSingleFinalResult(t, events)
}

func TestEventResult_TerminalFailoverError(t *testing.T) {
	u1 := "https://u1.test/rpc"
	u2 := "https://u2.test/rpc"
	firstErr := errors.New("first failed")
	secondErr := errors.New("second failed")

	base := &scriptRT{
		results: map[string][]rtResult{
			u1: {{err: firstErr}},
			u2: {{err: secondErr}},
		},
	}
	var events []Event
	rt := mustNewTransport(t, Config{
		Endpoints: testEndpoints(u1, u2),
		Base:      base,
		OnEvent: func(_ context.Context, event Event) {
			events = append(events, event)
		},
	})

	req, err := http.NewRequest(http.MethodGet, u1, nil)
	if err != nil {
		t.Fatalf("http.NewRequest: %v", err)
	}
	resp, err := rt.RoundTrip(req)
	if resp != nil {
		t.Fatalf("expected nil response, got %#v", resp)
	}
	fe := mustAsFailoverError(t, err)
	if len(fe.Attempts) != 2 {
		t.Fatalf("expected 2 attempt errors, got %d", len(fe.Attempts))
	}
	if fe.Attempts[0].Endpoint != "endpoint-1" || !errors.Is(fe.Attempts[0].Err, firstErr) {
		t.Fatalf("unexpected first attempt error: %#v", fe.Attempts[0])
	}
	if fe.Attempts[1].Endpoint != "endpoint-2" || !errors.Is(fe.Attempts[1].Err, secondErr) {
		t.Fatalf("unexpected second attempt error: %#v", fe.Attempts[1])
	}
	if !errors.Is(err, secondErr) {
		t.Fatalf("expected terminal cause %v, got %v", secondErr, err)
	}
	assertCalls(t, base, u1, u2)

	if len(events) != 3 {
		t.Fatalf("unexpected event count: got=%d events=%#v", len(events), events)
	}
	assertEvent(t, events[0], Event{
		Kind:     EventAttempt,
		Endpoint: "endpoint-1",
		Attempt:  1,
		Err:      firstErr,
	})
	assertEvent(t, events[1], Event{
		Kind:     EventAttempt,
		Endpoint: "endpoint-2",
		Attempt:  2,
		Err:      secondErr,
	})
	assertEvent(t, events[2], Event{
		Kind: EventResult,
		Err:  err,
	})
	if events[2].Err != err {
		t.Fatalf("expected EventResult.Err to be returned error: event=%v returned=%v", events[2].Err, err)
	}
	assertSingleFinalResult(t, events)
}

func TestEventResult_NoExternallyEligibleEndpoint(t *testing.T) {
	u1 := "https://u1.test/rpc"
	u2 := "https://u2.test/rpc"

	base := &scriptRT{results: map[string][]rtResult{}}
	var events []Event
	rt := mustNewTransport(t, Config{
		Endpoints: testEndpoints(u1, u2),
		Base:      base,
		Eligible: func(EndpointID) bool {
			return false
		},
		OnEvent: func(_ context.Context, event Event) {
			events = append(events, event)
		},
	})

	resp, err := rt.RoundTrip(newRPCRequest(t, u1, "eth_blockNumber"))
	if resp != nil {
		t.Fatalf("expected nil response, got %#v", resp)
	}
	if !errors.Is(err, ErrNoUsableEndpoint) {
		t.Fatalf("expected ErrNoUsableEndpoint, got %v", err)
	}
	assertCalls(t, base)

	if len(events) != 1 {
		t.Fatalf("unexpected event count: got=%d events=%#v", len(events), events)
	}
	assertEvent(t, events[0], Event{
		Kind: EventResult,
		Err:  ErrNoUsableEndpoint,
	})
	if events[0].Err != err {
		t.Fatalf("expected EventResult.Err to be returned error: event=%v returned=%v", events[0].Err, err)
	}
	assertSingleFinalResult(t, events)
}

func TestEventResult_AllCandidatesCooling(t *testing.T) {
	u1 := "https://u1.test/rpc"
	u2 := "https://u2.test/rpc"
	fixedNow := time.Unix(1200, 0)

	base := &scriptRT{results: map[string][]rtResult{}}
	var events []Event
	rt := mustNewTransport(t, Config{
		Endpoints: testEndpoints(u1, u2),
		Base:      base,
		OnEvent: func(_ context.Context, event Event) {
			events = append(events, event)
		},
	})
	rt.now = func() time.Time { return fixedNow }

	rt.cooldown.mu.Lock()
	rt.cooldown.coolingTo[0] = fixedNow.Add(time.Hour)
	rt.cooldown.coolingTo[1] = fixedNow.Add(time.Hour)
	rt.cooldown.mu.Unlock()

	resp, err := rt.RoundTrip(newRPCRequest(t, u1, "eth_blockNumber"))
	if resp != nil {
		t.Fatalf("expected nil response, got %#v", resp)
	}
	if !errors.Is(err, ErrNoUsableEndpoint) {
		t.Fatalf("expected ErrNoUsableEndpoint, got %v", err)
	}
	assertCalls(t, base)

	if len(events) != 3 {
		t.Fatalf("unexpected event count: got=%d events=%#v", len(events), events)
	}
	assertEvent(t, events[0], Event{
		Kind:     EventCooldownSkip,
		Endpoint: "endpoint-1",
	})
	assertEvent(t, events[1], Event{
		Kind:     EventCooldownSkip,
		Endpoint: "endpoint-2",
	})
	assertEvent(t, events[2], Event{
		Kind: EventResult,
		Err:  ErrNoUsableEndpoint,
	})
	if events[2].Err != err {
		t.Fatalf("expected EventResult.Err to be returned error: event=%v returned=%v", events[2].Err, err)
	}
	assertSingleFinalResult(t, events)
}

func TestEventResult_UnknownPreferredEndpoint(t *testing.T) {
	u1 := "https://u1.test/rpc"

	base := &scriptRT{results: map[string][]rtResult{}}
	var events []Event
	rt := mustNewTransport(t, Config{
		Endpoints: testEndpoints(u1),
		Base:      base,
		OnEvent: func(_ context.Context, event Event) {
			events = append(events, event)
		},
	})

	req := newRPCRequest(t, u1, "eth_blockNumber")
	req = req.WithContext(WithPreferredEndpoint(req.Context(), "missing"))
	resp, err := rt.RoundTrip(req)
	if resp != nil {
		t.Fatalf("expected nil response, got %#v", resp)
	}
	if !errors.Is(err, ErrUnknownEndpoint) {
		t.Fatalf("expected ErrUnknownEndpoint, got %v", err)
	}
	assertCalls(t, base)

	if len(events) != 1 {
		t.Fatalf("unexpected event count: got=%d events=%#v", len(events), events)
	}
	assertEvent(t, events[0], Event{
		Kind: EventResult,
		Err:  err,
	})
	if events[0].Err != err {
		t.Fatalf("expected EventResult.Err to be returned error: event=%v returned=%v", events[0].Err, err)
	}
	assertSingleFinalResult(t, events)
}

func TestEventResult_LogicalCancellationOverridesRetainedResponse(t *testing.T) {
	u1 := "https://u1.test/rpc"
	u2 := "https://u2.test/rpc"
	retainedBody := newTrackingBody("service unavailable")

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	var calls []string
	base := roundTripperFunc(func(req *http.Request) (*http.Response, error) {
		calls = append(calls, req.URL.String())
		switch req.URL.String() {
		case u1:
			return &http.Response{
				StatusCode: http.StatusServiceUnavailable,
				Body:       retainedBody,
				Header:     make(http.Header),
				Request:    req,
			}, nil
		case u2:
			cancel()
			return nil, io.EOF
		default:
			return nil, errors.New("unexpected URL: " + req.URL.String())
		}
	})
	var events []Event
	rt := mustNewTransport(t, Config{
		Endpoints: testEndpoints(u1, u2),
		Base:      base,
		OnEvent: func(_ context.Context, event Event) {
			events = append(events, event)
		},
	})

	req := newRPCRequest(t, u1, "eth_blockNumber")
	req = req.WithContext(WithFailoverAllowed(ctx))
	resp, err := rt.RoundTrip(req)
	if resp != nil {
		t.Fatalf("expected nil response, got %#v", resp)
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
	if !retainedBody.Closed() {
		t.Fatal("expected retained response body to be closed on logical cancellation")
	}
	if len(calls) != 2 || calls[0] != u1 || calls[1] != u2 {
		t.Fatalf("unexpected physical attempts: got=%v want=%v", calls, []string{u1, u2})
	}

	if len(events) != 3 {
		t.Fatalf("unexpected event count: got=%d events=%#v", len(events), events)
	}
	assertEvent(t, events[0], Event{
		Kind:       EventAttempt,
		Endpoint:   "endpoint-1",
		Attempt:    1,
		StatusCode: http.StatusServiceUnavailable,
	})
	assertEvent(t, events[1], Event{
		Kind:     EventAttempt,
		Endpoint: "endpoint-2",
		Attempt:  2,
		Err:      io.EOF,
	})
	assertEvent(t, events[2], Event{
		Kind: EventResult,
		Err:  context.Canceled,
	})
	if events[2].Err != err {
		t.Fatalf("expected EventResult.Err to be returned error: event=%v returned=%v", events[2].Err, err)
	}
	assertSingleFinalResult(t, events)
}

func assertSingleFinalResult(t *testing.T, events []Event) {
	t.Helper()

	count := 0
	resultIndex := -1
	for i, event := range events {
		if event.Kind == EventResult {
			count++
			resultIndex = i
		}
	}
	if count != 1 {
		t.Fatalf("expected exactly one EventResult, got %d events=%#v", count, events)
	}
	if resultIndex != len(events)-1 {
		t.Fatalf("expected EventResult to be last: index=%d events=%#v", resultIndex, events)
	}
}

func assertEvent(t *testing.T, got, want Event) {
	t.Helper()

	if got.Kind != want.Kind {
		t.Fatalf("event kind: got=%v want=%v event=%#v", got.Kind, want.Kind, got)
	}
	if got.Endpoint != want.Endpoint {
		t.Fatalf("event endpoint: got=%q want=%q event=%#v", got.Endpoint, want.Endpoint, got)
	}
	if got.Attempt != want.Attempt {
		t.Fatalf("event attempt: got=%d want=%d event=%#v", got.Attempt, want.Attempt, got)
	}
	if got.StatusCode != want.StatusCode {
		t.Fatalf("event status: got=%d want=%d event=%#v", got.StatusCode, want.StatusCode, got)
	}
	if want.Err == nil {
		if got.Err != nil {
			t.Fatalf("event error: got=%v want=nil event=%#v", got.Err, got)
		}
		return
	}
	if !errors.Is(got.Err, want.Err) {
		t.Fatalf("event error: got=%v want=%v event=%#v", got.Err, want.Err, got)
	}
}
