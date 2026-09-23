package failnext

import (
	"context"
	"errors"
	"io"
	"net/http"
	"testing"
	"time"
)

func TestRoundTrip_TriggerResponseSupersededBySuccess(t *testing.T) {
	u1 := "https://u1.test/rpc"
	u2 := "https://u2.test/rpc"

	firstBody := newTrackingBody("unavailable")
	finalBody := newTrackingBody("ok")
	base := &scriptRT{
		results: map[string][]rtResult{
			u1: {{resp: &http.Response{StatusCode: http.StatusServiceUnavailable, Body: firstBody}}},
			u2: {{resp: &http.Response{StatusCode: http.StatusOK, Body: finalBody}}},
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
	resp, err := rt.RoundTrip(req)
	if err != nil {
		t.Fatalf("RoundTrip error: %v", err)
	}
	t.Cleanup(func() { resp.Body.Close() })

	assertStatus(t, resp, http.StatusOK)
	if !firstBody.Closed() {
		t.Fatal("expected retained 503 response body to be closed when 200 supersedes it")
	}
	if finalBody.Closed() {
		t.Fatal("expected returned 200 response body to remain open for caller")
	}
	assertCalls(t, base, u1, u2)
}

func TestRoundTrip_Retained503SurvivesLaterTransportError(t *testing.T) {
	u1 := "https://u1.test/rpc"
	u2 := "https://u2.test/rpc"

	retainedBody := newTrackingBody("unavailable")
	retainedResp := &http.Response{StatusCode: http.StatusServiceUnavailable, Body: retainedBody}
	base := &scriptRT{
		results: map[string][]rtResult{
			u1: {{resp: retainedResp}},
			u2: {{err: io.EOF}},
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
	resp, err := rt.RoundTrip(req)
	if err != nil {
		t.Fatalf("RoundTrip error: %v", err)
	}
	t.Cleanup(func() { resp.Body.Close() })

	if resp != retainedResp {
		t.Fatalf("expected retained 503 response, got %#v", resp)
	}
	if retainedBody.Closed() {
		t.Fatal("expected returned retained response body to remain open for caller")
	}
	assertCalls(t, base, u1, u2)
}

func TestRoundTrip_HTTPResponseOutranksEarlierTransportError(t *testing.T) {
	u1 := "https://u1.test/rpc"
	u2 := "https://u2.test/rpc"

	finalBody := newTrackingBody("unavailable")
	finalResp := &http.Response{StatusCode: http.StatusServiceUnavailable, Body: finalBody}
	base := &scriptRT{
		results: map[string][]rtResult{
			u1: {{err: io.EOF}},
			u2: {{resp: finalResp}},
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
	resp, err := rt.RoundTrip(req)
	if err != nil {
		t.Fatalf("RoundTrip error: %v", err)
	}
	t.Cleanup(func() { resp.Body.Close() })

	if resp != finalResp {
		t.Fatalf("expected final 503 response, got %#v", resp)
	}
	if finalBody.Closed() {
		t.Fatal("expected returned 503 response body to remain open for caller")
	}
	assertCalls(t, base, u1, u2)
}

func TestRoundTrip_LatestTriggerResponseSupersedesEarlierResponse(t *testing.T) {
	u1 := "https://u1.test/rpc"
	u2 := "https://u2.test/rpc"

	firstBody := newTrackingBody("503")
	latestBody := newTrackingBody("504")
	latestResp := &http.Response{StatusCode: http.StatusGatewayTimeout, Body: latestBody}
	base := &scriptRT{
		results: map[string][]rtResult{
			u1: {{resp: &http.Response{StatusCode: http.StatusServiceUnavailable, Body: firstBody}}},
			u2: {{resp: latestResp}},
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
	resp, err := rt.RoundTrip(req)
	if err != nil {
		t.Fatalf("RoundTrip error: %v", err)
	}
	t.Cleanup(func() { resp.Body.Close() })

	if resp != latestResp {
		t.Fatalf("expected latest 504 response, got %#v", resp)
	}
	if !firstBody.Closed() {
		t.Fatal("expected superseded 503 response body to be closed")
	}
	if latestBody.Closed() {
		t.Fatal("expected returned 504 response body to remain open for caller")
	}
	assertCalls(t, base, u1, u2)
}

func TestRoundTrip_LatestTriggerResponseSurvivesLaterTransportError(t *testing.T) {
	u1 := "https://u1.test/rpc"
	u2 := "https://u2.test/rpc"
	u3 := "https://u3.test/rpc"

	firstBody := newTrackingBody("503")
	latestBody := newTrackingBody("504")
	latestResp := &http.Response{StatusCode: http.StatusGatewayTimeout, Body: latestBody}
	base := &scriptRT{
		results: map[string][]rtResult{
			u1: {{resp: &http.Response{StatusCode: http.StatusServiceUnavailable, Body: firstBody}}},
			u2: {{resp: latestResp}},
			u3: {{err: io.ErrUnexpectedEOF}},
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
	if err != nil {
		t.Fatalf("RoundTrip error: %v", err)
	}
	t.Cleanup(func() { resp.Body.Close() })

	if resp != latestResp {
		t.Fatalf("expected retained 504 response, got %#v", resp)
	}
	if !firstBody.Closed() {
		t.Fatal("expected superseded 503 response body to be closed")
	}
	if latestBody.Closed() {
		t.Fatal("expected returned 504 response body to remain open for caller")
	}
	assertCalls(t, base, u1, u2, u3)
}

func TestRoundTrip_TriggerStatusExhaustionReturnsLatestResponse(t *testing.T) {
	u1 := "https://u1.test/rpc"
	u2 := "https://u2.test/rpc"
	u3 := "https://u3.test/rpc"

	body503 := newTrackingBody("503")
	body502 := newTrackingBody("502")
	body504 := newTrackingBody("504")
	latestResp := &http.Response{StatusCode: http.StatusGatewayTimeout, Body: body504}
	base := &scriptRT{
		results: map[string][]rtResult{
			u1: {{resp: &http.Response{StatusCode: http.StatusServiceUnavailable, Body: body503}}},
			u2: {{resp: &http.Response{StatusCode: http.StatusBadGateway, Body: body502}}},
			u3: {{resp: latestResp}},
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
	if err != nil {
		t.Fatalf("RoundTrip error: %v", err)
	}
	t.Cleanup(func() { resp.Body.Close() })

	if resp != latestResp {
		t.Fatalf("expected latest 504 response, got %#v", resp)
	}
	if !body503.Closed() {
		t.Fatal("expected superseded 503 response body to be closed")
	}
	if !body502.Closed() {
		t.Fatal("expected superseded 502 response body to be closed")
	}
	if body504.Closed() {
		t.Fatal("expected returned 504 response body to remain open for caller")
	}
	assertCalls(t, base, u1, u2, u3)
}

func TestRoundTrip_PermissionDenialReturnsTriggerResponse(t *testing.T) {
	u1 := "https://u1.test/rpc"
	u2 := "https://u2.test/rpc"

	body := newTrackingBody("unavailable")
	triggerResp := &http.Response{StatusCode: http.StatusServiceUnavailable, Body: body}
	base := &scriptRT{
		results: map[string][]rtResult{
			u1: {{resp: triggerResp}},
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
	req = req.WithContext(WithFailoverDenied(req.Context()))
	resp, err := rt.RoundTrip(req)
	if err != nil {
		t.Fatalf("RoundTrip error: %v", err)
	}
	t.Cleanup(func() { resp.Body.Close() })

	if resp != triggerResp {
		t.Fatalf("expected retained 503 response, got %#v", resp)
	}
	if body.Closed() {
		t.Fatal("expected returned 503 response body to remain open for caller")
	}
	assertCalls(t, base, u1)
}

func TestRoundTrip_CoolingLaterCandidatesReturnRetainedResponse(t *testing.T) {
	u1 := "https://u1.test/rpc"
	u2 := "https://u2.test/rpc"
	u3 := "https://u3.test/rpc"
	fixedNow := time.Unix(1700, 0)

	body := newTrackingBody("unavailable")
	triggerResp := &http.Response{StatusCode: http.StatusServiceUnavailable, Body: body}
	base := &scriptRT{
		results: map[string][]rtResult{
			u1: {{resp: triggerResp}},
			u2: {{resp: httpResp(http.StatusOK, "unexpected-2")}},
			u3: {{resp: httpResp(http.StatusOK, "unexpected-3")}},
		},
	}
	tr := mustNewTransport(t, Config{
		Endpoints: testEndpoints(u1, u2, u3),
		Base:      base,
	})
	tr.now = func() time.Time { return fixedNow }

	tr.cooldown.mu.Lock()
	tr.cooldown.coolingTo[1] = fixedNow.Add(time.Hour)
	tr.cooldown.coolingTo[2] = fixedNow.Add(time.Hour)
	tr.cooldown.mu.Unlock()

	req, err := http.NewRequest(http.MethodGet, u1, nil)
	if err != nil {
		t.Fatalf("http.NewRequest: %v", err)
	}
	resp, err := tr.RoundTrip(req)
	if err != nil {
		t.Fatalf("RoundTrip error: %v", err)
	}
	t.Cleanup(func() { resp.Body.Close() })

	if resp != triggerResp {
		t.Fatalf("expected retained 503 response, got %#v", resp)
	}
	if body.Closed() {
		t.Fatal("expected returned retained response body to remain open for caller")
	}
	assertCalls(t, base, u1)
}

func TestRoundTrip_ContextCancellationDiscardsRetainedResponse(t *testing.T) {
	u1 := "https://u1.test/rpc"
	u2 := "https://u2.test/rpc"

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	retainedBody := newTrackingBody("unavailable")
	calls := make([]string, 0, 2)
	base := roundTripperFunc(func(req *http.Request) (*http.Response, error) {
		calls = append(calls, req.URL.String())
		switch req.URL.String() {
		case u1:
			return &http.Response{
				StatusCode: http.StatusServiceUnavailable,
				Body:       retainedBody,
			}, nil
		case u2:
			cancel()
			return nil, io.EOF
		default:
			return nil, errors.New("unexpected URL: " + req.URL.String())
		}
	})
	rt := mustNewTransport(t, Config{
		Endpoints: testEndpoints(u1, u2),
		Base:      base,
	})

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u1, nil)
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
	if !retainedBody.Closed() {
		t.Fatal("expected retained response body to be closed when logical cancellation wins")
	}

	want := []string{u1, u2}
	if len(calls) != len(want) {
		t.Fatalf("unexpected base calls: got=%v want=%v", calls, want)
	}
	for i := range want {
		if calls[i] != want[i] {
			t.Fatalf("unexpected base calls: got=%v want=%v", calls, want)
		}
	}
}
