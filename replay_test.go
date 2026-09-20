package rcpx

import (
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
)

func TestRoundTrip_BodylessRequestCanContinue(t *testing.T) {
	u1 := "https://u1.test/rpc"
	u2 := "https://u2.test/rpc"

	base := &scriptRT{
		results: map[string][]rtResult{
			u1: {{err: io.EOF}},
			u2: {{resp: httpResp(http.StatusOK, "ok")}},
		},
	}
	rt := mustNewTransport(t, Config{
		Endpoints: testEndpoints(u1, u2),
		Base:      base,
	})

	req := newBodylessRequest(t, http.MethodPost, u1)
	req = req.WithContext(WithFailoverAllowed(req.Context()))
	resp, err := rt.RoundTrip(req)
	if err != nil {
		t.Fatalf("RoundTrip error: %v", err)
	}
	resp.Body.Close()

	assertCalls(t, base, u1, u2)
}

func TestRoundTrip_OneShotBodyIsValidForFirstAttempt(t *testing.T) {
	u1 := "https://u1.test/rpc"
	body := newTrackingBody("one-shot")

	base := roundTripperFunc(func(req *http.Request) (*http.Response, error) {
		got, err := io.ReadAll(req.Body)
		if err != nil {
			t.Fatalf("read request body: %v", err)
		}
		if got := string(got); got != "one-shot" {
			t.Fatalf("unexpected request body: got %q want %q", got, "one-shot")
		}
		if err := req.Body.Close(); err != nil {
			t.Fatalf("close request body: %v", err)
		}
		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       io.NopCloser(strings.NewReader("ok")),
			Header:     make(http.Header),
			Request:    req,
		}, nil
	})
	rt := mustNewTransport(t, Config{
		Endpoints: testEndpoints(u1),
		Base:      base,
	})

	req, err := http.NewRequest(http.MethodPost, u1, body)
	if err != nil {
		t.Fatalf("http.NewRequest: %v", err)
	}
	if req.GetBody != nil {
		t.Fatal("expected one-shot request GetBody to be nil")
	}

	resp, err := rt.RoundTrip(req)
	if err != nil {
		t.Fatalf("RoundTrip error: %v", err)
	}
	resp.Body.Close()

	if !body.Closed() {
		t.Fatal("expected base transport to close handed-off one-shot body")
	}
}

func TestRoundTrip_LaterAttemptUsesGetBody(t *testing.T) {
	u1 := "https://u1.test/rpc"
	u2 := "https://u2.test/rpc"

	original := newTrackingBody("original")
	var replay *trackingBody
	getBodyCalls := 0
	var gotBodies []string

	base := roundTripperFunc(func(req *http.Request) (*http.Response, error) {
		b, err := io.ReadAll(req.Body)
		if err != nil {
			t.Fatalf("read request body: %v", err)
		}
		gotBodies = append(gotBodies, string(b))
		if err := req.Body.Close(); err != nil {
			t.Fatalf("close request body: %v", err)
		}

		switch req.URL.String() {
		case u1:
			return nil, io.EOF
		case u2:
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
		getBodyCalls++
		replay = newTrackingBody("replay")
		return replay, nil
	}
	req = req.WithContext(WithFailoverAllowed(req.Context()))

	resp, err := rt.RoundTrip(req)
	if err != nil {
		t.Fatalf("RoundTrip error: %v", err)
	}
	resp.Body.Close()

	if getBodyCalls != 1 {
		t.Fatalf("expected GetBody calls=1, got %d", getBodyCalls)
	}
	if len(gotBodies) != 2 || gotBodies[0] != "original" || gotBodies[1] != "replay" {
		t.Fatalf("unexpected attempt bodies: %q", gotBodies)
	}
	if !original.Closed() {
		t.Fatal("expected base transport to close original body")
	}
	if replay == nil || !replay.Closed() {
		t.Fatal("expected base transport to close replay body")
	}
}

func TestRoundTrip_MissingGetBodyPreventsLaterAttempt(t *testing.T) {
	u1 := "https://u1.test/rpc"
	u2 := "https://u2.test/rpc"
	body := newTrackingBody("one-shot")

	base := &scriptRT{
		results: map[string][]rtResult{
			u1: {{err: io.EOF}},
			u2: {{resp: httpResp(http.StatusOK, "ok")}},
		},
	}
	rt := mustNewTransport(t, Config{
		Endpoints: testEndpoints(u1, u2),
		Base:      base,
	})

	req, err := http.NewRequest(http.MethodPost, u1, body)
	if err != nil {
		t.Fatalf("http.NewRequest: %v", err)
	}
	if req.GetBody != nil {
		t.Fatal("expected one-shot request GetBody to be nil")
	}
	req = req.WithContext(WithFailoverAllowed(req.Context()))

	resp, err := rt.RoundTrip(req)
	if resp != nil {
		t.Fatalf("expected nil response, got %#v", resp)
	}
	ae := mustAsAllUpstreamsFailed(t, err)
	if ae.Attempted != 1 {
		t.Fatalf("expected Attempted=1, got %d", ae.Attempted)
	}
	assertCalls(t, base, u1)
}

func TestRoundTrip_GetBodyErrorPreventsProspectiveAttempt(t *testing.T) {
	u1 := "https://u1.test/rpc"
	u2 := "https://u2.test/rpc"
	replayErr := errors.New("replay failed")

	base := &scriptRT{
		results: map[string][]rtResult{
			u1: {{err: io.EOF}},
			u2: {{resp: httpResp(http.StatusOK, "ok")}},
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

	req, err := http.NewRequest(http.MethodPost, u1, newTrackingBody("original"))
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
	if !errors.Is(err, replayErr) {
		t.Fatalf("expected replay error, got %v", err)
	}
	if getBodyCalls != 1 {
		t.Fatalf("expected GetBody calls=1, got %d", getBodyCalls)
	}
	assertCalls(t, base, u1)

	if len(attempts) != 1 {
		t.Fatalf("expected one physical attempt observation, got %d: %#v", len(attempts), attempts)
	}
	if attempts[0].Attempt != 1 || attempts[0].Upstream != u1 || !attempts[0].Final {
		t.Fatalf("unexpected attempt observation: %#v", attempts[0])
	}
}

func TestRoundTrip_ReplayabilityDoesNotGrantPermission(t *testing.T) {
	u1 := "https://u1.test/rpc"
	u2 := "https://u2.test/rpc"

	base := &scriptRT{
		results: map[string][]rtResult{
			u1: {{err: io.EOF}},
			u2: {{resp: httpResp(http.StatusOK, "ok")}},
		},
	}
	rt := mustNewTransport(t, Config{
		Endpoints: testEndpoints(u1, u2),
		Base:      base,
	})

	req, err := http.NewRequest(http.MethodPost, u1, strings.NewReader("replayable"))
	if err != nil {
		t.Fatalf("http.NewRequest: %v", err)
	}
	if req.GetBody == nil {
		t.Fatal("expected standard request to provide GetBody")
	}
	getBodyCalls := 0
	getBody := req.GetBody
	req.GetBody = func() (io.ReadCloser, error) {
		getBodyCalls++
		return getBody()
	}

	resp, err := rt.RoundTrip(req)
	if resp != nil {
		t.Fatalf("expected nil response, got %#v", resp)
	}
	mustAsAllUpstreamsFailed(t, err)
	if getBodyCalls != 0 {
		t.Fatalf("expected GetBody not to be called when permission denies continuation, got %d calls", getBodyCalls)
	}
	assertCalls(t, base, u1)
}

func TestRoundTrip_RetryPolicyStopDoesNotCallGetBody(t *testing.T) {
	u1 := "https://u1.test/rpc"
	u2 := "https://u2.test/rpc"

	base := &scriptRT{
		results: map[string][]rtResult{
			u1: {{err: io.EOF}},
			u2: {{resp: httpResp(http.StatusOK, "ok")}},
		},
	}
	policy, retryPolicy := newPolicyRecorder(false)
	rt := mustNewTransport(t, Config{
		Endpoints:   testEndpoints(u1, u2),
		Base:        base,
		RetryPolicy: retryPolicy,
	})

	req, err := http.NewRequest(http.MethodPost, u1, strings.NewReader("replayable"))
	if err != nil {
		t.Fatalf("http.NewRequest: %v", err)
	}
	getBodyCalls := 0
	getBody := req.GetBody
	req.GetBody = func() (io.ReadCloser, error) {
		getBodyCalls++
		return getBody()
	}
	req = req.WithContext(WithFailoverAllowed(req.Context()))

	resp, err := rt.RoundTrip(req)
	if resp != nil {
		t.Fatalf("expected nil response, got %#v", resp)
	}
	mustAsAllUpstreamsFailed(t, err)
	assertPolicyCalls(t, policy, 1)
	if getBodyCalls != 0 {
		t.Fatalf("expected GetBody not to be called when RetryPolicy stops continuation, got %d calls", getBodyCalls)
	}
	assertCalls(t, base, u1)
}

func TestRoundTrip_DoesNotPreReadRequestBody(t *testing.T) {
	u1 := "https://u1.test/rpc"
	body := &readCountingBody{r: strings.NewReader("payload")}

	base := roundTripperFunc(func(req *http.Request) (*http.Response, error) {
		if body.reads != 0 {
			t.Fatalf("expected body to be unread before base handoff, got %d reads", body.reads)
		}
		if _, err := io.ReadAll(req.Body); err != nil {
			t.Fatalf("read request body: %v", err)
		}
		if err := req.Body.Close(); err != nil {
			t.Fatalf("close request body: %v", err)
		}
		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       io.NopCloser(strings.NewReader("ok")),
			Header:     make(http.Header),
			Request:    req,
		}, nil
	})
	rt := mustNewTransport(t, Config{
		Endpoints: testEndpoints(u1),
		Base:      base,
	})

	req, err := http.NewRequest(http.MethodPost, u1, body)
	if err != nil {
		t.Fatalf("http.NewRequest: %v", err)
	}
	resp, err := rt.RoundTrip(req)
	if err != nil {
		t.Fatalf("RoundTrip error: %v", err)
	}
	resp.Body.Close()

	if body.reads == 0 {
		t.Fatal("expected base transport to read request body")
	}
}

type readCountingBody struct {
	r     io.Reader
	reads int
}

func (b *readCountingBody) Read(p []byte) (int, error) {
	b.reads++
	return b.r.Read(p)
}

func (*readCountingBody) Close() error { return nil }
