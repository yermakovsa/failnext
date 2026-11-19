package failnext

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestRoundTrip_AttemptTimeout_FailsOverWithEventsAndCooldown(t *testing.T) {
	const timeout = 30 * time.Millisecond
	u1, u2 := "https://primary.test/rpc", "https://backup.test/rpc"
	var firstCtx, backupCtx context.Context
	var calls []string
	body := newTrackingBody("backup response")
	base := roundTripperFunc(func(req *http.Request) (*http.Response, error) {
		calls = append(calls, req.URL.String())
		switch req.URL.String() {
		case u1:
			firstCtx = req.Context()
			<-firstCtx.Done()
			return nil, firstCtx.Err()
		case u2:
			backupCtx = req.Context()
			return &http.Response{StatusCode: http.StatusOK, Body: body}, nil
		default:
			return nil, errors.New("unexpected endpoint")
		}
	})

	var events []Event
	tr := mustNewTransport(t, Config{
		Endpoints:      testEndpoints(u1, u2),
		Base:           base,
		AttemptTimeout: timeout,
		Cooldown:       CooldownConfig{Threshold: 1, Duration: time.Hour},
		OnEvent: func(_ context.Context, e Event) {
			events = append(events, e)
		},
	})
	tr.now = time.Now // mustNewTransport freezes its clock by default.
	parent, cancelParent := context.WithCancel(context.Background())
	defer cancelParent()
	req, err := http.NewRequestWithContext(parent, http.MethodGet, u1, nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := tr.RoundTrip(req)
	if err != nil || resp == nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("final result: resp=%#v err=%v", resp, err)
	}
	defer resp.Body.Close()
	if got := strings.Join(calls, ","); got != u1+","+u2 {
		t.Fatalf("base calls = %s", got)
	}
	if parent.Err() != nil || firstCtx.Err() != context.Canceled || backupCtx.Err() != nil {
		t.Fatalf("context states: logical=%v first=%v backup=%v", parent.Err(), firstCtx.Err(), backupCtx.Err())
	}
	if len(events) != 3 || events[0].Kind != EventAttempt || events[1].Kind != EventAttempt || events[2].Kind != EventResult {
		t.Fatalf("event sequence = %#v", events)
	}
	if events[0].Endpoint != "endpoint-1" || events[0].Attempt != 1 || events[0].StatusCode != 0 ||
		!errors.Is(events[0].Err, ErrAttemptTimeout) || !errors.Is(events[0].Err, context.DeadlineExceeded) || events[0].Duration <= 0 {
		t.Fatalf("first attempt event = %#v", events[0])
	}
	if events[1].Endpoint != "endpoint-2" || events[1].Attempt != 2 || events[1].StatusCode != http.StatusOK || events[1].Err != nil {
		t.Fatalf("second attempt event = %#v", events[1])
	}
	if events[2].Endpoint != "endpoint-2" || events[2].StatusCode != http.StatusOK || events[2].Err != nil {
		t.Fatalf("final result event = %#v", events[2])
	}
	tr.cooldown.mu.Lock()
	cooling := tr.cooldown.coolingTo[0]
	tr.cooldown.mu.Unlock()
	if cooling.IsZero() {
		t.Fatal("timed-out primary was not counted as a cooldown failure")
	}

	// The backup's timer was stopped: a later body read must remain usable.
	time.Sleep(3 * timeout)
	if backupCtx.Err() != nil {
		t.Fatalf("accepted backup context canceled by expired attempt timer: %v", backupCtx.Err())
	}
	content, err := io.ReadAll(resp.Body)
	if err != nil || string(content) != "backup response" {
		t.Fatalf("backup body: %q, %v", content, err)
	}
	resp.Body.Close()
	if !body.Closed() || backupCtx.Err() != context.Canceled {
		t.Fatalf("backup body close did not release context: bodyClosed=%v ctx=%v", body.Closed(), backupCtx.Err())
	}
}

func TestRoundTrip_AttemptTimeout_PermissionAndReplayability(t *testing.T) {
	for _, tc := range []struct {
		name            string
		explicitlyAllow bool
	}{
		{name: "permission denied by default"},
		{name: "permission allowed but body not replayable", explicitlyAllow: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			u1, u2 := "https://primary.test/rpc", "https://backup.test/rpc"
			var calls int
			body := newTrackingBody("request payload")
			base := roundTripperFunc(func(req *http.Request) (*http.Response, error) {
				calls++
				defer req.Body.Close() // Base owns the request body after handoff.
				<-req.Context().Done()
				return nil, req.Context().Err()
			})
			tr := mustNewTransport(t, Config{
				Endpoints:      testEndpoints(u1, u2),
				Base:           base,
				AttemptTimeout: 30 * time.Millisecond,
				Cooldown:       CooldownConfig{Threshold: 1, Duration: time.Hour},
			})
			req, err := http.NewRequest(http.MethodPost, u1, body)
			if err != nil {
				t.Fatal(err)
			}
			req.GetBody = nil
			if tc.explicitlyAllow {
				req = req.WithContext(WithFailoverAllowed(req.Context()))
			}
			resp, err := tr.RoundTrip(req)
			if resp != nil || !errors.Is(err, ErrAttemptTimeout) || !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("final result: resp=%#v err=%v", resp, err)
			}
			var fe *FailoverError
			if !errors.As(err, &fe) || len(fe.Attempts) != 1 || !errors.Is(fe.Attempts[0].Err, ErrAttemptTimeout) {
				t.Fatalf("attempt errors: %#v err=%v", fe, err)
			}
			if calls != 1 || !body.Closed() {
				t.Fatalf("unexpected replay or request-body ownership: calls=%d bodyClosed=%v", calls, body.Closed())
			}
			tr.cooldown.mu.Lock()
			cooling := tr.cooldown.coolingTo[0]
			tr.cooldown.mu.Unlock()
			if cooling.IsZero() {
				t.Fatal("timeout was not counted toward cooldown despite terminal failover denial")
			}
		})
	}
}

func TestRoundTrip_AttemptTimeout_ReturnsReadableRetainedResponse(t *testing.T) {
	u1, u2 := "https://primary.test/rpc", "https://backup.test/rpc"
	var retainedCtx, timedOutCtx context.Context
	body := newTrackingBody("retained 503 body")
	retained := &http.Response{StatusCode: http.StatusServiceUnavailable, Body: body}
	base := roundTripperFunc(func(req *http.Request) (*http.Response, error) {
		switch req.URL.String() {
		case u1:
			retainedCtx = req.Context()
			return retained, nil
		case u2:
			timedOutCtx = req.Context()
			<-req.Context().Done()
			return nil, req.Context().Err()
		default:
			return nil, errors.New("unexpected endpoint")
		}
	})
	var events []Event
	tr := mustNewTransport(t, Config{
		Endpoints:      testEndpoints(u1, u2),
		Base:           base,
		AttemptTimeout: 30 * time.Millisecond,
		OnEvent: func(_ context.Context, e Event) {
			events = append(events, e)
		},
	})
	req, err := http.NewRequest(http.MethodGet, u1, nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := tr.RoundTrip(req)
	if err != nil || resp != retained {
		t.Fatalf("retained result: resp=%#v err=%v", resp, err)
	}
	defer resp.Body.Close()
	if timedOutCtx.Err() != context.Canceled || retainedCtx.Err() != nil || body.Closed() {
		t.Fatalf("retained response was not preserved: timedOutCtx=%v retainedCtx=%v closed=%v", timedOutCtx.Err(), retainedCtx.Err(), body.Closed())
	}
	if len(events) != 3 || events[2].Kind != EventResult || events[2].Endpoint != "endpoint-1" ||
		events[2].StatusCode != http.StatusServiceUnavailable || events[2].Err != nil {
		t.Fatalf("unexpected events: %#v", events)
	}
	time.Sleep(90 * time.Millisecond)
	if retainedCtx.Err() != nil {
		t.Fatalf("retained context canceled after timeout: %v", retainedCtx.Err())
	}
	data, err := io.ReadAll(resp.Body)
	if err != nil || string(data) != "retained 503 body" {
		t.Fatalf("retained body: %q, %v", data, err)
	}
	resp.Body.Close()
	if !body.Closed() || retainedCtx.Err() != context.Canceled {
		t.Fatalf("retained body close: closed=%v ctx=%v", body.Closed(), retainedCtx.Err())
	}
}

func TestRoundTrip_AttemptTimeout_LogicalCancellationWinsAndDiscardsRetained(t *testing.T) {
	u1, u2 := "https://primary.test/rpc", "https://backup.test/rpc"
	parent, cancel := context.WithCancel(context.Background())
	defer cancel()
	retainedBody := newTrackingBody("unavailable")
	var events []Event
	tr := mustNewTransport(t, Config{
		Endpoints: testEndpoints(u1, u2),
		Base: roundTripperFunc(func(req *http.Request) (*http.Response, error) {
			if req.URL.String() == u1 {
				return &http.Response{StatusCode: http.StatusServiceUnavailable, Body: retainedBody}, nil
			}
			<-req.Context().Done()
			return nil, req.Context().Err()
		}),
		AttemptTimeout: 30 * time.Millisecond,
		OnEvent: func(_ context.Context, e Event) {
			events = append(events, e)
			if e.Kind == EventAttempt && e.Attempt == 2 {
				cancel()
			}
		},
	})
	req, err := http.NewRequestWithContext(parent, http.MethodGet, u1, nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := tr.RoundTrip(req)
	if resp != nil || !errors.Is(err, context.Canceled) || !retainedBody.Closed() {
		t.Fatalf("context precedence: resp=%#v err=%v retainedClosed=%v", resp, err, retainedBody.Closed())
	}
	if errors.Is(err, ErrAttemptTimeout) {
		t.Fatalf("logical cancellation incorrectly reported as attempt timeout: %v", err)
	}
	if len(events) != 3 || !errors.Is(events[1].Err, ErrAttemptTimeout) ||
		!errors.Is(events[2].Err, context.Canceled) || events[2].Kind != EventResult {
		t.Fatalf("events = %#v", events)
	}
	tr.cooldown.mu.Lock()
	secondFailures := tr.cooldown.consec[1]
	tr.cooldown.mu.Unlock()
	if secondFailures != 0 {
		t.Fatalf("logical cancellation counted as cooldown failure: %d", secondFailures)
	}
}

func TestRoundTrip_AttemptTimeout_NormalizationStillClosesResponseWithError(t *testing.T) {
	u1, u2 := "https://primary.test/rpc", "https://backup.test/rpc"
	body := newTrackingBody("invalid response")
	var firstCtx context.Context
	baseErr := io.EOF
	base := roundTripperFunc(func(req *http.Request) (*http.Response, error) {
		if req.URL.String() == u1 {
			firstCtx = req.Context()
			return &http.Response{StatusCode: http.StatusServiceUnavailable, Body: body}, baseErr
		}
		return newHTTPResp(http.StatusOK, "ok"), nil
	})
	var events []Event
	tr := mustNewTransport(t, Config{
		Endpoints:      testEndpoints(u1, u2),
		Base:           base,
		AttemptTimeout: time.Second,
		OnEvent: func(_ context.Context, e Event) {
			events = append(events, e)
		},
	})
	req, err := http.NewRequest(http.MethodGet, u1, nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := tr.RoundTrip(req)
	if err != nil || resp == nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("response: %#v err=%v", resp, err)
	}
	defer resp.Body.Close()
	if !body.Closed() || firstCtx.Err() != context.Canceled {
		t.Fatalf("invalid response not closed: closed=%v ctx=%v", body.Closed(), firstCtx.Err())
	}
	if len(events) != 3 || events[0].StatusCode != 0 || !errors.Is(events[0].Err, baseErr) ||
		errors.Is(events[0].Err, ErrAttemptTimeout) {
		t.Fatalf("normalization event = %#v", events)
	}
}

func TestRoundTrip_AttemptTimeout_BasePanicClosesRetainedAndAttemptContext(t *testing.T) {
	u1, u2 := "https://primary.test/rpc", "https://backup.test/rpc"
	retainedBody := newTrackingBody("503")
	var retainedCtx, panicCtx context.Context
	const panicValue = "base transport panic"
	tr := mustNewTransport(t, Config{
		Endpoints: testEndpoints(u1, u2),
		Base: roundTripperFunc(func(req *http.Request) (*http.Response, error) {
			if req.URL.String() == u1 {
				retainedCtx = req.Context()
				return &http.Response{StatusCode: http.StatusServiceUnavailable, Body: retainedBody}, nil
			}
			panicCtx = req.Context()
			panic(panicValue)
		}),
		AttemptTimeout: time.Second,
	})
	req, err := http.NewRequest(http.MethodGet, u1, nil)
	if err != nil {
		t.Fatal(err)
	}
	func() {
		defer func() {
			if got := recover(); got != panicValue {
				t.Errorf("panic = %v, want %v", got, panicValue)
			}
		}()
		_, _ = tr.RoundTrip(req)
	}()
	if !retainedBody.Closed() || retainedCtx.Err() != context.Canceled || panicCtx.Err() != context.Canceled {
		t.Fatalf("panic cleanup: retainedClosed=%v retainedCtx=%v panicCtx=%v", retainedBody.Closed(), retainedCtx.Err(), panicCtx.Err())
	}
}

func TestRoundTrip_AttemptTimeout_EventDurationMeasuresOnlyBaseCall(t *testing.T) {
	u1 := "https://primary.test/rpc"
	clock := time.Unix(1234, 0)
	var events []Event
	tr := mustNewTransport(t, Config{
		Endpoints: testEndpoints(u1),
		Base: roundTripperFunc(func(_ *http.Request) (*http.Response, error) {
			clock = clock.Add(275 * time.Millisecond)
			return newHTTPResp(http.StatusOK, "body"), nil
		}),
		AttemptTimeout: time.Second,
		OnEvent: func(_ context.Context, e Event) {
			events = append(events, e)
		},
	})
	tr.now = func() time.Time { return clock }
	req, err := http.NewRequest(http.MethodGet, u1, nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := tr.RoundTrip(req)
	if err != nil || resp == nil {
		t.Fatalf("response: %#v err=%v", resp, err)
	}
	defer resp.Body.Close()
	if len(events) != 2 || events[0].Duration != 275*time.Millisecond || events[1].Duration != 0 {
		t.Fatalf("unexpected event durations = %#v", events)
	}
}

func TestRoundTrip_AttemptTimeout_DisabledPreservesBaseRequestAndBody(t *testing.T) {
	url := "https://primary.test/rpc"
	body := newTrackingBody("ok")
	var baseCtx context.Context
	tr := mustNewTransport(t, Config{
		Endpoints: testEndpoints(url),
		Base: roundTripperFunc(func(req *http.Request) (*http.Response, error) {
			baseCtx = req.Context()
			return &http.Response{StatusCode: http.StatusOK, Body: body}, nil
		}),
		// AttemptTimeout intentionally left at its zero value.
	})
	parent := context.Background()
	req, err := http.NewRequestWithContext(parent, http.MethodGet, url, nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := tr.RoundTrip(req)
	if err != nil || resp == nil || resp.Body != body || baseCtx != parent {
		t.Fatalf("disabled behavior changed: resp=%#v err=%v bodySame=%v ctxSame=%v", resp, err, resp != nil && resp.Body == body, baseCtx == parent)
	}
	resp.Body.Close()
}

func TestRoundTrip_AttemptTimeout_ExhaustedAttemptsPreserveErrorIdentity(t *testing.T) {
	u1, u2 := "https://primary.test/rpc", "https://backup.test/rpc"
	tr := mustNewTransport(t, Config{
		Endpoints: testEndpoints(u1, u2),
		Base: roundTripperFunc(func(req *http.Request) (*http.Response, error) {
			<-req.Context().Done()
			return nil, req.Context().Err()
		}),
		AttemptTimeout: 30 * time.Millisecond,
	})
	req, err := http.NewRequest(http.MethodGet, u1, nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := tr.RoundTrip(req)
	if resp != nil || !errors.Is(err, ErrAttemptTimeout) || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("exhausted error: resp=%#v err=%v", resp, err)
	}
	var fe *FailoverError
	if !errors.As(err, &fe) || len(fe.Attempts) != 2 {
		t.Fatalf("failover error: %v", err)
	}
	for _, attempt := range fe.Attempts {
		if !errors.Is(attempt.Err, ErrAttemptTimeout) {
			t.Fatalf("missing timeout identity: %#v", attempt)
		}
	}
}

func TestRoundTrip_AttemptTimeout_DiscardsLateBaseResponseBeforeFailover(t *testing.T) {
	u1, u2 := "https://primary.test/rpc", "https://backup.test/rpc"
	lateBody := newTrackingBody("too late")
	var events []Event
	tr := mustNewTransport(t, Config{
		Endpoints: testEndpoints(u1, u2),
		Base: roundTripperFunc(func(req *http.Request) (*http.Response, error) {
			if req.URL.String() == u1 {
				<-req.Context().Done()
				return &http.Response{StatusCode: http.StatusOK, Body: lateBody}, nil
			}
			return newHTTPResp(http.StatusOK, "backup"), nil
		}),
		AttemptTimeout: 30 * time.Millisecond,
		OnEvent: func(_ context.Context, e Event) {
			events = append(events, e)
		},
	})
	req, err := http.NewRequest(http.MethodGet, u1, nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := tr.RoundTrip(req)
	if err != nil || resp == nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("expected backup response: resp=%#v err=%v", resp, err)
	}
	defer resp.Body.Close()
	content, err := io.ReadAll(resp.Body)
	if err != nil || string(content) != "backup" || !lateBody.Closed() {
		t.Fatalf("late response leaked or returned: content=%q err=%v lateClosed=%v", content, err, lateBody.Closed())
	}
	if len(events) != 3 || events[0].StatusCode != 0 || !errors.Is(events[0].Err, ErrAttemptTimeout) {
		t.Fatalf("late HTTP 200 incorrectly accepted: events=%#v", events)
	}
}

func TestRoundTrip_AttemptTimeout_EventPanicClosesCurrentAndRetainedBodies(t *testing.T) {
	u1, u2 := "https://primary.test/rpc", "https://backup.test/rpc"
	const panicValue = "attempt event panic"
	retainedBody := newTrackingBody("503")
	currentBody := newTrackingBody("200")
	var retainedCtx, currentCtx context.Context
	tr := mustNewTransport(t, Config{
		Endpoints: testEndpoints(u1, u2),
		Base: roundTripperFunc(func(req *http.Request) (*http.Response, error) {
			if req.URL.String() == u1 {
				retainedCtx = req.Context()
				return &http.Response{StatusCode: http.StatusServiceUnavailable, Body: retainedBody}, nil
			}
			currentCtx = req.Context()
			return &http.Response{StatusCode: http.StatusOK, Body: currentBody}, nil
		}),
		AttemptTimeout: time.Second,
		OnEvent: func(_ context.Context, e Event) {
			if e.Kind == EventAttempt && e.Attempt == 2 {
				panic(panicValue)
			}
		},
	})
	req, err := http.NewRequest(http.MethodGet, u1, nil)
	if err != nil {
		t.Fatal(err)
	}
	func() {
		defer func() {
			if got := recover(); got != panicValue {
				t.Errorf("panic = %v, want %v", got, panicValue)
			}
		}()
		_, _ = tr.RoundTrip(req)
	}()
	if !retainedBody.Closed() || !currentBody.Closed() ||
		retainedCtx.Err() != context.Canceled || currentCtx.Err() != context.Canceled {
		t.Fatalf("panic cleanup: retainedClosed=%v currentClosed=%v retainedCtx=%v currentCtx=%v",
			retainedBody.Closed(), currentBody.Closed(), retainedCtx.Err(), currentCtx.Err())
	}
}

// These tests exercise the timed helper through the real failover loop. They
// deliberately avoid depending on exact scheduler ordering at a timeout edge.
func TestRoundTrip_AttemptTimeout_RealHTTPStreamingBodyRemainsReadable(t *testing.T) {
	bodyRelease := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.(http.Flusher).Flush()
		<-bodyRelease
		_, _ = io.WriteString(w, "streamed after the attempt limit")
	}))
	defer server.Close()
	defer func() {
		select {
		case <-bodyRelease:
		default:
			close(bodyRelease)
		}
	}()

	const limit = 200 * time.Millisecond
	tr := mustNewTransport(t, Config{
		Endpoints:      []Endpoint{{ID: "server", URL: server.URL}},
		Base:           http.DefaultTransport,
		AttemptTimeout: limit,
	})
	req, err := http.NewRequest(http.MethodGet, server.URL, nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := tr.RoundTrip(req)
	if err != nil {
		t.Fatalf("headers should arrive before timeout: %v", err)
	}
	defer resp.Body.Close()
	// Headers were delivered; the attempt timeout must no longer govern body reads.
	time.Sleep(260 * time.Millisecond)
	close(bodyRelease)
	data, err := io.ReadAll(resp.Body)
	if err != nil || string(data) != "streamed after the attempt limit" {
		t.Fatalf("streaming body: data=%q err=%v", data, err)
	}
}

func TestRoundTrip_AttemptTimeout_PostReplayAfterTimeout(t *testing.T) {
	const payload = "replayed request body"
	u1, u2 := "https://first.test/rpc", "https://second.test/rpc"
	original := newTrackingBody(payload)
	var replayed *trackingBody
	var firstCtx, secondCtx context.Context
	var observed []string
	var getBodyCalls int
	tr := mustNewTransport(t, Config{
		Endpoints:      testEndpoints(u1, u2),
		AttemptTimeout: 15 * time.Millisecond,
		Base: roundTripperFunc(func(req *http.Request) (*http.Response, error) {
			data, err := io.ReadAll(req.Body)
			if err != nil {
				return nil, err
			}
			observed = append(observed, string(data))
			_ = req.Body.Close() // A Base transport owns each handed-off body.
			switch req.URL.String() {
			case u1:
				firstCtx = req.Context()
				<-req.Context().Done()
				return nil, req.Context().Err()
			case u2:
				secondCtx = req.Context()
				return newHTTPResp(http.StatusOK, "success"), nil
			default:
				return nil, errors.New("unexpected endpoint")
			}
		}),
	})
	req, err := http.NewRequest(http.MethodPost, u1, original)
	if err != nil {
		t.Fatal(err)
	}
	req.GetBody = func() (io.ReadCloser, error) {
		getBodyCalls++
		replayed = newTrackingBody(payload)
		return replayed, nil
	}
	req = req.WithContext(WithFailoverAllowed(req.Context()))
	resp, err := tr.RoundTrip(req)
	if err != nil || resp == nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("replay result: response=%#v err=%v", resp, err)
	}
	if len(observed) != 2 || observed[0] != payload || observed[1] != payload || getBodyCalls != 1 {
		t.Fatalf("body replay: observed=%v GetBody calls=%d", observed, getBodyCalls)
	}
	if !original.Closed() || replayed == nil || !replayed.Closed() {
		t.Fatal("Base did not close the original and replayed request bodies")
	}
	if firstCtx.Err() != context.Canceled || secondCtx.Err() != nil {
		t.Fatalf("attempt contexts: first=%v second=%v", firstCtx.Err(), secondCtx.Err())
	}
	_ = resp.Body.Close()
	if secondCtx.Err() != context.Canceled {
		t.Fatal("closing the returned response did not release second context")
	}
}

type countCloseBody struct {
	io.Reader
	closes atomic.Int32
}

func (b *countCloseBody) Close() error {
	b.closes.Add(1)
	return nil
}

func TestRoundTrip_AttemptTimeout_ReplacedResponsesCloseOnce(t *testing.T) {
	u1, u2, u3 := "https://first.test", "https://second.test", "https://third.test"
	bodies := []*countCloseBody{
		{Reader: strings.NewReader("503")},
		{Reader: strings.NewReader("504")},
		{Reader: strings.NewReader("200")},
	}
	var contexts [3]context.Context
	var next int
	tr := mustNewTransport(t, Config{
		Endpoints:      testEndpoints(u1, u2, u3),
		AttemptTimeout: time.Second,
		Base: roundTripperFunc(func(req *http.Request) (*http.Response, error) {
			i := next
			next++
			contexts[i] = req.Context()
			status := []int{http.StatusServiceUnavailable, http.StatusGatewayTimeout, http.StatusOK}[i]
			return &http.Response{StatusCode: status, Body: bodies[i]}, nil
		}),
	})
	req, err := http.NewRequest(http.MethodGet, u1, nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := tr.RoundTrip(req)
	if err != nil || resp == nil || resp.StatusCode != http.StatusOK || next != 3 {
		t.Fatalf("result: resp=%#v err=%v calls=%d", resp, err, next)
	}
	if bodies[0].closes.Load() != 1 || bodies[1].closes.Load() != 1 || bodies[2].closes.Load() != 0 {
		t.Fatalf("close counts before caller close: %d %d %d", bodies[0].closes.Load(), bodies[1].closes.Load(), bodies[2].closes.Load())
	}
	if contexts[0].Err() != context.Canceled || contexts[1].Err() != context.Canceled || contexts[2].Err() != nil {
		t.Fatalf("context states: %v %v %v", contexts[0].Err(), contexts[1].Err(), contexts[2].Err())
	}
	_ = resp.Body.Close()
	if bodies[2].closes.Load() != 1 || contexts[2].Err() != context.Canceled {
		t.Fatalf("returned response not closed exactly once: count=%d ctx=%v", bodies[2].closes.Load(), contexts[2].Err())
	}
}

func TestRoundTrip_AttemptTimeout_LogicalDeadlineWinsAndClosesRetained(t *testing.T) {
	u1, u2 := "https://first.test", "https://second.test"
	body := newTrackingBody("503")
	var retainedCtx context.Context
	var calls int
	var events []Event
	tr := mustNewTransport(t, Config{
		Endpoints:      testEndpoints(u1, u2),
		AttemptTimeout: time.Second,
		Base: roundTripperFunc(func(req *http.Request) (*http.Response, error) {
			calls++
			if req.URL.String() == u1 {
				retainedCtx = req.Context()
				return &http.Response{StatusCode: http.StatusServiceUnavailable, Body: body}, nil
			}
			<-req.Context().Done()
			return nil, req.Context().Err()
		}),
		OnEvent: func(_ context.Context, e Event) { events = append(events, e) },
	})
	tr.now = time.Now
	parent, cancel := context.WithTimeout(context.Background(), 35*time.Millisecond)
	defer cancel()
	req, err := http.NewRequestWithContext(parent, http.MethodGet, u1, nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := tr.RoundTrip(req)
	if resp != nil || !errors.Is(err, context.DeadlineExceeded) || errors.Is(err, ErrAttemptTimeout) {
		t.Fatalf("logical deadline: resp=%#v err=%v", resp, err)
	}
	if calls != 2 || !body.Closed() || retainedCtx.Err() != context.DeadlineExceeded {
		t.Fatalf("logical deadline cleanup: calls=%d retainedClosed=%v retainedCtx=%v", calls, body.Closed(), retainedCtx.Err())
	}
	if len(events) != 3 || events[2].Kind != EventResult || !errors.Is(events[2].Err, context.DeadlineExceeded) {
		t.Fatalf("final result event: %#v", events)
	}
	tr.cooldown.mu.Lock()
	failures := tr.cooldown.consec[1]
	tr.cooldown.mu.Unlock()
	if failures != 0 {
		t.Fatalf("logical deadline wrongly counted as cooldown failure: %d", failures)
	}
}

func TestRoundTrip_AttemptTimeout_NoOverlappingBaseCalls(t *testing.T) {
	u1, u2 := "https://first.test", "https://second.test"
	entered := make(chan context.Context, 1)
	release := make(chan struct{})
	var once sync.Once
	defer once.Do(func() { close(release) })
	var active atomic.Int32
	var overlap atomic.Bool
	tr := mustNewTransport(t, Config{
		Endpoints:      testEndpoints(u1, u2),
		AttemptTimeout: 15 * time.Millisecond,
		Base: roundTripperFunc(func(req *http.Request) (*http.Response, error) {
			if active.Add(1) != 1 {
				overlap.Store(true)
			}
			defer active.Add(-1)
			if req.URL.String() == u1 {
				entered <- req.Context()
				<-release // deliberately ignore cancellation
				return nil, io.EOF
			}
			return newHTTPResp(http.StatusOK, "backup"), nil
		}),
	})
	type result struct {
		resp *http.Response
		err  error
	}
	done := make(chan result, 1)
	go func() {
		req, err := http.NewRequest(http.MethodGet, u1, nil)
		if err != nil {
			done <- result{err: err}
			return
		}
		resp, err := tr.RoundTrip(req)
		done <- result{resp, err}
	}()
	var firstContext context.Context
	select {
	case firstContext = <-entered:
	case <-time.After(time.Second):
		t.Fatal("first Base.RoundTrip did not start")
	}
	select {
	case <-firstContext.Done():
	case <-time.After(time.Second):
		t.Fatal("attempt timeout did not cancel first Base request")
	}
	if active.Load() != 1 || overlap.Load() {
		t.Fatalf("attempt continued before Base returned: active=%d overlap=%v", active.Load(), overlap.Load())
	}
	once.Do(func() { close(release) })
	select {
	case got := <-done:
		if got.err != nil || got.resp == nil || overlap.Load() {
			t.Fatalf("non-overlap result: resp=%#v err=%v overlap=%v", got.resp, got.err, overlap.Load())
		}
		_ = got.resp.Body.Close()
	case <-time.After(time.Second):
		t.Fatal("RoundTrip did not complete after releasing first Base")
	}
}

func TestRoundTrip_AttemptTimeout_ConcurrentRequestsIndependent(t *testing.T) {
	const requests = 32
	const limit = 10 * time.Millisecond
	u1, u2 := "https://first.test", "https://second.test"
	var primary, backup atomic.Int32
	tr := mustNewTransport(t, Config{
		Endpoints:      testEndpoints(u1, u2),
		AttemptTimeout: limit,
		Cooldown:       CooldownConfig{Disabled: true},
		Base: roundTripperFunc(func(req *http.Request) (*http.Response, error) {
			if req.URL.String() == u1 {
				primary.Add(1)
				<-req.Context().Done()
				return nil, req.Context().Err()
			}
			backup.Add(1)
			return newHTTPResp(http.StatusOK, "ok"), nil
		}),
	})
	var wg sync.WaitGroup
	errCh := make(chan error, requests)
	for i := 0; i < requests; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			req, err := http.NewRequest(http.MethodGet, u1, nil)
			if err != nil {
				errCh <- err
				return
			}
			resp, err := tr.RoundTrip(req)
			if err != nil {
				errCh <- err
				return
			}
			if resp == nil || resp.StatusCode != http.StatusOK {
				errCh <- errors.New("missing successful backup response")
				return
			}
			_, err = io.ReadAll(resp.Body)
			_ = resp.Body.Close()
			if err != nil {
				errCh <- err
			}
		}()
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Error(err)
	}
	if int(primary.Load()) != requests || int(backup.Load()) != requests {
		t.Fatalf("attempt counts: primary=%d backup=%d want=%d", primary.Load(), backup.Load(), requests)
	}
}

type panicCloseBody struct{}

func (panicCloseBody) Read([]byte) (int, error) { return 0, io.EOF }
func (panicCloseBody) Close() error             { panic("body close panic") }

func TestTimedAttempt_BodyClosePanicStillCancelsContext(t *testing.T) {
	var attemptCtx context.Context
	tr := newTimedTestTransport(t, time.Second, roundTripperFunc(func(req *http.Request) (*http.Response, error) {
		attemptCtx = req.Context()
		return &http.Response{StatusCode: http.StatusOK, Body: panicCloseBody{}}, nil
	}))
	var resp *http.Response
	if _, err := tr.roundTripWithAttemptTimeout(newTimedTestRequest(t), &resp); err != nil {
		t.Fatal(err)
	}
	func() {
		defer func() {
			if got := recover(); got != "body close panic" {
				t.Errorf("Close panic = %v", got)
			}
		}()
		_ = resp.Body.Close()
	}()
	if attemptCtx.Err() != context.Canceled {
		t.Fatalf("body-close panic leaked attempt context: %v", attemptCtx.Err())
	}
}

func TestTimedAttempt_CompletionTimeoutRacePreservesOwnership(t *testing.T) {
	const limit = 2 * time.Millisecond
	for i := 0; i < 80; i++ {
		body := &countCloseBody{Reader: strings.NewReader("still readable")}
		var attemptCtx context.Context
		tr := newTimedTestTransport(t, limit, roundTripperFunc(func(req *http.Request) (*http.Response, error) {
			attemptCtx = req.Context()
			// Compete with the timeout; either outcome is valid at the edge.
			time.Sleep(limit)
			return &http.Response{StatusCode: http.StatusOK, Body: body}, nil
		}))
		var resp *http.Response
		_, err := tr.roundTripWithAttemptTimeout(newTimedTestRequest(t), &resp)
		if errors.Is(err, ErrAttemptTimeout) {
			if resp != nil || body.closes.Load() != 1 || attemptCtx.Err() != context.Canceled {
				t.Fatalf("iteration %d: timeout leaked response: resp=%#v closes=%d ctx=%v", i, resp, body.closes.Load(), attemptCtx.Err())
			}
			continue
		}
		if err != nil || resp == nil {
			t.Fatalf("iteration %d: unexpected response=%#v err=%v", i, resp, err)
		}
		// Even after the old limit, the timer cannot invalidate an accepted body.
		time.Sleep(2 * limit)
		if attemptCtx.Err() != nil || body.closes.Load() != 0 {
			t.Fatalf("iteration %d: successful attempt canceled before body close: ctx=%v closes=%d", i, attemptCtx.Err(), body.closes.Load())
		}
		data, readErr := io.ReadAll(resp.Body)
		if readErr != nil || string(data) != "still readable" {
			t.Fatalf("iteration %d: body=%q err=%v", i, data, readErr)
		}
		_ = resp.Body.Close()
		if body.closes.Load() != 1 || attemptCtx.Err() != context.Canceled {
			t.Fatalf("iteration %d: successful body cleanup: closes=%d ctx=%v", i, body.closes.Load(), attemptCtx.Err())
		}
	}
}
