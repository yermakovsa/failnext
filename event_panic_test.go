package rcpx

import (
	"context"
	"errors"
	"io"
	"net/http"
	"testing"
	"time"
)

func TestOnEventPanic_EventAttemptClosesCurrentAndRetainedResponses(t *testing.T) {
	u1 := "https://u1.test/rpc"
	u2 := "https://u2.test/rpc"
	retainedBody := newTrackingBody("service unavailable")
	currentBody := newTrackingBody("ok")
	panicValue := "event-attempt panic"

	base := &scriptRT{
		results: map[string][]rtResult{
			u1: {{resp: &http.Response{StatusCode: http.StatusServiceUnavailable, Body: retainedBody}}},
			u2: {{resp: &http.Response{StatusCode: http.StatusOK, Body: currentBody}}},
		},
	}
	rt := mustNewTransport(t, Config{
		Endpoints: testEndpoints(u1, u2),
		Base:      base,
		OnEvent: func(_ context.Context, event Event) {
			if event.Kind == EventAttempt && event.Attempt == 2 {
				panic(panicValue)
			}
		},
	})

	assertPanicValue(t, panicValue, func() {
		resp, err := rt.RoundTrip(newTestGETRequest(t, u1))
		if resp != nil {
			resp.Body.Close()
		}
		if err != nil {
			t.Fatalf("RoundTrip returned instead of panicking: %v", err)
		}
	})

	if !retainedBody.Closed() {
		t.Fatal("expected retained response body to close during panic unwinding")
	}
	if !currentBody.Closed() {
		t.Fatal("expected current response body to close during panic unwinding")
	}
	assertCalls(t, base, u1, u2)
}

func TestOnEventPanic_EventCooldownSkipClosesOriginalBody(t *testing.T) {
	u1 := "https://u1.test/rpc"
	fixedNow := time.Unix(1400, 0)
	body := newTrackingBody("one-shot")
	panicValue := "cooldown-skip panic"

	base := &scriptRT{results: map[string][]rtResult{}}
	rt := mustNewTransport(t, Config{
		Endpoints: testEndpoints(u1),
		Base:      base,
		OnEvent: func(_ context.Context, event Event) {
			if event.Kind == EventCooldownSkip {
				panic(panicValue)
			}
		},
	})
	rt.now = func() time.Time { return fixedNow }

	rt.cooldown.mu.Lock()
	rt.cooldown.coolingTo[0] = fixedNow.Add(time.Hour)
	rt.cooldown.mu.Unlock()

	req, err := http.NewRequest(http.MethodPost, u1, body)
	if err != nil {
		t.Fatalf("http.NewRequest: %v", err)
	}

	assertPanicValue(t, panicValue, func() {
		resp, err := rt.RoundTrip(req)
		if resp != nil {
			resp.Body.Close()
		}
		if err != nil {
			t.Fatalf("RoundTrip returned instead of panicking: %v", err)
		}
	})

	if !body.Closed() {
		t.Fatal("expected original request body to close during panic unwinding")
	}
	assertCalls(t, base)
}

func TestOnEventPanic_EventReplayErrorClosesFailedReplayAndRetainedResponse(t *testing.T) {
	u1 := "https://u1.test/rpc"
	u2 := "https://u2.test/rpc"
	original := newTrackingBody("original")
	fresh := newTrackingBody("fresh")
	retainedBody := newTrackingBody("service unavailable")
	replayErr := errors.New("replay failed")
	panicValue := "replay-error panic"

	var calls []string
	base := roundTripperFunc(func(req *http.Request) (*http.Response, error) {
		calls = append(calls, req.URL.String())
		if err := req.Body.Close(); err != nil {
			t.Fatalf("close original request body: %v", err)
		}
		if req.URL.String() != u1 {
			return nil, errors.New("unexpected physical attempt: " + req.URL.String())
		}
		return &http.Response{
			StatusCode: http.StatusServiceUnavailable,
			Body:       retainedBody,
			Header:     make(http.Header),
			Request:    req,
		}, nil
	})
	rt := mustNewTransport(t, Config{
		Endpoints: testEndpoints(u1, u2),
		Base:      base,
		OnEvent: func(_ context.Context, event Event) {
			if event.Kind == EventReplayError {
				panic(panicValue)
			}
		},
	})

	req, err := http.NewRequest(http.MethodPost, u1, original)
	if err != nil {
		t.Fatalf("http.NewRequest: %v", err)
	}
	req.GetBody = func() (io.ReadCloser, error) {
		return fresh, replayErr
	}
	req = req.WithContext(WithFailoverAllowed(req.Context()))

	assertPanicValue(t, panicValue, func() {
		resp, err := rt.RoundTrip(req)
		if resp != nil {
			resp.Body.Close()
		}
		if err != nil {
			t.Fatalf("RoundTrip returned instead of panicking: %v", err)
		}
	})

	if !original.Closed() {
		t.Fatal("expected original request body to be closed by the base transport")
	}
	if !fresh.Closed() {
		t.Fatal("expected body returned with GetBody error to be closed before observer panic")
	}
	if !retainedBody.Closed() {
		t.Fatal("expected retained response body to close during panic unwinding")
	}
	if len(calls) != 1 || calls[0] != u1 {
		t.Fatalf("unexpected physical attempts: got=%v want=%v", calls, []string{u1})
	}
}

func TestOnEventPanic_HTTPEventResultClosesImmediateResponse(t *testing.T) {
	u1 := "https://u1.test/rpc"
	responseBody := newTrackingBody("ok")
	panicValue := "http-result panic"

	base := &scriptRT{
		results: map[string][]rtResult{
			u1: {{resp: &http.Response{StatusCode: http.StatusOK, Body: responseBody}}},
		},
	}
	rt := mustNewTransport(t, Config{
		Endpoints: testEndpoints(u1),
		Base:      base,
		OnEvent: func(_ context.Context, event Event) {
			if event.Kind == EventResult {
				panic(panicValue)
			}
		},
	})

	assertPanicValue(t, panicValue, func() {
		resp, err := rt.RoundTrip(newTestGETRequest(t, u1))
		if resp != nil {
			resp.Body.Close()
		}
		if err != nil {
			t.Fatalf("RoundTrip returned instead of panicking: %v", err)
		}
	})

	if !responseBody.Closed() {
		t.Fatal("expected final response body to close when EventResult panics")
	}
	assertCalls(t, base, u1)
}

func TestOnEventPanic_HTTPEventResultClosesRetainedResponse(t *testing.T) {
	u1 := "https://u1.test/rpc"
	u2 := "https://u2.test/rpc"
	retainedBody := newTrackingBody("service unavailable")
	panicValue := "retained-result panic"

	base := &scriptRT{
		results: map[string][]rtResult{
			u1: {{resp: &http.Response{StatusCode: http.StatusServiceUnavailable, Body: retainedBody}}},
			u2: {{err: io.EOF}},
		},
	}
	rt := mustNewTransport(t, Config{
		Endpoints: testEndpoints(u1, u2),
		Base:      base,
		OnEvent: func(_ context.Context, event Event) {
			if event.Kind == EventResult {
				panic(panicValue)
			}
		},
	})

	assertPanicValue(t, panicValue, func() {
		resp, err := rt.RoundTrip(newTestGETRequest(t, u1))
		if resp != nil {
			resp.Body.Close()
		}
		if err != nil {
			t.Fatalf("RoundTrip returned instead of panicking: %v", err)
		}
	})

	if !retainedBody.Closed() {
		t.Fatal("expected retained final response body to close when EventResult panics")
	}
	assertCalls(t, base, u1, u2)
}

func TestOnEventPanic_ErrorEventResultClosesOriginalBody(t *testing.T) {
	u1 := "https://u1.test/rpc"
	body := newTrackingBody("one-shot")
	panicValue := "error-result panic"

	base := &scriptRT{results: map[string][]rtResult{}}
	rt := mustNewTransport(t, Config{
		Endpoints: testEndpoints(u1),
		Base:      base,
		Eligible: func(EndpointID) bool {
			return false
		},
		OnEvent: func(_ context.Context, event Event) {
			if event.Kind == EventResult {
				panic(panicValue)
			}
		},
	})

	req, err := http.NewRequest(http.MethodPost, u1, body)
	if err != nil {
		t.Fatalf("http.NewRequest: %v", err)
	}

	assertPanicValue(t, panicValue, func() {
		resp, err := rt.RoundTrip(req)
		if resp != nil {
			resp.Body.Close()
		}
		if err != nil {
			t.Fatalf("RoundTrip returned instead of panicking: %v", err)
		}
	})

	if !body.Closed() {
		t.Fatal("expected original request body to close when error EventResult panics")
	}
	assertCalls(t, base)
}

func assertPanicValue(t *testing.T, want any, fn func()) {
	t.Helper()

	var got any
	func() {
		defer func() {
			got = recover()
		}()
		fn()
	}()

	if got != want {
		t.Fatalf("unexpected panic: got=%v want=%v", got, want)
	}
}
