package failnext

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
)

func newTimedTestTransport(t *testing.T, timeout time.Duration, base http.RoundTripper) *Transport {
	t.Helper()
	return mustNewTransport(t, Config{
		Endpoints:      testEndpoints("http://upstream.test/rpc"),
		Base:           base,
		AttemptTimeout: timeout,
	})
}

func newTimedTestRequest(t *testing.T) *http.Request {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, "http://upstream.test/rpc", nil)
	if err != nil {
		t.Fatal(err)
	}
	return req
}

func TestTimedAttempt_TimeoutCancelsOnlyAttempt(t *testing.T) {
	parent, cancelParent := context.WithCancel(context.Background())
	defer cancelParent()
	var attemptCtx context.Context
	tr := newTimedTestTransport(t, 5*time.Millisecond, roundTripperFunc(func(req *http.Request) (*http.Response, error) {
		attemptCtx = req.Context()
		select {
		case <-req.Context().Done():
			return nil, req.Context().Err()
		case <-time.After(time.Second):
			return nil, errors.New("attempt did not observe cancellation")
		}
	}))

	var resp *http.Response
	_, err := tr.roundTripWithAttemptTimeout(newTimedTestRequest(t).WithContext(parent), &resp)
	if resp != nil || !errors.Is(err, ErrAttemptTimeout) || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("timed attempt: resp=%#v err=%v", resp, err)
	}
	if attemptCtx == nil || !errors.Is(attemptCtx.Err(), context.Canceled) {
		t.Fatalf("attempt context should be canceled: %v", attemptCtx)
	}
	if parent.Err() != nil {
		t.Fatalf("parent context must remain active: %v", parent.Err())
	}
}

func TestTimedAttempt_CompletionStopsTimeoutAndBodyCloseCancels(t *testing.T) {
	const timeout = 15 * time.Millisecond
	var attemptCtx context.Context
	tr := newTimedTestTransport(t, timeout, roundTripperFunc(func(req *http.Request) (*http.Response, error) {
		attemptCtx = req.Context()
		return newHTTPResp(http.StatusOK, "body is still readable"), nil
	}))
	var resp *http.Response
	if _, err := tr.roundTripWithAttemptTimeout(newTimedTestRequest(t), &resp); err != nil {
		t.Fatal(err)
	}
	if resp == nil {
		t.Fatal("expected response")
	}
	t.Cleanup(func() { resp.Body.Close() })
	<-time.After(3 * timeout)
	if err := attemptCtx.Err(); err != nil {
		t.Fatalf("completed attempt canceled by its old timer: %v", err)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil || string(body) != "body is still readable" {
		t.Fatalf("body read: got %q err=%v", body, err)
	}
	if err := resp.Body.Close(); err != nil {
		t.Fatal(err)
	}
	if !errors.Is(attemptCtx.Err(), context.Canceled) {
		t.Fatalf("body close must release context: %v", attemptCtx.Err())
	}
}

func TestTimedAttempt_LateBaseResponseIsDiscarded(t *testing.T) {
	lateBody := newTrackingBody("too late")
	var attemptCtx context.Context
	tr := newTimedTestTransport(t, 5*time.Millisecond, roundTripperFunc(func(req *http.Request) (*http.Response, error) {
		attemptCtx = req.Context()
		<-req.Context().Done()
		return &http.Response{StatusCode: http.StatusOK, Body: lateBody}, nil
	}))
	var resp *http.Response
	_, err := tr.roundTripWithAttemptTimeout(newTimedTestRequest(t), &resp)
	if resp != nil || !errors.Is(err, ErrAttemptTimeout) {
		t.Fatalf("late response: resp=%#v err=%v", resp, err)
	}
	if !lateBody.Closed() || !errors.Is(attemptCtx.Err(), context.Canceled) {
		t.Fatal("late response body and context must be cleaned up")
	}
}

func TestTimedAttempt_WaitsForNonCooperativeBaseToReturn(t *testing.T) {
	started := make(chan context.Context, 1)
	release := make(chan struct{})
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
	}()

	tr := newTimedTestTransport(t, 5*time.Millisecond, roundTripperFunc(func(req *http.Request) (*http.Response, error) {
		started <- req.Context()
		<-release // Deliberately ignore request cancellation.
		return nil, io.EOF
	}))
	type result struct {
		resp *http.Response
		err  error
	}
	done := make(chan result, 1)
	req := newTimedTestRequest(t)
	go func() {
		var resp *http.Response
		_, err := tr.roundTripWithAttemptTimeout(req, &resp)
		done <- result{resp: resp, err: err}
	}()

	var attemptCtx context.Context
	select {
	case attemptCtx = <-started:
	case <-time.After(time.Second):
		t.Fatal("Base.RoundTrip did not start")
	}
	select {
	case <-attemptCtx.Done():
	case <-time.After(time.Second):
		t.Fatal("attempt timeout did not request cancellation")
	}
	select {
	case <-done:
		t.Fatal("attempt helper returned before Base.RoundTrip completed")
	default:
	}
	close(release)
	select {
	case got := <-done:
		if got.resp != nil || !errors.Is(got.err, ErrAttemptTimeout) {
			t.Fatalf("late base result: resp=%#v err=%v", got.resp, got.err)
		}
	case <-time.After(time.Second):
		t.Fatal("attempt helper did not return after Base.RoundTrip completed")
	}
}

func TestTimedAttempt_NormalBaseErrorCancelsContext(t *testing.T) {
	baseErr := io.EOF
	var attemptCtx context.Context
	tr := newTimedTestTransport(t, time.Second, roundTripperFunc(func(req *http.Request) (*http.Response, error) {
		attemptCtx = req.Context()
		return nil, baseErr
	}))
	var resp *http.Response
	_, err := tr.roundTripWithAttemptTimeout(newTimedTestRequest(t), &resp)
	if resp != nil || err != baseErr {
		t.Fatalf("base error: resp=%#v err=%v", resp, err)
	}
	if !errors.Is(attemptCtx.Err(), context.Canceled) {
		t.Fatalf("base error should release context: %v", attemptCtx.Err())
	}
}

func TestTimedAttempt_ResponseWithErrorRemainsOwnedUntilNormalization(t *testing.T) {
	baseErr := errors.New("base error")
	body := newTrackingBody("discard")
	var attemptCtx context.Context
	tr := newTimedTestTransport(t, time.Second, roundTripperFunc(func(req *http.Request) (*http.Response, error) {
		attemptCtx = req.Context()
		return &http.Response{StatusCode: http.StatusServiceUnavailable, Body: body}, baseErr
	}))
	var resp *http.Response
	_, err := tr.roundTripWithAttemptTimeout(newTimedTestRequest(t), &resp)
	if resp == nil || err != baseErr || attemptCtx.Err() != nil {
		t.Fatalf("before normalization: resp=%#v err=%v ctxerr=%v", resp, err, attemptCtx.Err())
	}
	resp, err = normalizeBaseRoundTrip(resp, err, "upstream")
	if resp != nil || err != baseErr || !body.Closed() {
		t.Fatalf("normalization: resp=%#v err=%v bodyClosed=%v", resp, err, body.Closed())
	}
	if !errors.Is(attemptCtx.Err(), context.Canceled) {
		t.Fatal("normalization must cancel context by closing body")
	}
}

func TestTimedAttempt_BodylessResponseCancelsImmediately(t *testing.T) {
	for _, body := range []io.ReadCloser{nil, http.NoBody} {
		var attemptCtx context.Context
		tr := newTimedTestTransport(t, time.Second, roundTripperFunc(func(req *http.Request) (*http.Response, error) {
			attemptCtx = req.Context()
			return &http.Response{StatusCode: http.StatusNoContent, Body: body}, nil
		}))
		var resp *http.Response
		if _, err := tr.roundTripWithAttemptTimeout(newTimedTestRequest(t), &resp); err != nil {
			t.Fatal(err)
		}
		if resp == nil || resp.Body != body || !errors.Is(attemptCtx.Err(), context.Canceled) {
			t.Fatalf("bodyless response: resp=%#v ctxerr=%v", resp, attemptCtx.Err())
		}
	}
}

type writableTrackingBody struct {
	*trackingBody
	written bytes.Buffer
}

func (b *writableTrackingBody) Write(p []byte) (int, error) { return b.written.Write(p) }

func TestTimedAttempt_PreservesHTTP101BodyWriter(t *testing.T) {
	body := &writableTrackingBody{trackingBody: newTrackingBody("")}
	var attemptCtx context.Context
	tr := newTimedTestTransport(t, time.Second, roundTripperFunc(func(req *http.Request) (*http.Response, error) {
		attemptCtx = req.Context()
		return &http.Response{StatusCode: http.StatusSwitchingProtocols, Body: body}, nil
	}))
	var resp *http.Response
	if _, err := tr.roundTripWithAttemptTimeout(newTimedTestRequest(t), &resp); err != nil {
		t.Fatal(err)
	}
	writer, ok := resp.Body.(io.Writer)
	if !ok {
		t.Fatal("HTTP 101 response body lost io.Writer")
	}
	if _, err := io.Copy(writer, strings.NewReader("upgrade")); err != nil {
		t.Fatal(err)
	}
	if got := body.written.String(); got != "upgrade" {
		t.Fatalf("forwarded write = %q", got)
	}
	if attemptCtx.Err() != nil {
		t.Fatalf("unexpected early cancellation: %v", attemptCtx.Err())
	}
	resp.Body.Close()
	if !body.Closed() || !errors.Is(attemptCtx.Err(), context.Canceled) {
		t.Fatal("HTTP 101 body close did not clean up")
	}
}

func TestTimedAttempt_BasePanicCancelsContext(t *testing.T) {
	panicValue := "base panic"
	var attemptCtx context.Context
	tr := newTimedTestTransport(t, time.Second, roundTripperFunc(func(req *http.Request) (*http.Response, error) {
		attemptCtx = req.Context()
		panic(panicValue)
	}))
	func() {
		defer func() {
			if got := recover(); got != panicValue {
				t.Errorf("panic = %v, want %v", got, panicValue)
			}
		}()
		var resp *http.Response
		_, _ = tr.roundTripWithAttemptTimeout(newTimedTestRequest(t), &resp)
	}()
	if attemptCtx == nil || !errors.Is(attemptCtx.Err(), context.Canceled) {
		t.Fatal("panic left attempt context alive")
	}
}

func TestTimedAttempt_PanicBeforeRegistrationCleansResponse(t *testing.T) {
	body := newTrackingBody("unregistered")
	var attemptCtx context.Context
	tr := newTimedTestTransport(t, time.Second, roundTripperFunc(func(req *http.Request) (*http.Response, error) {
		attemptCtx = req.Context()
		return &http.Response{StatusCode: http.StatusOK, Body: body}, nil
	}))
	func() {
		defer func() {
			if got := recover(); got == nil {
				t.Error("expected panic when registering response without a slot")
			}
		}()
		// Force registration to panic after the timer was stopped.
		_, _ = tr.roundTripWithAttemptTimeout(newTimedTestRequest(t), nil)
	}()
	if !body.Closed() || !errors.Is(attemptCtx.Err(), context.Canceled) {
		t.Fatal("panic before registration left response or context unowned")
	}
}

func TestTimedAttempt_LogicalCancellationIsNotAttemptTimeout(t *testing.T) {
	parent, cancelParent := context.WithCancel(context.Background())
	defer cancelParent()
	tr := newTimedTestTransport(t, time.Second, roundTripperFunc(func(req *http.Request) (*http.Response, error) {
		cancelParent()
		return nil, req.Context().Err()
	}))
	var resp *http.Response
	_, err := tr.roundTripWithAttemptTimeout(newTimedTestRequest(t).WithContext(parent), &resp)
	if resp != nil || !errors.Is(err, context.Canceled) || errors.Is(err, ErrAttemptTimeout) {
		t.Fatalf("logical cancellation: resp=%#v err=%v", resp, err)
	}
}
