package failnext

import (
	"context"
	"io"
	"net/http"
	"time"
)

type attemptTimeoutKey struct{}

// WithAttemptTimeout returns a context that overrides Config.AttemptTimeout for a request.
// Zero disables the per-attempt timeout. Negative durations cause RoundTrip to
// return an error before trying an endpoint. A later override takes precedence.
func WithAttemptTimeout(ctx context.Context, timeout time.Duration) context.Context {
	return context.WithValue(ctx, attemptTimeoutKey{}, timeout)
}

func attemptTimeoutFromContext(ctx context.Context) (time.Duration, bool) {
	timeout, ok := ctx.Value(attemptTimeoutKey{}).(time.Duration)
	return timeout, ok
}

// roundTripWithAttemptTimeout runs one Base.RoundTrip call with a per-attempt timeout.
// The response slot must be covered by the caller's deferred cleanup.
// The returned duration measures Base.RoundTrip only.
func (t *Transport) roundTripWithAttemptTimeout(req *http.Request, timeout time.Duration, response **http.Response) (time.Duration, error) {
	ctx, cancel := context.WithCancel(req.Context())
	timedReq := req.WithContext(ctx)
	timer := time.AfterFunc(timeout, cancel)

	var resp *http.Response
	cleanupPending := true
	defer func() {
		if cleanupPending {
			timer.Stop() // Ignore the result during cleanup.
			cancel()
			closeResponseBody(resp)
		}
	}()

	started := t.now()
	var err error
	resp, err = t.cfg.base.RoundTrip(timedReq)
	duration := t.now().Sub(started)
	if !timer.Stop() {
		// The timer fired, but its callback may still be running.
		// Cancel directly; deferred cleanup closes any response.
		return duration, ErrAttemptTimeout
	}

	if resp == nil || resp.Body == nil || resp.Body == http.NoBody {
		cancel()
	} else {
		// Keep the child context active until the response body is closed.
		resp.Body = withCancelOnClose(resp.Body, cancel)
	}

	// The caller now owns response cleanup.
	*response = resp
	cleanupPending = false
	return duration, err
}

type cancelOnCloseBody struct {
	io.ReadCloser
	cancel context.CancelFunc
}

func (b *cancelOnCloseBody) Close() error {
	defer b.cancel()
	return b.ReadCloser.Close()
}

// Preserve the documented io.Writer capability of HTTP 101 response bodies.
type cancelOnCloseWriterBody struct {
	*cancelOnCloseBody
	writer io.Writer
}

func (b *cancelOnCloseWriterBody) Write(p []byte) (int, error) {
	return b.writer.Write(p)
}

func withCancelOnClose(body io.ReadCloser, cancel context.CancelFunc) io.ReadCloser {
	wrapped := &cancelOnCloseBody{ReadCloser: body, cancel: cancel}
	if writer, ok := body.(io.Writer); ok {
		return &cancelOnCloseWriterBody{cancelOnCloseBody: wrapped, writer: writer}
	}
	return wrapped
}
