package rcpx

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"
)

type Transport struct {
	cfg      resolvedConfig
	cooldown *cooldownTracker

	// now is injectable for deterministic tests. Defaults to time.Now.
	now func() time.Time
}

func newTransport(cfg resolvedConfig) *Transport {
	return &Transport{
		cfg:      cfg,
		cooldown: newCooldownTracker(len(cfg.endpoints), cfg.cooldown),
		now:      time.Now,
	}
}

// CloseIdleConnections closes idle connections on the configured base transport
// when it supports that operation.
func (t *Transport) CloseIdleConnections() {
	if t == nil {
		return
	}

	if closer, ok := t.cfg.base.(interface{ CloseIdleConnections() }); ok {
		closer.CloseIdleConnections()
	}
}

// httpStatusError is used as a cause when an upstream returns a retryable HTTP
// status (429/502/503/504).
type httpStatusError struct {
	code     int
	upstream string
}

func (e *httpStatusError) Error() string {
	if e == nil {
		return "rcpx: upstream http error"
	}
	if e.upstream != "" {
		return fmt.Sprintf("rcpx: upstream %s returned HTTP %d", e.upstream, e.code)
	}
	return fmt.Sprintf("rcpx: upstream returned HTTP %d", e.code)
}

type nilResponseError struct {
	upstream string
}

func (e *nilResponseError) Error() string {
	if e == nil {
		return "rcpx: upstream returned nil response"
	}
	if e.upstream != "" {
		return fmt.Sprintf("rcpx: upstream %s returned nil response", e.upstream)
	}
	return "rcpx: upstream returned nil response"
}

func closeResponseBody(resp *http.Response) {
	if resp != nil && resp.Body != nil {
		resp.Body.Close()
	}
}

func normalizeBaseRoundTrip(resp *http.Response, err error, upstream string) (*http.Response, error) {
	// Normalize misbehaving base transports:
	//   - never return (nil, nil)
	//   - close bodies when err != nil to avoid leaks
	if resp != nil && err != nil {
		closeResponseBody(resp)
		resp = nil
	}
	if resp == nil && err == nil {
		err = &nilResponseError{upstream: upstream}
	}
	return resp, err
}

func (t *Transport) eligibleEndpoints(now time.Time) (eligible []int, skippedCooldown int) {
	eligible = make([]int, 0, len(t.cfg.endpoints))
	for i := range t.cfg.endpoints {
		if t.cooldown == nil || t.cooldown.eligible(now, i) {
			eligible = append(eligible, i)
			continue
		}
		skippedCooldown++
	}
	return eligible, skippedCooldown
}

func (t *Transport) notifyAttempt(attempt int, upstream, method string, batch bool, statusCode int, err error, final bool) {
	if t.cfg.onAttempt == nil {
		return
	}

	t.cfg.onAttempt(AttemptInfo{
		Attempt:    attempt,
		Upstream:   upstream,
		Method:     method,
		Batch:      batch,
		StatusCode: statusCode,
		Err:        err,
		Final:      final,
	})
}

func (t *Transport) RoundTrip(req *http.Request) (*http.Response, error) {
	if req == nil {
		return nil, errors.New("rcpx: nil request")
	}

	ctx := req.Context()
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	body, err := bufferRequestBody(req, t.cfg.bodyCap)
	if err != nil {
		return nil, err
	}

	// Best-effort method parsing; parse failure => treat as non-idempotent.
	method, batch, nonIdempotent, ok := parseJSONRPCMethod(body, t.cfg.nonIdempotentMethods)
	if !ok {
		nonIdempotent = true
	}

	now := t.now()
	eligible, skippedCooldown := t.eligibleEndpoints(now)
	if len(eligible) == 0 {
		return nil, &AllUpstreamsFailedError{
			Attempted:       0,
			SkippedCooldown: skippedCooldown,
			Failures:        nil,
		}
	}

	failures := make([]AttemptFailure, 0, len(eligible))
	attemptNo := 0

	for pos, idx := range eligible {
		attemptNo++
		endpoint := t.cfg.endpoints[idx]

		areq := cloneRequestForEndpoint(req, endpoint.url, body)

		resp, rerr := t.cfg.base.RoundTrip(areq)
		resp, rerr = normalizeBaseRoundTrip(resp, rerr, endpoint.raw)

		// Cancellation rail: return immediately; policy is not called.
		if isCanceledOrDeadline(ctx, rerr) || ctx.Err() != nil {
			finalErr := rerr
			if ctx.Err() != nil {
				finalErr = ctx.Err()
			}
			closeResponseBody(resp)
			t.notifyAttempt(attemptNo, endpoint.raw, method, batch, 0, finalErr, true)

			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			return nil, rerr
		}

		status := 0
		if resp != nil {
			status = resp.StatusCode
		}

		// Success = err==nil and status is not retryable.
		// Non-retryable HTTP statuses are treated as "success" from rcpx's
		// perspective and returned unchanged.
		if t.cfg.isAttemptSuccess(status, rerr) {
			t.notifyAttempt(attemptNo, endpoint.raw, method, batch, status, nil, true)
			if t.cooldown != nil {
				t.cooldown.recordSuccess(idx)
			}
			return resp, nil
		}

		// Non-success: choose a cause error.
		cause := rerr
		if cause == nil {
			cause = &httpStatusError{code: status, upstream: endpoint.raw}
		}

		out := t.cfg.buildAttemptOutcome(attemptNo, endpoint.raw, method, batch, status, rerr)

		hasNext := pos < len(eligible)-1
		continueToNext := false
		if hasNext {
			// Policy is called only on non-success when considering continuing.
			continueToNext = shouldContinue(t.cfg.policy, out)
		}

		// Idempotency rail: non-idempotent requests never fail over unless allowed.
		if nonIdempotent && !t.cfg.allowNI && continueToNext {
			closeResponseBody(resp)
			t.notifyAttempt(attemptNo, endpoint.raw, method, batch, status, cause, true)
			return nil, &NonIdempotentBlockedError{
				Outcome: out,
				Cause:   cause,
			}
		}

		if continueToNext {
			closeResponseBody(resp)
			t.notifyAttempt(attemptNo, endpoint.raw, method, batch, status, cause, false)

			if t.cooldown != nil {
				t.cooldown.recordFailoverFailure(now, idx)
			}
			failures = append(failures, AttemptFailure{
				Upstream:   endpoint.raw,
				Method:     method,
				Batch:      batch,
				StatusCode: status,
				Err:        cause,
				Retryable:  true,
			})
			continue
		}

		closeResponseBody(resp)

		t.notifyAttempt(attemptNo, endpoint.raw, method, batch, status, cause, true)

		failures = append(failures, AttemptFailure{
			Upstream:   endpoint.raw,
			Method:     method,
			Batch:      batch,
			StatusCode: status,
			Err:        cause,
			Retryable:  false,
		})

		return nil, &AllUpstreamsFailedError{
			Attempted:       attemptNo,
			SkippedCooldown: skippedCooldown,
			Failures:        failures,
		}
	}

	return nil, &AllUpstreamsFailedError{
		Attempted:       attemptNo,
		SkippedCooldown: skippedCooldown,
		Failures:        failures,
	}
}

// bufferRequestBody reads and buffers req.Body up to cap bytes.
func bufferRequestBody(req *http.Request, capBytes int) ([]byte, error) {
	if req == nil || req.Body == nil {
		return nil, nil
	}
	if capBytes <= 0 {
		capBytes = DefaultBodyBufferBytes
	}

	defer req.Body.Close()

	limited := io.LimitReader(req.Body, int64(capBytes)+1)
	b, err := io.ReadAll(limited)
	if err != nil {
		return nil, errors.Join(ErrBodyUnreadable, err)
	}
	if len(b) > capBytes {
		return nil, ErrBodyTooLarge
	}
	return b, nil
}

// cloneRequestForEndpoint clones orig and targets the provided endpoint URL.
//
// If orig.Body is nil and bodyBytes is empty, the clone preserves a nil Body (and
// nil GetBody).
//
// NOTE: endpoint must be a full target URL; there is no path joining.
func cloneRequestForEndpoint(orig *http.Request, endpoint *url.URL, bodyBytes []byte) *http.Request {
	r := orig.Clone(orig.Context())

	if endpoint != nil {
		u := *endpoint
		r.URL = &u
	}

	r.RequestURI = ""

	if orig.Body == nil && len(bodyBytes) == 0 {
		r.Body = nil
		r.GetBody = nil
		r.ContentLength = 0
		return r
	}

	br := bytes.NewReader(bodyBytes)
	r.Body = io.NopCloser(br)
	r.ContentLength = int64(len(bodyBytes))

	r.GetBody = func() (io.ReadCloser, error) {
		return io.NopCloser(bytes.NewReader(bodyBytes)), nil
	}

	return r
}
