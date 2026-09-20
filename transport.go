package rcpx

import (
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

func (t *Transport) notifyAttempt(attempt int, upstream string, statusCode int, err error, final bool) {
	if t.cfg.onAttempt == nil {
		return
	}

	t.cfg.onAttempt(AttemptInfo{
		Attempt:    attempt,
		Upstream:   upstream,
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
	originalBodyOwned := req.Body != nil
	defer func() {
		if originalBodyOwned {
			req.Body.Close()
		}
	}()

	if err := ctx.Err(); err != nil {
		return nil, err
	}

	permission := resolvePermission(req, t.cfg.permissionPolicy)

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
	attemptBody := req.Body
	var replayBody io.ReadCloser
	var replayBodyOwned bool
	defer func() {
		if replayBodyOwned && replayBody != nil {
			replayBody.Close()
		}
	}()

	for pos, idx := range eligible {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		attemptNo++
		endpoint := t.cfg.endpoints[idx]
		areq := cloneRequestForEndpoint(req, endpoint.url, attemptBody)

		if attemptNo == 1 {
			originalBodyOwned = false
		} else {
			replayBodyOwned = false
		}

		resp, rerr := t.cfg.base.RoundTrip(areq)
		resp, rerr = normalizeBaseRoundTrip(resp, rerr, endpoint.raw)

		// Cancellation rail: return immediately; policy is not called.
		if isCanceledOrDeadline(ctx, rerr) || ctx.Err() != nil {
			finalErr := rerr
			if ctx.Err() != nil {
				finalErr = ctx.Err()
			}
			closeResponseBody(resp)
			t.notifyAttempt(attemptNo, endpoint.raw, 0, finalErr, true)

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
			t.notifyAttempt(attemptNo, endpoint.raw, status, nil, true)
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

		out := t.cfg.buildAttemptOutcome(attemptNo, endpoint.raw, status, rerr)

		hasNext := pos < len(eligible)-1
		continueToNext := false
		if hasNext {
			// RetryPolicy remains the temporary trigger decision. Semantic permission
			// is an independent logical-request gate on cross-endpoint continuation.
			continueToNext = shouldContinue(t.cfg.policy, out) && permission == PermissionAllow
		}

		if continueToNext {
			nextBody, replayable, replayErr := replayBodyForNextAttempt(req)
			if replayErr != nil {
				closeResponseBody(resp)
				t.notifyAttempt(attemptNo, endpoint.raw, status, cause, true)
				return nil, replayErr
			}
			if replayable {
				replayBody = nextBody
				replayBodyOwned = nextBody != nil && nextBody != http.NoBody
				attemptBody = nextBody

				if err := ctx.Err(); err != nil {
					closeResponseBody(resp)
					t.notifyAttempt(attemptNo, endpoint.raw, status, cause, true)
					return nil, err
				}

				closeResponseBody(resp)
				t.notifyAttempt(attemptNo, endpoint.raw, status, cause, false)

				if t.cooldown != nil {
					t.cooldown.recordFailoverFailure(now, idx)
				}
				failures = append(failures, AttemptFailure{
					Upstream:   endpoint.raw,
					StatusCode: status,
					Err:        cause,
					Retryable:  true,
				})
				continue
			}
		}

		closeResponseBody(resp)

		t.notifyAttempt(attemptNo, endpoint.raw, status, cause, true)

		failures = append(failures, AttemptFailure{
			Upstream:   endpoint.raw,
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

func replayBodyForNextAttempt(req *http.Request) (body io.ReadCloser, replayable bool, err error) {
	if req.Body == nil {
		return nil, true, nil
	}
	if req.Body == http.NoBody {
		return http.NoBody, true, nil
	}
	if req.GetBody == nil {
		return nil, false, nil
	}

	body, err = req.GetBody()
	if err != nil {
		if body != nil {
			body.Close()
		}
		return nil, false, err
	}
	return body, true, nil
}

// cloneRequestForEndpoint clones orig and targets the provided endpoint URL.
// endpoint must be a full target URL; there is no path joining.
func cloneRequestForEndpoint(orig *http.Request, endpoint *url.URL, body io.ReadCloser) *http.Request {
	r := orig.Clone(orig.Context())

	if endpoint != nil {
		u := *endpoint
		r.URL = &u
	}

	r.RequestURI = ""
	r.Body = body
	return r
}
