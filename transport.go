package rcpx

import (
	"context"
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

// httpStatusError is used as a cause when an upstream returns a failover-trigger
// HTTP status.
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

func (t *Transport) considerationOrder(ctx context.Context) ([]int, error) {
	order := make([]int, 0, len(t.cfg.endpoints))
	for i, endpoint := range t.cfg.endpoints {
		if t.cfg.eligible == nil || t.cfg.eligible(endpoint.id) {
			order = append(order, i)
		}
	}

	preferred, ok := preferredEndpoint(ctx)
	if !ok {
		return order, nil
	}

	preferredIdx, ok := t.cfg.endpointIndex[preferred]
	if !ok {
		return nil, fmt.Errorf("%w %q", ErrUnknownEndpoint, preferred)
	}

	for pos, idx := range order {
		if idx != preferredIdx {
			continue
		}
		if pos > 0 {
			copy(order[1:pos+1], order[:pos])
			order[0] = preferredIdx
		}
		break
	}

	return order, nil
}

func (t *Transport) nextAdmittedCandidate(order []int, start int) (idx int, next int, skipped int, ok bool) {
	for pos := start; pos < len(order); pos++ {
		idx := order[pos]
		now := t.now()
		if t.cooldown == nil || t.cooldown.eligible(now, idx) {
			return idx, pos + 1, skipped, true
		}
		skipped++
	}
	return 0, len(order), skipped, false
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

	considerationOrder, err := t.considerationOrder(ctx)
	if err != nil {
		return nil, err
	}
	if len(considerationOrder) == 0 {
		return nil, ErrNoUsableEndpoint
	}

	attempts := make([]AttemptError, 0, len(considerationOrder))
	attemptNo := 0
	attemptBody := req.Body
	var replayBody io.ReadCloser
	var replayBodyOwned bool
	defer func() {
		if replayBodyOwned && replayBody != nil {
			replayBody.Close()
		}
	}()

	var retainedResp *http.Response
	closeRetained := func() {
		if retainedResp != nil {
			closeResponseBody(retainedResp)
			retainedResp = nil
		}
	}
	defer closeRetained()

	retainResponse := func(resp *http.Response) {
		if retainedResp != nil && retainedResp != resp {
			closeResponseBody(retainedResp)
		}
		retainedResp = resp
	}
	terminalResult := func(cause error) (*http.Response, error) {
		if err := ctx.Err(); err != nil {
			closeRetained()
			return nil, err
		}
		if retainedResp != nil {
			resp := retainedResp
			retainedResp = nil
			return resp, nil
		}
		return nil, &FailoverError{
			Attempts: attempts,
			cause:    cause,
		}
	}

	idx, nextPos, _, ok := t.nextAdmittedCandidate(considerationOrder, 0)
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if !ok {
		return nil, ErrNoUsableEndpoint
	}

	for {
		if err := ctx.Err(); err != nil {
			closeRetained()
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

		// The logical request context is authoritative for cancellation. A base
		// error shaped like context.Canceled or context.DeadlineExceeded remains an
		// ordinary transport failure while this context is still live.
		if err := ctx.Err(); err != nil {
			if resp != retainedResp {
				closeResponseBody(resp)
			}
			closeRetained()
			t.notifyAttempt(attemptNo, endpoint.raw, 0, err, true)
			return nil, err
		}

		status := 0
		if resp != nil {
			status = resp.StatusCode
		}

		// Cooldown evidence is classified from the physical outcome before any
		// continuation gate is considered. Trigger configuration does not affect it.
		if t.cooldown != nil {
			if rerr != nil || isCooldownFailureStatus(status) {
				t.cooldown.recordFailure(t.now(), idx)
			} else {
				t.cooldown.recordNonFailure(idx)
			}
		}

		if rerr != nil {
			attempts = append(attempts, AttemptError{
				Endpoint: endpoint.id,
				Err:      rerr,
			})
		}

		// A non-trigger HTTP response is immediately caller-visible. Any older
		// retained fallback response is superseded and no longer owned by rcpx.
		if rerr == nil && !t.cfg.isTriggerStatus(status) {
			if err := ctx.Err(); err != nil {
				closeResponseBody(resp)
				closeRetained()
				t.notifyAttempt(attemptNo, endpoint.raw, status, err, true)
				return nil, err
			}
			if retainedResp != nil && retainedResp != resp {
				closeResponseBody(retainedResp)
			}
			retainedResp = nil
			t.notifyAttempt(attemptNo, endpoint.raw, status, nil, true)
			return resp, nil
		}

		// A failover-trigger HTTP response remains a valid fallback while later
		// candidates are considered. A newer response supersedes any older one.
		if resp != nil {
			retainResponse(resp)
		}

		cause := rerr
		if cause == nil {
			cause = &httpStatusError{code: status, upstream: endpoint.raw}
		}

		continueToNext := false
		if nextPos < len(considerationOrder) {
			continueToNext = permission == PermissionAllow
		}

		if continueToNext {
			if err := ctx.Err(); err != nil {
				closeRetained()
				t.notifyAttempt(attemptNo, endpoint.raw, status, cause, true)
				return nil, err
			}

			nextIdx, afterNext, _, admitted := t.nextAdmittedCandidate(considerationOrder, nextPos)
			nextPos = afterNext

			if err := ctx.Err(); err != nil {
				closeRetained()
				t.notifyAttempt(attemptNo, endpoint.raw, status, cause, true)
				return nil, err
			}

			if admitted {
				nextBody, replayable, replayErr := replayBodyForNextAttempt(req)
				if replayErr != nil {
					t.notifyAttempt(attemptNo, endpoint.raw, status, cause, true)
					return terminalResult(replayErr)
				}
				if !replayable {
					t.notifyAttempt(attemptNo, endpoint.raw, status, cause, true)
					return terminalResult(cause)
				}

				replayBody = nextBody
				replayBodyOwned = nextBody != nil && nextBody != http.NoBody
				attemptBody = nextBody

				if err := ctx.Err(); err != nil {
					closeRetained()
					t.notifyAttempt(attemptNo, endpoint.raw, status, cause, true)
					return nil, err
				}

				t.notifyAttempt(attemptNo, endpoint.raw, status, cause, false)

				idx = nextIdx
				continue
			}
		}

		t.notifyAttempt(attemptNo, endpoint.raw, status, cause, true)
		return terminalResult(cause)
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
