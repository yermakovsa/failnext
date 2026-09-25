package failnext

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"
)

// Transport is an http.RoundTripper that fails over across configured endpoints.
//
// A Transport is intended for concurrent reuse.
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

// httpStatusError is used as a cause when an endpoint returns an HTTP status
// that triggers failover.
type httpStatusError struct {
	code     int
	upstream string
}

func (e *httpStatusError) Error() string {
	if e == nil {
		return "failnext: upstream http error"
	}
	if e.upstream != "" {
		return fmt.Sprintf("failnext: upstream %s returned HTTP %d", e.upstream, e.code)
	}
	return fmt.Sprintf("failnext: upstream returned HTTP %d", e.code)
}

type nilResponseError struct {
	upstream string
}

func (e *nilResponseError) Error() string {
	if e == nil {
		return "failnext: upstream returned nil response"
	}
	if e.upstream != "" {
		return fmt.Sprintf("failnext: upstream %s returned nil response", e.upstream)
	}
	return "failnext: upstream returned nil response"
}

func closeResponseBody(resp *http.Response) {
	if resp != nil && resp.Body != nil {
		resp.Body.Close()
	}
}

func normalizeBaseRoundTrip(resp *http.Response, err error, upstream string) (*http.Response, error) {
	// Normalize invalid base transport results:
	//   - never propagate (nil, nil)
	//   - close and discard a response returned together with an error
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

func (t *Transport) nextAdmittedCandidate(ctx context.Context, order []int, start int) (idx int, nextPos int, ok bool) {
	for pos := start; pos < len(order); pos++ {
		idx := order[pos]
		now := t.now()

		if t.cooldown == nil || t.cooldown.eligible(now, idx) {
			return idx, pos + 1, true
		}

		t.notifyEvent(ctx, Event{
			Kind:     EventCooldownSkip,
			Endpoint: t.cfg.endpoints[idx].id,
		})
		if ctx.Err() != nil {
			return 0, pos + 1, false
		}
	}

	return 0, len(order), false
}

func (t *Transport) notifyEvent(ctx context.Context, event Event) {
	if t.cfg.onEvent == nil {
		return
	}

	t.cfg.onEvent(ctx, event)
}

func (t *Transport) notifyResult(ctx context.Context, endpoint EndpointID, resp *http.Response, err error) {
	event := Event{
		Kind: EventResult,
		Err:  err,
	}
	if resp != nil && err == nil {
		event.Endpoint = endpoint
		event.StatusCode = resp.StatusCode
	}

	t.notifyEvent(ctx, event)
}

// RoundTrip executes req using the configured endpoint failover rules.
func (t *Transport) RoundTrip(req *http.Request) (*http.Response, error) {
	if req == nil {
		return nil, errors.New("failnext: nil request")
	}

	ctx := req.Context()
	ownsOriginalBody := req.Body != nil
	defer func() {
		if ownsOriginalBody {
			req.Body.Close()
		}
	}()

	returnError := func(err error) (*http.Response, error) {
		t.notifyResult(ctx, "", nil, err)
		return nil, err
	}

	if err := ctx.Err(); err != nil {
		return returnError(err)
	}

	permission := resolvePermission(req, t.cfg.permissionPolicy)

	considerationOrder, err := t.considerationOrder(ctx)
	if err != nil {
		return returnError(err)
	}
	if len(considerationOrder) == 0 {
		return returnError(ErrNoUsableEndpoint)
	}

	attempts := make([]AttemptError, 0, len(considerationOrder))
	attemptNo := 0
	attemptBody := req.Body

	var replayBody io.ReadCloser
	var ownsReplayBody bool
	defer func() {
		if ownsReplayBody && replayBody != nil {
			replayBody.Close()
		}
	}()

	var retainedResp *http.Response
	var retainedEndpoint EndpointID
	closeRetained := func() {
		if retainedResp != nil {
			closeResponseBody(retainedResp)
			retainedResp = nil
			retainedEndpoint = ""
		}
	}
	defer closeRetained()

	retainResponse := func(endpoint EndpointID, resp *http.Response) {
		if retainedResp != nil && retainedResp != resp {
			closeResponseBody(retainedResp)
		}

		retainedResp = resp
		retainedEndpoint = endpoint
	}

	var attemptResp *http.Response
	closeAttemptResp := func() {
		if attemptResp == nil {
			return
		}

		if attemptResp != retainedResp {
			closeResponseBody(attemptResp)
		}
		attemptResp = nil
	}
	defer closeAttemptResp()

	// Final response ownership transfers to the caller only after EventResult
	// returns, so deferred cleanup still applies if the callback panics.
	returnAttemptResponse := func(endpoint EndpointID) (*http.Response, error) {
		resp := attemptResp
		t.notifyResult(ctx, endpoint, resp, nil)
		attemptResp = nil
		return resp, nil
	}

	returnRetainedResponse := func() (*http.Response, error) {
		resp := retainedResp
		endpoint := retainedEndpoint
		t.notifyResult(ctx, endpoint, resp, nil)
		retainedResp = nil
		retainedEndpoint = ""
		return resp, nil
	}

	terminalResult := func(cause error) (*http.Response, error) {
		if err := ctx.Err(); err != nil {
			closeRetained()
			return returnError(err)
		}

		if retainedResp != nil {
			return returnRetainedResponse()
		}

		return returnError(&FailoverError{
			Attempts: attempts,
			cause:    cause,
		})
	}

	idx, nextPos, ok := t.nextAdmittedCandidate(ctx, considerationOrder, 0)
	if err := ctx.Err(); err != nil {
		return returnError(err)
	}
	if !ok {
		return returnError(ErrNoUsableEndpoint)
	}

	for {
		if err := ctx.Err(); err != nil {
			closeRetained()
			return returnError(err)
		}

		attemptNo++
		endpoint := t.cfg.endpoints[idx]
		attemptReq := cloneRequestForEndpoint(req, endpoint.url, attemptBody)

		// Base owns the request body once the attempt is handed off, so clear
		// failnext's cleanup responsibility immediately before RoundTrip.
		if attemptNo == 1 {
			ownsOriginalBody = false
		} else {
			ownsReplayBody = false
		}

		resp, attemptErr := t.cfg.base.RoundTrip(attemptReq)
		resp, attemptErr = normalizeBaseRoundTrip(resp, attemptErr, endpoint.raw)
		attemptResp = resp

		status := 0
		if resp != nil {
			status = resp.StatusCode
		}

		t.notifyEvent(ctx, Event{
			Kind:       EventAttempt,
			Endpoint:   endpoint.id,
			Attempt:    attemptNo,
			StatusCode: status,
			Err:        attemptErr,
		})

		// Request context cancellation takes precedence. An error matching
		// context.Canceled or context.DeadlineExceeded from Base is still treated
		// as a transport failure while the request context itself remains active.
		if err := ctx.Err(); err != nil {
			closeAttemptResp()
			closeRetained()
			return returnError(err)
		}

		// Record cooldown state from the attempt result before deciding whether to
		// fail over. Additional failover trigger statuses do not count as cooldown
		// failures.
		if t.cooldown != nil {
			if attemptErr != nil || isCooldownFailureStatus(status) {
				t.cooldown.recordFailure(t.now(), idx)
			} else {
				t.cooldown.recordNonFailure(idx)
			}
		}

		if attemptErr != nil {
			attempts = append(attempts, AttemptError{
				Endpoint: endpoint.id,
				Err:      attemptErr,
			})
		}

		// A non-trigger response is final. Discard any previously retained
		// failover-trigger response before returning it.
		if attemptErr == nil && !t.cfg.isTriggerStatus(status) {
			if err := ctx.Err(); err != nil {
				closeAttemptResp()
				closeRetained()
				return returnError(err)
			}

			if retainedResp != nil {
				if retainedResp == attemptResp {
					retainedResp = nil
					retainedEndpoint = ""
				} else {
					closeRetained()
				}
			}

			return returnAttemptResponse(endpoint.id)
		}

		// Keep a failover-trigger response as a fallback while trying another
		// endpoint. A newer response replaces the previous fallback.
		if resp != nil {
			retainResponse(endpoint.id, resp)
			attemptResp = nil
		}

		cause := attemptErr
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
				return returnError(err)
			}

			nextIdx, afterNext, admitted := t.nextAdmittedCandidate(ctx, considerationOrder, nextPos)
			nextPos = afterNext

			if err := ctx.Err(); err != nil {
				closeRetained()
				return returnError(err)
			}

			if admitted {
				nextBody, replayable, replayErr := replayBodyForNextAttempt(req)
				if replayErr != nil {
					t.notifyEvent(ctx, Event{
						Kind:     EventReplayError,
						Endpoint: t.cfg.endpoints[nextIdx].id,
						Err:      replayErr,
					})
					return terminalResult(replayErr)
				}
				if !replayable {
					return terminalResult(cause)
				}

				replayBody = nextBody
				ownsReplayBody = nextBody != nil && nextBody != http.NoBody
				attemptBody = nextBody

				if err := ctx.Err(); err != nil {
					closeRetained()
					return returnError(err)
				}

				idx = nextIdx
				continue
			}
		}

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

// cloneRequestForEndpoint clones orig and replaces its destination with endpoint.
// endpoint is a complete target URL; no path or query joining is performed.
func cloneRequestForEndpoint(orig *http.Request, endpoint *url.URL, body io.ReadCloser) *http.Request {
	hostFollowsURL := orig.Host != "" && orig.URL != nil && orig.Host == orig.URL.Host
	r := orig.Clone(orig.Context())

	if endpoint != nil {
		u := *endpoint
		r.URL = &u
		if hostFollowsURL {
			r.Host = u.Host
		}
	}

	r.RequestURI = ""
	r.Body = body
	return r
}
