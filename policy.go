package rcpx

import (
	"context"
	"errors"
)

// RetryPolicy decides whether the temporary trigger machinery should consider
// continuing to another upstream after a non-success attempt. Semantic failover
// permission is resolved separately for the logical request.
type RetryPolicy func(out AttemptOutcome) (retry bool)

// AttemptOutcome describes the outcome of one upstream attempt.
//
// It intentionally excludes *http.Response to avoid response-body lifecycle issues.
type AttemptOutcome struct {
	Attempt  int
	Upstream string

	// Method and Batch are retained temporarily for the legacy trigger surface.
	Method string
	Batch  bool

	// StatusCode is 0 when no HTTP response was obtained.
	StatusCode int
	Err        error

	RetryableByDefault bool // whether rcpx classifies this outcome as retryable
}

func defaultRetryPolicy(out AttemptOutcome) bool {
	return out.RetryableByDefault
}

func isBuiltInRetryableStatus(code int) bool {
	switch code {
	case 429, 502, 503, 504:
		return true
	default:
		return false
	}
}

func (cfg resolvedConfig) isRetryableStatus(code int) bool {
	if cfg.retryableStatuses == nil {
		return isBuiltInRetryableStatus(code)
	}

	_, ok := cfg.retryableStatuses[code]
	return ok
}

func (cfg resolvedConfig) isAttemptSuccess(statusCode int, err error) bool {
	return err == nil && !cfg.isRetryableStatus(statusCode)
}

func (cfg resolvedConfig) retryableByOutcome(statusCode int, err error) bool {
	if err != nil {
		return true
	}
	return cfg.isRetryableStatus(statusCode)
}

// statusCode should be 0 when no HTTP response was obtained.
func (cfg resolvedConfig) buildAttemptOutcome(attempt int, upstream string, statusCode int, err error) AttemptOutcome {
	return AttemptOutcome{
		Attempt:            attempt,
		Upstream:           upstream,
		StatusCode:         statusCode,
		Err:                err,
		RetryableByDefault: cfg.retryableByOutcome(statusCode, err),
	}
}

// shouldContinue reports whether the temporary trigger policy wants to
// continue after a non-success attempt. RoundTrip returns immediately on
// cancellation/deadline before calling this; semantic permission is separate.
func shouldContinue(policy RetryPolicy, out AttemptOutcome) bool {
	if policy == nil {
		return defaultRetryPolicy(out)
	}
	return policy(out)
}

func isCanceledOrDeadline(ctx context.Context, err error) bool {
	if ctx != nil {
		if ctxErr := ctx.Err(); errors.Is(ctxErr, context.Canceled) || errors.Is(ctxErr, context.DeadlineExceeded) {
			return true
		}
	}
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}
