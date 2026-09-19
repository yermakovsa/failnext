package rcpx

import (
	"errors"
	"fmt"
)

var (
	// ErrNoEligibleUpstreams indicates that no upstreams were eligible to try
	// (e.g., all cooling down).
	ErrNoEligibleUpstreams = errors.New("rcpx: no eligible upstreams")

	// ErrBodyTooLarge is returned when the request body exceeds the configured cap.
	ErrBodyTooLarge = errors.New("rcpx: request body too large")

	// ErrBodyUnreadable is returned when the request body cannot be read.
	ErrBodyUnreadable = errors.New("rcpx: request body unreadable")
)

// AllUpstreamsFailedError is returned when no upstream attempt succeeded.
//
// For Attempted==0, Unwrap() returns ErrNoEligibleUpstreams.
type AllUpstreamsFailedError struct {
	Attempted       int
	SkippedCooldown int

	// Failures are recorded in attempt order (one per attempt; bounded).
	Failures []AttemptFailure
}

func (e *AllUpstreamsFailedError) Error() string {
	if e == nil {
		return "rcpx: all upstreams failed"
	}
	if e.Attempted == 0 {
		return "rcpx: no eligible upstreams"
	}

	cause := e.Unwrap()
	if cause == nil {
		return fmt.Sprintf("rcpx: all upstreams failed (attempted=%d)", e.Attempted)
	}
	return fmt.Sprintf("rcpx: all upstreams failed (attempted=%d): %v", e.Attempted, cause)
}

// Unwrap returns the last failure cause, or ErrNoEligibleUpstreams when no
// attempts were made.
func (e *AllUpstreamsFailedError) Unwrap() error {
	if e == nil {
		return nil
	}
	if e.Attempted == 0 {
		return ErrNoEligibleUpstreams
	}

	for i := len(e.Failures) - 1; i >= 0; i-- {
		if err := e.Failures[i].Err; err != nil {
			return err
		}
	}
	return nil
}

// AttemptFailure records a failed attempt.
type AttemptFailure struct {
	Upstream   string
	Method     string
	Batch      bool
	StatusCode int
	Err        error
	Retryable  bool // whether rcpx continued after this attempt
}

// NonIdempotentBlockedError is returned when a request classified as
// non-idempotent would otherwise retry/failover but AllowNonIdempotent is false.
//
// It wraps the underlying failure cause.
type NonIdempotentBlockedError struct {
	Outcome AttemptOutcome
	Cause   error
}

func (e *NonIdempotentBlockedError) Error() string {
	if e == nil {
		return "rcpx: non-idempotent request blocked"
	}

	if e.Outcome.Method != "" {
		if e.Cause != nil {
			return fmt.Sprintf("rcpx: non-idempotent request blocked (%s): %v", e.Outcome.Method, e.Cause)
		}
		return fmt.Sprintf("rcpx: non-idempotent request blocked (%s)", e.Outcome.Method)
	}

	if e.Cause != nil {
		return fmt.Sprintf("rcpx: non-idempotent request blocked: %v", e.Cause)
	}
	return "rcpx: non-idempotent request blocked"
}

func (e *NonIdempotentBlockedError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Cause
}
