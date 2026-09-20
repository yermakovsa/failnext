package rcpx

import (
	"errors"
	"fmt"
)

var (
	// ErrNoEligibleUpstreams indicates that no upstreams were eligible to try
	// (e.g., all cooling down).
	ErrNoEligibleUpstreams = errors.New("rcpx: no eligible upstreams")
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
	Upstream string

	// Method and Batch are retained temporarily for the legacy error surface.
	Method string
	Batch  bool

	StatusCode int
	Err        error
	Retryable  bool // whether rcpx continued after this attempt
}
