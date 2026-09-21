package rcpx

import (
	"errors"
	"fmt"
)

var (
	// ErrNoUsableEndpoint indicates that no physical endpoint could be admitted
	// for the logical request.
	ErrNoUsableEndpoint = errors.New("rcpx: no usable endpoint")

	// ErrUnknownEndpoint indicates that a request-scoped endpoint reference does
	// not identify an endpoint configured on the processing Transport.
	ErrUnknownEndpoint = errors.New("rcpx: unknown endpoint")
)

// AllUpstreamsFailedError is returned when one or more upstream attempts were
// made and no attempt succeeded.
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

	cause := e.Unwrap()
	if cause == nil {
		return fmt.Sprintf("rcpx: all upstreams failed (attempted=%d)", e.Attempted)
	}
	return fmt.Sprintf("rcpx: all upstreams failed (attempted=%d): %v", e.Attempted, cause)
}

// Unwrap returns the last failure cause, if any.
func (e *AllUpstreamsFailedError) Unwrap() error {
	if e == nil {
		return nil
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
