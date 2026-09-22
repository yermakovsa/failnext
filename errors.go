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

// AttemptError records one physical attempt that failed to obtain an HTTP response.
type AttemptError struct {
	Endpoint EndpointID
	Err      error
}

// FailoverError reports a logical request that has no HTTP response available
// after one or more physical no-response attempts.
type FailoverError struct {
	Attempts []AttemptError

	cause error
}

func (e *FailoverError) Error() string {
	if e == nil {
		return "rcpx: failover failed without HTTP response"
	}
	if e.cause == nil {
		return fmt.Sprintf("rcpx: failover failed without HTTP response (attempts=%d)", len(e.Attempts))
	}
	return fmt.Sprintf("rcpx: failover failed without HTTP response (attempts=%d): %v", len(e.Attempts), e.cause)
}

// Unwrap returns the terminal cause.
func (e *FailoverError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.cause
}
