package failnext

import (
	"errors"
	"fmt"
)

var (
	// ErrNoUsableEndpoint indicates that endpoint selection left no usable endpoint to try.
	ErrNoUsableEndpoint = errors.New("failnext: no usable endpoint")

	// ErrUnknownEndpoint indicates that a request refers to an endpoint ID that
	// is not configured on the Transport.
	ErrUnknownEndpoint = errors.New("failnext: unknown endpoint")
)

// AttemptError records an endpoint attempt that failed without producing a
// usable HTTP response.
type AttemptError struct {
	// Endpoint identifies the endpoint that was tried.
	Endpoint EndpointID

	// Err is the error associated with the attempt.
	Err error
}

// FailoverError reports that one or more endpoint attempts failed without
// producing a usable HTTP response and no HTTP response is available to return.
type FailoverError struct {
	// Attempts contains the failed endpoint attempts in order.
	Attempts []AttemptError

	cause error
}

func (e *FailoverError) Error() string {
	if e == nil {
		return "failnext: failover failed without HTTP response"
	}
	if e.cause == nil {
		return fmt.Sprintf(
			"failnext: failover failed without HTTP response (attempts=%d)",
			len(e.Attempts),
		)
	}
	return fmt.Sprintf(
		"failnext: failover failed without HTTP response (attempts=%d): %v",
		len(e.Attempts),
		e.cause,
	)
}

// Unwrap returns the terminal cause.
func (e *FailoverError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.cause
}
