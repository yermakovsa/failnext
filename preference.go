package failnext

import "context"

type preferredEndpointKey struct{}

// WithPreferredEndpoint returns a context that prefers id as the first endpoint to try.
// Preference does not override eligibility or cooldown and does not pin the request to that endpoint
func WithPreferredEndpoint(ctx context.Context, id EndpointID) context.Context {
	return context.WithValue(ctx, preferredEndpointKey{}, id)
}

func preferredEndpoint(ctx context.Context) (EndpointID, bool) {
	id, ok := ctx.Value(preferredEndpointKey{}).(EndpointID)
	return id, ok
}
