package failnext

import "context"

type preferredEndpointKey struct{}

// WithPreferredEndpoint marks id as the logical operation's soft preferred
// endpoint. Preference changes endpoint consideration order only.
func WithPreferredEndpoint(ctx context.Context, id EndpointID) context.Context {
	return context.WithValue(ctx, preferredEndpointKey{}, id)
}

func preferredEndpoint(ctx context.Context) (EndpointID, bool) {
	id, ok := ctx.Value(preferredEndpointKey{}).(EndpointID)
	return id, ok
}
