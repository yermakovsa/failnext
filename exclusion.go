package failnext

import "context"

type excludedEndpointsKey struct{}

type excludedEndpointSet map[EndpointID]struct{}

// WithExcludedEndpoints returns a context that excludes ids from endpoint selection.
// Exclusions inherited from ctx are retained, so repeated calls accumulate.
// Exclusion affects endpoint selection only; it does not change failover permission,
// request replayability, cooldown, or failover trigger rules.
// Unknown endpoint IDs cause ErrUnknownEndpoint when a Transport handles the request.
func WithExcludedEndpoints(ctx context.Context, ids ...EndpointID) context.Context {
	if len(ids) == 0 {
		return ctx
	}

	inherited := excludedEndpoints(ctx)
	excluded := make(excludedEndpointSet, len(inherited)+len(ids))
	for id := range inherited {
		excluded[id] = struct{}{}
	}
	for _, id := range ids {
		excluded[id] = struct{}{}
	}

	return context.WithValue(ctx, excludedEndpointsKey{}, excluded)
}

func excludedEndpoints(ctx context.Context) excludedEndpointSet {
	excluded, _ := ctx.Value(excludedEndpointsKey{}).(excludedEndpointSet)
	return excluded
}
