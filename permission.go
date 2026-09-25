package failnext

import (
	"context"
	"net/http"
)

// Permission describes whether a request may fail over to another endpoint.
type Permission uint8

const (
	// PermissionDefer falls back to the built-in HTTP method rules.
	PermissionDefer Permission = iota

	// PermissionAllow allows the request to fail over.
	PermissionAllow

	// PermissionDeny prevents the request from failing over.
	PermissionDeny
)

type failoverPermissionKey struct{}

// WithFailoverAllowed returns a context that explicitly permits failover to another endpoint.
// Other failover conditions still apply.
func WithFailoverAllowed(ctx context.Context) context.Context {
	return context.WithValue(ctx, failoverPermissionKey{}, PermissionAllow)
}

// WithFailoverDenied returns a context that explicitly prevents the request from
// failing over. A later failnext permission on a derived context takes precedence.
func WithFailoverDenied(ctx context.Context) context.Context {
	return context.WithValue(ctx, failoverPermissionKey{}, PermissionDeny)
}

func resolvePermission(req *http.Request, policy func(*http.Request) Permission) Permission {
	if explicit, ok := explicitPermission(req.Context()); ok {
		return explicit
	}

	if policy != nil {
		switch policy(req) {
		case PermissionAllow:
			return PermissionAllow
		case PermissionDeny:
			return PermissionDeny
		case PermissionDefer:
			// Fall through to HTTP method inference.
		default:
			return PermissionDeny
		}
	}

	switch req.Method {
	case http.MethodGet, http.MethodHead:
		return PermissionAllow
	default:
		return PermissionDeny
	}
}

func explicitPermission(ctx context.Context) (Permission, bool) {
	permission, ok := ctx.Value(failoverPermissionKey{}).(Permission)
	if !ok {
		return PermissionDefer, false
	}

	switch permission {
	case PermissionAllow, PermissionDeny:
		return permission, true
	default:
		return PermissionDefer, false
	}
}
