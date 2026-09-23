package failnext

import (
	"context"
	"net/http"
)

// Permission describes whether one logical request may continue to another
// configured endpoint.
type Permission uint8

const (
	// PermissionDefer delegates the decision to lower-authority inference.
	PermissionDefer Permission = iota
	// PermissionAllow permits cross-endpoint continuation.
	PermissionAllow
	// PermissionDeny prevents cross-endpoint continuation.
	PermissionDeny
)

type failoverPermissionKey struct{}

// WithFailoverAllowed marks the logical operation carried by ctx as permitted
// to continue across configured endpoints. A later failnext permission value on a
// derived context overrides this value.
func WithFailoverAllowed(ctx context.Context) context.Context {
	return context.WithValue(ctx, failoverPermissionKey{}, PermissionAllow)
}

// WithFailoverDenied marks the logical operation carried by ctx as not permitted
// to continue across configured endpoints. A later failnext permission value on a
// derived context overrides this value.
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
			// Fall through to generic HTTP-method inference.
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
