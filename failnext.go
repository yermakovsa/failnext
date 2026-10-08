// Package failnext provides client-side HTTP failover for Go applications
// through the http.RoundTripper interface. It is designed for a small,
// ordered set of fixed HTTP endpoints, such as JSON-RPC providers.
//
// A Transport normally tries configured endpoints in priority order. Applications can
// filter endpoints with Eligible, exclude endpoints for a request with
// WithExcludedEndpoints, or prefer one endpoint with WithPreferredEndpoint.
// Cooldown can temporarily skip endpoints after qualifying failures.
//
// Permission to fail over is separate from endpoint selection. Applications can
// allow or deny failover through request context or PermissionPolicy. Trying
// another endpoint with a request body requires Request.GetBody; failnext does
// not buffer request bodies to make them replayable.
//
// failnext operates at the HTTP transport layer. It does not inspect JSON-RPC
// or other protocol payloads, and it does not interpret application-level
// results or errors. Applications own those decisions and may make a new
// request with different endpoint selection.
//
// failnext provides failover, not load balancing. It does not provide active
// health checks, general-purpose retry scheduling, or general-purpose rewriting
// of destination-specific request state.
//
// Use a Transport as http.Client.Transport. Config.Base handles each endpoint
// attempt.
package failnext

import "time"

const (
	// Default cooldown values used when resolving configuration.
	defaultCooldownThreshold = 3
	defaultCooldownDuration  = 30 * time.Second
)

// New validates the configuration and returns a reusable Transport.
func New(cfg Config) (*Transport, error) {
	rcfg, err := resolveConfig(cfg)
	if err != nil {
		return nil, err
	}
	return newTransport(rcfg), nil
}
