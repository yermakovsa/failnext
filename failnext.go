// Package failnext provides an HTTP failover RoundTripper for applications
// with a small, ordered set of fixed endpoints.
//
// A Transport normally tries configured endpoints in priority order. Applications can
// exclude endpoints with Eligible or prefer one endpoint for a request.
// Cooldown can temporarily skip endpoints after qualifying failures.
//
// Permission to fail over is separate from endpoint selection. Applications can
// allow or deny failover through request context or PermissionPolicy. Trying
// another endpoint with a request body requires Request.GetBody; failnext does
// not buffer request bodies to make them replayable.
//
// failnext operates at the HTTP transport layer. It does not inspect protocol
// payloads or provide load balancing, active health checks, general-purpose
// retry scheduling, or general-purpose rewriting of destination-specific request state.
//
// Use a Transport as http.Client.Transport. Base handles each provider attempt.
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
