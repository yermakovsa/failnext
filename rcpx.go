// Package rcpx provides a specialized HTTP failover RoundTripper for applications
// that use a small, ordered set of fixed endpoint destinations.
//
// A Transport selects a configured endpoint for each physical attempt. Configured
// order is authoritative; optional eligibility and one request-scoped preference
// affect the request-local consideration order, while passive cooldown is checked
// live before each attempt.
//
// Cross-endpoint continuation is separate from endpoint selection. Applications
// can allow or deny it through request context or PermissionPolicy. Later
// body-bearing attempts use Request.GetBody; rcpx does not buffer bodies to make
// them replayable.
//
// rcpx operates at the HTTP transport layer. It does not inspect protocol payloads
// or provide generic load balancing, health checking, retry scheduling, or
// destination-specific request rewriting. Use a Transport as http.Client.Transport;
// Base handles each physical attempt.
package rcpx

import "time"

const (
	// Internal cooldown defaults. They remain implementation details so the public
	// surface stays limited to CooldownConfig.
	defaultCooldownThreshold = 3
	defaultCooldownDuration  = 30 * time.Second
)

// New validates cfg and returns a reusable Transport.
func New(cfg Config) (*Transport, error) {
	rcfg, err := resolveConfig(cfg)
	if err != nil {
		return nil, err
	}
	return newTransport(rcfg), nil
}
