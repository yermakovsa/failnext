// Package rcpx provides an HTTP JSON-RPC failover transport for go-ethereum
// clients (rpc/ethclient).
//
// Configure an http.Client with an rcpx transport (for example via
// rpc.WithHTTPClient). For each request, rcpx tries the configured upstream URLs
// in priority order until one succeeds.
//
// rcpx selects an upstream URL per attempt. Provider auth is expected to be
// encoded in the upstream URL (path/query); per-upstream header customization is
// not supported.
package rcpx

import "time"

const (
	// DefaultCooldownFailAfterConsecutive is the default threshold of consecutive
	// failover-causing failures required to cool down an endpoint.
	DefaultCooldownFailAfterConsecutive = 3

	// DefaultCooldownDuration is the default cooldown duration.
	DefaultCooldownDuration = 30 * time.Second

	// DefaultBodyBufferBytes is the default per-request cap for buffering request
	// bodies to enable failover/retry.
	//
	// This is a conservative default.
	DefaultBodyBufferBytes = 1 << 20 // 1 MiB
)

// New validates cfg and returns a reusable Transport.
func New(cfg Config) (*Transport, error) {
	rcfg, err := resolveConfig(cfg)
	if err != nil {
		return nil, err
	}
	return newTransport(rcfg), nil
}
