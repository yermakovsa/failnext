package rcpx

import (
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// EndpointID is the stable application-visible identity of a configured endpoint.
type EndpointID string

// Endpoint configures one complete physical HTTP attempt destination.
type Endpoint struct {
	ID  EndpointID
	URL string
}

// Config configures an rcpx RoundTripper.
//
// Endpoints define the fixed priority order for physical attempts. Each endpoint
// URL is a complete destination; rcpx does not join request paths or queries.
type Config struct {
	Endpoints []Endpoint

	// Base transport used for all attempts. If nil, http.DefaultTransport is used.
	Base http.RoundTripper

	// Cooldown behavior after consecutive retryable failures. The zero value
	// enables cooldown with defaults.
	Cooldown CooldownConfig

	// If false, rcpx will not retry/failover non-idempotent methods.
	AllowNonIdempotent bool

	// Per-request request-body buffer cap in bytes.
	// 0 => DefaultBodyBufferBytes
	// <0 => invalid
	BodyBufferBytes int

	// Retry/failover policy hook. If nil, the default policy is used.
	RetryPolicy RetryPolicy

	// AdditionalTriggerStatusCodes are additional three-digit HTTP status codes
	// that trigger the existing failover machinery.
	AdditionalTriggerStatusCodes []int

	// AdditionalNonIdempotentMethods are JSON-RPC method names treated as
	// non-idempotent in addition to the built-ins.
	AdditionalNonIdempotentMethods []string

	// OnAttempt, if non-nil, is called after each upstream attempt with basic
	// attempt outcome information. The callback is called synchronously.
	OnAttempt func(AttemptInfo)
}

// AttemptInfo describes one upstream attempt observed by Config.OnAttempt.
type AttemptInfo struct {
	// Attempt is the 1-based attempt number for the current request.
	Attempt int

	// Upstream is the configured upstream URL attempted.
	//
	// It is not redacted and may contain credentials if the configured URL
	// contains them.
	Upstream string

	// Method and Batch describe the JSON-RPC request body, best-effort.
	Method string
	Batch  bool

	// StatusCode is 0 when no HTTP response was obtained.
	StatusCode int

	// Err is the attempt failure cause, if any.
	Err error

	// Final reports whether rcpx will make no further upstream attempts for
	// this request after this attempt.
	Final bool
}

// CooldownConfig configures passive endpoint cooldown behavior.
//
// The zero value enables cooldown with default threshold and duration. Set
// Disabled to true only when Threshold and Duration are both zero.
type CooldownConfig struct {
	Disabled bool

	// 0 => DefaultCooldownFailAfterConsecutive (unless Disabled=true)
	Threshold int

	// 0 => DefaultCooldownDuration (unless Disabled=true)
	Duration time.Duration
}

// resolvedEndpoint is the normalized, transport-owned form of a configured
// endpoint. The parsed URL is a complete physical attempt destination.
type resolvedEndpoint struct {
	id  EndpointID
	raw string
	url *url.URL
}

type effectiveCooldown struct {
	enabled   bool
	threshold int
	duration  time.Duration
}

// resolvedConfig is the internal, fully-normalized configuration used at runtime.
type resolvedConfig struct {
	endpoints     []resolvedEndpoint
	endpointIndex map[EndpointID]int
	base          http.RoundTripper
	cooldown      effectiveCooldown
	allowNI       bool
	bodyCap       int

	policy    RetryPolicy
	onAttempt func(AttemptInfo)

	retryableStatuses    map[int]struct{}
	nonIdempotentMethods map[string]struct{}
}

func resolveConfig(cfg Config) (resolvedConfig, error) {
	if len(cfg.Endpoints) == 0 {
		return resolvedConfig{}, fmt.Errorf("rcpx: no endpoints")
	}

	endpoints := make([]resolvedEndpoint, 0, len(cfg.Endpoints))
	endpointIndex := make(map[EndpointID]int, len(cfg.Endpoints))

	for i, endpoint := range cfg.Endpoints {
		if endpoint.ID == "" {
			return resolvedConfig{}, fmt.Errorf("rcpx: invalid endpoint at index %d: empty id", i)
		}
		if _, exists := endpointIndex[endpoint.ID]; exists {
			return resolvedConfig{}, fmt.Errorf("rcpx: duplicate endpoint id %q", endpoint.ID)
		}

		u, err := url.Parse(endpoint.URL)
		if err != nil {
			return resolvedConfig{}, fmt.Errorf("rcpx: invalid endpoint %q url %q: %w", endpoint.ID, endpoint.URL, err)
		}

		if !u.IsAbs() {
			return resolvedConfig{}, fmt.Errorf("rcpx: invalid endpoint %q url %q: must be absolute", endpoint.ID, endpoint.URL)
		}
		if u.Scheme != "http" && u.Scheme != "https" {
			return resolvedConfig{}, fmt.Errorf("rcpx: invalid endpoint %q url %q: unsupported scheme %q", endpoint.ID, endpoint.URL, u.Scheme)
		}
		if u.Host == "" {
			return resolvedConfig{}, fmt.Errorf("rcpx: invalid endpoint %q url %q: missing host", endpoint.ID, endpoint.URL)
		}
		if strings.Contains(endpoint.URL, "#") {
			return resolvedConfig{}, fmt.Errorf("rcpx: invalid endpoint %q url %q: fragments are not allowed", endpoint.ID, endpoint.URL)
		}

		endpointIndex[endpoint.ID] = len(endpoints)
		endpoints = append(endpoints, resolvedEndpoint{
			id:  endpoint.ID,
			raw: endpoint.URL,
			url: u,
		})
	}

	base := cfg.Base
	if base == nil {
		base = http.DefaultTransport
	}

	bodyCap := cfg.BodyBufferBytes
	if bodyCap < 0 {
		return resolvedConfig{}, fmt.Errorf("rcpx: invalid BodyBufferBytes %d", bodyCap)
	}
	if bodyCap == 0 {
		bodyCap = DefaultBodyBufferBytes
	}

	cooldown, err := resolveCooldown(cfg.Cooldown)
	if err != nil {
		return resolvedConfig{}, err
	}

	policy := cfg.RetryPolicy
	if policy == nil {
		policy = defaultRetryPolicy
	}

	statuses, err := resolveTriggerStatusCodes(cfg.AdditionalTriggerStatusCodes)
	if err != nil {
		return resolvedConfig{}, err
	}

	nonIdempotentMethods, err := resolveNonIdempotentMethods(cfg.AdditionalNonIdempotentMethods)
	if err != nil {
		return resolvedConfig{}, err
	}

	return resolvedConfig{
		endpoints:     endpoints,
		endpointIndex: endpointIndex,
		base:          base,
		cooldown:      cooldown,
		allowNI:       cfg.AllowNonIdempotent,
		bodyCap:       bodyCap,
		policy:        policy,
		onAttempt:     cfg.OnAttempt,

		retryableStatuses:    statuses,
		nonIdempotentMethods: nonIdempotentMethods,
	}, nil
}

func resolveCooldown(cc CooldownConfig) (effectiveCooldown, error) {
	if cc.Threshold < 0 {
		return effectiveCooldown{}, fmt.Errorf("rcpx: invalid Cooldown.Threshold %d", cc.Threshold)
	}
	if cc.Duration < 0 {
		return effectiveCooldown{}, fmt.Errorf("rcpx: invalid Cooldown.Duration %s", cc.Duration)
	}
	if cc.Disabled {
		if cc.Threshold != 0 || cc.Duration != 0 {
			return effectiveCooldown{}, fmt.Errorf("rcpx: invalid Cooldown: Disabled cannot be combined with Threshold or Duration")
		}
		return effectiveCooldown{enabled: false}, nil
	}

	threshold := cc.Threshold
	if threshold == 0 {
		threshold = DefaultCooldownFailAfterConsecutive
	}

	duration := cc.Duration
	if duration == 0 {
		duration = DefaultCooldownDuration
	}

	return effectiveCooldown{
		enabled:   true,
		threshold: threshold,
		duration:  duration,
	}, nil
}

func resolveTriggerStatusCodes(additional []int) (map[int]struct{}, error) {
	// Preserve the existing runtime trigger set in this issue. Final v1 trigger
	// semantics, including 429, are owned by a later issue.
	statuses := map[int]struct{}{
		429: {},
		502: {},
		503: {},
		504: {},
	}

	for i, code := range additional {
		if !validHTTPStatusCode(code) {
			return nil, fmt.Errorf("rcpx: invalid AdditionalTriggerStatusCodes[%d] %d", i, code)
		}
		statuses[code] = struct{}{}
	}

	return statuses, nil
}

func validHTTPStatusCode(code int) bool {
	return code >= 100 && code <= 999
}

func resolveNonIdempotentMethods(additional []string) (map[string]struct{}, error) {
	methods := map[string]struct{}{
		"eth_sendTransaction":    {},
		"eth_sendRawTransaction": {},
	}

	for i, method := range additional {
		if strings.TrimSpace(method) == "" {
			return nil, fmt.Errorf("rcpx: invalid AdditionalNonIdempotentMethods[%d]: empty method name", i)
		}

		methods[method] = struct{}{}
	}

	return methods, nil
}
