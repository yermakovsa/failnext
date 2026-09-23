package failnext

import (
	"context"
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

// EventKind identifies one kind of failnext observability event.
type EventKind uint8

const (
	EventAttempt EventKind = iota + 1
	EventCooldownSkip
	EventReplayError
	EventResult
)

// Event describes one observable failnext event for a logical request.
type Event struct {
	Kind EventKind

	Endpoint EndpointID
	Attempt  int

	StatusCode int
	Err        error
}

// Config configures an failnext RoundTripper.
//
// Endpoints define the fixed priority order for physical attempts. Each endpoint
// URL is a complete destination; failnext does not join request paths or queries.
type Config struct {
	Endpoints []Endpoint

	// Base transport used for all attempts. If nil, http.DefaultTransport is used.
	Base http.RoundTripper

	// PermissionPolicy classifies whether a logical request may continue to
	// another configured endpoint. It is evaluated at most once per RoundTrip
	// when no explicit request-scoped permission is present. It may be called
	// concurrently for different requests and is invoked outside failnext internal
	// cooldown/state locks. It must be concurrency-safe if it shares mutable state
	// and should return promptly. It must not mutate the request or consume, close,
	// or replace Request.Body.
	PermissionPolicy func(*http.Request) Permission

	// Eligible reports whether a configured endpoint may participate in a logical
	// request. If nil, all configured endpoints are externally eligible. It may
	// be called concurrently for different requests and is invoked outside failnext
	// internal cooldown/state locks. It must be concurrency-safe if it shares
	// mutable state and should return promptly.
	Eligible func(EndpointID) bool

	// Cooldown behavior after consecutive qualifying failures. The zero value
	// enables cooldown with defaults.
	Cooldown CooldownConfig

	// AdditionalTriggerStatusCodes are additional three-digit HTTP status codes
	// that trigger failover consideration.
	AdditionalTriggerStatusCodes []int

	// OnEvent, if non-nil, is called synchronously for failnext observability events.
	// Events for one logical request are delivered in causal order. Different
	// logical requests may invoke the callback concurrently, so callbacks that
	// share mutable state must be concurrency-safe and should return promptly.
	// OnEvent is invoked outside failnext internal cooldown/state locks. failnext does
	// not recover callback panics.
	OnEvent func(context.Context, Event)
}

// CooldownConfig configures passive endpoint cooldown behavior.
//
// The zero value enables cooldown with default threshold and duration. Set
// Disabled to true only when Threshold and Duration are both zero.
type CooldownConfig struct {
	Disabled bool

	// 0 => default threshold (3), unless Disabled=true
	Threshold int

	// 0 => default duration (30s), unless Disabled=true
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
	endpoints        []resolvedEndpoint
	endpointIndex    map[EndpointID]int
	base             http.RoundTripper
	permissionPolicy func(*http.Request) Permission
	eligible         func(EndpointID) bool
	cooldown         effectiveCooldown

	onEvent func(context.Context, Event)

	triggerStatuses map[int]struct{}
}

func resolveConfig(cfg Config) (resolvedConfig, error) {
	if len(cfg.Endpoints) == 0 {
		return resolvedConfig{}, fmt.Errorf("failnext: no endpoints")
	}

	endpoints := make([]resolvedEndpoint, 0, len(cfg.Endpoints))
	endpointIndex := make(map[EndpointID]int, len(cfg.Endpoints))

	for i, endpoint := range cfg.Endpoints {
		if endpoint.ID == "" {
			return resolvedConfig{}, fmt.Errorf("failnext: invalid endpoint at index %d: empty id", i)
		}
		if _, exists := endpointIndex[endpoint.ID]; exists {
			return resolvedConfig{}, fmt.Errorf("failnext: duplicate endpoint id %q", endpoint.ID)
		}

		u, err := url.Parse(endpoint.URL)
		if err != nil {
			return resolvedConfig{}, fmt.Errorf("failnext: invalid endpoint %q url %q: %w", endpoint.ID, endpoint.URL, err)
		}

		if !u.IsAbs() {
			return resolvedConfig{}, fmt.Errorf("failnext: invalid endpoint %q url %q: must be absolute", endpoint.ID, endpoint.URL)
		}
		if u.Scheme != "http" && u.Scheme != "https" {
			return resolvedConfig{}, fmt.Errorf("failnext: invalid endpoint %q url %q: unsupported scheme %q", endpoint.ID, endpoint.URL, u.Scheme)
		}
		if u.Host == "" {
			return resolvedConfig{}, fmt.Errorf("failnext: invalid endpoint %q url %q: missing host", endpoint.ID, endpoint.URL)
		}
		if strings.Contains(endpoint.URL, "#") {
			return resolvedConfig{}, fmt.Errorf("failnext: invalid endpoint %q url %q: fragments are not allowed", endpoint.ID, endpoint.URL)
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

	cooldown, err := resolveCooldown(cfg.Cooldown)
	if err != nil {
		return resolvedConfig{}, err
	}

	statuses, err := resolveTriggerStatusCodes(cfg.AdditionalTriggerStatusCodes)
	if err != nil {
		return resolvedConfig{}, err
	}

	return resolvedConfig{
		endpoints:        endpoints,
		endpointIndex:    endpointIndex,
		base:             base,
		permissionPolicy: cfg.PermissionPolicy,
		eligible:         cfg.Eligible,
		cooldown:         cooldown,
		onEvent:          cfg.OnEvent,

		triggerStatuses: statuses,
	}, nil
}

func resolveCooldown(cc CooldownConfig) (effectiveCooldown, error) {
	if cc.Threshold < 0 {
		return effectiveCooldown{}, fmt.Errorf("failnext: invalid Cooldown.Threshold %d", cc.Threshold)
	}
	if cc.Duration < 0 {
		return effectiveCooldown{}, fmt.Errorf("failnext: invalid Cooldown.Duration %s", cc.Duration)
	}
	if cc.Disabled {
		if cc.Threshold != 0 || cc.Duration != 0 {
			return effectiveCooldown{}, fmt.Errorf("failnext: invalid Cooldown: Disabled cannot be combined with Threshold or Duration")
		}
		return effectiveCooldown{enabled: false}, nil
	}

	threshold := cc.Threshold
	if threshold == 0 {
		threshold = defaultCooldownThreshold
	}

	duration := cc.Duration
	if duration == 0 {
		duration = defaultCooldownDuration
	}

	return effectiveCooldown{
		enabled:   true,
		threshold: threshold,
		duration:  duration,
	}, nil
}

func resolveTriggerStatusCodes(additional []int) (map[int]struct{}, error) {
	statuses := map[int]struct{}{
		502: {},
		503: {},
		504: {},
	}

	for i, code := range additional {
		if !validHTTPStatusCode(code) {
			return nil, fmt.Errorf("failnext: invalid AdditionalTriggerStatusCodes[%d] %d", i, code)
		}
		statuses[code] = struct{}{}
	}

	return statuses, nil
}

func (cfg resolvedConfig) isTriggerStatus(code int) bool {
	_, ok := cfg.triggerStatuses[code]
	return ok
}

func validHTTPStatusCode(code int) bool {
	return code >= 100 && code <= 999
}
