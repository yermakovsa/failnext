package failnext

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// EndpointID identifies a configured endpoint.
type EndpointID string

// Endpoint defines one HTTP destination.
type Endpoint struct {
	// ID is the application-visible identity of the endpoint.
	ID EndpointID

	// URL is the complete destination used when this endpoint is tried.
	// It must be an absolute HTTP or HTTPS URL with a host and no fragment.
	// failnext does not join request paths or merge query strings with it.
	URL string
}

// EventKind identifies a kind of observability event.
type EventKind uint8

const (
	// EventAttempt reports a completed endpoint attempt.
	EventAttempt EventKind = iota + 1

	// EventCooldownSkip reports an endpoint skipped because it is cooling down.
	EventCooldownSkip

	// EventReplayError reports that another endpoint could not be tried because
	// Request.GetBody failed.
	EventReplayError

	// EventResult reports the final response or error RoundTrip is about to return.
	EventResult
)

// Event describes an observability event emitted while handling a request.
type Event struct {
	// Kind identifies the event.
	Kind EventKind

	// Endpoint identifies the endpoint associated with the event, if any.
	Endpoint EndpointID

	// Attempt is the 1-based endpoint attempt number when applicable.
	Attempt int

	// StatusCode is the HTTP response status code when applicable.
	StatusCode int

	// Err is the error associated with the event, if any.
	Err error
}

// Config configures a Transport.
//
// Endpoints define the fixed priority order. Each endpoint URL is a complete
// destination; failnext does not join request paths or merge query strings.
type Config struct {
	// Endpoints is the ordered set of configured HTTP destinations.
	Endpoints []Endpoint

	// Base handles each endpoint attempt. If nil, http.DefaultTransport is used.
	Base http.RoundTripper

	// PermissionPolicy decides whether a request may fail over when no explicit
	// request-scoped permission is set.
	//
	// It is called at most once per RoundTrip. PermissionDefer falls back to the
	// default HTTP method rules.
	//
	// PermissionPolicy may be called concurrently for different requests. It
	// should return promptly and must not mutate the request or consume, close,
	// or replace Request.Body.
	PermissionPolicy func(*http.Request) Permission

	// Eligible reports whether an endpoint may be used for a request. If nil,
	// all configured endpoints are eligible.
	//
	// It is called once per endpoint per request, and each result stays fixed
	// for the lifetime of that request.
	//
	// Eligible may be called concurrently for different requests and should
	// return promptly.
	Eligible func(EndpointID) bool

	// Cooldown configures passive endpoint cooldown after qualifying failures.
	// The zero value enables cooldown with default settings.
	Cooldown CooldownConfig

	// AdditionalTriggerStatusCodes extends the built-in failover status codes
	// 502, 503, and 504.
	//
	// Additional trigger statuses cause failover consideration but do not
	// automatically count as cooldown failures.
	AdditionalTriggerStatusCodes []int

	// OnEvent, if non-nil, is called synchronously for observability events.
	//
	// Events for one request are delivered in causal order. Different requests
	// may invoke OnEvent concurrently, so shared mutable state must be protected.
	// The callback should return promptly.
	//
	// failnext does not recover panics from OnEvent.
	OnEvent func(context.Context, Event)
}

// CooldownConfig configures passive endpoint cooldown.
//
// The zero value enables cooldown with a threshold of 3 qualifying failures
// and a duration of 30 seconds.
type CooldownConfig struct {
	// Disabled disables cooldown. It cannot be combined with Threshold or Duration.
	Disabled bool

	// Threshold is the number of consecutive qualifying failures required
	// before an endpoint enters cooldown. Zero uses the default of 3 unless
	// Disabled is true.
	Threshold int

	// Duration is how long an endpoint remains in cooldown. Zero uses the
	// default of 30 seconds unless Disabled is true.
	Duration time.Duration
}

// resolvedEndpoint is the normalized internal form of an Endpoint.
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

// resolvedConfig is the internal, fully normalized configuration used at runtime.
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
