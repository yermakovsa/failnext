package rcpx

import (
	"net/http"
	"testing"
	"time"
)

const endpointURL = "https://u1.test/rpc"

func TestNewValidation(t *testing.T) {
	t.Run("empty endpoints", func(t *testing.T) {
		_, err := New(Config{})
		if err == nil {
			t.Fatal("expected error for empty endpoints, got nil")
		}
	})

	t.Run("invalid endpoint", func(t *testing.T) {
		tests := []struct {
			name     string
			endpoint Endpoint
		}{
			{
				name:     "empty id",
				endpoint: Endpoint{URL: endpointURL},
			},
			{
				name:     "malformed url",
				endpoint: Endpoint{ID: "primary", URL: "://"},
			},
			{
				name:     "relative url",
				endpoint: Endpoint{ID: "primary", URL: "u1.test/rpc"},
			},
			{
				name:     "missing host",
				endpoint: Endpoint{ID: "primary", URL: "https:///rpc"},
			},
			{
				name:     "unsupported scheme",
				endpoint: Endpoint{ID: "primary", URL: "ftp://u1.test/rpc"},
			},
			{
				name:     "fragment",
				endpoint: Endpoint{ID: "primary", URL: "https://u1.test/rpc#fragment"},
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				_, err := New(Config{Endpoints: []Endpoint{tt.endpoint}})
				if err == nil {
					t.Fatalf("expected error for endpoint %#v, got nil", tt.endpoint)
				}
			})
		}
	})

	t.Run("duplicate endpoint ids", func(t *testing.T) {
		_, err := New(Config{
			Endpoints: []Endpoint{
				{ID: "same", URL: "https://u1.test/rpc"},
				{ID: "same", URL: "https://u2.test/rpc"},
			},
		})
		if err == nil {
			t.Fatal("expected error for duplicate endpoint ids, got nil")
		}
	})

	t.Run("negative body buffer bytes", func(t *testing.T) {
		_, err := New(Config{
			Endpoints:       testEndpoints(endpointURL),
			BodyBufferBytes: -1,
		})
		if err == nil {
			t.Fatal("expected error for negative BodyBufferBytes, got nil")
		}
	})

	t.Run("negative cooldown values", func(t *testing.T) {
		tests := []struct {
			name     string
			cooldown CooldownConfig
		}{
			{
				name:     "negative threshold",
				cooldown: CooldownConfig{Threshold: -1},
			},
			{
				name:     "negative duration",
				cooldown: CooldownConfig{Duration: -time.Second},
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				_, err := New(Config{
					Endpoints: testEndpoints(endpointURL),
					Cooldown:  tt.cooldown,
				})
				if err == nil {
					t.Fatalf("expected error for cooldown %#v, got nil", tt.cooldown)
				}
			})
		}
	})

	t.Run("disabled cooldown rejects explicit values", func(t *testing.T) {
		tests := []struct {
			name     string
			cooldown CooldownConfig
		}{
			{
				name:     "threshold",
				cooldown: CooldownConfig{Disabled: true, Threshold: 1},
			},
			{
				name:     "duration",
				cooldown: CooldownConfig{Disabled: true, Duration: time.Second},
			},
			{
				name:     "threshold and duration",
				cooldown: CooldownConfig{Disabled: true, Threshold: 1, Duration: time.Second},
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				_, err := New(Config{
					Endpoints: testEndpoints(endpointURL),
					Cooldown:  tt.cooldown,
				})
				if err == nil {
					t.Fatalf("expected error for cooldown %#v, got nil", tt.cooldown)
				}
			})
		}
	})

	t.Run("invalid additional trigger status codes", func(t *testing.T) {
		tests := []struct {
			name string
			code int
		}{
			{name: "negative", code: -1},
			{name: "zero", code: 0},
			{name: "below three digits", code: 99},
			{name: "above three digits", code: 1000},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				_, err := New(Config{
					Endpoints:                    testEndpoints(endpointURL),
					AdditionalTriggerStatusCodes: []int{tt.code},
				})
				if err == nil {
					t.Fatalf("expected error for status code %d, got nil", tt.code)
				}
			})
		}
	})

	t.Run("invalid additional non idempotent methods", func(t *testing.T) {
		tests := []struct {
			name   string
			method string
		}{
			{name: "empty", method: ""},
			{name: "whitespace only", method: " \t\n"},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				_, err := New(Config{
					Endpoints:                      testEndpoints(endpointURL),
					AdditionalNonIdempotentMethods: []string{tt.method},
				})
				if err == nil {
					t.Fatalf("expected error for method %q, got nil", tt.method)
				}
			})
		}
	})
}

func TestNewAcceptsDuplicateURLsWithDistinctIDs(t *testing.T) {
	tr := mustNewTransport(t, Config{
		Endpoints: []Endpoint{
			{ID: "primary", URL: endpointURL},
			{ID: "backup", URL: endpointURL},
		},
	})

	if len(tr.cfg.endpoints) != 2 {
		t.Fatalf("expected 2 normalized endpoints, got %d", len(tr.cfg.endpoints))
	}
	if tr.cfg.endpoints[0].id != "primary" || tr.cfg.endpoints[1].id != "backup" {
		t.Fatalf("unexpected endpoint identities: %#v", tr.cfg.endpoints)
	}
}

func TestNewReturnsTransport(t *testing.T) {
	tr, err := New(Config{Endpoints: testEndpoints(endpointURL)})
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}
	if tr == nil {
		t.Fatal("New() returned nil Transport")
	}

	var rt http.RoundTripper = tr
	if rt == nil {
		t.Fatal("*Transport does not satisfy http.RoundTripper")
	}
}

func TestNewNormalizesEndpointOrderAndLookup(t *testing.T) {
	endpoints := []Endpoint{
		{ID: "second", URL: "https://second.test/rpc"},
		{ID: "first", URL: "https://first.test/rpc"},
	}
	tr := mustNewTransport(t, Config{Endpoints: endpoints})

	if len(tr.cfg.endpoints) != 2 {
		t.Fatalf("expected 2 normalized endpoints, got %d", len(tr.cfg.endpoints))
	}
	if got := tr.cfg.endpoints[0].id; got != "second" {
		t.Fatalf("expected first configured endpoint id %q, got %q", EndpointID("second"), got)
	}
	if got := tr.cfg.endpoints[1].id; got != "first" {
		t.Fatalf("expected second configured endpoint id %q, got %q", EndpointID("first"), got)
	}
	if got := tr.cfg.endpointIndex["second"]; got != 0 {
		t.Fatalf("expected endpoint index second=0, got %d", got)
	}
	if got := tr.cfg.endpointIndex["first"]; got != 1 {
		t.Fatalf("expected endpoint index first=1, got %d", got)
	}
}

func TestNewCopiesEndpointConfiguration(t *testing.T) {
	endpoints := []Endpoint{
		{ID: "primary", URL: "https://primary.test/rpc?key=one"},
		{ID: "backup", URL: "https://backup.test/rpc?key=two"},
	}
	tr := mustNewTransport(t, Config{Endpoints: endpoints})

	endpoints[0] = Endpoint{ID: "changed", URL: "https://changed.test/other"}
	endpoints[1].URL = "https://changed-again.test/other"

	if got := tr.cfg.endpoints[0].id; got != "primary" {
		t.Fatalf("first endpoint id changed after caller mutation: got %q", got)
	}
	if got := tr.cfg.endpoints[0].raw; got != "https://primary.test/rpc?key=one" {
		t.Fatalf("first endpoint url changed after caller mutation: got %q", got)
	}
	if got := tr.cfg.endpoints[1].id; got != "backup" {
		t.Fatalf("second endpoint id changed after caller mutation: got %q", got)
	}
	if got := tr.cfg.endpoints[1].raw; got != "https://backup.test/rpc?key=two" {
		t.Fatalf("second endpoint url changed after caller mutation: got %q", got)
	}
	if got := tr.cfg.endpointIndex["primary"]; got != 0 {
		t.Fatalf("expected endpoint index primary=0 after caller mutation, got %d", got)
	}
	if got := tr.cfg.endpointIndex["backup"]; got != 1 {
		t.Fatalf("expected endpoint index backup=1 after caller mutation, got %d", got)
	}
	if _, ok := tr.cfg.endpointIndex["changed"]; ok {
		t.Fatal("caller mutation changed endpoint index")
	}
}

func TestNewCopiesAdditionalTriggerStatusCodes(t *testing.T) {
	additional := []int{500}
	tr := mustNewTransport(t, Config{
		Endpoints:                    testEndpoints(endpointURL),
		AdditionalTriggerStatusCodes: additional,
	})

	additional[0] = 501

	if _, ok := tr.cfg.retryableStatuses[500]; !ok {
		t.Fatal("expected normalized trigger status 500")
	}
	if _, ok := tr.cfg.retryableStatuses[501]; ok {
		t.Fatal("caller mutation changed normalized trigger statuses")
	}
}

func TestNewBaseNilUsesDefaultTransport(t *testing.T) {
	tr := mustNewTransport(t, Config{Endpoints: testEndpoints(endpointURL)})
	if tr.cfg.base != http.DefaultTransport {
		t.Fatalf("expected http.DefaultTransport, got %T", tr.cfg.base)
	}
}

func TestNewCooldownConfiguration(t *testing.T) {
	t.Run("zero value uses defaults", func(t *testing.T) {
		tr := mustNewTransport(t, Config{Endpoints: testEndpoints(endpointURL)})

		if !tr.cfg.cooldown.enabled {
			t.Fatal("expected cooldown enabled by default")
		}
		if tr.cfg.cooldown.threshold != DefaultCooldownFailAfterConsecutive {
			t.Fatalf("expected threshold=%d, got %d", DefaultCooldownFailAfterConsecutive, tr.cfg.cooldown.threshold)
		}
		if tr.cfg.cooldown.duration != DefaultCooldownDuration {
			t.Fatalf("expected duration=%s, got %s", DefaultCooldownDuration, tr.cfg.cooldown.duration)
		}
	})

	t.Run("explicit threshold uses default duration", func(t *testing.T) {
		tr := mustNewTransport(t, Config{
			Endpoints: testEndpoints(endpointURL),
			Cooldown: CooldownConfig{
				Threshold: 5,
			},
		})

		if !tr.cfg.cooldown.enabled {
			t.Fatal("expected cooldown enabled")
		}
		if tr.cfg.cooldown.threshold != 5 {
			t.Fatalf("expected threshold=5, got %d", tr.cfg.cooldown.threshold)
		}
		if tr.cfg.cooldown.duration != DefaultCooldownDuration {
			t.Fatalf("expected duration=%s, got %s", DefaultCooldownDuration, tr.cfg.cooldown.duration)
		}
	})

	t.Run("explicit duration uses default threshold", func(t *testing.T) {
		tr := mustNewTransport(t, Config{
			Endpoints: testEndpoints(endpointURL),
			Cooldown: CooldownConfig{
				Duration: 2 * time.Minute,
			},
		})

		if !tr.cfg.cooldown.enabled {
			t.Fatal("expected cooldown enabled")
		}
		if tr.cfg.cooldown.threshold != DefaultCooldownFailAfterConsecutive {
			t.Fatalf("expected threshold=%d, got %d", DefaultCooldownFailAfterConsecutive, tr.cfg.cooldown.threshold)
		}
		if tr.cfg.cooldown.duration != 2*time.Minute {
			t.Fatalf("expected duration=%s, got %s", 2*time.Minute, tr.cfg.cooldown.duration)
		}
	})

	t.Run("disabled", func(t *testing.T) {
		tr := mustNewTransport(t, Config{
			Endpoints: testEndpoints(endpointURL),
			Cooldown:  CooldownConfig{Disabled: true},
		})

		if tr.cfg.cooldown.enabled {
			t.Fatal("expected cooldown disabled")
		}
	})
}

func TestNewAdditionalTriggerStatusCodes(t *testing.T) {
	t.Run("valid status is accepted", func(t *testing.T) {
		tr := mustNewTransport(t, Config{
			Endpoints:                    testEndpoints(endpointURL),
			AdditionalTriggerStatusCodes: []int{500},
		})

		if _, ok := tr.cfg.retryableStatuses[500]; !ok {
			t.Fatal("expected normalized trigger status 500")
		}
	})

	t.Run("duplicate statuses are accepted", func(t *testing.T) {
		tr := mustNewTransport(t, Config{
			Endpoints:                    testEndpoints(endpointURL),
			AdditionalTriggerStatusCodes: []int{500, 500},
		})

		if _, ok := tr.cfg.retryableStatuses[500]; !ok {
			t.Fatal("expected normalized trigger status 500")
		}
	})
}

func TestNewLegacyConfigurationStillResolves(t *testing.T) {
	t.Run("body buffer bytes default", func(t *testing.T) {
		tr := mustNewTransport(t, Config{Endpoints: testEndpoints(endpointURL)})
		if tr.cfg.bodyCap != DefaultBodyBufferBytes {
			t.Fatalf("expected bodyCap=%d, got %d", DefaultBodyBufferBytes, tr.cfg.bodyCap)
		}
	})

	t.Run("duplicate additional non idempotent methods are accepted", func(t *testing.T) {
		tr := mustNewTransport(t, Config{
			Endpoints:                      testEndpoints(endpointURL),
			AdditionalNonIdempotentMethods: []string{"custom_send", "custom_send"},
		})
		if _, ok := tr.cfg.nonIdempotentMethods["custom_send"]; !ok {
			t.Fatal("expected custom_send to be normalized")
		}
	})
}
