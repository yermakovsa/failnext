package rcpx

import (
	"net/http"
	"testing"
)

func TestRoundTrip_CustomHostIsPreserved(t *testing.T) {
	const (
		logicalURL  = "https://logical.test/request"
		endpointURL = "https://endpoint.test/rpc"
		customHost  = "signed.example"
	)

	base := roundTripperFunc(func(req *http.Request) (*http.Response, error) {
		if got := req.URL.String(); got != endpointURL {
			t.Fatalf("expected physical URL %q, got %q", endpointURL, got)
		}
		if got := req.Host; got != customHost {
			t.Fatalf("expected custom Host %q, got %q", customHost, got)
		}
		return httpResp(http.StatusOK, "ok"), nil
	})
	tr := mustNewTransport(t, Config{
		Endpoints: []Endpoint{{ID: "primary", URL: endpointURL}},
		Base:      base,
	})

	req, err := http.NewRequest(http.MethodGet, logicalURL, nil)
	if err != nil {
		t.Fatalf("http.NewRequest: %v", err)
	}
	req.Host = customHost
	if req.URL == nil || req.Host == req.URL.Host {
		t.Fatalf("test setup requires custom Host to differ from logical URL host")
	}

	resp, err := tr.RoundTrip(req)
	if err != nil {
		t.Fatalf("RoundTrip error: %v", err)
	}
	resp.Body.Close()
}

func TestRoundTrip_EmptyHostRemainsEmpty(t *testing.T) {
	const (
		logicalURL  = "https://logical.test/request"
		endpointURL = "https://endpoint.test/rpc"
	)

	base := roundTripperFunc(func(req *http.Request) (*http.Response, error) {
		if got := req.URL.String(); got != endpointURL {
			t.Fatalf("expected physical URL %q, got %q", endpointURL, got)
		}
		if req.Host != "" {
			t.Fatalf("expected empty physical Host, got %q", req.Host)
		}
		return httpResp(http.StatusOK, "ok"), nil
	})
	tr := mustNewTransport(t, Config{
		Endpoints: []Endpoint{{ID: "primary", URL: endpointURL}},
		Base:      base,
	})

	req, err := http.NewRequest(http.MethodGet, logicalURL, nil)
	if err != nil {
		t.Fatalf("http.NewRequest: %v", err)
	}
	req.Host = ""

	resp, err := tr.RoundTrip(req)
	if err != nil {
		t.Fatalf("RoundTrip error: %v", err)
	}
	resp.Body.Close()
}
