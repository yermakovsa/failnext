package rcpx

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestHTTPClient_DoUsesTransportFailover(t *testing.T) {
	u1 := "https://u1.test/rpc"
	u2 := "https://u2.test/rpc"

	triggerBody := newTrackingBody("unavailable")
	finalBody := newTrackingBody("ok")
	base := &scriptRT{results: map[string][]rtResult{
		u1: {{resp: &http.Response{StatusCode: http.StatusServiceUnavailable, Body: triggerBody}}},
		u2: {{resp: &http.Response{StatusCode: http.StatusOK, Body: finalBody}}},
	}}
	tr := mustNewTransport(t, Config{
		Endpoints: testEndpoints(u1, u2),
		Base:      base,
	})
	client := &http.Client{Transport: tr}

	req, err := http.NewRequest(http.MethodGet, "https://logical.test/request", nil)
	if err != nil {
		t.Fatalf("http.NewRequest: %v", err)
	}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("Client.Do error: %v", err)
	}
	t.Cleanup(func() { resp.Body.Close() })

	assertStatus(t, resp, http.StatusOK)
	assertCalls(t, base, u1, u2)
	if !triggerBody.Closed() {
		t.Fatal("expected superseded trigger response body to be closed")
	}
	if finalBody.Closed() {
		t.Fatal("expected returned response body to remain open for caller")
	}
}

type clientCloseIdleTrackingRT struct {
	closeCalls     int
	roundTripCalls int
}

func (r *clientCloseIdleTrackingRT) RoundTrip(*http.Request) (*http.Response, error) {
	r.roundTripCalls++
	return nil, errors.New("unexpected RoundTrip")
}

func (r *clientCloseIdleTrackingRT) CloseIdleConnections() {
	r.closeCalls++
}

func TestHTTPClient_CloseIdleConnectionsForwardsThroughTransport(t *testing.T) {
	base := &clientCloseIdleTrackingRT{}
	tr := mustNewTransport(t, Config{
		Endpoints: testEndpoints("https://u1.test/rpc"),
		Base:      base,
	})
	client := &http.Client{Transport: tr}

	client.CloseIdleConnections()

	if base.closeCalls != 1 {
		t.Fatalf("expected CloseIdleConnections calls=1, got %d", base.closeCalls)
	}
	if base.roundTripCalls != 0 {
		t.Fatalf("expected RoundTrip calls=0, got %d", base.roundTripCalls)
	}
}

func TestHTTPClient_URLDerivedHostFollowsSelectedEndpointAuthority(t *testing.T) {
	var primaryHost string
	var backupHost string

	primary := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		primaryHost = r.Host
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	t.Cleanup(primary.Close)

	backup := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		backupHost = r.Host
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	}))
	t.Cleanup(backup.Close)

	baseHTTP := http.DefaultTransport.(*http.Transport).Clone()
	t.Cleanup(baseHTTP.CloseIdleConnections)

	tr := mustNewTransport(t, Config{
		Endpoints: []Endpoint{
			{ID: "primary", URL: primary.URL},
			{ID: "backup", URL: backup.URL},
		},
		Base: baseHTTP,
	})
	client := &http.Client{Transport: tr}

	req, err := http.NewRequest(
		http.MethodPost,
		primary.URL,
		strings.NewReader(`{"jsonrpc":"2.0","id":1,"method":"eth_blockNumber","params":[]}`),
	)
	if err != nil {
		t.Fatalf("http.NewRequest: %v", err)
	}
	if req.Host == "" {
		t.Fatal("expected logical request Host to be populated from the request URL")
	}
	if req.Host != req.URL.Host {
		t.Fatalf("expected logical request Host=%q, got %q", req.URL.Host, req.Host)
	}
	if req.GetBody == nil {
		t.Fatal("expected request body to be replayable")
	}
	req = req.WithContext(WithFailoverAllowed(req.Context()))

	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("Client.Do error: %v", err)
	}
	t.Cleanup(func() { resp.Body.Close() })

	assertStatus(t, resp, http.StatusOK)
	if primaryHost != primary.Listener.Addr().String() {
		t.Fatalf(
			"expected primary Host=%q, got %q",
			primary.Listener.Addr().String(),
			primaryHost,
		)
	}
	if backupHost != backup.Listener.Addr().String() {
		t.Fatalf(
			"expected backup Host=%q, got %q",
			backup.Listener.Addr().String(),
			backupHost,
		)
	}
	if backupHost == primaryHost {
		t.Fatalf(
			"expected backup Host to follow backup authority, got primary Host %q",
			backupHost,
		)
	}
}
