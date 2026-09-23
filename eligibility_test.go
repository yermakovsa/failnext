package failnext

import (
	"io"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestRoundTrip_NilEligibilityUsesConfiguredOrder(t *testing.T) {
	u1 := "https://u1.test/rpc"
	u2 := "https://u2.test/rpc"
	u3 := "https://u3.test/rpc"

	base := &scriptRT{
		results: map[string][]rtResult{
			u1: {{err: io.EOF}},
			u2: {{err: io.EOF}},
			u3: {{resp: httpResp(http.StatusOK, "ok")}},
		},
	}
	rt := mustNewTransport(t, Config{
		Endpoints: testEndpoints(u1, u2, u3),
		Base:      base,
	})

	req := newTestGETRequest(t, u1)
	mustRoundTripCode(t, rt, req, http.StatusOK)

	assertCalls(t, base, u1, u2, u3)
}

func TestRoundTrip_EligibilityCapturedOncePerEndpoint(t *testing.T) {
	u1 := "https://u1.test/rpc"
	u2 := "https://u2.test/rpc"

	var endpoint1Calls atomic.Int32
	var endpoint2Calls atomic.Int32
	var endpoint2Eligible atomic.Bool
	endpoint2Eligible.Store(true)

	base := roundTripperFunc(func(req *http.Request) (*http.Response, error) {
		switch req.URL.String() {
		case u1:
			endpoint2Eligible.Store(false)
			return nil, io.EOF
		case u2:
			return httpResp(http.StatusOK, "ok"), nil
		default:
			return nil, io.ErrUnexpectedEOF
		}
	})
	rt := mustNewTransport(t, Config{
		Endpoints: testEndpoints(u1, u2),
		Base:      base,
		Eligible: func(id EndpointID) bool {
			switch id {
			case "endpoint-1":
				endpoint1Calls.Add(1)
				return true
			case "endpoint-2":
				endpoint2Calls.Add(1)
				return endpoint2Eligible.Load()
			default:
				t.Fatalf("unexpected endpoint id %q", id)
				return false
			}
		},
	})

	req := newTestGETRequest(t, u1)
	mustRoundTripCode(t, rt, req, http.StatusOK)

	if got := endpoint1Calls.Load(); got != 1 {
		t.Fatalf("expected endpoint-1 eligibility calls=1, got %d", got)
	}
	if got := endpoint2Calls.Load(); got != 1 {
		t.Fatalf("expected endpoint-2 eligibility calls=1, got %d", got)
	}
}

func TestRoundTrip_EligibilityFilteringPreservesConfiguredOrder(t *testing.T) {
	u1 := "https://u1.test/rpc"
	u2 := "https://u2.test/rpc"
	u3 := "https://u3.test/rpc"
	u4 := "https://u4.test/rpc"

	base := &scriptRT{
		results: map[string][]rtResult{
			u2: {{err: io.EOF}},
			u4: {{resp: httpResp(http.StatusOK, "ok")}},
		},
	}
	rt := mustNewTransport(t, Config{
		Endpoints: testEndpoints(u1, u2, u3, u4),
		Base:      base,
		Eligible: func(id EndpointID) bool {
			return id == "endpoint-2" || id == "endpoint-4"
		},
	})

	req := newTestGETRequest(t, u1)
	mustRoundTripCode(t, rt, req, http.StatusOK)

	assertCalls(t, base, u2, u4)
}

func TestRoundTrip_EligibilityMayRunConcurrently(t *testing.T) {
	u1 := "https://u1.test/rpc"

	entered := make(chan struct{}, 2)
	release := make(chan struct{})
	var eligibilityCalls atomic.Int32

	rt := mustNewTransport(t, Config{
		Endpoints: testEndpoints(u1),
		Base: roundTripperFunc(func(req *http.Request) (*http.Response, error) {
			return httpResp(http.StatusOK, "ok"), nil
		}),
		Eligible: func(EndpointID) bool {
			eligibilityCalls.Add(1)
			entered <- struct{}{}
			<-release
			return true
		},
	})

	requests := []*http.Request{
		newTestGETRequest(t, u1),
		newTestGETRequest(t, u1),
	}
	errCh := make(chan error, len(requests))
	var wg sync.WaitGroup
	for _, req := range requests {
		wg.Add(1)
		go func(req *http.Request) {
			defer wg.Done()
			resp, err := rt.RoundTrip(req)
			if resp != nil {
				resp.Body.Close()
			}
			errCh <- err
		}(req)
	}

	for i := 0; i < len(requests); i++ {
		select {
		case <-entered:
		case <-time.After(time.Second):
			close(release)
			wg.Wait()
			t.Fatal("Eligible calls were serialized; expected both logical requests to enter concurrently")
		}
	}
	close(release)
	wg.Wait()
	close(errCh)

	for err := range errCh {
		if err != nil {
			t.Fatalf("RoundTrip error: %v", err)
		}
	}
	if got := eligibilityCalls.Load(); got != int32(len(requests)) {
		t.Fatalf("expected Eligible calls=%d, got %d", len(requests), got)
	}
}

func TestRoundTrip_EligibilityCallbackRunsOutsideCooldownLock(t *testing.T) {
	u1 := "https://u1.test/rpc"

	var rt *Transport
	rt = mustNewTransport(t, Config{
		Endpoints: testEndpoints(u1),
		Base: roundTripperFunc(func(req *http.Request) (*http.Response, error) {
			return httpResp(http.StatusOK, "ok"), nil
		}),
		Eligible: func(EndpointID) bool {
			return rt.cooldown.eligible(rt.now(), 0)
		},
	})

	done := make(chan error, 1)
	go func() {
		resp, err := rt.RoundTrip(newTestGETRequest(t, u1))
		if resp != nil {
			resp.Body.Close()
		}
		done <- err
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("RoundTrip error: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Eligible appears to run while the cooldown lock is held")
	}
}

func newTestGETRequest(t *testing.T, url string) *http.Request {
	t.Helper()

	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		t.Fatalf("http.NewRequest: %v", err)
	}
	return req
}
