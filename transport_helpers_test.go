package rcpx

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"
)

type trackingBody struct {
	mu     sync.Mutex
	closed bool
	r      io.Reader
}

func (b *trackingBody) Read(p []byte) (int, error) { return b.r.Read(p) }

func (b *trackingBody) Close() error {
	b.mu.Lock()
	b.closed = true
	b.mu.Unlock()
	return nil
}

func (b *trackingBody) Closed() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.closed
}

type rtResult struct {
	resp *http.Response
	err  error
}

type scriptRT struct {
	mu    sync.Mutex
	calls []string

	// keyed by full URL string
	results map[string][]rtResult
}

type roundTripperFunc func(*http.Request) (*http.Response, error)

func (f roundTripperFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

type policyRecorder struct {
	calls int
	ret   bool
}

func (s *scriptRT) RoundTrip(req *http.Request) (*http.Response, error) {
	s.mu.Lock()
	s.calls = append(s.calls, req.URL.String())
	queue := s.results[req.URL.String()]
	if len(queue) == 0 {
		s.mu.Unlock()
		return nil, errors.New("unexpected call: " + req.URL.String())
	}
	res := queue[0]
	s.results[req.URL.String()] = queue[1:]
	s.mu.Unlock()

	if res.resp != nil {
		res.resp.Request = req
		if res.resp.Header == nil {
			res.resp.Header = make(http.Header)
		}
	}
	return res.resp, res.err
}

func (s *scriptRT) Calls() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]string, len(s.calls))
	copy(out, s.calls)
	return out
}

// Builders

func newTrackingBody(s string) *trackingBody {
	return &trackingBody{r: strings.NewReader(s)}
}

func newPolicyRecorder(ret bool) (*policyRecorder, func(AttemptOutcome) bool) {
	pr := &policyRecorder{ret: ret}
	return pr, func(out AttemptOutcome) bool {
		pr.calls++
		return pr.ret
	}
}

func newRPCRequest(t *testing.T, url, method string) *http.Request {
	t.Helper()

	body := fmt.Sprintf(`{"jsonrpc":"2.0","id":1,"method":%q,"params":[]}`, method)
	return newJSONRequest(t, url, body)
}

func newJSONRequest(t *testing.T, url, body string) *http.Request {
	t.Helper()

	req, err := http.NewRequest("POST", url, strings.NewReader(body))
	if err != nil {
		t.Fatalf("http.NewRequest: %v", err)
	}
	return req
}

func newHTTPResp(code int, body string) *http.Response {
	return &http.Response{
		StatusCode: code,
		Body:       io.NopCloser(strings.NewReader(body)),
	}
}

// httpResp is kept to minimize churn across tests. Prefer newHTTPResp in new code.
func httpResp(code int, body string) *http.Response { return newHTTPResp(code, body) }

// Transport constructor

func testEndpoints(urls ...string) []Endpoint {
	endpoints := make([]Endpoint, len(urls))
	for i, url := range urls {
		endpoints[i] = Endpoint{
			ID:  EndpointID(fmt.Sprintf("endpoint-%d", i+1)),
			URL: url,
		}
	}
	return endpoints
}

func mustNewTransport(t *testing.T, cfg Config) *Transport {
	t.Helper()

	tr, err := New(cfg)
	if err != nil {
		t.Fatalf("New error: %v", err)
	}

	// Keep time deterministic for any internal cooldown bookkeeping.
	tr.now = func() time.Time { return time.Unix(1, 0) }

	return tr
}

// Assertions

func assertStatus(t *testing.T, resp *http.Response, want int) {
	t.Helper()

	if resp == nil {
		t.Fatalf("expected HTTP %d, got nil response", want)
	}
	if resp.StatusCode != want {
		t.Fatalf("expected HTTP %d, got %d", want, resp.StatusCode)
	}
}

func assertCalls(t *testing.T, base *scriptRT, want ...string) {
	t.Helper()

	got := base.Calls()
	if len(got) != len(want) {
		t.Fatalf("unexpected base call count: got=%v want=%v", got, want)
	}
	for i := range got {
		if got[i] != want[i] {
			t.Fatalf("unexpected base calls: got=%v want=%v", got, want)
		}
	}
}

func assertPolicyCalls(t *testing.T, pr *policyRecorder, want int) {
	t.Helper()

	if pr.calls != want {
		t.Fatalf("unexpected policy call count: got=%d want=%d", pr.calls, want)
	}
}

// Must helpers

func mustRoundTrip(t *testing.T, rt http.RoundTripper, req *http.Request) *http.Response {
	t.Helper()

	resp, err := rt.RoundTrip(req)
	if err != nil {
		t.Fatalf("RoundTrip error: %v", err)
	}
	if resp == nil {
		t.Fatalf("expected non nil response")
	}

	t.Cleanup(func() { resp.Body.Close() })
	return resp
}

func mustRoundTripCode(t *testing.T, rt http.RoundTripper, req *http.Request, want int) {
	t.Helper()

	resp, err := rt.RoundTrip(req)
	if err != nil {
		t.Fatalf("RoundTrip error: %v", err)
	}
	assertStatus(t, resp, want)
	resp.Body.Close()
}

func mustAsAllUpstreamsFailed(t *testing.T, err error) *AllUpstreamsFailedError {
	t.Helper()

	var ae *AllUpstreamsFailedError
	if !errors.As(err, &ae) {
		t.Fatalf("expected AllUpstreamsFailedError, got %v", err)
	}
	return ae
}
