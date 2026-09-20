package rcpx_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"

	"github.com/yermakovsa/rcpx"
)

func ExampleNew_failover() {
	var hits1 atomic.Int32
	var hits2 atomic.Int32

	// Upstream #1: always returns a retryable HTTP 503.
	srv1 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits1.Add(1)
		w.Header().Set("Content-Type", "text/plain")
		w.WriteHeader(http.StatusServiceUnavailable)
		w.Write([]byte("upstream unavailable"))
	}))
	defer srv1.Close()

	// Upstream #2: succeeds.
	srv2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits2.Add(1)
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"jsonrpc":"2.0","id":1,"result":"0x1"}`))
	}))
	defer srv2.Close()

	rt, err := rcpx.New(rcpx.Config{
		Endpoints: []rcpx.Endpoint{
			{ID: "primary", URL: srv1.URL},
			{ID: "backup", URL: srv2.URL},
		}, // priority order
	})
	if err != nil {
		panic(err)
	}

	httpClient := &http.Client{Transport: rt}

	// rcpx picks the upstream per attempt; the initial URL is just a placeholder.
	ctx := rcpx.WithFailoverAllowed(context.Background())
	req := jsonRPCRequest(ctx, srv1.URL, `{"jsonrpc":"2.0","id":1,"method":"eth_blockNumber","params":[]}`)

	resp, err := httpClient.Do(req)
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()

	var out struct {
		Result string `json:"result"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		panic(err)
	}

	fmt.Printf("server1=%d server2=%d result=%s\n", hits1.Load(), hits2.Load(), out.Result)

	// Output:
	// server1=1 server2=1 result=0x1
}

func ExampleAllUpstreamsFailedError() {
	var hits1 atomic.Int32
	var hits2 atomic.Int32

	// Both upstreams return retryable 503, so the request will exhaust all upstreams.
	srv1 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits1.Add(1)
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer srv1.Close()

	srv2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits2.Add(1)
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer srv2.Close()

	rt, err := rcpx.New(rcpx.Config{
		Endpoints: []rcpx.Endpoint{
			{ID: "primary", URL: srv1.URL},
			{ID: "backup", URL: srv2.URL},
		},
	})
	if err != nil {
		panic(err)
	}

	httpClient := &http.Client{Transport: rt}

	ctx := rcpx.WithFailoverAllowed(context.Background())
	req := jsonRPCRequest(ctx, srv1.URL, `{"jsonrpc":"2.0","id":1,"method":"eth_blockNumber","params":[]}`)
	_, err = httpClient.Do(req)
	if err == nil {
		panic("expected error")
	}

	var ae *rcpx.AllUpstreamsFailedError
	if !errors.As(err, &ae) {
		panic("unexpected error type")
	}
	fmt.Printf("attempted=%d skippedCooldown=%d failures=%d\n", ae.Attempted, ae.SkippedCooldown, len(ae.Failures))
	fmt.Printf("server1=%d server2=%d\n", hits1.Load(), hits2.Load())
	// Output:
	// attempted=2 skippedCooldown=0 failures=2
	// server1=1 server2=1
}

func ExampleWithFailoverAllowed() {
	var hits1 atomic.Int32
	var hits2 atomic.Int32

	// Upstream #1 fails in a retryable way.
	srv1 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits1.Add(1)
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer srv1.Close()

	// Upstream #2 succeeds with a JSON-RPC response.
	srv2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits2.Add(1)
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"jsonrpc":"2.0","id":1,"result":"0xdeadbeef"}`))
	}))
	defer srv2.Close()

	rt, err := rcpx.New(rcpx.Config{
		Endpoints: []rcpx.Endpoint{
			{ID: "primary", URL: srv1.URL},
			{ID: "backup", URL: srv2.URL},
		},
	})
	if err != nil {
		panic(err)
	}

	httpClient := &http.Client{Transport: rt}

	ctx := rcpx.WithFailoverAllowed(context.Background())
	req := jsonRPCRequest(ctx, srv1.URL, `{"jsonrpc":"2.0","id":1,"method":"eth_sendRawTransaction","params":["0xdeadbeef"]}`)

	resp, err := httpClient.Do(req)
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()

	var out struct {
		Result string `json:"result"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		panic(err)
	}

	fmt.Printf("server1=%d server2=%d result=%s\n", hits1.Load(), hits2.Load(), out.Result)
	// Output:
	// server1=1 server2=1 result=0xdeadbeef
}

func ExampleNew_cooldownDisabled() {
	var hits1 atomic.Int32
	var hits2 atomic.Int32

	// Upstream #1: always returns a retryable HTTP 503.
	srv1 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits1.Add(1)
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer srv1.Close()

	// Upstream #2: always succeeds.
	srv2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits2.Add(1)
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"jsonrpc":"2.0","id":1,"result":"0x1"}`))
	}))
	defer srv2.Close()

	rt, err := rcpx.New(rcpx.Config{
		Endpoints: []rcpx.Endpoint{
			{ID: "primary", URL: srv1.URL},
			{ID: "backup", URL: srv2.URL},
		},
		Cooldown: rcpx.CooldownConfig{Disabled: true},
	})
	if err != nil {
		panic(err)
	}

	httpClient := &http.Client{Transport: rt}

	doCall := func() error {
		ctx := rcpx.WithFailoverAllowed(context.Background())
		req := jsonRPCRequest(ctx, srv1.URL, `{"jsonrpc":"2.0","id":1,"method":"eth_blockNumber","params":[]}`)
		resp, err := httpClient.Do(req)
		if err != nil {
			return err
		}
		_ = resp.Body.Close()
		return nil
	}

	// With cooldown disabled, rcpx still tries srv1 first on every request.
	if err := doCall(); err != nil {
		panic(err)
	}
	if err := doCall(); err != nil {
		panic(err)
	}

	fmt.Printf("server1=%d server2=%d\n", hits1.Load(), hits2.Load())
	// Output:
	// server1=2 server2=2
}

func ExampleNew_customRetryPolicy() {
	var hits1 atomic.Int32
	var hits2 atomic.Int32
	var policyCalls atomic.Int32

	// Upstream #1: retryable failure (503).
	srv1 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits1.Add(1)
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer srv1.Close()

	// Upstream #2: would succeed, but policy will prevent failover.
	srv2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits2.Add(1)
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"jsonrpc":"2.0","id":1,"result":"0x1"}`))
	}))
	defer srv2.Close()

	rt, err := rcpx.New(rcpx.Config{
		Endpoints: []rcpx.Endpoint{
			{ID: "primary", URL: srv1.URL},
			{ID: "backup", URL: srv2.URL},
		},
		RetryPolicy: func(out rcpx.AttemptOutcome) bool {
			policyCalls.Add(1)
			return false // stop after the first failure
		},
	})
	if err != nil {
		panic(err)
	}

	httpClient := &http.Client{Transport: rt}

	ctx := rcpx.WithFailoverAllowed(context.Background())
	req := jsonRPCRequest(ctx, srv1.URL, `{"jsonrpc":"2.0","id":1,"method":"eth_blockNumber","params":[]}`)

	_, err = httpClient.Do(req)
	if err == nil {
		panic("expected error")
	}

	var ae *rcpx.AllUpstreamsFailedError
	if !errors.As(err, &ae) {
		panic("expected AllUpstreamsFailedError")
	}

	fmt.Printf("policyCalls=%d attempted=%d server1=%d server2=%d\n",
		policyCalls.Load(), ae.Attempted, hits1.Load(), hits2.Load(),
	)

	// Output:
	// policyCalls=1 attempted=1 server1=1 server2=0
}

func jsonRPCRequest(ctx context.Context, url, body string) *http.Request {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewBufferString(body))
	if err != nil {
		panic(err)
	}
	req.Header.Set("Content-Type", "application/json")
	return req
}
