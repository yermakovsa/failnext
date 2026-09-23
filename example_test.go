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

	// Endpoint #1: always returns a failover-triggering HTTP 503.
	srv1 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits1.Add(1)
		w.Header().Set("Content-Type", "text/plain")
		w.WriteHeader(http.StatusServiceUnavailable)
		w.Write([]byte("endpoint unavailable"))
	}))
	defer srv1.Close()

	// Endpoint #2: succeeds.
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

	// JSON-RPC reads use POST, so allow this logical read to cross endpoints.
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

func ExampleFailoverError() {
	errPrimary := errors.New("primary transport failure")
	errBackup := errors.New("backup transport failure")

	rt, err := rcpx.New(rcpx.Config{
		Endpoints: []rcpx.Endpoint{
			{ID: "primary", URL: "https://primary.example/rpc"},
			{ID: "backup", URL: "https://backup.example/rpc"},
		},
		Base: roundTripperFunc(func(req *http.Request) (*http.Response, error) {
			switch req.URL.Host {
			case "primary.example":
				return nil, errPrimary
			case "backup.example":
				return nil, errBackup
			default:
				return nil, fmt.Errorf("unexpected destination %s", req.URL)
			}
		}),
	})
	if err != nil {
		panic(err)
	}

	req, err := http.NewRequest(http.MethodGet, "https://logical.example/rpc", nil)
	if err != nil {
		panic(err)
	}
	_, err = rt.RoundTrip(req)

	var fe *rcpx.FailoverError
	if !errors.As(err, &fe) {
		panic("expected FailoverError")
	}
	fmt.Printf("attempts=%d first=%s last=%s final-is-backup=%v\n",
		len(fe.Attempts),
		fe.Attempts[0].Endpoint,
		fe.Attempts[1].Endpoint,
		errors.Is(err, errBackup),
	)
	// Output:
	// attempts=2 first=primary last=backup final-is-backup=true
}

func ExampleWithFailoverAllowed() {
	var hits1 atomic.Int32
	var hits2 atomic.Int32

	// Endpoint #1 returns a failover-triggering 503.
	srv1 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits1.Add(1)
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer srv1.Close()

	// Endpoint #2 succeeds with a JSON-RPC response.
	srv2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits2.Add(1)
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"jsonrpc":"2.0","id":1,"result":"0x2a"}`))
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

	// The operation is a read, but JSON-RPC uses POST. Mark this logical read as
	// safe to continue across configured endpoints.
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
	// server1=1 server2=1 result=0x2a
}

func ExampleWithFailoverDenied() {
	var physicalAttempts atomic.Int32
	primaryErr := errors.New("primary transport failure")

	rt, err := rcpx.New(rcpx.Config{
		Endpoints: []rcpx.Endpoint{
			{ID: "primary", URL: "https://primary.example/rpc"},
			{ID: "backup", URL: "https://backup.example/rpc"},
		},
		Base: roundTripperFunc(func(req *http.Request) (*http.Response, error) {
			physicalAttempts.Add(1)
			if req.Body != nil {
				_ = req.Body.Close()
			}
			if req.URL.Host == "primary.example" {
				return nil, primaryErr
			}
			return nil, errors.New("backup should not be attempted")
		}),
	})
	if err != nil {
		panic(err)
	}

	// A broad parent context may allow failover for surrounding reads. Derive an
	// explicit deny for a write so the write cannot cross endpoints.
	parent := rcpx.WithFailoverAllowed(context.Background())
	writeCtx := rcpx.WithFailoverDenied(parent)
	req := jsonRPCRequest(writeCtx, "https://logical.example/rpc", `{"jsonrpc":"2.0","id":1,"method":"eth_sendRawTransaction","params":["0xdeadbeef"]}`)

	_, err = rt.RoundTrip(req)
	var fe *rcpx.FailoverError
	if !errors.As(err, &fe) {
		panic("expected FailoverError")
	}

	fmt.Printf("physical-attempts=%d recorded-attempts=%d\n", physicalAttempts.Load(), len(fe.Attempts))
	// Output:
	// physical-attempts=1 recorded-attempts=1
}

func ExampleNew_cooldownDisabled() {
	var hits1 atomic.Int32
	var hits2 atomic.Int32

	// Endpoint #1: always returns a failover-triggering HTTP 503.
	srv1 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits1.Add(1)
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer srv1.Close()

	// Endpoint #2: always succeeds.
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

func jsonRPCRequest(ctx context.Context, url, body string) *http.Request {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewBufferString(body))
	if err != nil {
		panic(err)
	}
	req.Header.Set("Content-Type", "application/json")
	return req
}

type roundTripperFunc func(*http.Request) (*http.Response, error)

func (f roundTripperFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}
