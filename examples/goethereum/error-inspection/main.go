package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/ethereum/go-ethereum/rpc"

	"github.com/yermakovsa/rcpx"
)

func main() {
	timeout := 5 * time.Second

	// Deterministic demo: both endpoints are closed local ports.
	// If one is unexpectedly open on your machine, change the ports.
	endpoints := []rcpx.Endpoint{
		{ID: "primary", URL: "http://127.0.0.1:65534"},
		{ID: "backup", URL: "http://127.0.0.1:65533"},
	}

	fmt.Println("== demo 1: AllUpstreamsFailedError (exhaust all upstreams) ==")
	demoAllUpstreamsFailed(timeout, endpoints)

	fmt.Println()
	fmt.Println("== demo 2: NonIdempotentBlockedError (no failover for writes by default) ==")
	demoNonIdempotentBlocked(timeout, endpoints)
}

func demoAllUpstreamsFailed(timeout time.Duration, endpoints []rcpx.Endpoint) {
	rpcClient, err := dialRPC(timeout, endpoints, false /* allowNonIdempotent */)
	if err != nil {
		fmt.Fprintf(os.Stderr, "setup: %v\n", err)
		return
	}
	defer rpcClient.Close()

	ec := ethclient.NewClient(rpcClient)

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	_, err = ec.BlockNumber(ctx)
	if err == nil {
		fmt.Println("unexpected: call succeeded")
		return
	}

	var ae *rcpx.AllUpstreamsFailedError
	if !errors.As(err, &ae) {
		fmt.Printf("unexpected error type: %T: %v\n", err, err)
		return
	}

	fmt.Printf("attempted=%d skippedCooldown=%d failures=%d\n",
		ae.Attempted, ae.SkippedCooldown, len(ae.Failures))

	for i, f := range ae.Failures {
		fmt.Printf("  #%d upstream=%s status=%d retryable=%v err=%v\n",
			i+1, f.Upstream, f.StatusCode, f.Retryable, f.Err)
	}

	fmt.Printf("errors.Is(ErrNoEligibleUpstreams)=%v errors.Is(context.Canceled)=%v\n",
		errors.Is(err, rcpx.ErrNoEligibleUpstreams),
		errors.Is(err, context.Canceled),
	)
}

func demoNonIdempotentBlocked(timeout time.Duration, endpoints []rcpx.Endpoint) {
	rpcClient, err := dialRPC(timeout, endpoints, false /* allowNonIdempotent */)
	if err != nil {
		fmt.Fprintf(os.Stderr, "setup: %v\n", err)
		return
	}
	defer rpcClient.Close()

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	// Dummy payload. rcpx may attempt once, but won't retry/fail over for writes by default.
	var txHash string
	err = rpcClient.CallContext(ctx, &txHash, "eth_sendRawTransaction", "0xdeadbeef")
	if err == nil {
		fmt.Println("unexpected: call succeeded")
		return
	}

	var be *rcpx.NonIdempotentBlockedError
	if !errors.As(err, &be) {
		fmt.Printf("unexpected error type: %T: %v\n", err, err)
		return
	}

	fmt.Printf("blocked method=%s retryableByDefault=%v\n", be.Outcome.Method, be.Outcome.RetryableByDefault)
	fmt.Printf("cause=%v\n", be.Unwrap())
}

func dialRPC(timeout time.Duration, endpoints []rcpx.Endpoint, allowNonIdempotent bool) (*rpc.Client, error) {
	rt, err := rcpx.New(rcpx.Config{
		Endpoints:          endpoints,
		Base:               http.DefaultTransport,
		AllowNonIdempotent: allowNonIdempotent,
	})
	if err != nil {
		return nil, err
	}

	httpClient := &http.Client{
		Timeout:   timeout,
		Transport: rt,
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	return rpc.DialOptions(ctx, endpoints[0].URL, rpc.WithHTTPClient(httpClient))
}
