package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/ethereum/go-ethereum/rpc"

	"github.com/yermakovsa/rcpx"
)

func main() {
	timeout := 5 * time.Second

	// Deterministic demo: both endpoints are closed local ports.
	endpoints := []rcpx.Endpoint{
		{ID: "primary", URL: "http://127.0.0.1:65534"},
		{ID: "backup", URL: "http://127.0.0.1:65533"},
	}

	rt, err := rcpx.New(rcpx.Config{
		Endpoints: endpoints,
		Base:      http.DefaultTransport,
		// AllowNonIdempotent defaults to false (safety): no retry/failover for write methods.
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "create rcpx transport: %v\n", err)
		os.Exit(1)
	}

	httpClient := &http.Client{
		Timeout:   timeout,
		Transport: rt,
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	rpcClient, err := rpc.DialOptions(ctx, endpoints[0].URL, rpc.WithHTTPClient(httpClient))
	if err != nil {
		fmt.Fprintf(os.Stderr, "dial rpc: %v\n", err)
		os.Exit(1)
	}
	defer rpcClient.Close()

	// Non-idempotent call. rcpx may try it once, but won't retry/fail over by default.
	var txHash string
	err = rpcClient.CallContext(ctx, &txHash, "eth_sendRawTransaction", "0xdeadbeef")
	if err == nil {
		fmt.Println("unexpected: call succeeded")
		return
	}

	var blocked *rcpx.NonIdempotentBlockedError
	if !errors.As(err, &blocked) {
		fmt.Printf("unexpected error type: %T: %v\n", err, err)
		return
	}

	fmt.Printf("blocked method=%s retryableByDefault=%v\n", blocked.Outcome.Method, blocked.Outcome.RetryableByDefault)
	fmt.Printf("cause=%v\n", blocked.Unwrap())
	fmt.Println("hint: AllowNonIdempotent=true enables failover for writes (use with care; it can duplicate side effects)")
}
