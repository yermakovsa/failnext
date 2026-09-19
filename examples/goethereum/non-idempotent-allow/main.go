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

	// AllowNonIdempotent=true lets rcpx fail over even for write methods.
	// This can duplicate side effects. We use closed local ports so nothing is sent.
	endpoints := []rcpx.Endpoint{
		{ID: "primary", URL: "http://127.0.0.1:65534"},
		{ID: "backup", URL: "http://127.0.0.1:65533"},
	}

	rt, err := rcpx.New(rcpx.Config{
		Endpoints:          endpoints,
		Base:               http.DefaultTransport,
		AllowNonIdempotent: true, // explicit opt-in
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

	var txHash string
	err = rpcClient.CallContext(ctx, &txHash, "eth_sendRawTransaction", "0xdeadbeef")
	if err == nil {
		fmt.Println("unexpected: call succeeded")
		return
	}

	var ae *rcpx.AllUpstreamsFailedError
	if errors.As(err, &ae) {
		fmt.Printf("attempted=%d skippedCooldown=%d failures=%d\n",
			ae.Attempted, ae.SkippedCooldown, len(ae.Failures))

		for i, f := range ae.Failures {
			fmt.Printf("  #%d upstream=%s status=%d retryable=%v err=%v\n",
				i+1, f.Upstream, f.StatusCode, f.Retryable, f.Err)
		}
		return
	}

	var blocked *rcpx.NonIdempotentBlockedError
	if errors.As(err, &blocked) {
		fmt.Printf("unexpected: still blocked method=%s cause=%v\n", blocked.Outcome.Method, blocked.Unwrap())
		return
	}

	fmt.Printf("unexpected error type: %T: %v\n", err, err)
}
