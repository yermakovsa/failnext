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

	// This example deliberately allows one write operation to cross endpoints.
	// That can duplicate side effects. Closed local ports keep the demo harmless.
	endpoints := []rcpx.Endpoint{
		{ID: "primary", URL: "http://127.0.0.1:65534"},
		{ID: "backup", URL: "http://127.0.0.1:65533"},
	}

	rt, err := rcpx.New(rcpx.Config{
		Endpoints: endpoints,
		Base:      http.DefaultTransport,
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

	callCtx := rcpx.WithFailoverAllowed(ctx)
	var txHash string
	err = rpcClient.CallContext(callCtx, &txHash, "eth_sendRawTransaction", "0xdeadbeef")
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

	fmt.Printf("unexpected error type: %T: %v\n", err, err)
}
