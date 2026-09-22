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

	fmt.Println("== FailoverError (no HTTP response available) ==")
	demoFailoverError(timeout, endpoints)
}

func demoFailoverError(timeout time.Duration, endpoints []rcpx.Endpoint) {
	rpcClient, err := dialRPC(timeout, endpoints)
	if err != nil {
		fmt.Fprintf(os.Stderr, "setup: %v\n", err)
		return
	}
	defer rpcClient.Close()

	ec := ethclient.NewClient(rpcClient)

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	_, err = ec.BlockNumber(rcpx.WithFailoverAllowed(ctx))
	if err == nil {
		fmt.Println("unexpected: call succeeded")
		return
	}

	var fe *rcpx.FailoverError
	if !errors.As(err, &fe) {
		fmt.Printf("unexpected error type: %T: %v\n", err, err)
		return
	}

	fmt.Printf("attempts=%d\n", len(fe.Attempts))
	for i, attempt := range fe.Attempts {
		fmt.Printf("  #%d endpoint=%s err=%v\n", i+1, attempt.Endpoint, attempt.Err)
	}

	fmt.Printf("errors.Is(ErrNoUsableEndpoint)=%v errors.Is(context.Canceled)=%v\n",
		errors.Is(err, rcpx.ErrNoUsableEndpoint),
		errors.Is(err, context.Canceled),
	)
}

func dialRPC(timeout time.Duration, endpoints []rcpx.Endpoint) (*rpc.Client, error) {
	rt, err := rcpx.New(rcpx.Config{
		Endpoints: endpoints,
		Base:      http.DefaultTransport,
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
