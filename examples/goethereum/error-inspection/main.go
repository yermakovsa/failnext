package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/ethereum/go-ethereum/rpc"

	"github.com/yermakovsa/failnext"
)

func main() {
	// Both endpoints are intentionally unavailable so every attempt fails.
	endpoints := []failnext.Endpoint{
		{ID: "primary", URL: "http://127.0.0.1:65534"},
		{ID: "backup", URL: "http://127.0.0.1:65533"},
	}

	tr, err := failnext.New(failnext.Config{
		Endpoints: endpoints,

		OnEvent: func(_ context.Context, event failnext.Event) {
			if event.Kind == failnext.EventAttempt && event.Err != nil {
				fmt.Printf(
					"attempt %d: %s failed\n",
					event.Attempt,
					event.Endpoint,
				)
			}
		},
	})
	if err != nil {
		log.Fatal(err)
	}

	httpClient := &http.Client{Transport: tr}

	rpcClient, err := rpc.DialOptions(
		context.Background(),
		endpoints[0].URL,
		rpc.WithHTTPClient(httpClient),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer rpcClient.Close()

	eth := ethclient.NewClient(rpcClient)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// JSON-RPC reads use POST, so allow this read to try another provider.
	_, err = eth.BlockNumber(failnext.WithFailoverAllowed(ctx))
	if err == nil {
		log.Fatal("expected request to fail")
	}

	var failoverErr *failnext.FailoverError
	if !errors.As(err, &failoverErr) {
		log.Fatal(err)
	}

	fmt.Printf("\nrequest failed after %d attempts:\n", len(failoverErr.Attempts))
	for _, attempt := range failoverErr.Attempts {
		fmt.Printf("%s: %v\n", attempt.Endpoint, attempt.Err)
	}
}
