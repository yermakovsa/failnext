package main

import (
	"context"
	"fmt"
	"log"
	"math/big"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/ethereum/go-ethereum/rpc"

	"github.com/yermakovsa/rcpx"
)

type resultRecorderKey struct{}

type resultRecorder struct {
	mu       sync.Mutex
	endpoint rcpx.EndpointID
}

func (r *resultRecorder) record(endpoint rcpx.EndpointID) {
	r.mu.Lock()
	r.endpoint = endpoint
	r.mu.Unlock()
}

func (r *resultRecorder) result() (rcpx.EndpointID, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.endpoint, r.endpoint != ""
}

func withResultRecorder(ctx context.Context) (context.Context, *resultRecorder) {
	recorder := &resultRecorder{}
	return context.WithValue(ctx, resultRecorderKey{}, recorder), recorder
}

func resultRecorderFromContext(ctx context.Context) *resultRecorder {
	recorder, _ := ctx.Value(resultRecorderKey{}).(*resultRecorder)
	return recorder
}

func main() {
	// Intentionally unavailable so the first read fails over to backup.
	const primaryURL = "http://127.0.0.1:65534"

	backupURL := os.Getenv("ETH_RPC_URL")
	if backupURL == "" {
		log.Fatal("set ETH_RPC_URL to a working Ethereum HTTP RPC endpoint")
	}

	tr, err := rcpx.New(rcpx.Config{
		Endpoints: []rcpx.Endpoint{
			{ID: "primary", URL: primaryURL},
			{ID: "backup", URL: backupURL},
		},

		OnEvent: func(ctx context.Context, event rcpx.Event) {
			if event.Kind == rcpx.EventAttempt {
				fmt.Printf("attempt %d: %s\n", event.Attempt, event.Endpoint)
			}

			if event.Kind != rcpx.EventResult || event.Endpoint == "" {
				return
			}

			if recorder := resultRecorderFromContext(ctx); recorder != nil {
				recorder.record(event.Endpoint)
			}
		},
	})
	if err != nil {
		log.Fatal(err)
	}

	httpClient := &http.Client{Transport: tr}

	rpcClient, err := rpc.DialOptions(
		context.Background(),
		primaryURL,
		rpc.WithHTTPClient(httpClient),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer rpcClient.Close()

	eth := ethclient.NewClient(rpcClient)

	firstCtx, cancelFirst := context.WithTimeout(context.Background(), 10*time.Second)
	firstCtx, recorder := withResultRecorder(firstCtx)
	firstCtx = rcpx.WithFailoverAllowed(firstCtx)

	blockNumber, err := eth.BlockNumber(firstCtx)
	cancelFirst()
	if err != nil {
		log.Fatal(err)
	}

	preferred, ok := recorder.result()
	if !ok {
		log.Fatal("first read completed without a result endpoint")
	}

	fmt.Printf("first read: block=%d endpoint=%s\n\n", blockNumber, preferred)

	secondCtx, cancelSecond := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelSecond()

	secondCtx = rcpx.WithFailoverAllowed(secondCtx)

	// Prefer the provider that completed the first read.
	// Preference is not pinning or a consistency guarantee.
	secondCtx = rcpx.WithPreferredEndpoint(secondCtx, preferred)

	header, err := eth.HeaderByNumber(
		secondCtx,
		new(big.Int).SetUint64(blockNumber),
	)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf(
		"related read: block=%d hash=%s\n",
		header.Number.Uint64(),
		header.Hash(),
	)
}
