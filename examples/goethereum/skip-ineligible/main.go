package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/ethereum/go-ethereum/rpc"

	"github.com/yermakovsa/failnext"
)

type providerState struct {
	mu       sync.Mutex
	eligible map[failnext.EndpointID]bool
}

func (s *providerState) isEligible(id failnext.EndpointID) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.eligible[id]
}

func main() {
	const primaryURL = "http://127.0.0.1:65534"

	backupURL := os.Getenv("ETH_RPC_URL")
	if backupURL == "" {
		log.Fatal("set ETH_RPC_URL to a working Ethereum HTTP RPC endpoint")
	}

	// The application owns provider suitability. In production, this state
	// might be updated from its own provider monitoring.
	state := &providerState{
		eligible: map[failnext.EndpointID]bool{
			"primary": false,
			"backup":  true,
		},
	}

	endpoints := []failnext.Endpoint{
		{ID: "primary", URL: primaryURL},
		{ID: "backup", URL: backupURL},
	}

	tr, err := failnext.New(failnext.Config{
		Endpoints: endpoints,
		Eligible:  state.isEligible,
		OnEvent: func(_ context.Context, event failnext.Event) {
			if event.Kind == failnext.EventAttempt {
				fmt.Printf("attempt %d: %s\n", event.Attempt, event.Endpoint)
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

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	blockNumber, err := eth.BlockNumber(ctx)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("block number: %d\n", blockNumber)
}
