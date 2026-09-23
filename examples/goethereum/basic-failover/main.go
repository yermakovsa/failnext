package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/ethereum/go-ethereum/rpc"

	"github.com/yermakovsa/rcpx"
)

func main() {
	// Intentionally unavailable so the example exercises failover.
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

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// JSON-RPC reads use POST, so explicitly allow this read to continue
	// from the unavailable primary to the backup provider.
	readCtx := rcpx.WithFailoverAllowed(ctx)

	blockNumber, err := eth.BlockNumber(readCtx)
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("block number: %d", blockNumber)

	// Disable failover for writes.
	// writeCtx := rcpx.WithFailoverDenied(readCtx)
	// err = eth.SendTransaction(writeCtx, tx)
}
