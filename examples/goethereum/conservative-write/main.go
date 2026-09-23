package main

import (
	"context"
	"fmt"
	"log"
	"math/big"
	"net/http"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/ethereum/go-ethereum/rpc"

	"github.com/yermakovsa/failnext"
)

func main() {
	endpoints := []failnext.Endpoint{
		{ID: "primary", URL: "http://127.0.0.1:65534"},
		{ID: "backup", URL: "http://127.0.0.1:65533"},
	}

	tr, err := failnext.New(failnext.Config{
		Endpoints: endpoints,
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

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// The parent context allows failover.
	allowedCtx := failnext.WithFailoverAllowed(ctx)

	// Disable failover for writes.
	writeCtx := failnext.WithFailoverDenied(allowedCtx)

	// Dummy transaction used only to exercise SendTransaction.
	// Both configured endpoints are intentionally unavailable.
	tx := types.NewTx(&types.LegacyTx{
		Nonce:    0,
		To:       &common.Address{},
		Value:    big.NewInt(0),
		Gas:      21_000,
		GasPrice: big.NewInt(1),
	})

	err = eth.SendTransaction(writeCtx, tx)
	if err == nil {
		log.Fatal("expected request to fail")
	}

	fmt.Println("write failed; backup was not attempted")
}
