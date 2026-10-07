package integration

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/ethereum/go-ethereum/rpc"

	"github.com/yermakovsa/failnext"
)

type jsonRPCRequest struct {
	ID     json.RawMessage `json:"id"`
	Method string          `json:"method"`
}

func TestEthclientFailover(t *testing.T) {
	var primaryCalls atomic.Int32
	var backupCalls atomic.Int32

	primary := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		primaryCalls.Add(1)
		http.Error(w, "service unavailable", http.StatusServiceUnavailable)
	}))
	t.Cleanup(primary.Close)

	backup := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		backupCalls.Add(1)

		if r.Method != http.MethodPost {
			http.Error(w, "expected POST", http.StatusMethodNotAllowed)
			return
		}

		var req jsonRPCRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "invalid JSON-RPC request", http.StatusBadRequest)
			return
		}
		if req.Method != "eth_blockNumber" {
			http.Error(w, "unexpected JSON-RPC method", http.StatusBadRequest)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(map[string]any{
			"jsonrpc": "2.0",
			"id":      req.ID,
			"result":  "0x2a",
		}); err != nil {
			t.Errorf("encode backup response: %v", err)
		}
	}))
	t.Cleanup(backup.Close)

	endpoints := []failnext.Endpoint{
		{ID: "primary", URL: primary.URL},
		{ID: "backup", URL: backup.URL},
	}

	tr, err := failnext.New(failnext.Config{
		Endpoints: endpoints,
	})
	if err != nil {
		t.Fatal(err)
	}

	httpClient := &http.Client{Transport: tr}
	t.Cleanup(httpClient.CloseIdleConnections)

	rpcClient, err := rpc.DialOptions(
		context.Background(),
		endpoints[0].URL,
		rpc.WithHTTPClient(httpClient),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(rpcClient.Close)

	eth := ethclient.NewClient(rpcClient)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	block, err := eth.BlockNumber(failnext.WithFailoverAllowed(ctx))
	if err != nil {
		t.Fatal(err)
	}
	if block != 42 {
		t.Fatalf("block = %d, want 42", block)
	}
	if got := primaryCalls.Load(); got != 1 {
		t.Fatalf("primary calls = %d, want 1", got)
	}
	if got := backupCalls.Load(); got != 1 {
		t.Fatalf("backup calls = %d, want 1", got)
	}
}
