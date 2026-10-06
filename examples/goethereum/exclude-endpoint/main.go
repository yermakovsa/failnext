package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"net/http/httptest"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/rpc"

	"github.com/yermakovsa/failnext"
)

// Example provider-specific code. This is not an Ethereum standard error code.
const historicalStateUnavailableCode = -32001

type resultRecorderKey struct{}

type resultRecorder struct {
	mu       sync.Mutex
	endpoint failnext.EndpointID
}

func (r *resultRecorder) record(endpoint failnext.EndpointID) {
	r.mu.Lock()
	r.endpoint = endpoint
	r.mu.Unlock()
}

func (r *resultRecorder) result() (failnext.EndpointID, bool) {
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

func isHistoricalStateUnavailable(err error) bool {
	var rpcErr rpc.Error
	return errors.As(err, &rpcErr) &&
		rpcErr.ErrorCode() == historicalStateUnavailableCode
}

type jsonRPCRequest struct {
	ID json.RawMessage `json:"id"`
}

type jsonRPCError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

type jsonRPCResponse struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id"`
	Result  string          `json:"result,omitempty"`
	Error   *jsonRPCError   `json:"error,omitempty"`
}

func newRPCProvider(response jsonRPCResponse) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request jsonRPCRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		response.ID = request.ID

		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(response); err != nil {
			panic(err)
		}
	}))
}

func main() {
	// Local providers keep the example deterministic. In production these
	// would be normal Ethereum RPC provider URLs.
	primary := newRPCProvider(jsonRPCResponse{
		JSONRPC: "2.0",
		Error: &jsonRPCError{
			Code:    historicalStateUnavailableCode,
			Message: "historical state unavailable",
		},
	})
	defer primary.Close()

	backup := newRPCProvider(jsonRPCResponse{
		JSONRPC: "2.0",
		Result:  "0x2a",
	})
	defer backup.Close()

	endpoints := []failnext.Endpoint{
		{ID: "primary", URL: primary.URL},
		{ID: "backup", URL: backup.URL},
	}

	tr, err := failnext.New(failnext.Config{
		Endpoints: endpoints,
		OnEvent: func(ctx context.Context, event failnext.Event) {
			if event.Kind != failnext.EventResult || event.Endpoint == "" {
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
		endpoints[0].URL,
		rpc.WithHTTPClient(httpClient),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer rpcClient.Close()

	operationCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	const account = "0x0000000000000000000000000000000000000000"
	const block = "0x1"

	firstCtx, firstRecorder := withResultRecorder(operationCtx)

	var balance string
	err = rpcClient.CallContext(
		firstCtx,
		&balance,
		"eth_getBalance",
		account,
		block,
	)
	if err == nil {
		log.Fatal("expected provider-specific RPC error")
	}

	rejectedEndpoint, ok := firstRecorder.result()
	if !ok {
		log.Fatal("first call completed without a result endpoint")
	}

	fmt.Printf(
		"first call: endpoint=%s error=%v\n",
		rejectedEndpoint,
		err,
	)

	if !isHistoricalStateUnavailable(err) {
		log.Fatal(err)
	}

	// failnext returned the HTTP 200 response normally. The application
	// recognizes the JSON-RPC error and makes a new logical request without
	// the provider that returned it.
	retryCtx, retryRecorder := withResultRecorder(operationCtx)
	retryCtx = failnext.WithExcludedEndpoints(retryCtx, rejectedEndpoint)

	err = rpcClient.CallContext(
		retryCtx,
		&balance,
		"eth_getBalance",
		account,
		block,
	)
	if err != nil {
		log.Fatal(err)
	}

	retryEndpoint, ok := retryRecorder.result()
	if !ok {
		log.Fatal("retry completed without a result endpoint")
	}

	fmt.Printf(
		"retry: endpoint=%s balance=%s\n",
		retryEndpoint,
		balance,
	)
}
