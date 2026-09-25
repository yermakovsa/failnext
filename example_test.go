package failnext_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/yermakovsa/failnext"
)

func ExampleNew() {
	tr, err := failnext.New(failnext.Config{
		Endpoints: []failnext.Endpoint{
			{ID: "primary", URL: "https://primary.example/api"},
			{ID: "backup", URL: "https://backup.example/api"},
		},
		Base: roundTripperFunc(func(req *http.Request) (*http.Response, error) {
			switch req.URL.Host {
			case "primary.example":
				return nil, errors.New("primary unavailable")
			case "backup.example":
				return &http.Response{
					StatusCode: http.StatusOK,
					Body:       io.NopCloser(strings.NewReader("served by backup")),
					Header:     make(http.Header),
				}, nil
			default:
				return nil, fmt.Errorf("unexpected destination %s", req.URL)
			}
		}),
	})
	if err != nil {
		panic(err)
	}

	client := &http.Client{Transport: tr}

	req, err := http.NewRequest(
		http.MethodGet,
		"https://primary.example/api",
		nil,
	)
	if err != nil {
		panic(err)
	}

	resp, err := client.Do(req)
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		panic(err)
	}

	fmt.Println(string(body))
	// Output:
	// served by backup
}

func ExampleWithFailoverAllowed() {
	tr, err := failnext.New(failnext.Config{
		Endpoints: []failnext.Endpoint{
			{ID: "primary", URL: "https://primary.example/rpc"},
			{ID: "backup", URL: "https://backup.example/rpc"},
		},
		Base: roundTripperFunc(func(req *http.Request) (*http.Response, error) {
			switch req.URL.Host {
			case "primary.example":
				return nil, errors.New("primary unavailable")
			case "backup.example":
				return &http.Response{
					StatusCode: http.StatusOK,
					Body: io.NopCloser(strings.NewReader(
						`{"jsonrpc":"2.0","id":1,"result":"0x2a"}`,
					)),
					Header: make(http.Header),
				}, nil
			default:
				return nil, fmt.Errorf("unexpected destination %s", req.URL)
			}
		}),
	})
	if err != nil {
		panic(err)
	}

	client := &http.Client{Transport: tr}

	req, err := http.NewRequest(
		http.MethodPost,
		"https://primary.example/rpc",
		strings.NewReader(`{"jsonrpc":"2.0","id":1,"method":"eth_blockNumber","params":[]}`),
	)
	if err != nil {
		panic(err)
	}

	// This RPC operation is a read, so it is safe to send to another provider.
	req = req.WithContext(failnext.WithFailoverAllowed(req.Context()))

	resp, err := client.Do(req)
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		panic(err)
	}

	fmt.Println(string(body))
	// Output:
	// {"jsonrpc":"2.0","id":1,"result":"0x2a"}
}

func ExampleConfig_eligibility() {
	// The application provides a read-only snapshot of provider availability.
	available := map[failnext.EndpointID]bool{
		"primary": false,
		"backup":  true,
	}

	tr, err := failnext.New(failnext.Config{
		Endpoints: []failnext.Endpoint{
			{ID: "primary", URL: "https://primary.example/api"},
			{ID: "backup", URL: "https://backup.example/api"},
		},
		Eligible: func(id failnext.EndpointID) bool {
			return available[id]
		},
		Base: roundTripperFunc(func(req *http.Request) (*http.Response, error) {
			if req.URL.Host != "backup.example" {
				return nil, fmt.Errorf("unexpected destination %s", req.URL)
			}

			return &http.Response{
				StatusCode: http.StatusOK,
				Body:       io.NopCloser(strings.NewReader("served by backup")),
				Header:     make(http.Header),
			}, nil
		}),
	})
	if err != nil {
		panic(err)
	}

	client := &http.Client{Transport: tr}

	resp, err := client.Get("https://primary.example/api")
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		panic(err)
	}

	fmt.Println(string(body))
	// Output:
	// served by backup
}

func ExampleWithPreferredEndpoint() {
	type resultRecorderKey struct{}
	type resultRecorder struct {
		endpoint failnext.EndpointID
	}

	var calls []string

	tr, err := failnext.New(failnext.Config{
		Endpoints: []failnext.Endpoint{
			{ID: "primary", URL: "https://primary.example/api"},
			{ID: "backup", URL: "https://backup.example/api"},
		},
		Base: roundTripperFunc(func(req *http.Request) (*http.Response, error) {
			calls = append(calls, req.URL.Host)

			switch req.URL.Host {
			case "primary.example":
				return nil, errors.New("primary unavailable")
			case "backup.example":
				return &http.Response{
					StatusCode: http.StatusOK,
					Body:       io.NopCloser(strings.NewReader("ok")),
					Header:     make(http.Header),
				}, nil
			default:
				return nil, fmt.Errorf("unexpected destination %s", req.URL)
			}
		}),
		OnEvent: func(ctx context.Context, event failnext.Event) {
			if event.Kind != failnext.EventResult || event.Endpoint == "" {
				return
			}

			if recorder, ok := ctx.Value(resultRecorderKey{}).(*resultRecorder); ok {
				recorder.endpoint = event.Endpoint
			}
		},
	})
	if err != nil {
		panic(err)
	}

	client := &http.Client{Transport: tr}

	// Record which provider produced the first result.
	recorder := &resultRecorder{}
	firstCtx := context.WithValue(
		context.Background(),
		resultRecorderKey{},
		recorder,
	)

	first, err := http.NewRequestWithContext(
		firstCtx,
		http.MethodGet,
		"https://primary.example/api",
		nil,
	)
	if err != nil {
		panic(err)
	}

	resp, err := client.Do(first)
	if err != nil {
		panic(err)
	}
	resp.Body.Close()

	if recorder.endpoint == "" {
		panic("request completed without a result endpoint")
	}

	// Prefer that provider for the related request.
	secondCtx := failnext.WithPreferredEndpoint(
		context.Background(),
		recorder.endpoint,
	)
	second, err := http.NewRequestWithContext(
		secondCtx,
		http.MethodGet,
		"https://primary.example/api",
		nil,
	)
	if err != nil {
		panic(err)
	}

	resp, err = client.Do(second)
	if err != nil {
		panic(err)
	}
	resp.Body.Close()

	fmt.Printf(
		"preferred=%s calls=%s\n",
		recorder.endpoint,
		strings.Join(calls, ","),
	)
	// Output:
	// preferred=backup calls=primary.example,backup.example,backup.example
}

func ExampleFailoverError() {
	primaryErr := errors.New("primary unavailable")
	backupErr := errors.New("backup unavailable")

	tr, err := failnext.New(failnext.Config{
		Endpoints: []failnext.Endpoint{
			{ID: "primary", URL: "https://primary.example/api"},
			{ID: "backup", URL: "https://backup.example/api"},
		},
		Base: roundTripperFunc(func(req *http.Request) (*http.Response, error) {
			switch req.URL.Host {
			case "primary.example":
				return nil, primaryErr
			case "backup.example":
				return nil, backupErr
			default:
				return nil, fmt.Errorf("unexpected destination %s", req.URL)
			}
		}),
	})
	if err != nil {
		panic(err)
	}

	client := &http.Client{Transport: tr}

	_, err = client.Get("https://primary.example/api")

	var failoverErr *failnext.FailoverError
	if !errors.As(err, &failoverErr) {
		panic("expected FailoverError")
	}

	for _, attempt := range failoverErr.Attempts {
		fmt.Printf("%s: %v\n", attempt.Endpoint, attempt.Err)
	}
	fmt.Printf("final error is backup: %v\n", errors.Is(err, backupErr))

	// Output:
	// primary: primary unavailable
	// backup: backup unavailable
	// final error is backup: true
}

type roundTripperFunc func(*http.Request) (*http.Response, error)

func (f roundTripperFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}
