package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"strings"
	"sync/atomic"
	"time"

	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/ethereum/go-ethereum/rpc"

	"github.com/yermakovsa/rcpx"
)

func main() {
	const (
		timeout   = 15 * time.Second
		n         = 3
		failAfter = 1
		cooldown  = 30 * time.Second
	)

	upstreams := splitNonEmpty(os.Getenv("RCPX_UPSTREAMS"))
	if len(upstreams) < 2 {
		fmt.Fprintln(os.Stderr, "need at least 2 upstream URLs (comma-separated) in RCPX_UPSTREAMS")
		fmt.Fprintln(os.Stderr, "tip for demo: make the first upstream intentionally bad, e.g.:")
		fmt.Fprintln(os.Stderr, `  RCPX_UPSTREAMS="http://127.0.0.1:65534,https://YOUR_GOOD_RPC" go run .`)
		os.Exit(2)
	}

	// Count attempts and hits per upstream.
	var attempts atomic.Int32
	hits := make([]atomic.Int32, len(upstreams))

	baseTransport := &loggingRoundTripper{
		rt: http.DefaultTransport,
		onReq: func(url string) {
			attempts.Add(1)

			for i, up := range upstreams {
				if strings.HasPrefix(url, up) {
					hits[i].Add(1)
					break
				}
			}

			fmt.Printf("attempt -> %s\n", url)
		},
	}

	rt, err := rcpx.New(rcpx.Config{
		Endpoints: endpointsFromURLs(upstreams),
		Base:      baseTransport,
		Cooldown: rcpx.CooldownConfig{
			Threshold: failAfter,
			Duration:  cooldown,
		},
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "create rcpx transport: %v\n", err)
		os.Exit(1)
	}

	httpClient := &http.Client{
		Timeout:   timeout,
		Transport: rt,
	}

	ctxDial, cancelDial := context.WithTimeout(context.Background(), timeout)
	defer cancelDial()

	// Dial using the first URL (can be bad); rcpx selects the actual endpoint per attempt.
	rpcClient, err := rpc.DialOptions(ctxDial, upstreams[0], rpc.WithHTTPClient(httpClient))
	if err != nil {
		fmt.Fprintf(os.Stderr, "dial rpc: %v\n", err)
		os.Exit(1)
	}
	defer rpcClient.Close()

	ec := ethclient.NewClient(rpcClient)

	fmt.Printf("cooldown demo: n=%d failAfter=%d cooldown=%s\n", n, failAfter, cooldown)
	for i := 1; i <= n; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		_, err := ec.BlockNumber(ctx)
		cancel()

		if err != nil {
			fmt.Printf("req %d: ERROR: %v\n", i, err)
			continue
		}
		fmt.Printf("req %d: ok\n", i)
	}

	fmt.Println("---- summary ----")
	fmt.Printf("attempts_total=%d\n", attempts.Load())
	for i := range upstreams {
		fmt.Printf("hit_upstream%d=%d (%s)\n", i+1, hits[i].Load(), upstreams[i])
	}

	fmt.Println()
	fmt.Println("What to look for (make upstream #1 unhealthy; failAfter=1):")
	fmt.Println("- Request 1 usually logs two attempts: #1 then failover to the next healthy upstream.")
	fmt.Println("- Requests 2..N typically skip #1 during cooldown and go straight to a healthy upstream.")
}

type loggingRoundTripper struct {
	rt    http.RoundTripper
	onReq func(url string)
}

func (l *loggingRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	if l.onReq != nil && req != nil && req.URL != nil {
		l.onReq(req.URL.String())
	}
	return l.rt.RoundTrip(req)
}

func endpointsFromURLs(urls []string) []rcpx.Endpoint {
	endpoints := make([]rcpx.Endpoint, len(urls))
	for i, url := range urls {
		endpoints[i] = rcpx.Endpoint{
			ID:  rcpx.EndpointID(fmt.Sprintf("endpoint-%d", i+1)),
			URL: url,
		}
	}
	return endpoints
}

func splitNonEmpty(csv string) []string {
	if csv == "" {
		return nil
	}
	parts := strings.Split(csv, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		out = append(out, p)
	}
	return out
}
