# rcpx

`rcpx` is a Go `http.RoundTripper` for ordered failover across a small set of fixed HTTP endpoints.

Use it when your application already uses `http.Client` and has a clear primary/backup order for a few provider or RPC endpoints. A primary use case is go-ethereum over HTTP JSON-RPC, but `rcpx` itself is protocol-neutral: it does not parse JSON-RPC or decide which application operations are safe to repeat.

**`rcpx` is failover, not load balancing.** Applications remain responsible for deciding when a logical operation may safely continue to another provider.

## Installation

```bash
go get github.com/yermakovsa/rcpx
```

The module requires Go 1.24.

## Quick start

Configure complete endpoint URLs and use the transport with a normal `http.Client`:

```go
package main

import (
	"log"
	"net/http"

	"github.com/yermakovsa/rcpx"
)

func main() {
	tr, err := rcpx.New(rcpx.Config{
		Endpoints: []rcpx.Endpoint{
			{ID: "primary", URL: "https://rpc-a.example/rpc"},
			{ID: "backup", URL: "https://rpc-b.example/rpc"},
		},
	})
	if err != nil {
		log.Fatal(err)
	}

	client := &http.Client{Transport: tr}

	req, err := http.NewRequest(
		http.MethodGet,
		"https://rpc-a.example/rpc",
		nil,
	)
	if err != nil {
		log.Fatal(err)
	}

	resp, err := client.Do(req)
	if err != nil {
		log.Fatal(err)
	}
	defer resp.Body.Close()
}
```

Endpoint order is priority order. If an admitted attempt fails with a failover-triggering outcome, `rcpx` may continue to the next usable endpoint.

Configured endpoint URLs are complete physical destinations. `rcpx` does not treat them as base URLs or combine them with the incoming request path or query.

## go-ethereum

`rcpx` can sit underneath go-ethereum through `rpc.WithHTTPClient`:

```go
endpoints := []rcpx.Endpoint{
	{ID: "primary", URL: "https://provider-a.example/rpc"},
	{ID: "backup", URL: "https://provider-b.example/rpc"},
}

tr, err := rcpx.New(rcpx.Config{
	Endpoints: endpoints,
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
```

### Basic failover for reads

Ethereum JSON-RPC reads use HTTP `POST`, so the built-in `GET`/`HEAD` permission rule does not automatically allow them to cross providers.

When the application knows that a logical read may safely continue to another configured provider, allow it explicitly:

```go
ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
defer cancel()

readCtx := rcpx.WithFailoverAllowed(ctx)

blockNumber, err := eth.BlockNumber(readCtx)
if err != nil {
	log.Fatal(err)
}
```

`rcpx` does not inspect the JSON-RPC method name. Whether an operation is safe to continue is application knowledge.

See [`examples/goethereum/basic-failover`](examples/goethereum/basic-failover) for a complete example.

### Keep writes conservative

A replayable HTTP request is not necessarily an operation that should execute against another provider.

If a parent context broadly allows failover, derive an explicit deny for a sensitive operation:

```go
allowedCtx := rcpx.WithFailoverAllowed(ctx)
writeCtx := rcpx.WithFailoverDenied(allowedCtx)

err := eth.SendTransaction(writeCtx, tx)
```

This keeps the write on its first admitted provider attempt even if that attempt fails.

See [`examples/goethereum/conservative-write`](examples/goethereum/conservative-write).

### Related reads and endpoint preference

For related operations, an application may prefer a provider that completed an earlier request:

```go
ctx := rcpx.WithFailoverAllowed(parent)
ctx = rcpx.WithPreferredEndpoint(ctx, preferred)
```

Preference is a **soft ordering hint**. It is not pinning and does not provide a cross-provider consistency guarantee. It does not bypass eligibility, cooldown, permission, replayability, or failover-trigger rules.

Applications that depend on provider-local state, pending state, sessions, or read-after-write behavior must handle those semantics explicitly.

See [`examples/goethereum/preferred-endpoint`](examples/goethereum/preferred-endpoint).

## How failover works

A later endpoint is not tried merely because another endpoint exists. Endpoint selection, failover permission, failure classification, and request replayability are separate decisions.

### Endpoints and priority

Each endpoint has an application-visible ID and a complete HTTP destination:

```go
Endpoints: []rcpx.Endpoint{
	{ID: "primary", URL: "https://provider-a.example/rpc?key=..."},
	{ID: "backup", URL: "https://provider-b.example/rpc?key=..."},
}
```

Configured order is the normal priority order.

For each physical attempt, `rcpx` uses the complete URL of the selected endpoint. It does not join paths, merge query strings, or perform service discovery.

Endpoint IDs are used by features such as eligibility, preference, events, and attempt errors.

### Failover triggers

With a live logical request context, a base-transport failure that leaves no usable HTTP response is a failover trigger.

The built-in HTTP status triggers are:

- `502 Bad Gateway`
- `503 Service Unavailable`
- `504 Gateway Timeout`

Applications may add other status codes:

```go
tr, err := rcpx.New(rcpx.Config{
	Endpoints: endpoints,
	AdditionalTriggerStatusCodes: []int{
		http.StatusTooManyRequests,
	},
})
```

A non-trigger HTTP response is terminal from `rcpx`'s point of view, even if its body represents an application-level failure.

For example, HTTP 200 containing a JSON-RPC error object does not trigger failover. `rcpx` does not inspect response payloads.

### Permission to cross endpoints

By default, cross-endpoint continuation is inferred as allowed for `GET` and `HEAD`. Other methods are denied unless the application explicitly allows the logical operation or provides a `PermissionPolicy`.

The authority order is:

```text
request-scoped allow or deny
        ↓
PermissionPolicy
        ↓
GET / HEAD inference
        ↓
deny
```

Use request-scoped permission when calling code knows whether an operation may safely cross providers:

```go
ctx := rcpx.WithFailoverAllowed(parent)
```

An inherited allow can be overridden for a more sensitive operation:

```go
ctx := rcpx.WithFailoverDenied(parent)
```

Permission is operation metadata. It does not:

- make a request body replayable;
- make an endpoint eligible;
- bypass cooldown;
- change which outcomes trigger failover.

When configured, `PermissionPolicy` receives the logical `*http.Request`. An explicit request-scoped allow or deny takes precedence.

### Request bodies and replay

The first admitted endpoint may use the original request body even when that body cannot be replayed.

A later body-bearing attempt requires a fresh body. `rcpx` uses the standard `http.Request.GetBody` mechanism:

```text
no body
    -> another attempt can be constructed

body + working GetBody
    -> another attempt can be constructed

body + no GetBody
    -> first attempt is valid
    -> later body-bearing attempts are unavailable
```

`rcpx` does not buffer arbitrary request bodies to manufacture replayability.

Permission and replayability are independent: allowing an operation to cross endpoints does not make its body replayable, and having a replayable body does not make the operation safe to repeat.

## Endpoint selection

### Eligibility

Applications may exclude configured endpoints through `Eligible`:

```go
tr, err := rcpx.New(rcpx.Config{
	Endpoints: endpoints,
	Eligible: func(id rcpx.EndpointID) bool {
		return !disabled(id)
	},
})
```

Eligibility is captured for the logical request. The decisions observed by `rcpx` remain fixed for that request.

### Preferred endpoint

A request may carry one preferred endpoint:

```go
ctx := rcpx.WithPreferredEndpoint(parent, "backup")
```

If the endpoint exists and is externally eligible, it is promoted to the front of the request's consideration order. The relative order of the remaining endpoints does not change.

Preference does not override eligibility, cooldown, permission, replayability, or failover-trigger rules.

An unknown preferred endpoint produces an error matching `rcpx.ErrUnknownEndpoint` before a physical attempt is made.

### Cooldown

Cooldown passively suppresses endpoints after qualifying failures. It is enabled by default with:

- 3 consecutive cooldown failures;
- a 30-second cooldown duration.

It can be configured:

```go
Cooldown: rcpx.CooldownConfig{
	Threshold: 2,
	Duration:  time.Minute,
},
```

or disabled:

```go
Cooldown: rcpx.CooldownConfig{
	Disabled: true,
},
```

Cooldown failures are deliberately narrow: live-context transport failures with no usable response, plus HTTP `502`, `503`, and `504`.

Other obtained HTTP responses reset the consecutive cooldown failure streak, even when an additional status code is configured as a failover trigger.

Eligibility and cooldown are different mechanisms. Eligibility is captured for the logical request; cooldown is checked when an endpoint's turn arrives.

Cooldown is not a health checker. There are no active probes, half-open states, background workers, or adaptive retry scheduling.

## Results and errors

When failover follows an HTTP trigger response, `rcpx` may retain that response while trying a later endpoint.

If a later endpoint produces another HTTP response, that newer response replaces the earlier retained response. A later transport failure does not erase a real HTTP response that is still available.

For example:

```text
primary -> HTTP 503
backup  -> transport error

result  -> primary's HTTP 503 response
```

If preparing a later body-bearing attempt fails through `GetBody`, that later endpoint is not physically attempted. A previously retained HTTP response can still be returned.

Logical request cancellation or deadline expiration takes priority over a retained response.

Responses that remain internal to `rcpx` are closed when they are discarded or superseded. Once a response is returned, its body belongs to the caller and should be closed normally.

### Errors

`ErrNoUsableEndpoint` is returned directly when no physical attempt can be admitted, for example when all captured eligibility decisions exclude their endpoints or every candidate is cooling before the first attempt.

`ErrUnknownEndpoint` identifies an invalid request-scoped endpoint reference such as an unknown preferred endpoint.

`FailoverError` is used when one or more physical attempts ended without a usable HTTP response and no HTTP response is available to return. Its `Attempts` slice records those attempts in order, and the terminal cause is available through normal Go error unwrapping.

```go
var fe *rcpx.FailoverError
if errors.As(err, &fe) {
	for _, attempt := range fe.Attempts {
		log.Printf("endpoint=%s err=%v", attempt.Endpoint, attempt.Err)
	}
}

if errors.Is(err, rcpx.ErrNoUsableEndpoint) {
	log.Printf("no endpoint could be attempted")
}

if errors.Is(err, rcpx.ErrUnknownEndpoint) {
	log.Printf("request referred to an unknown endpoint")
}
```

Logical cancellation and deadline expiration remain normal context errors rather than being wrapped in `FailoverError`.

See [`examples/goethereum/error-inspection`](examples/goethereum/error-inspection) for a complete example.

## Observability and `http.Client` composition

### Events

`Config.OnEvent` provides a synchronous observation hook:

```go
OnEvent: func(ctx context.Context, event rcpx.Event) {
	log.Printf(
		"kind=%d endpoint=%s attempt=%d status=%d err=%v",
		event.Kind,
		event.Endpoint,
		event.Attempt,
		event.StatusCode,
		event.Err,
	)
},
```

The event kinds are:

| Event | Meaning |
| --- | --- |
| `EventAttempt` | A physical endpoint attempt completed. |
| `EventCooldownSkip` | An endpoint's turn was reached, but live cooldown suppressed it. |
| `EventReplayError` | A later attempt could not be constructed because `GetBody` failed. |
| `EventResult` | The logical `RoundTrip` is about to return its final response or error. |

Events for one logical request are delivered in causal order. Different logical requests may invoke the callback concurrently, so application-owned shared state must be protected.

The callback should return promptly. `rcpx` does not recover panics from `OnEvent`.

### Base transport

The configured `Base` transport handles every physical attempt. If `Base` is nil, `http.DefaultTransport` is used.

`Base` is the composition point for behavior that needs to run separately for each selected destination, such as:

- endpoint-specific authentication;
- host- or path-bound signing;
- tracing;
- custom networking behavior.

```go
tr, err := rcpx.New(rcpx.Config{
	Endpoints: endpoints,
	Base:      customTransport,
})
```

### Concurrency and `CloseIdleConnections`

A constructed `Transport` is intended for concurrent reuse.

Custom base transports and application callbacks must satisfy their own concurrency requirements. `PermissionPolicy`, `Eligible`, and `OnEvent` may run concurrently for different logical requests.

`Transport.CloseIdleConnections` forwards to the base transport when the base supports that operation, so `http.Client.CloseIdleConnections` composes normally.

## Destination-sensitive request state

Changing the physical destination can change the meaning of request state prepared for a particular host or URL.

For `Request.Host`, `rcpx` uses these rules:

```text
Host == ""
    -> remains empty

Host == original URL.Host
    -> treated as URL-derived
    -> follows the selected endpoint URL.Host

Host != original URL.Host
    -> treated as custom
    -> preserved
```

The comparison is exact. A distinguishably custom Host is preserved across physical attempts.

One ambiguity remains: if an application deliberately wants a sticky custom Host whose value is exactly equal to the original `URL.Host`, that value is indistinguishable from ordinary URL-derived Host state and will follow the selected endpoint instead.

If an application requires different Host behavior, `Config.Base` middleware can reapply the intended value for every physical attempt.

Host is only one form of destination-sensitive state. Endpoint-specific credentials, host- or path-bound signatures, provider-specific headers, cookies, session state, and similar metadata may also need application-specific handling.

`rcpx` does not automatically regenerate, rewrite, or validate those values when it selects another destination.

Redirect handling belongs to `http.Client`. A followed redirect starts another logical transport invocation rather than becoming part of `rcpx`'s endpoint-attempt sequence.

## Examples

The repository includes go-ethereum examples for the main integration patterns:

- [`examples/goethereum/basic-failover`](examples/goethereum/basic-failover) — use `rcpx` through `rpc.WithHTTPClient` and allow a JSON-RPC read to fail over.
- [`examples/goethereum/conservative-write`](examples/goethereum/conservative-write) — explicitly prevent a write from crossing providers.
- [`examples/goethereum/error-inspection`](examples/goethereum/error-inspection) — inspect failed physical attempts through `FailoverError`.
- [`examples/goethereum/preferred-endpoint`](examples/goethereum/preferred-endpoint) — prefer the provider that completed an earlier related read without treating preference as pinning.

## What rcpx does not do

`rcpx` is a focused HTTP failover transport, not a general resilience or routing framework.

It does not provide:

- load balancing or service discovery;
- active health checks;
- a general retry, backoff, or `Retry-After` scheduler;
- application-protocol parsing or JSON-RPC error interpretation;
- arbitrary request-body buffering to create replayability;
- a general signing, authentication, or header-rewrite framework;
- hard provider pinning or cross-provider state consistency;
- base-URL path/query composition;
- WebSocket failover;
- Ethereum transaction, nonce, or pending-state management.

Applications that depend on provider-local state, sessions, pending state, or read-after-write behavior must design for those semantics explicitly.

## License

MIT. See [LICENSE](LICENSE).