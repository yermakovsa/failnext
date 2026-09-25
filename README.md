# failnext

[![CI](https://github.com/yermakovsa/failnext/actions/workflows/ci.yml/badge.svg)](https://github.com/yermakovsa/failnext/actions/workflows/ci.yml) [![Go Reference](https://pkg.go.dev/badge/github.com/yermakovsa/failnext.svg)](https://pkg.go.dev/github.com/yermakovsa/failnext) [![Release](https://img.shields.io/github/v/release/yermakovsa/failnext)](https://github.com/yermakovsa/failnext/releases)

**fail → next**

Client-side failover for Go applications with a small, fixed set of HTTP or RPC providers.

* **Primary RPC unavailable?** Try the next provider.
* **One RPC is lagging or degraded?** Skip any provider your application decides not to use.
* **Want reads to fail over, but not writes?** Keep writes from failing over to another provider.
* **Reading after a write?** Try the provider that handled the write first.
* **Everything failed?** See which providers were tried and what went wrong.

A primary use case is [go-ethereum](https://github.com/ethereum/go-ethereum) over HTTP JSON-RPC, but `failnext` itself is protocol-neutral. It plugs into `http.Client` as an `http.RoundTripper`.

`failnext` handles the mechanics of trying providers in order. Your application decides which operations may safely continue to another provider; `failnext` does not parse JSON-RPC or make that decision for you.

`failnext` is failover, not load balancing.

## Quick start

Configure your providers in priority order and use `failnext` underneath go-ethereum through `rpc.WithHTTPClient`:

```go
endpoints := []failnext.Endpoint{
	{ID: "primary", URL: "https://provider-a.example/rpc"},
	{ID: "backup", URL: "https://provider-b.example/rpc"},
}

tr, err := failnext.New(failnext.Config{
	Endpoints: endpoints,
})
if err != nil {
	return err
}

httpClient := &http.Client{Transport: tr}

rpcClient, err := rpc.DialOptions(
	context.Background(),
	endpoints[0].URL,
	rpc.WithHTTPClient(httpClient),
)
if err != nil {
	return err
}
defer rpcClient.Close()

eth := ethclient.NewClient(rpcClient)

// JSON-RPC reads use HTTP POST, so explicitly allow this read to fail over.
ctx := failnext.WithFailoverAllowed(context.Background())

blockNumber, err := eth.BlockNumber(ctx)
```

If the primary fails in a way that triggers failover, `failnext` can try the next provider.

Ethereum JSON-RPC reads use HTTP `POST`, so `failnext` cannot tell from the HTTP method whether a read may safely be sent to another provider. It does not inspect JSON-RPC method names; your application makes that decision explicitly.

See [`examples/goethereum/basic-failover`](examples/goethereum/basic-failover) for a complete runnable example.

## Installation

```bash
go get github.com/yermakovsa/failnext
```

The module requires Go 1.24.

> `failnext` was previously named `rcpx`.

## When failnext fits

`failnext` fits best when:

* you have a small set of known HTTP or RPC providers;
* those providers have a clear priority, such as primary and backup;
* you want failover to happen inside your Go application;
* your application decides which operations may safely fail over;
* your application may need to skip providers it considers unavailable, lagging, or degraded;
* some follow-up requests should try a previously used provider first.

`failnext` plugs directly into `http.Client`, including go-ethereum over HTTP JSON-RPC.

### When a proxy or gateway may fit better

A proxy or gateway may be a better fit when you want provider failover and routing to be shared across multiple applications rather than handled inside each Go client.

For example, when you need:

* one shared RPC endpoint for many applications;
* service discovery or dynamic load balancing;
* active checks for provider health or blockchain lag;
* centralized caching or routing rules;
* provider selection to happen outside application code;
* WebSocket failover.

`failnext` is intentionally smaller. It keeps failover inside your Go application and works with a small, fixed set of configured HTTP or RPC providers.

## Common patterns

### Allow a read to fail over

By default, `failnext` allows HTTP `GET` and `HEAD` requests to continue to another provider.

JSON-RPC reads normally use `POST`, so your application must explicitly allow a read to fail over when it knows that operation is safe to repeat:

```go
ctx := failnext.WithFailoverAllowed(parent)

blockNumber, err := eth.BlockNumber(ctx)
```

Allowing failover only gives `failnext` permission to try another provider. It does not make the request replayable, make another provider eligible, bypass cooldown, or change which failures trigger failover.

### Keep writes from failing over

Some operations should not be sent to another provider after the first attempt fails.

Even if failover is allowed by a parent context, you can explicitly disable it for a write:

```go
allowedCtx := failnext.WithFailoverAllowed(ctx)
writeCtx := failnext.WithFailoverDenied(allowedCtx)

err := eth.SendTransaction(writeCtx, tx)
```

With failover denied, `failnext` will not try another provider for that operation.

`failnext` does not inspect JSON-RPC method names or decide which operations are safe to repeat. Your application makes that decision.

A replayable HTTP request is not necessarily an operation that should be sent to another provider.

See [`examples/goethereum/conservative-write`](examples/goethereum/conservative-write).

### Skip providers your application marks as unsuitable

If your application knows a provider should not be used, for example because it is lagging, degraded, or disabled, exclude it with `Eligible`:

```go
tr, err := failnext.New(failnext.Config{
	Endpoints: endpoints,
	Eligible: func(id failnext.EndpointID) bool {
		return eligible(id)
	},
})
```

`failnext` does not detect provider health, chain lag, or stale blockchain state. Your application owns that decision.

For each request, `failnext` checks eligibility once per endpoint and keeps those decisions fixed for that request.

See [`examples/goethereum/skip-ineligible`](examples/goethereum/skip-ineligible).

### Prefer a provider for a follow-up request

If one provider handled an earlier request, you can ask `failnext` to try that provider first for a follow-up request:

```go
ctx := failnext.WithFailoverAllowed(parent)
ctx = failnext.WithPreferredEndpoint(ctx, preferred)
```

If that provider is eligible, `failnext` tries it first. The order of the remaining providers stays the same.

Preference only changes which provider is tried first. It does not pin the request to that provider or guarantee cross-provider consistency.

It does not bypass:

* eligibility;
* cooldown;
* permission to fail over;
* request replayability;
* failover trigger rules.

If your application depends on provider-local state, pending state, sessions, or read-after-write behavior, it still needs to handle those consistency requirements itself.

See [`examples/goethereum/preferred-endpoint`](examples/goethereum/preferred-endpoint).

### Inspect failed providers

If one or more provider attempts fail and the request ends without an HTTP response to return, `FailoverError` records those failed attempts in order:

```go
var fe *failnext.FailoverError
if errors.As(err, &fe) {
	for _, attempt := range fe.Attempts {
		log.Printf("endpoint=%s err=%v", attempt.Endpoint, attempt.Err)
	}
}
```

The terminal cause remains available through normal Go error unwrapping.

For more detailed visibility, `Config.OnEvent` can report provider attempts, cooldown skips, replay failures, and the final result.

See [`examples/goethereum/error-inspection`](examples/goethereum/error-inspection).

## How failover works

The sections below define the exact rules behind the common patterns above.

A later provider is not tried just because another provider is available.

Four decisions are separate:

1. which provider should be tried;
2. whether the request is allowed to fail over;
3. whether the failure actually triggers failover;
4. whether the request can be replayed for another attempt.

### Endpoints and priority

Each provider has an ID and a complete HTTP destination:

```go
Endpoints: []failnext.Endpoint{
	{ID: "primary", URL: "https://provider-a.example/rpc?key=..."},
	{ID: "backup", URL: "https://provider-b.example/rpc?key=..."},
}
```

Providers are normally tried in the order they are configured.

For each attempt, `failnext` uses the complete URL of the selected provider.

Configured endpoint URLs are not base URLs. `failnext` does not:

* join paths;
* merge query strings;
* perform service discovery.

Endpoint IDs are used by eligibility, preference, events, and attempt errors.

### Failover triggers

A transport failure can trigger failover while the request context is still active, as long as there is no usable HTTP response to return.

The built-in HTTP status triggers are:

* `502 Bad Gateway`;
* `503 Service Unavailable`;
* `504 Gateway Timeout`.

Applications may add other HTTP status codes:

```go
tr, err := failnext.New(failnext.Config{
	Endpoints: endpoints,
	AdditionalTriggerStatusCodes: []int{
		http.StatusTooManyRequests,
	},
})
```

Additional status codes extend the built-in set.

An HTTP response that does not match a failover trigger is returned without trying another provider, even if the response body contains an RPC or application error.

For example, an HTTP `200` response containing a JSON-RPC error object does **not** trigger failover.

`failnext` does not inspect response payloads.

### Permission to fail over

A failure can trigger failover, but the request must also be allowed to continue to another provider.

The permission order is:

```text
request-scoped allow or deny
        ↓
PermissionPolicy
        ↓
GET / HEAD inference
        ↓
deny
```

By default:

* `GET` and `HEAD` are allowed to fail over;
* other HTTP methods are denied unless the application explicitly allows failover or a `PermissionPolicy` allows it.

Use request-scoped permission when your code knows that an operation may safely fail over:

```go
ctx := failnext.WithFailoverAllowed(parent)
```

An inherited allow can be overridden when a specific operation should not fail over:

```go
ctx := failnext.WithFailoverDenied(parent)
```

When configured, `PermissionPolicy` receives the `*http.Request`.

An explicit request-scoped allow or deny takes precedence over the policy.

Permission only controls whether `failnext` may continue to another provider. It does not:

* make a request body replayable;
* make an endpoint eligible;
* bypass cooldown;
* change which failures trigger failover.

### Request bodies and replay

The first provider can use the original request body, even if that body cannot be replayed.

If `failnext` needs to try another provider, a request with a body needs a fresh copy from `http.Request.GetBody`:

```text
no body
    -> another provider can be tried

body + working GetBody
    -> another provider can be tried

body + no GetBody
    -> first provider can be tried
    -> another provider cannot be tried
```

`failnext` does not buffer request bodies to make them replayable.

Permission and replayability are separate:

* allowing an operation to fail over does not make its request body replayable;
* having a replayable body does not make the operation safe to repeat.

## Endpoint selection

### Eligibility

Applications can exclude configured providers with `Eligible`:

```go
tr, err := failnext.New(failnext.Config{
	Endpoints: endpoints,
	Eligible: func(id failnext.EndpointID) bool {
		return !disabled(id)
	},
})
```

For each request, `failnext` checks eligibility once per endpoint. Those decisions stay fixed for the lifetime of that request.

### Preferred endpoint

A request can specify one preferred endpoint:

```go
ctx := failnext.WithPreferredEndpoint(parent, "backup")
```

If the preferred endpoint exists and is eligible, `failnext` tries it first.

The remaining providers keep their configured order.

Preference does not override:

* eligibility;
* cooldown;
* permission to fail over;
* request replayability;
* failover trigger rules.

If the preferred endpoint is unknown, `failnext` returns an error matching `failnext.ErrUnknownEndpoint` before any provider is tried.

### Cooldown

Cooldown temporarily skips a provider after a configured number of qualifying failures.

It is enabled by default with:

* 3 consecutive cooldown failures;
* a 30-second cooldown duration.

Configure it with:

```go
Cooldown: failnext.CooldownConfig{
	Threshold: 2,
	Duration:  time.Minute,
},
```

Or disable it:

```go
Cooldown: failnext.CooldownConfig{
	Disabled: true,
},
```

Not every failure that triggers failover counts toward cooldown.

Cooldown failures are:

* transport failures while the request context is still active and no usable HTTP response is available;
* HTTP `502`;
* HTTP `503`;
* HTTP `504`.

Other HTTP responses reset the consecutive cooldown failure streak, even if an additional status code is configured to trigger failover.

For example, if you add `429 Too Many Requests` as a failover trigger, a `429` can cause failover, but it does not count as a cooldown failure.

Eligibility and cooldown are separate:

* eligibility stays fixed for the lifetime of a request;
* cooldown is checked when a provider is about to be tried.

Cooldown is **not** a health checker.

There are no:

* active probes;
* half-open states;
* background workers;
* adaptive retry scheduling.

## Results and errors

### Trigger responses and later failures

If a provider returns an HTTP response that triggers failover, `failnext` can keep that response while trying another provider.

If another provider later returns an HTTP response, the newer response replaces the earlier one.

If the later provider fails with a transport error, the earlier HTTP response can still be returned.

For example:

```text
primary -> HTTP 503
backup  -> transport error

result  -> primary's HTTP 503 response
```

If `GetBody` fails while preparing a request for another provider, that provider is not tried. A previously retained HTTP response can still be returned.

Request cancellation or deadline expiration takes priority over a retained response.

Responses that remain internal to `failnext` are closed when they are discarded or replaced.

Once a response is returned, its body belongs to the caller and should be closed normally.

### Errors

`ErrNoUsableEndpoint` is returned when endpoint selection leaves no usable provider to try.

Examples include:

* every provider is excluded by eligibility;
* every provider is cooling before the first attempt.

`ErrUnknownEndpoint` is returned when a request refers to an endpoint ID that does not exist, such as an unknown preferred endpoint.

`FailoverError` is used when:

* one or more provider attempts fail without producing a usable HTTP response; and
* there is no HTTP response available to return.

Its `Attempts` slice records those failed provider attempts in order.

The terminal cause remains available through normal Go error unwrapping:

```go
var fe *failnext.FailoverError
if errors.As(err, &fe) {
	for _, attempt := range fe.Attempts {
		log.Printf("endpoint=%s err=%v", attempt.Endpoint, attempt.Err)
	}
}

if errors.Is(err, failnext.ErrNoUsableEndpoint) {
	log.Printf("no provider could be tried")
}

if errors.Is(err, failnext.ErrUnknownEndpoint) {
	log.Printf("request referred to an unknown endpoint")
}
```

Cancellation and deadline expiration remain normal context errors and are not wrapped in `FailoverError`.

## Observability and transport composition

### Events

`Config.OnEvent` provides a synchronous observation hook:

```go
OnEvent: func(ctx context.Context, event failnext.Event) {
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

| Event               | Meaning                                                       |
| ------------------- | ------------------------------------------------------------- |
| `EventAttempt`      | A provider attempt completed.                                 |
| `EventCooldownSkip` | A provider was skipped because it is currently cooling down.  |
| `EventReplayError`  | Another provider could not be tried because `GetBody` failed. |
| `EventResult`       | `RoundTrip` is about to return the final response or error.   |

Events for one request are delivered in causal order.

Different requests may invoke the callback concurrently, so shared application state must be protected.

`OnEvent` is synchronous. The callback should return promptly.

`failnext` does not recover panics from `OnEvent`.

### Base transport

The configured `Base` transport handles every provider attempt.

If `Base` is nil, `http.DefaultTransport` is used.

Use `Base` for behavior that needs to run separately for each provider attempt, such as:

* endpoint-specific authentication;
* host- or path-bound signing;
* tracing;
* custom networking behavior.

```go
tr, err := failnext.New(failnext.Config{
	Endpoints: endpoints,
	Base:      customTransport,
})
```

### Concurrency and `CloseIdleConnections`

A constructed `Transport` is intended for concurrent reuse.

Custom base transports and application callbacks must satisfy their own concurrency requirements.

`PermissionPolicy`, `Eligible`, and `OnEvent` may run concurrently for different requests.

`Transport.CloseIdleConnections` forwards to the base transport when the base supports that operation, so `http.Client.CloseIdleConnections` composes normally.

## Destination-sensitive request state

Changing providers can also change the meaning of request state tied to a specific host or URL.

### `Request.Host`

`failnext` uses these rules:

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

The comparison is exact.

If a custom `Host` differs from the original `URL.Host`, `failnext` preserves it when trying another provider.

One ambiguity remains. If your application intentionally sets a custom `Host` to exactly the same value as the original `URL.Host`, `failnext` cannot distinguish that custom value from the normal URL-derived host value. In that case, the `Host` follows the selected provider.

If your application needs different `Host` behavior, `Config.Base` middleware can reapply the intended value for each provider attempt.

### Other destination-sensitive state

`Host` is only one example.

These values may also need application-specific handling:

* credentials;
* host- or path-bound signatures;
* provider-specific headers;
* cookies;
* session state;
* other destination-bound metadata.

`failnext` does not automatically regenerate, rewrite, or validate those values when it switches to another provider.

### Redirects

Redirect handling belongs to `http.Client`.

If `http.Client` follows a redirect, the redirected request starts a new `failnext` request. It is not another provider attempt in the current failover sequence.

## Generic `http.Client` usage

go-ethereum is a primary use case, but the transport itself is protocol-neutral.

Use `failnext` with a normal `http.Client`:

```go
package main

import (
	"log"
	"net/http"

	"github.com/yermakovsa/failnext"
)

func main() {
	tr, err := failnext.New(failnext.Config{
		Endpoints: []failnext.Endpoint{
			{ID: "primary", URL: "https://service-a.example/api"},
			{ID: "backup", URL: "https://service-b.example/api"},
		},
	})
	if err != nil {
		log.Fatal(err)
	}

	client := &http.Client{Transport: tr}

	req, err := http.NewRequest(
		http.MethodGet,
		"https://service-a.example/api",
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

`GET` requests are allowed to fail over by default, as long as the other failover conditions are satisfied.

Providers are tried in their configured priority order.

## Examples

The repository includes go-ethereum examples for the main integration patterns:

* [`examples/goethereum/basic-failover`](examples/goethereum/basic-failover): use `failnext` through `rpc.WithHTTPClient` and allow a JSON-RPC read to fail over.
* [`examples/goethereum/conservative-write`](examples/goethereum/conservative-write): keep a write from failing over to another provider.
* [`examples/goethereum/error-inspection`](examples/goethereum/error-inspection): inspect failed provider attempts with `FailoverError`.
* [`examples/goethereum/preferred-endpoint`](examples/goethereum/preferred-endpoint): try the provider that handled a previous request first for a follow-up request, without pinning to it.
* [`examples/goethereum/skip-ineligible`](examples/goethereum/skip-ineligible): skip providers the application marks as unsuitable.

## What failnext does not do

`failnext` is a focused HTTP failover transport, not a general resilience or RPC routing framework.

It does not provide:

* load balancing or service discovery;
* active health checks;
* automatic blockchain lag or stale-state detection;
* general-purpose retry, backoff, or `Retry-After` scheduling;
* application-protocol parsing, including JSON-RPC payload and error interpretation;
* buffering request bodies to make them replayable;
* a general signing, authentication, or header-rewrite framework;
* hard provider pinning;
* cross-provider state consistency;
* base-URL path or query composition;
* WebSocket failover;
* Ethereum transaction, nonce, or pending-state management.

If your application depends on provider-local state, sessions, pending state, or read-after-write behavior, it must handle those requirements itself.

## License

MIT. See [LICENSE](LICENSE).