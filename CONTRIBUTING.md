# Contributing

Contributions to `failnext`, a Go library for client-side HTTP failover, are welcome. Bug fixes, tests, documentation improvements, and other focused contributions are appreciated.

## Before You Start

Check existing issues and pull requests before starting work.

For new features or significant changes to behavior or the public API, please open an issue before starting work.

`failnext` is intentionally focused on client-side HTTP failover. Contributions should preserve that scope.

## Development

The library requires Go 1.24 or newer.

Run the tests from the repository root:

```bash
go test ./...
go test -race ./...
```

The go-ethereum examples use a separate Go module. Run their tests with:

```bash
cd examples/goethereum
go test ./...
```

Format Go code with `gofmt` before submitting changes.

## Pull Requests

Keep pull requests focused and explain what changed and why.

Include tests for behavior changes and update documentation when relevant. Make sure the applicable tests pass before submitting.

CI runs on pull requests and checks formatting, static analysis, supported Go versions, race detection, go-ethereum integration, and known vulnerabilities. See the [CI workflow](.github/workflows/ci.yml) for details.

## Security Issues

Please report suspected security vulnerabilities privately, following the [Security Policy](SECURITY.md), rather than opening a public issue.