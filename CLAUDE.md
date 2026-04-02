# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build & Test Commands

```bash
go build ./...                          # Build all packages
go test ./... -timeout 30s              # Run all tests
go test ./... -v -timeout 30s           # Verbose test output
go test -run TestConsumer_Integration   # Run a single test by name
go test ./sqs/... -v                    # Run tests for a specific package
go test ./otel/... -v
golangci-lint run ./...                 # Run linter
```

## Architecture

This is a consumer-focused SQS library with an interface-heavy design. The root package defines abstractions; subpackages provide concrete implementations.

### Package Dependency Graph

```
queuer/          (zero external deps - interfaces + Consumer orchestrator)
├── sqs/         (depends on: queuer, aws-sdk-go-v2)
└── otel/        (depends on: queuer, opentelemetry-go)
```

Subpackages are deliberately isolated so callers only pull the dependencies they use.

### Message Flow Through Consumer.Run()

```
Receiver.Receive() → buffered channel (cap=workers) → worker goroutines → Handler.Handle()
                                                                              │
                                                            success: Acknowledger.Ack()
                                                            error:   ErrorResolver.Resolve() → ErrorAction → Acknowledger method
```

- Poll loop and workers run as goroutines; the channel provides backpressure when all workers are busy.
- On context cancellation: polling stops, workers drain in-flight messages up to `shutdownTimeout`, then `Run()` returns.

### ErrorAction Dispatch

The `ErrorResolver` returns an `ErrorAction` that maps to Acknowledger calls:
- `Retry` → `ChangeVisibility` (30s backoff)
- `Nack` → `Nack` (visibility → 0, immediate retry)
- `Skip` → `Ack` (delete despite error)
- `DeadLetter` → no-op (SQS redrive policy handles it)

### sqs/ Package

Defines a `Client` interface (subset of `*sqs.Client`) for testability. `NewConsumer()` is the convenience constructor that wires `Receiver` + `Acknowledger` into a `queuer.Consumer`.

### otel/ Package

`Metrics` implements `MetricsCollector` using OTel instruments. `SetActiveWorkers` receives absolute counts but uses `sync/atomic.Int64` to compute deltas for the underlying `Int64UpDownCounter`.

## Testing Patterns

- Root package tests use mock implementations of all interfaces (no AWS/OTel deps needed).
- `mockReceiver` returns messages on first `Receive()` call, blocks on subsequent calls until context cancellation.
- SQS and OTel tests use their own mock clients (mock SQS `Client` interface, OTel `ManualReader`).
- Concurrency-safe assertions use `sync/atomic` counters and `sync.Mutex`-guarded slices.
