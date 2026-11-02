# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/),
and this project adheres to [Semantic Versioning](https://semver.org/).

## [0.2.0] - Unreleased

### Added
- Google Cloud Pub/Sub adapter (`queuer/pubsub`): receiver, acknowledger, publisher and `NewConsumer`.
- Lease extension (`WithLeaseExtension`): messages are kept leased from receipt until they are settled.
- Configurable retry backoff (`WithRetryBackoff`, `ExponentialBackoff`, `ConstantBackoff`), driven by the delivery count.
- Handler middleware (`Middleware`, `Chain`, `WithMiddleware`) and per-message handler timeouts (`WithHandlerTimeout`).
- `Publisher` interface with SQS (including FIFO group and deduplication IDs) and Pub/Sub (ordering keys) implementations.
- Opt-in batched acknowledgements for SQS and Pub/Sub (`WithBatching`).
- OpenTelemetry producer spans with trace context injection (`otel.NewPublisher`).
- `queuer.messages.settle_failed` metric and `MetricsCollector.IncSettleFailed`.
- `ErrorResolverFunc`, `PublisherFunc` and `ErrAlreadyRunning`.
- `Message.SentAt`, `Message.System` and `Message.Queue`.
- Integration tests against ElasticMQ and the Pub/Sub emulator, govulncheck and Dependabot in CI.

### Changed
- Breaking: `sqs`, `otel` and the new `pubsub` package are separate Go modules; the core module has no dependencies.
- Breaking: `Message.Body` is `[]byte`, and `Message.ReceiptHandle` is renamed to `Message.AckID`.
- Breaking: `MetricsCollector` has a new `IncSettleFailed` method.
- Breaking: `otel.WithTracerQueueLabel` is removed; spans take the system and destination from the message. Successful spans no longer set an Ok status, following the semantic conventions.
- Breaking: the default `Retry` backoff is exponential (30s doubling up to 15m) instead of a fixed 30s.
- `WithMaxMessages` no longer caps at 10; each adapter clamps to its provider's limit.
- Messages received but not yet handed to a worker at shutdown are nacked for prompt redelivery.
- `queuer.messages.processed` only counts messages whose ack succeeded.
- `ApproximateReceiveCount` is parsed with `strconv`, and SQS visibility timeouts are clamped to 12h.
- Lint configuration moved to `.golangci.yml` (golangci-lint v2); the old file was never picked up.

### Removed
- v0.1.0 is retracted.

## [0.1.0] - 2026-04-02

### Added
- Consumer with configurable worker pool and graceful shutdown
- Pluggable interfaces: Handler, Receiver, Acknowledger, ErrorResolver, MetricsCollector, Tracer
- SQS receiver and acknowledger implementations (`queuer/sqs`)
- OpenTelemetry metrics and distributed tracing (`queuer/otel`)
- Trace context propagation from SQS message attributes
- Panic recovery in worker goroutines with PanicError type
- Exponential backoff on receive errors (1s to 30s cap)
- ErrShutdownTimeout sentinel error
