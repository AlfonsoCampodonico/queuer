# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/),
and this project adheres to [Semantic Versioning](https://semver.org/).

## [0.1.0] - 2026-04-02

### Added
- Consumer with configurable worker pool and graceful shutdown
- Pluggable interfaces: Handler, Receiver, Acknowledger, ErrorResolver, MetricsCollector, Tracer
- SQS receiver and acknowledger implementations (`queuer/sqs`)
- OpenTelemetry metrics and distributed tracing (`queuer/otel`)
- Trace context propagation from SQS message attributes
- Functional options: WithWorkers, WithShutdownTimeout, WithWaitTime, WithMaxMessages, WithErrorResolver, WithMetrics, WithTracer, WithLogger
- HandlerFunc adapter for using plain functions as handlers
- DefaultErrorResolver (always Retry) and NoopMetrics/NoopTracer defaults
- Panic recovery in worker goroutines with PanicError type
- Exponential backoff on receive errors (1s to 30s cap)
- Separate processing context during shutdown (handlers finish without cancelled ctx)
- Input validation in New() constructor
- ErrShutdownTimeout sentinel error
- CI pipeline with Go 1.22+ matrix, race detector, golangci-lint
- Release pipeline triggered by version tags
