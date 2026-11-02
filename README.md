# queuer

[![CI](https://github.com/AlfonsoCampodonico/queuer/actions/workflows/ci.yml/badge.svg)](https://github.com/AlfonsoCampodonico/queuer/actions/workflows/ci.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/AlfonsoCampodonico/queuer.svg)](https://pkg.go.dev/github.com/AlfonsoCampodonico/queuer)

A Go library for consuming (and publishing) queue messages on **Amazon SQS** and **Google Cloud Pub/Sub**. One consumer runs on either provider: worker pool, lease extension, retries with backoff, graceful shutdown, OpenTelemetry metrics and tracing.

## Modules

Each provider lives in its own Go module, so you only download the SDK you use.

| Module | Import path | Go |
|---|---|---|
| Core (no dependencies) | `github.com/AlfonsoCampodonico/queuer` | 1.25+ |
| Amazon SQS | `github.com/AlfonsoCampodonico/queuer/sqs` | 1.25+ |
| Google Cloud Pub/Sub | `github.com/AlfonsoCampodonico/queuer/pubsub` | 1.26+ |
| OpenTelemetry | `github.com/AlfonsoCampodonico/queuer/otel` | 1.26+ |

```bash
go get github.com/AlfonsoCampodonico/queuer/sqs     # or
go get github.com/AlfonsoCampodonico/queuer/pubsub
go get github.com/AlfonsoCampodonico/queuer/otel    # optional
```

## Quick start: Amazon SQS

```go
cfg, err := config.LoadDefaultConfig(ctx)
if err != nil {
	log.Fatal(err)
}
client := awssqs.NewFromConfig(cfg)

handler := queuer.HandlerFunc(func(ctx context.Context, msg *queuer.Message) error {
	slog.Info("processing", "id", msg.ID, "body", string(msg.Body))
	return nil
})

consumer := sqs.NewConsumer(client, "https://sqs.us-east-1.amazonaws.com/123456789012/orders", handler,
	queuer.WithWorkers(10),
)

ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
defer stop()
if err := consumer.Run(ctx); err != nil {
	log.Fatal(err)
}
```

## Quick start: Google Cloud Pub/Sub

The adapter uses the low-level `cloud.google.com/go/pubsub/v2/apiv1` clients, which pick up Application Default Credentials (or `PUBSUB_EMULATOR_HOST`).

```go
client, err := apiv1.NewSubscriptionAdminClient(ctx)
if err != nil {
	log.Fatal(err)
}
defer client.Close()

consumer := pubsub.NewConsumer(client, pubsub.SubscriptionName("my-project", "orders-sub"), handler,
	queuer.WithWorkers(10),
	queuer.WithMaxMessages(100),
)
if err := consumer.Run(ctx); err != nil {
	log.Fatal(err)
}
```

`pubsub.NewConsumer` turns on lease extension (60s) by default, because Pub/Sub's default ack deadline is only 10s.

## How the providers map

| queuer | SQS | Pub/Sub |
|---|---|---|
| `Receive` | `ReceiveMessage` (max 10, long poll up to 20s) | `Pull` (max 1000) |
| `Ack` | `DeleteMessage` | `Acknowledge` |
| `Nack` | `ChangeMessageVisibility(0)` | `ModifyAckDeadline(0)` |
| `ChangeVisibility` | `ChangeMessageVisibility` (max 12h) | `ModifyAckDeadline` (max 600s) |
| `Message.AckID` | receipt handle | ack ID |
| `Message.ReceiveCount` | `ApproximateReceiveCount` | `delivery_attempt` (needs a dead-letter policy) |
| `Message.SentAt` | `SentTimestamp` | `publish_time` |

## Features

- **Worker pool** with backpressure: polling pauses while every worker is busy.
- **Lease extension**: messages stay invisible to other consumers while they wait for a worker and while the handler runs.
- **Retries with backoff**: per-error choice of retry, nack, skip or dead-letter, with exponential backoff based on the delivery count.
- **Graceful shutdown**: in-flight handlers finish (up to a timeout), and messages that were received but never started are released for immediate redelivery.
- **Batched acknowledgements** (opt-in) to cut API calls and cost.
- **Middleware** and per-message **handler timeouts**.
- **Panic recovery** in workers.
- **OpenTelemetry** metrics and tracing, with trace context propagated from producer to consumer.
- **Structured logging** with `log/slog`.

## Configuration

```go
queuer.WithWorkers(10)                                        // worker goroutines (default 5)
queuer.WithMaxMessages(10)                                    // per receive call, clamped by the adapter (default 10)
queuer.WithWaitTime(20 * time.Second)                         // long-poll / pull wait (default 20s)
queuer.WithLeaseExtension(2 * time.Minute)                    // keep messages leased while processing (default off; Pub/Sub on)
queuer.WithHandlerTimeout(time.Minute)                        // cancel the handler context (default off)
queuer.WithRetryBackoff(queuer.ExponentialBackoff(30*time.Second, 15*time.Minute)) // default
queuer.WithErrorResolver(myResolver)                          // default: always Retry
queuer.WithMiddleware(logging, idempotency)                   // wrap the handler
queuer.WithShutdownTimeout(30 * time.Second)                  // drain timeout (default 30s)
queuer.WithMetrics(metrics)                                   // default: no-op
queuer.WithTracer(tracer)                                     // default: no-op
queuer.WithLogger(slog.Default())                             // default: slog.Default()
```

## Error handling

Return an error from the handler and the `ErrorResolver` decides what happens:

```go
resolver := queuer.ErrorResolverFunc(func(ctx context.Context, msg *queuer.Message, err error) (queuer.ErrorAction, error) {
	switch {
	case errors.Is(err, ErrInvalidPayload):
		return queuer.Skip, nil // delete it, retrying won't help
	case msg.ReceiveCount >= 5:
		return queuer.DeadLetter, nil // let the redrive / dead-letter policy move it
	default:
		return queuer.Retry, nil // hide it for the retry backoff
	}
})
```

| Action | What happens |
|---|---|
| `Retry` | Hidden for `WithRetryBackoff(msg.ReceiveCount)`, then redelivered |
| `Nack` | Redelivered immediately |
| `Skip` | Acknowledged (deleted) despite the error |
| `DeadLetter` | Left unsettled for the provider's dead-letter policy |

Handler panics are recovered and reported to the resolver as `*queuer.PanicError`.

## Batched acknowledgements

Batching groups acks and visibility changes into `DeleteMessageBatch` / `ChangeMessageVisibilityBatch` (SQS, up to 10) or shared `Acknowledge` / `ModifyAckDeadline` requests (Pub/Sub, up to 1000). Each settle call still gets its own result. It adds up to `maxWait` of latency per settle, so use it with many workers.

```go
consumer := queuer.New(
	sqs.NewReceiver(client, queueURL),
	handler,
	sqs.NewAcknowledger(client, queueURL, sqs.WithBatching(50*time.Millisecond)),
	queuer.WithWorkers(50),
)
```

## Publishing

```go
pub := sqs.NewPublisher(client, queueURL)                       // or
pub := pubsub.NewPublisher(topicClient, pubsub.TopicName("my-project", "orders"))

id, err := pub.Publish(ctx, &queuer.Message{
	Body:       []byte(`{"order_id":42}`),
	Attributes: map[string]string{"type": "order.created"},
})
```

FIFO queues: `sqs.WithMessageGroupID` and `sqs.WithDeduplicationID`. Pub/Sub ordering: `pubsub.WithOrderingKey`.

## OpenTelemetry

```go
import qotel "github.com/AlfonsoCampodonico/queuer/otel"

metrics, err := qotel.NewMetrics(otel.GetMeterProvider(),
	qotel.WithAttributes(attribute.String("queue", "orders")),
)

consumer := sqs.NewConsumer(client, queueURL, handler,
	queuer.WithMetrics(metrics),
	queuer.WithTracer(qotel.NewTracer(otel.GetTracerProvider())),
)

// Producer side: a "send" span plus W3C trace context in the message attributes.
pub := qotel.NewPublisher(sqs.NewPublisher(client, queueURL), otel.GetTracerProvider())
```

Spans follow the OpenTelemetry messaging semantic conventions (`process <queue>` / `send <queue>`, `messaging.system`, `messaging.destination.name`, `messaging.message.id`, `messaging.message.delivery_count`).

| Metric | Type | Description |
|---|---|---|
| `queuer.messages.received` | Counter | Messages received |
| `queuer.messages.processed` | Counter | Messages handled and acknowledged |
| `queuer.messages.failed` | Counter | Handler failures, by `action` |
| `queuer.messages.settle_failed` | Counter | Failed ack / nack / visibility / lease calls, by `operation` |
| `queuer.messages.duration` | Histogram | Handler duration (seconds) |
| `queuer.workers.active` | UpDownCounter | Busy workers |

## Custom providers

Implement `queuer.Receiver` and `queuer.Acknowledger` (and optionally `queuer.Publisher`) for any other broker, then use `queuer.New`.

## Development

```bash
go test -race ./...                          # in each module directory: ., sqs, otel, pubsub
golangci-lint run ./...

# Integration tests
docker run --rm -p 9324:9324 softwaremill/elasticmq-native
(cd sqs && QUEUER_SQS_ENDPOINT=http://localhost:9324 go test -tags integration ./...)

docker run --rm -p 8085:8085 gcr.io/google.com/cloudsdktool/google-cloud-cli:emulators \
  gcloud beta emulators pubsub start --host-port=0.0.0.0:8085 --project=queuer-test
(cd pubsub && PUBSUB_EMULATOR_HOST=localhost:8085 go test -tags integration ./...)
```

### Releasing

Tag the core module first, then each adapter, at the same commit:

```bash
git tag v0.2.0 && git tag sqs/v0.2.0 && git tag otel/v0.2.0 && git tag pubsub/v0.2.0
git push origin v0.2.0 sqs/v0.2.0 otel/v0.2.0 pubsub/v0.2.0
```

The adapters' `go.mod` files require the core version and use a `replace ../` directive for local development; the directive is ignored by consumers.

## License

MIT
