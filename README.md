# queuer

A Go library for consuming messages from AWS SQS queues. Interface-heavy design with pluggable implementations for maximum flexibility and testability.

## Install

```bash
go get github.com/AlfonsoCampodonico/queuer
go get github.com/AlfonsoCampodonico/queuer/sqs   # AWS SQS implementation
go get github.com/AlfonsoCampodonico/queuer/otel  # OpenTelemetry metrics
```

## Quick Start

```go
package main

import (
	"context"
	"log"
	"log/slog"
	"os/signal"
	"syscall"

	"github.com/aws/aws-sdk-go-v2/config"
	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"

	"github.com/AlfonsoCampodonico/queuer"
	"github.com/AlfonsoCampodonico/queuer/sqs"
)

func main() {
	cfg, err := config.LoadDefaultConfig(context.Background())
	if err != nil {
		log.Fatal(err)
	}

	client := awssqs.NewFromConfig(cfg)

	handler := queuer.HandlerFunc(func(ctx context.Context, msg *queuer.Message) error {
		slog.Info("processing message", "id", msg.ID, "body", msg.Body)
		return nil
	})

	consumer := sqs.NewConsumer(client, "https://sqs.us-east-1.amazonaws.com/123456789/my-queue", handler,
		queuer.WithWorkers(10),
		queuer.WithShutdownTimeout(30*time.Second),
	)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if err := consumer.Run(ctx); err != nil {
		log.Fatal(err)
	}
}
```

## Features

- **Worker pool** with configurable concurrency
- **Graceful shutdown** with timeout — drains in-flight messages before exiting
- **Pluggable error handling** — per-error control over retry, nack, skip, or dead-letter behavior
- **OpenTelemetry metrics** — counters, histograms, and active worker tracking (Grafana/Mimir compatible)
- **Structured logging** via `log/slog`
- **Interface-driven** — swap or mock any component (receiver, acknowledger, error resolver, metrics)

## Configuration

```go
queuer.WithWorkers(10)                           // Worker goroutines (default: 5)
queuer.WithShutdownTimeout(60 * time.Second)     // Shutdown drain timeout (default: 30s)
queuer.WithWaitTime(20 * time.Second)            // SQS long-poll duration (default: 20s)
queuer.WithMaxMessages(10)                       // Messages per receive call (default: 10)
queuer.WithErrorResolver(myResolver)             // Custom error handling (default: always Retry)
queuer.WithMetrics(myMetrics)                    // Metrics collector (default: no-op)
queuer.WithLogger(slog.Default())                // Structured logger (default: slog.Default())
```

## Error Handling

Implement `ErrorResolver` to control what happens when a handler returns an error:

```go
type MyResolver struct{}

func (r *MyResolver) Resolve(ctx context.Context, msg *queuer.Message, err error) (queuer.ErrorAction, error) {
	if errors.Is(err, ErrInvalidPayload) {
		return queuer.Skip, nil // delete the message
	}
	if msg.ReceiveCount > 5 {
		return queuer.DeadLetter, nil // let SQS redrive policy handle it
	}
	return queuer.Nack, nil // immediate retry
}
```

| Action | Behavior |
|--------|----------|
| `Retry` | Changes visibility timeout (30s backoff) |
| `Nack` | Sets visibility to 0 for immediate retry |
| `Skip` | Deletes the message despite the error |
| `DeadLetter` | Leaves message in queue for SQS redrive policy |

## OpenTelemetry Metrics

```go
import qotelotel "github.com/AlfonsoCampodonico/queuer/otel"

metrics, err := qotelotel.NewMetrics(otel.GetMeterProvider(),
	qotelotel.WithQueueLabel("https://sqs.us-east-1.amazonaws.com/123456789/my-queue"),
)

consumer := sqs.NewConsumer(client, queueURL, handler,
	queuer.WithMetrics(metrics),
)
```

| Metric | Type | Description |
|--------|------|-------------|
| `queuer.messages.received` | Counter | Messages received from source |
| `queuer.messages.processed` | Counter | Successfully processed messages |
| `queuer.messages.failed` | Counter | Failed messages (labeled by action) |
| `queuer.messages.duration` | Histogram | Processing time per message (seconds) |
| `queuer.workers.active` | UpDownCounter | Currently busy workers |

## Custom Implementations

Use the generic constructor to bring your own receiver, acknowledger, or metrics:

```go
consumer := queuer.New(myReceiver, myHandler, myAcknowledger,
	queuer.WithErrorResolver(myResolver),
	queuer.WithMetrics(myMetrics),
)
```

Any type satisfying the `Receiver`, `Acknowledger`, `ErrorResolver`, or `MetricsCollector` interfaces can be plugged in.

## License

MIT
