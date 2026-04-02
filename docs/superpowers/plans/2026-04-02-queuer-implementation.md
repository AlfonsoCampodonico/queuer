# Queuer Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a consumer-focused SQS library in Go with pluggable interfaces for receiving, handling, acknowledging, error resolution, and metrics collection.

**Architecture:** Interface-heavy design with a `Consumer` orchestrator in the root package. AWS SDK v2 and OTel implementations live in isolated subpackages (`sqs/`, `otel/`) so callers only pull dependencies they use. The `Consumer` uses a fixed worker pool, buffered channel dispatch, and graceful shutdown with configurable timeout.

**Tech Stack:** Go 1.22+, AWS SDK for Go v2 (`github.com/aws/aws-sdk-go-v2`), OpenTelemetry Go (`go.opentelemetry.io/otel`), stdlib `log/slog`

**Module:** `github.com/AlfonsoCampodonico/queuer`

**Note on `NewSQS`:** The spec places `NewSQS` in the root package, but that would pull the AWS SDK into the root module. To keep dependencies isolated, `NewConsumer` lives in the `sqs/` subpackage instead. Usage: `sqsconsumer.NewConsumer(client, url, handler, opts...)`.

---

## File Structure

| File | Responsibility |
|------|---------------|
| `go.mod` | Root module declaration |
| `handler.go` | `Message` type, `Handler` interface |
| `errors.go` | `ErrorAction`, `ErrorResolver` interface, `DefaultErrorResolver` |
| `receiver.go` | `Receiver` interface |
| `acknowledger.go` | `Acknowledger` interface |
| `metrics.go` | `MetricsCollector` interface, `NoopMetrics` |
| `options.go` | `Option` type, all `With...` functions |
| `consumer.go` | `Consumer` struct, `New`, `Run` |
| `consumer_test.go` | All root-package tests (mocks + consumer behavior) |
| `sqs/receiver.go` | SQS `Receiver` implementation |
| `sqs/acknowledger.go` | SQS `Acknowledger` implementation |
| `sqs/consumer.go` | `NewConsumer` convenience constructor |
| `sqs/sqs_test.go` | SQS subpackage tests |
| `otel/metrics.go` | OTel `MetricsCollector` implementation |
| `otel/options.go` | OTel-specific options |
| `otel/metrics_test.go` | OTel subpackage tests |

---

### Task 1: Initialize Go Module and Core Types

**Files:**
- Create: `go.mod`
- Create: `handler.go`
- Create: `errors.go`

- [ ] **Step 1: Initialize Go module**

Run:
```bash
cd /Users/alfonso/Github/Personal/queuer
go mod init github.com/AlfonsoCampodonico/queuer
```

- [ ] **Step 2: Create `handler.go` with Message type and Handler interface**

```go
// handler.go
package queuer

import "context"

// Message represents a message received from a queue.
type Message struct {
	ID            string
	Body          string
	Attributes    map[string]string
	ReceiptHandle string
	ReceiveCount  int
}

// Handler processes a single message. Return nil to acknowledge
// the message, or an error to trigger the ErrorResolver.
type Handler interface {
	Handle(ctx context.Context, msg *Message) error
}

// HandlerFunc is an adapter to allow use of ordinary functions as Handlers.
type HandlerFunc func(ctx context.Context, msg *Message) error

func (f HandlerFunc) Handle(ctx context.Context, msg *Message) error {
	return f(ctx, msg)
}
```

- [ ] **Step 3: Create `errors.go` with ErrorAction and ErrorResolver**

```go
// errors.go
package queuer

import "context"

// ErrorAction tells the consumer what to do after a handler error.
type ErrorAction int

const (
	// Retry re-queues the message with a backoff (changes visibility timeout).
	Retry ErrorAction = iota
	// Nack sets visibility timeout to 0 for immediate retry.
	Nack
	// Skip deletes the message despite the error.
	Skip
	// DeadLetter leaves the message in the queue for SQS redrive policy to handle.
	DeadLetter
)

func (a ErrorAction) String() string {
	switch a {
	case Retry:
		return "retry"
	case Nack:
		return "nack"
	case Skip:
		return "skip"
	case DeadLetter:
		return "dead_letter"
	default:
		return "unknown"
	}
}

// ErrorResolver decides what action to take when a handler returns an error.
type ErrorResolver interface {
	Resolve(ctx context.Context, msg *Message, err error) (ErrorAction, error)
}

// DefaultErrorResolver returns Retry for all errors.
type DefaultErrorResolver struct{}

func (d *DefaultErrorResolver) Resolve(_ context.Context, _ *Message, _ error) (ErrorAction, error) {
	return Retry, nil
}
```

- [ ] **Step 4: Verify it compiles**

Run: `go build ./...`
Expected: no errors

- [ ] **Step 5: Commit**

```bash
git add go.mod handler.go errors.go
git commit -m "Initialize module with Message, Handler, ErrorAction, ErrorResolver"
```

---

### Task 2: Define Infrastructure Interfaces

**Files:**
- Create: `receiver.go`
- Create: `acknowledger.go`
- Create: `metrics.go`

- [ ] **Step 1: Create `receiver.go`**

```go
// receiver.go
package queuer

import (
	"context"
	"time"
)

// Receiver polls for messages from a queue source.
type Receiver interface {
	Receive(ctx context.Context, maxMessages int, waitTime time.Duration) ([]*Message, error)
}
```

- [ ] **Step 2: Create `acknowledger.go`**

```go
// acknowledger.go
package queuer

import (
	"context"
	"time"
)

// Acknowledger manages message lifecycle after processing.
type Acknowledger interface {
	Ack(ctx context.Context, msg *Message) error
	Nack(ctx context.Context, msg *Message) error
	ChangeVisibility(ctx context.Context, msg *Message, timeout time.Duration) error
}
```

- [ ] **Step 3: Create `metrics.go` with interface and NoopMetrics**

```go
// metrics.go
package queuer

import "time"

// MetricsCollector records operational metrics for the consumer.
type MetricsCollector interface {
	IncMessagesReceived(count int)
	IncMessagesProcessed()
	IncMessagesFailed(action ErrorAction)
	ObserveProcessingDuration(duration time.Duration)
	SetActiveWorkers(count int)
}

// NoopMetrics is a MetricsCollector that does nothing.
type NoopMetrics struct{}

func (n *NoopMetrics) IncMessagesReceived(_ int)              {}
func (n *NoopMetrics) IncMessagesProcessed()                  {}
func (n *NoopMetrics) IncMessagesFailed(_ ErrorAction)        {}
func (n *NoopMetrics) ObserveProcessingDuration(_ time.Duration) {}
func (n *NoopMetrics) SetActiveWorkers(_ int)                 {}
```

- [ ] **Step 4: Verify it compiles**

Run: `go build ./...`
Expected: no errors

- [ ] **Step 5: Commit**

```bash
git add receiver.go acknowledger.go metrics.go
git commit -m "Add Receiver, Acknowledger, MetricsCollector interfaces"
```

---

### Task 3: Consumer Struct, Options, and Constructor

**Files:**
- Create: `options.go`
- Create: `consumer.go`

- [ ] **Step 1: Create `options.go`**

```go
// options.go
package queuer

import (
	"log/slog"
	"time"
)

// Option configures a Consumer.
type Option func(*Consumer)

// WithWorkers sets the number of concurrent worker goroutines. Default: 5.
func WithWorkers(n int) Option {
	return func(c *Consumer) {
		if n > 0 {
			c.workers = n
		}
	}
}

// WithShutdownTimeout sets the maximum time to wait for in-flight
// messages during shutdown. Default: 30s.
func WithShutdownTimeout(d time.Duration) Option {
	return func(c *Consumer) {
		c.shutdownTimeout = d
	}
}

// WithWaitTime sets the SQS long-poll duration. Default: 20s.
func WithWaitTime(d time.Duration) Option {
	return func(c *Consumer) {
		c.waitTime = d
	}
}

// WithMaxMessages sets the max messages per receive call. Default: 10.
func WithMaxMessages(n int) Option {
	return func(c *Consumer) {
		if n > 0 && n <= 10 {
			c.maxMessages = n
		}
	}
}

// WithErrorResolver sets the error resolver. Default: DefaultErrorResolver (always Retry).
func WithErrorResolver(er ErrorResolver) Option {
	return func(c *Consumer) {
		if er != nil {
			c.errorResolver = er
		}
	}
}

// WithMetrics sets the metrics collector. Default: NoopMetrics.
func WithMetrics(mc MetricsCollector) Option {
	return func(c *Consumer) {
		if mc != nil {
			c.metrics = mc
		}
	}
}

// WithLogger sets the structured logger. Default: slog.Default().
func WithLogger(l *slog.Logger) Option {
	return func(c *Consumer) {
		if l != nil {
			c.logger = l
		}
	}
}
```

- [ ] **Step 2: Create `consumer.go` with struct and constructor (no Run yet)**

```go
// consumer.go
package queuer

import (
	"log/slog"
	"time"
)

// Consumer orchestrates polling, dispatching to workers, error handling,
// and graceful shutdown.
type Consumer struct {
	receiver      Receiver
	handler       Handler
	acknowledger  Acknowledger
	errorResolver ErrorResolver
	metrics       MetricsCollector
	logger        *slog.Logger

	workers         int
	shutdownTimeout time.Duration
	waitTime        time.Duration
	maxMessages     int
}

// New creates a Consumer with the given interfaces and options.
func New(receiver Receiver, handler Handler, acknowledger Acknowledger, opts ...Option) *Consumer {
	c := &Consumer{
		receiver:        receiver,
		handler:         handler,
		acknowledger:    acknowledger,
		errorResolver:   &DefaultErrorResolver{},
		metrics:         &NoopMetrics{},
		logger:          slog.Default(),
		workers:         5,
		shutdownTimeout: 30 * time.Second,
		waitTime:        20 * time.Second,
		maxMessages:     10,
	}
	for _, opt := range opts {
		opt(c)
	}
	return c
}
```

- [ ] **Step 3: Verify it compiles**

Run: `go build ./...`
Expected: no errors

- [ ] **Step 4: Commit**

```bash
git add options.go consumer.go
git commit -m "Add Consumer struct, constructor, and option functions"
```

---

### Task 4: Consumer.Run — Poll Loop and Worker Pool

**Files:**
- Modify: `consumer.go`
- Create: `consumer_test.go`

- [ ] **Step 1: Write failing test — consumer processes messages and acks on success**

```go
// consumer_test.go
package queuer_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/AlfonsoCampodonico/queuer"
)

// --- Mock implementations ---

type mockReceiver struct {
	messages []*queuer.Message
	mu       sync.Mutex
	calls    int
}

func (m *mockReceiver) Receive(ctx context.Context, maxMessages int, waitTime time.Duration) ([]*queuer.Message, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls++
	if m.calls == 1 {
		return m.messages, nil
	}
	// After first call, block until context is cancelled
	m.mu.Unlock()
	<-ctx.Done()
	m.mu.Lock()
	return nil, ctx.Err()
}

type mockAcknowledger struct {
	acked []*queuer.Message
	mu    sync.Mutex
}

func (m *mockAcknowledger) Ack(_ context.Context, msg *queuer.Message) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.acked = append(m.acked, msg)
	return nil
}

func (m *mockAcknowledger) Nack(_ context.Context, _ *queuer.Message) error { return nil }
func (m *mockAcknowledger) ChangeVisibility(_ context.Context, _ *queuer.Message, _ time.Duration) error {
	return nil
}

// --- Tests ---

func TestConsumer_ProcessesAndAcks(t *testing.T) {
	msgs := []*queuer.Message{
		{ID: "1", Body: "hello", ReceiptHandle: "r1"},
		{ID: "2", Body: "world", ReceiptHandle: "r2"},
	}

	recv := &mockReceiver{messages: msgs}
	ack := &mockAcknowledger{}

	var processed atomic.Int32
	handler := queuer.HandlerFunc(func(_ context.Context, msg *queuer.Message) error {
		processed.Add(1)
		return nil
	})

	c := queuer.New(recv, handler, ack, queuer.WithWorkers(2))

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	_ = c.Run(ctx)

	if got := processed.Load(); got != 2 {
		t.Errorf("processed = %d, want 2", got)
	}

	ack.mu.Lock()
	defer ack.mu.Unlock()
	if got := len(ack.acked); got != 2 {
		t.Errorf("acked = %d, want 2", got)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./... -run TestConsumer_ProcessesAndAcks -v`
Expected: FAIL (Run method doesn't exist yet)

- [ ] **Step 3: Implement `Consumer.Run` in `consumer.go`**

Add the following to `consumer.go`:

```go
import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"
)

// Run starts the poll loop and worker pool. It blocks until ctx is
// cancelled, then drains in-flight work up to shutdownTimeout.
func (c *Consumer) Run(ctx context.Context) error {
	c.logger.Info("consumer starting", "workers", c.workers, "max_messages", c.maxMessages)

	msgCh := make(chan *Message, c.workers)
	var wg sync.WaitGroup

	// Start workers
	for i := 0; i < c.workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for msg := range msgCh {
				c.metrics.SetActiveWorkers(1) // increment conceptually; refined in Task 6
				c.processMessage(ctx, msg)
				c.metrics.SetActiveWorkers(0)
			}
		}()
	}

	// Poll loop
	go func() {
		defer close(msgCh)
		for {
			if ctx.Err() != nil {
				return
			}
			msgs, err := c.receiver.Receive(ctx, c.maxMessages, c.waitTime)
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				c.logger.Warn("receive error", "error", err)
				continue
			}
			c.metrics.IncMessagesReceived(len(msgs))
			c.logger.Debug("received messages", "count", len(msgs))
			for _, msg := range msgs {
				select {
				case msgCh <- msg:
				case <-ctx.Done():
					return
				}
			}
		}
	}()

	// Wait for context cancellation
	<-ctx.Done()
	c.logger.Info("shutdown initiated, draining in-flight messages", "timeout", c.shutdownTimeout)

	// Drain with timeout
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		c.logger.Info("consumer stopped, all messages drained")
		return nil
	case <-time.After(c.shutdownTimeout):
		c.logger.Warn("shutdown timeout exceeded, some messages may not have been processed")
		return errors.New("queuer: shutdown timeout exceeded")
	}
}

func (c *Consumer) processMessage(ctx context.Context, msg *Message) {
	start := time.Now()

	err := c.handler.Handle(ctx, msg)
	duration := time.Since(start)
	c.metrics.ObserveProcessingDuration(duration)

	if err == nil {
		if ackErr := c.acknowledger.Ack(ctx, msg); ackErr != nil {
			c.logger.Warn("ack failed", "message_id", msg.ID, "error", ackErr)
		} else {
			c.logger.Debug("message acked", "message_id", msg.ID)
		}
		c.metrics.IncMessagesProcessed()
		return
	}

	c.logger.Warn("handler error", "message_id", msg.ID, "error", err)

	action, resolveErr := c.errorResolver.Resolve(ctx, msg, err)
	if resolveErr != nil {
		c.logger.Warn("error resolver failed, leaving message in queue",
			"message_id", msg.ID, "error", resolveErr)
		return
	}

	c.metrics.IncMessagesFailed(action)
	c.executeErrorAction(ctx, msg, action)
}

func (c *Consumer) executeErrorAction(ctx context.Context, msg *Message, action ErrorAction) {
	switch action {
	case Retry:
		timeout := 30 * time.Second // base backoff
		if err := c.acknowledger.ChangeVisibility(ctx, msg, timeout); err != nil {
			c.logger.Warn("change visibility failed", "message_id", msg.ID, "error", err)
		}
	case Nack:
		if err := c.acknowledger.Nack(ctx, msg); err != nil {
			c.logger.Warn("nack failed", "message_id", msg.ID, "error", err)
		}
	case Skip:
		if err := c.acknowledger.Ack(ctx, msg); err != nil {
			c.logger.Warn("ack (skip) failed", "message_id", msg.ID, "error", err)
		}
	case DeadLetter:
		c.logger.Warn("message sent to dead letter", "message_id", msg.ID)
		// Do nothing — SQS redrive policy handles it
	}
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./... -run TestConsumer_ProcessesAndAcks -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add consumer.go consumer_test.go
git commit -m "Implement Consumer.Run with poll loop, worker pool, and message processing"
```

---

### Task 5: Error Handling Flow

**Files:**
- Modify: `consumer_test.go`

- [ ] **Step 1: Write failing test — error resolver controls action**

Add to `consumer_test.go`:

```go
type mockErrorResolver struct {
	action queuer.ErrorAction
}

func (m *mockErrorResolver) Resolve(_ context.Context, _ *queuer.Message, _ error) (queuer.ErrorAction, error) {
	return m.action, nil
}

func TestConsumer_ErrorResolver_Nack(t *testing.T) {
	msgs := []*queuer.Message{{ID: "1", Body: "bad", ReceiptHandle: "r1"}}
	recv := &mockReceiver{messages: msgs}

	var nacked atomic.Int32
	ack := &nackTrackingAcknowledger{onNack: func() { nacked.Add(1) }}

	handler := queuer.HandlerFunc(func(_ context.Context, _ *queuer.Message) error {
		return errors.New("processing failed")
	})

	resolver := &mockErrorResolver{action: queuer.Nack}
	c := queuer.New(recv, handler, ack,
		queuer.WithWorkers(1),
		queuer.WithErrorResolver(resolver),
	)

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	_ = c.Run(ctx)

	if got := nacked.Load(); got != 1 {
		t.Errorf("nacked = %d, want 1", got)
	}
}

func TestConsumer_ErrorResolver_Skip(t *testing.T) {
	msgs := []*queuer.Message{{ID: "1", Body: "bad", ReceiptHandle: "r1"}}
	recv := &mockReceiver{messages: msgs}
	ack := &mockAcknowledger{}

	handler := queuer.HandlerFunc(func(_ context.Context, _ *queuer.Message) error {
		return errors.New("processing failed")
	})

	resolver := &mockErrorResolver{action: queuer.Skip}
	c := queuer.New(recv, handler, ack,
		queuer.WithWorkers(1),
		queuer.WithErrorResolver(resolver),
	)

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	_ = c.Run(ctx)

	ack.mu.Lock()
	defer ack.mu.Unlock()
	if got := len(ack.acked); got != 1 {
		t.Errorf("acked (skip) = %d, want 1", got)
	}
}

// nackTrackingAcknowledger tracks Nack calls
type nackTrackingAcknowledger struct {
	onNack func()
}

func (m *nackTrackingAcknowledger) Ack(_ context.Context, _ *queuer.Message) error { return nil }
func (m *nackTrackingAcknowledger) Nack(_ context.Context, _ *queuer.Message) error {
	m.onNack()
	return nil
}
func (m *nackTrackingAcknowledger) ChangeVisibility(_ context.Context, _ *queuer.Message, _ time.Duration) error {
	return nil
}
```

Add `"errors"` to the import block.

- [ ] **Step 2: Run tests to verify they pass**

Run: `go test ./... -run "TestConsumer_ErrorResolver" -v`
Expected: PASS (implementation already handles this in Task 4)

- [ ] **Step 3: Commit**

```bash
git add consumer_test.go
git commit -m "Add error resolver tests for Nack and Skip actions"
```

---

### Task 6: Graceful Shutdown and Active Worker Tracking

**Files:**
- Modify: `consumer.go`
- Modify: `consumer_test.go`

- [ ] **Step 1: Write failing test — shutdown waits for in-flight then exits**

Add to `consumer_test.go`:

```go
func TestConsumer_GracefulShutdown_WaitsForInFlight(t *testing.T) {
	msgs := []*queuer.Message{{ID: "1", Body: "slow", ReceiptHandle: "r1"}}
	recv := &mockReceiver{messages: msgs}
	ack := &mockAcknowledger{}

	var finished atomic.Bool
	handler := queuer.HandlerFunc(func(_ context.Context, _ *queuer.Message) error {
		time.Sleep(200 * time.Millisecond)
		finished.Store(true)
		return nil
	})

	c := queuer.New(recv, handler, ack,
		queuer.WithWorkers(1),
		queuer.WithShutdownTimeout(2*time.Second),
	)

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()

	err := c.Run(ctx)
	if err != nil {
		t.Errorf("Run returned error: %v", err)
	}
	if !finished.Load() {
		t.Error("handler did not finish before shutdown completed")
	}
}

func TestConsumer_GracefulShutdown_TimeoutExceeded(t *testing.T) {
	msgs := []*queuer.Message{{ID: "1", Body: "very-slow", ReceiptHandle: "r1"}}
	recv := &mockReceiver{messages: msgs}
	ack := &mockAcknowledger{}

	handler := queuer.HandlerFunc(func(_ context.Context, _ *queuer.Message) error {
		time.Sleep(5 * time.Second)
		return nil
	})

	c := queuer.New(recv, handler, ack,
		queuer.WithWorkers(1),
		queuer.WithShutdownTimeout(100*time.Millisecond),
	)

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()

	err := c.Run(ctx)
	if err == nil {
		t.Error("expected timeout error, got nil")
	}
}
```

- [ ] **Step 2: Run tests to verify they pass**

Run: `go test ./... -run "TestConsumer_GracefulShutdown" -v -timeout 30s`
Expected: PASS (implementation from Task 4 already handles shutdown)

- [ ] **Step 3: Refine active worker tracking in `consumer.go`**

Replace the worker goroutine in `Run` to use an atomic counter for accurate `SetActiveWorkers`:

```go
// In the Run method, replace the worker loop with:
	var activeWorkers atomic.Int64

	for i := 0; i < c.workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for msg := range msgCh {
				active := activeWorkers.Add(1)
				c.metrics.SetActiveWorkers(int(active))
				c.processMessage(ctx, msg)
				active = activeWorkers.Add(-1)
				c.metrics.SetActiveWorkers(int(active))
			}
		}()
	}
```

Add `"sync/atomic"` to the imports of `consumer.go`.

- [ ] **Step 4: Run all tests**

Run: `go test ./... -v -timeout 30s`
Expected: all PASS

- [ ] **Step 5: Commit**

```bash
git add consumer.go consumer_test.go
git commit -m "Add graceful shutdown tests and atomic active worker tracking"
```

---

### Task 7: SQS Receiver Implementation

**Files:**
- Create: `sqs/receiver.go`
- Create: `sqs/sqs_test.go`

- [ ] **Step 1: Initialize sqs submodule dependencies**

Run:
```bash
cd /Users/alfonso/Github/Personal/queuer
go get github.com/aws/aws-sdk-go-v2/service/sqs
go get github.com/aws/aws-sdk-go-v2/aws
```

- [ ] **Step 2: Create `sqs/receiver.go`**

```go
// sqs/receiver.go
package sqs

import (
	"context"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"

	"github.com/AlfonsoCampodonico/queuer"
)

// SQSAPI is the subset of the SQS client used by Receiver.
type SQSAPI interface {
	ReceiveMessage(ctx context.Context, params *sqs.ReceiveMessageInput, optFns ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error)
}

// Receiver polls an SQS queue for messages.
type Receiver struct {
	client   SQSAPI
	queueURL string
}

// NewReceiver creates a Receiver for the given queue.
func NewReceiver(client SQSAPI, queueURL string) *Receiver {
	return &Receiver{client: client, queueURL: queueURL}
}

// Receive polls SQS and returns up to maxMessages messages.
func (r *Receiver) Receive(ctx context.Context, maxMessages int, waitTime time.Duration) ([]*queuer.Message, error) {
	out, err := r.client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
		QueueUrl:              aws.String(r.queueURL),
		MaxNumberOfMessages:   int32(maxMessages),
		WaitTimeSeconds:       int32(waitTime.Seconds()),
		MessageAttributeNames: []string{"All"},
		AttributeNames:        []types.QueueAttributeName{types.QueueAttributeNameAll},
	})
	if err != nil {
		return nil, err
	}

	msgs := make([]*queuer.Message, 0, len(out.Messages))
	for _, m := range out.Messages {
		msg := &queuer.Message{
			ID:            aws.ToString(m.MessageId),
			Body:          aws.ToString(m.Body),
			ReceiptHandle: aws.ToString(m.ReceiptHandle),
			Attributes:    make(map[string]string),
		}
		for k, v := range m.MessageAttributes {
			if v.StringValue != nil {
				msg.Attributes[k] = *v.StringValue
			}
		}
		// Extract approximate receive count from system attributes
		if countStr, ok := m.Attributes[types.MessageSystemAttributeNameApproximateReceiveCount]; ok {
			var count int
			for _, ch := range countStr {
				count = count*10 + int(ch-'0')
			}
			msg.ReceiveCount = count
		}
		msgs = append(msgs, msg)
	}
	return msgs, nil
}
```

- [ ] **Step 3: Write test with mock SQS client**

```go
// sqs/sqs_test.go
package sqs_test

import (
	"context"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"

	sqspkg "github.com/AlfonsoCampodonico/queuer/sqs"
)

type mockSQSClient struct {
	output *awssqs.ReceiveMessageOutput
	err    error
}

func (m *mockSQSClient) ReceiveMessage(_ context.Context, _ *awssqs.ReceiveMessageInput, _ ...func(*awssqs.Options)) (*awssqs.ReceiveMessageOutput, error) {
	return m.output, m.err
}

func (m *mockSQSClient) DeleteMessage(_ context.Context, _ *awssqs.DeleteMessageInput, _ ...func(*awssqs.Options)) (*awssqs.DeleteMessageOutput, error) {
	return &awssqs.DeleteMessageOutput{}, nil
}

func (m *mockSQSClient) ChangeMessageVisibility(_ context.Context, _ *awssqs.ChangeMessageVisibilityInput, _ ...func(*awssqs.Options)) (*awssqs.ChangeMessageVisibilityOutput, error) {
	return &awssqs.ChangeMessageVisibilityOutput{}, nil
}

func TestReceiver_Receive(t *testing.T) {
	client := &mockSQSClient{
		output: &awssqs.ReceiveMessageOutput{
			Messages: []types.Message{
				{
					MessageId:     aws.String("msg-1"),
					Body:          aws.String(`{"key":"value"}`),
					ReceiptHandle: aws.String("handle-1"),
					Attributes: map[types.MessageSystemAttributeName]string{
						types.MessageSystemAttributeNameApproximateReceiveCount: "3",
					},
				},
			},
		},
	}

	recv := sqspkg.NewReceiver(client, "https://sqs.us-east-1.amazonaws.com/123/test-queue")
	msgs, err := recv.Receive(context.Background(), 10, 20*time.Second)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(msgs) != 1 {
		t.Fatalf("got %d messages, want 1", len(msgs))
	}
	if msgs[0].ID != "msg-1" {
		t.Errorf("ID = %q, want %q", msgs[0].ID, "msg-1")
	}
	if msgs[0].ReceiveCount != 3 {
		t.Errorf("ReceiveCount = %d, want 3", msgs[0].ReceiveCount)
	}
}
```

- [ ] **Step 4: Run tests**

Run: `go test ./sqs/... -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add sqs/ go.mod go.sum
git commit -m "Add SQS Receiver implementation"
```

---

### Task 8: SQS Acknowledger Implementation

**Files:**
- Create: `sqs/acknowledger.go`
- Modify: `sqs/sqs_test.go`

- [ ] **Step 1: Write failing test for Acknowledger**

Add to `sqs/sqs_test.go`:

```go
import (
	"sync/atomic"

	"github.com/AlfonsoCampodonico/queuer"
)

type trackingSQSClient struct {
	deleted    atomic.Int32
	nacked     atomic.Int32
	visibility atomic.Int32
}

func (m *trackingSQSClient) ReceiveMessage(_ context.Context, _ *awssqs.ReceiveMessageInput, _ ...func(*awssqs.Options)) (*awssqs.ReceiveMessageOutput, error) {
	return &awssqs.ReceiveMessageOutput{}, nil
}

func (m *trackingSQSClient) DeleteMessage(_ context.Context, _ *awssqs.DeleteMessageInput, _ ...func(*awssqs.Options)) (*awssqs.DeleteMessageOutput, error) {
	m.deleted.Add(1)
	return &awssqs.DeleteMessageOutput{}, nil
}

func (m *trackingSQSClient) ChangeMessageVisibility(_ context.Context, input *awssqs.ChangeMessageVisibilityInput, _ ...func(*awssqs.Options)) (*awssqs.ChangeMessageVisibilityOutput, error) {
	if aws.ToInt32(input.VisibilityTimeout) == 0 {
		m.nacked.Add(1)
	} else {
		m.visibility.Add(1)
	}
	return &awssqs.ChangeMessageVisibilityOutput{}, nil
}

func TestAcknowledger_Ack(t *testing.T) {
	client := &trackingSQSClient{}
	acker := sqspkg.NewAcknowledger(client, "https://sqs.us-east-1.amazonaws.com/123/test-queue")

	msg := &queuer.Message{ID: "1", ReceiptHandle: "r1"}
	if err := acker.Ack(context.Background(), msg); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := client.deleted.Load(); got != 1 {
		t.Errorf("deleted = %d, want 1", got)
	}
}

func TestAcknowledger_Nack(t *testing.T) {
	client := &trackingSQSClient{}
	acker := sqspkg.NewAcknowledger(client, "https://sqs.us-east-1.amazonaws.com/123/test-queue")

	msg := &queuer.Message{ID: "1", ReceiptHandle: "r1"}
	if err := acker.Nack(context.Background(), msg); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := client.nacked.Load(); got != 1 {
		t.Errorf("nacked = %d, want 1", got)
	}
}

func TestAcknowledger_ChangeVisibility(t *testing.T) {
	client := &trackingSQSClient{}
	acker := sqspkg.NewAcknowledger(client, "https://sqs.us-east-1.amazonaws.com/123/test-queue")

	msg := &queuer.Message{ID: "1", ReceiptHandle: "r1"}
	if err := acker.ChangeVisibility(context.Background(), msg, 60*time.Second); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := client.visibility.Load(); got != 1 {
		t.Errorf("visibility changes = %d, want 1", got)
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./sqs/... -v`
Expected: FAIL (NewAcknowledger doesn't exist)

- [ ] **Step 3: Create `sqs/acknowledger.go`**

```go
// sqs/acknowledger.go
package sqs

import (
	"context"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"

	"github.com/AlfonsoCampodonico/queuer"
)

// SQSDeleteChangeAPI is the subset of the SQS client used by Acknowledger.
type SQSDeleteChangeAPI interface {
	DeleteMessage(ctx context.Context, params *awssqs.DeleteMessageInput, optFns ...func(*awssqs.Options)) (*awssqs.DeleteMessageOutput, error)
	ChangeMessageVisibility(ctx context.Context, params *awssqs.ChangeMessageVisibilityInput, optFns ...func(*awssqs.Options)) (*awssqs.ChangeMessageVisibilityOutput, error)
}

// Acknowledger manages SQS message lifecycle.
type Acknowledger struct {
	client   SQSDeleteChangeAPI
	queueURL string
}

// NewAcknowledger creates an Acknowledger for the given queue.
func NewAcknowledger(client SQSDeleteChangeAPI, queueURL string) *Acknowledger {
	return &Acknowledger{client: client, queueURL: queueURL}
}

// Ack deletes the message from the queue.
func (a *Acknowledger) Ack(ctx context.Context, msg *queuer.Message) error {
	_, err := a.client.DeleteMessage(ctx, &awssqs.DeleteMessageInput{
		QueueUrl:      aws.String(a.queueURL),
		ReceiptHandle: aws.String(msg.ReceiptHandle),
	})
	return err
}

// Nack sets the message visibility timeout to 0, making it immediately available.
func (a *Acknowledger) Nack(ctx context.Context, msg *queuer.Message) error {
	_, err := a.client.ChangeMessageVisibility(ctx, &awssqs.ChangeMessageVisibilityInput{
		QueueUrl:          aws.String(a.queueURL),
		ReceiptHandle:     aws.String(msg.ReceiptHandle),
		VisibilityTimeout: 0,
	})
	return err
}

// ChangeVisibility sets the message visibility timeout.
func (a *Acknowledger) ChangeVisibility(ctx context.Context, msg *queuer.Message, timeout time.Duration) error {
	_, err := a.client.ChangeMessageVisibility(ctx, &awssqs.ChangeMessageVisibilityInput{
		QueueUrl:          aws.String(a.queueURL),
		ReceiptHandle:     aws.String(msg.ReceiptHandle),
		VisibilityTimeout: int32(timeout.Seconds()),
	})
	return err
}
```

- [ ] **Step 4: Run tests**

Run: `go test ./sqs/... -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add sqs/
git commit -m "Add SQS Acknowledger implementation"
```

---

### Task 9: SQS Convenience Constructor

**Files:**
- Create: `sqs/consumer.go`
- Modify: `sqs/receiver.go` (combine SQSAPI interfaces)

- [ ] **Step 1: Unify SQS client interface and create convenience constructor**

First, update `sqs/receiver.go` to define a combined interface:

```go
// Add to sqs/receiver.go, replacing the SQSAPI interface:

// Client is the subset of the SQS client used by this package.
type Client interface {
	ReceiveMessage(ctx context.Context, params *sqs.ReceiveMessageInput, optFns ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error)
	DeleteMessage(ctx context.Context, params *sqs.DeleteMessageInput, optFns ...func(*sqs.Options)) (*sqs.DeleteMessageOutput, error)
	ChangeMessageVisibility(ctx context.Context, params *sqs.ChangeMessageVisibilityInput, optFns ...func(*sqs.Options)) (*sqs.ChangeMessageVisibilityOutput, error)
}
```

Update `NewReceiver` to accept `Client` instead of `SQSAPI`.
Update `NewAcknowledger` to accept `Client` instead of `SQSDeleteChangeAPI`.
Remove the old `SQSAPI` and `SQSDeleteChangeAPI` interfaces.

- [ ] **Step 2: Create `sqs/consumer.go`**

```go
// sqs/consumer.go
package sqs

import "github.com/AlfonsoCampodonico/queuer"

// NewConsumer creates a queuer.Consumer wired to the given SQS queue.
func NewConsumer(client Client, queueURL string, handler queuer.Handler, opts ...queuer.Option) *queuer.Consumer {
	return queuer.New(
		NewReceiver(client, queueURL),
		handler,
		NewAcknowledger(client, queueURL),
		opts...,
	)
}
```

- [ ] **Step 3: Run all tests**

Run: `go test ./... -v -timeout 30s`
Expected: all PASS

- [ ] **Step 4: Commit**

```bash
git add sqs/
git commit -m "Add unified SQS Client interface and NewConsumer convenience constructor"
```

---

### Task 10: OTel MetricsCollector Implementation

**Files:**
- Create: `otel/metrics.go`
- Create: `otel/options.go`
- Create: `otel/metrics_test.go`

- [ ] **Step 1: Add OTel dependencies**

Run:
```bash
cd /Users/alfonso/Github/Personal/queuer
go get go.opentelemetry.io/otel
go get go.opentelemetry.io/otel/metric
go get go.opentelemetry.io/otel/sdk/metric
go get go.opentelemetry.io/otel/attribute
```

- [ ] **Step 2: Create `otel/options.go`**

```go
// otel/options.go
package otel

// MetricsOption configures OTel metrics.
type MetricsOption func(*Metrics)

// WithMeterName sets the OTel meter name. Default: "queuer".
func WithMeterName(name string) MetricsOption {
	return func(m *Metrics) {
		m.meterName = name
	}
}

// WithQueueLabel adds a queue_url attribute to all metrics.
func WithQueueLabel(url string) MetricsOption {
	return func(m *Metrics) {
		m.queueURL = url
	}
}
```

- [ ] **Step 3: Create `otel/metrics.go`**

```go
// otel/metrics.go
package otel

import (
	"context"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/AlfonsoCampodonico/queuer"
)

// Metrics implements queuer.MetricsCollector using OpenTelemetry.
type Metrics struct {
	meterName  string
	queueURL   string
	attrs      []attribute.KeyValue
	lastActive atomic.Int64

	received  metric.Int64Counter
	processed metric.Int64Counter
	failed    metric.Int64Counter
	duration  metric.Float64Histogram
	active    metric.Int64UpDownCounter
}

// NewMetrics creates an OTel-backed MetricsCollector.
func NewMetrics(provider metric.MeterProvider, opts ...MetricsOption) (*Metrics, error) {
	m := &Metrics{
		meterName: "queuer",
	}
	for _, opt := range opts {
		opt(m)
	}

	if m.queueURL != "" {
		m.attrs = append(m.attrs, attribute.String("queue_url", m.queueURL))
	}

	meter := provider.Meter(m.meterName)

	var err error

	m.received, err = meter.Int64Counter("queuer.messages.received",
		metric.WithDescription("Messages received from source"),
		metric.WithUnit("{message}"),
	)
	if err != nil {
		return nil, err
	}

	m.processed, err = meter.Int64Counter("queuer.messages.processed",
		metric.WithDescription("Successfully processed messages"),
		metric.WithUnit("{message}"),
	)
	if err != nil {
		return nil, err
	}

	m.failed, err = meter.Int64Counter("queuer.messages.failed",
		metric.WithDescription("Failed messages"),
		metric.WithUnit("{message}"),
	)
	if err != nil {
		return nil, err
	}

	m.duration, err = meter.Float64Histogram("queuer.messages.duration",
		metric.WithDescription("Processing time per message"),
		metric.WithUnit("s"),
	)
	if err != nil {
		return nil, err
	}

	m.active, err = meter.Int64UpDownCounter("queuer.workers.active",
		metric.WithDescription("Currently busy workers"),
		metric.WithUnit("{worker}"),
	)
	if err != nil {
		return nil, err
	}

	return m, nil
}

func (m *Metrics) IncMessagesReceived(count int) {
	m.received.Add(context.Background(), int64(count), metric.WithAttributes(m.attrs...))
}

func (m *Metrics) IncMessagesProcessed() {
	m.processed.Add(context.Background(), 1, metric.WithAttributes(m.attrs...))
}

func (m *Metrics) IncMessagesFailed(action queuer.ErrorAction) {
	attrs := append([]attribute.KeyValue{attribute.String("action", action.String())}, m.attrs...)
	m.failed.Add(context.Background(), 1, metric.WithAttributes(attrs...))
}

func (m *Metrics) ObserveProcessingDuration(d time.Duration) {
	m.duration.Record(context.Background(), d.Seconds(), metric.WithAttributes(m.attrs...))
}

func (m *Metrics) SetActiveWorkers(count int) {
	delta := int64(count) - m.lastActive.Swap(int64(count))
	if delta != 0 {
		m.active.Add(context.Background(), delta, metric.WithAttributes(m.attrs...))
	}
}
```

- [ ] **Step 4: Create `otel/metrics_test.go`**

```go
// otel/metrics_test.go
package otel_test

import (
	"testing"
	"time"

	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/AlfonsoCampodonico/queuer"
	otelpkg "github.com/AlfonsoCampodonico/queuer/otel"
)

func setupTestMetrics(t *testing.T) (*otelpkg.Metrics, *metric.ManualReader) {
	t.Helper()
	reader := metric.NewManualReader()
	provider := metric.NewMeterProvider(metric.WithReader(reader))

	m, err := otelpkg.NewMetrics(provider, otelpkg.WithQueueLabel("test-queue"))
	if err != nil {
		t.Fatalf("NewMetrics failed: %v", err)
	}
	return m, reader
}

func collectMetrics(t *testing.T, reader *metric.ManualReader) metricdata.ResourceMetrics {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := reader.Collect(t.Context(), &rm); err != nil {
		t.Fatalf("collect failed: %v", err)
	}
	return rm
}

func findMetric(rm metricdata.ResourceMetrics, name string) *metricdata.Metrics {
	for _, sm := range rm.ScopeMetrics {
		for i := range sm.Metrics {
			if sm.Metrics[i].Name == name {
				return &sm.Metrics[i]
			}
		}
	}
	return nil
}

func TestMetrics_IncMessagesReceived(t *testing.T) {
	m, reader := setupTestMetrics(t)
	m.IncMessagesReceived(5)

	rm := collectMetrics(t, reader)
	met := findMetric(rm, "queuer.messages.received")
	if met == nil {
		t.Fatal("metric queuer.messages.received not found")
	}
}

func TestMetrics_IncMessagesProcessed(t *testing.T) {
	m, reader := setupTestMetrics(t)
	m.IncMessagesProcessed()
	m.IncMessagesProcessed()

	rm := collectMetrics(t, reader)
	met := findMetric(rm, "queuer.messages.processed")
	if met == nil {
		t.Fatal("metric queuer.messages.processed not found")
	}
}

func TestMetrics_IncMessagesFailed(t *testing.T) {
	m, reader := setupTestMetrics(t)
	m.IncMessagesFailed(queuer.Nack)

	rm := collectMetrics(t, reader)
	met := findMetric(rm, "queuer.messages.failed")
	if met == nil {
		t.Fatal("metric queuer.messages.failed not found")
	}
}

func TestMetrics_ObserveProcessingDuration(t *testing.T) {
	m, reader := setupTestMetrics(t)
	m.ObserveProcessingDuration(150 * time.Millisecond)

	rm := collectMetrics(t, reader)
	met := findMetric(rm, "queuer.messages.duration")
	if met == nil {
		t.Fatal("metric queuer.messages.duration not found")
	}
}

func TestMetrics_SetActiveWorkers(t *testing.T) {
	m, reader := setupTestMetrics(t)
	m.SetActiveWorkers(3)
	m.SetActiveWorkers(1)

	rm := collectMetrics(t, reader)
	met := findMetric(rm, "queuer.workers.active")
	if met == nil {
		t.Fatal("metric queuer.workers.active not found")
	}
}
```

- [ ] **Step 5: Run tests**

Run: `go test ./otel/... -v`
Expected: PASS

- [ ] **Step 6: Run all tests**

Run: `go test ./... -v -timeout 30s`
Expected: all PASS

- [ ] **Step 7: Commit**

```bash
git add otel/ go.mod go.sum
git commit -m "Add OTel MetricsCollector implementation"
```

---

### Task 11: Final Integration Test

**Files:**
- Modify: `consumer_test.go`

- [ ] **Step 1: Write an end-to-end test using all mock interfaces together**

Add to `consumer_test.go`:

```go
type trackingMetrics struct {
	received  atomic.Int32
	processed atomic.Int32
	failed    atomic.Int32
}

func (m *trackingMetrics) IncMessagesReceived(count int) { m.received.Add(int32(count)) }
func (m *trackingMetrics) IncMessagesProcessed()         { m.processed.Add(1) }
func (m *trackingMetrics) IncMessagesFailed(_ queuer.ErrorAction) { m.failed.Add(1) }
func (m *trackingMetrics) ObserveProcessingDuration(_ time.Duration) {}
func (m *trackingMetrics) SetActiveWorkers(_ int)        {}

func TestConsumer_Integration(t *testing.T) {
	msgs := []*queuer.Message{
		{ID: "1", Body: "ok", ReceiptHandle: "r1"},
		{ID: "2", Body: "fail", ReceiptHandle: "r2"},
		{ID: "3", Body: "ok", ReceiptHandle: "r3"},
	}

	recv := &mockReceiver{messages: msgs}
	ack := &mockAcknowledger{}
	metrics := &trackingMetrics{}

	handler := queuer.HandlerFunc(func(_ context.Context, msg *queuer.Message) error {
		if msg.Body == "fail" {
			return errors.New("bad message")
		}
		return nil
	})

	resolver := &mockErrorResolver{action: queuer.Skip}

	c := queuer.New(recv, handler, ack,
		queuer.WithWorkers(3),
		queuer.WithErrorResolver(resolver),
		queuer.WithMetrics(metrics),
		queuer.WithShutdownTimeout(2*time.Second),
	)

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	_ = c.Run(ctx)

	if got := metrics.received.Load(); got < 3 {
		t.Errorf("received = %d, want >= 3", got)
	}
	if got := metrics.processed.Load(); got != 2 {
		t.Errorf("processed = %d, want 2", got)
	}
	if got := metrics.failed.Load(); got != 1 {
		t.Errorf("failed = %d, want 1", got)
	}

	// "fail" message was Skip'd, so it should also be acked
	ack.mu.Lock()
	defer ack.mu.Unlock()
	if got := len(ack.acked); got != 3 {
		t.Errorf("total acked = %d, want 3 (2 success + 1 skip)", got)
	}
}
```

- [ ] **Step 2: Run all tests**

Run: `go test ./... -v -timeout 30s`
Expected: all PASS

- [ ] **Step 3: Commit**

```bash
git add consumer_test.go
git commit -m "Add integration test covering full consumer lifecycle"
```
