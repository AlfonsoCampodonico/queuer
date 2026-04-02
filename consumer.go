package queuer

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"
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
	tracer        Tracer
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
		tracer:          &NoopTracer{},
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

// Run starts the consumer poll loop and worker pool. It blocks until ctx is
// cancelled, then drains in-flight messages up to the shutdown timeout.
func (c *Consumer) Run(ctx context.Context) error {
	c.logger.Info("consumer starting", "workers", c.workers, "max_messages", c.maxMessages)

	msgCh := make(chan *Message, c.workers)
	var wg sync.WaitGroup
	var activeWorkers atomic.Int64

	// Start worker goroutines.
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

	// Start the polling goroutine.
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

	// Block until context is cancelled.
	<-ctx.Done()
	c.logger.Info("shutdown initiated, draining in-flight messages", "timeout", c.shutdownTimeout)

	// Wait for workers to finish or timeout.
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
		return ErrShutdownTimeout
	}
}

func (c *Consumer) processMessage(ctx context.Context, msg *Message) {
	ctx, endSpan := c.tracer.Start(ctx, msg)
	start := time.Now()

	err := c.handler.Handle(ctx, msg)
	duration := time.Since(start)
	c.metrics.ObserveProcessingDuration(duration)
	endSpan(err)

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
		timeout := 30 * time.Second
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
	}
}
