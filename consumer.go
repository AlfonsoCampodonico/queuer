package queuer

import (
	"context"
	"log/slog"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

// Consumer orchestrates polling, dispatching to workers, error handling,
// lease extension and graceful shutdown.
type Consumer struct {
	receiver      Receiver
	handler       Handler
	acknowledger  Acknowledger
	errorResolver ErrorResolver
	metrics       MetricsCollector
	tracer        Tracer
	logger        *slog.Logger
	middleware    []Middleware

	workers         int
	shutdownTimeout time.Duration
	waitTime        time.Duration
	maxMessages     int
	retryBackoff    BackoffFunc
	leaseDuration   time.Duration
	handlerTimeout  time.Duration

	running atomic.Bool
}

// New creates a Consumer with the given interfaces and options.
// It panics if receiver, handler or acknowledger is nil.
func New(receiver Receiver, handler Handler, acknowledger Acknowledger, opts ...Option) *Consumer {
	if receiver == nil {
		panic("queuer: receiver must not be nil")
	}
	if handler == nil {
		panic("queuer: handler must not be nil")
	}
	if acknowledger == nil {
		panic("queuer: acknowledger must not be nil")
	}
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
		retryBackoff:    ExponentialBackoff(30*time.Second, 15*time.Minute),
	}
	for _, opt := range opts {
		opt(c)
	}
	c.handler = Chain(c.handler, c.middleware...)
	return c
}

// delivery is a received message together with the function that stops its
// lease extension.
type delivery struct {
	msg       *Message
	stopLease func()
}

// Run starts the consumer poll loop and worker pool. It blocks until ctx is
// cancelled, then drains in-flight messages up to the shutdown timeout.
// Messages that were received but never handed to a worker are nacked so
// they are redelivered promptly. Run returns ErrAlreadyRunning if called
// while another Run is in progress.
func (c *Consumer) Run(ctx context.Context) error {
	if !c.running.CompareAndSwap(false, true) {
		return ErrAlreadyRunning
	}
	defer c.running.Store(false)

	c.logger.Info("consumer starting", "workers", c.workers, "max_messages", c.maxMessages)

	deliveries := make(chan delivery, c.workers)
	var wg sync.WaitGroup
	var activeWorkers atomic.Int64

	// processingCtx outlives ctx so in-flight handlers and settle calls can
	// finish during the shutdown drain. It is cancelled when the drain times out.
	processingCtx, cancelProcessing := context.WithCancel(context.WithoutCancel(ctx))
	defer cancelProcessing()

	for i := 0; i < c.workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for d := range deliveries {
				c.metrics.SetActiveWorkers(int(activeWorkers.Add(1)))
				c.processMessage(processingCtx, d)
				c.metrics.SetActiveWorkers(int(activeWorkers.Add(-1)))
			}
		}()
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		defer close(deliveries)
		c.poll(ctx, processingCtx, deliveries)
	}()

	<-ctx.Done()
	c.logger.Info("shutdown initiated, draining in-flight messages", "timeout", c.shutdownTimeout)

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	timer := time.NewTimer(c.shutdownTimeout)
	defer timer.Stop()
	select {
	case <-done:
		c.logger.Info("consumer stopped, all messages drained")
		return nil
	case <-timer.C:
		cancelProcessing()
		c.logger.Warn("shutdown timeout exceeded, some messages may not have been processed")
		return ErrShutdownTimeout
	}
}

func (c *Consumer) poll(ctx, processingCtx context.Context, deliveries chan<- delivery) {
	var consecutiveErrors int
	for ctx.Err() == nil {
		msgs, err := c.receiver.Receive(ctx, c.maxMessages, c.waitTime)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			consecutiveErrors++
			backoff := c.receiveBackoff(consecutiveErrors)
			c.logger.Warn("receive error, backing off", "error", err, "backoff", backoff)
			t := time.NewTimer(backoff)
			select {
			case <-t.C:
			case <-ctx.Done():
				t.Stop()
				return
			}
			continue
		}
		consecutiveErrors = 0
		if len(msgs) == 0 {
			continue
		}
		c.metrics.IncMessagesReceived(len(msgs))
		c.logger.Debug("received messages", "count", len(msgs))

		pending := make([]delivery, len(msgs))
		for i, msg := range msgs {
			pending[i] = delivery{msg: msg, stopLease: c.startLease(processingCtx, msg)}
		}
		for i, d := range pending {
			select {
			case deliveries <- d:
			case <-ctx.Done():
				c.release(processingCtx, pending[i:])
				return
			}
		}
	}
}

// release nacks messages that were received but never handed to a worker,
// so they are redelivered without waiting for their lease to expire.
func (c *Consumer) release(ctx context.Context, ds []delivery) {
	for _, d := range ds {
		d.stopLease()
		if err := c.acknowledger.Nack(ctx, d.msg); err != nil {
			c.metrics.IncSettleFailed(SettleNack)
			c.logger.Warn("nack of undispatched message failed", "message_id", d.msg.ID, "error", err)
		}
	}
}

// startLease extends the message's lease right away and then every
// leaseDuration/2 until the returned stop function is called. stop waits
// for any in-flight extension to finish, so no extension races the settle call.
func (c *Consumer) startLease(ctx context.Context, msg *Message) (stop func()) {
	if c.leaseDuration <= 0 {
		return func() {}
	}
	leaseCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		interval := c.leaseDuration / 2
		if interval <= 0 {
			interval = c.leaseDuration
		}
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			if err := c.acknowledger.ChangeVisibility(leaseCtx, msg, c.leaseDuration); err != nil && leaseCtx.Err() == nil {
				c.metrics.IncSettleFailed(SettleExtendLease)
				c.logger.Warn("lease extension failed", "message_id", msg.ID, "error", err)
			}
			select {
			case <-ticker.C:
			case <-leaseCtx.Done():
				return
			}
		}
	}()
	var once sync.Once
	return func() {
		once.Do(func() {
			cancel()
			<-done
		})
	}
}

func (c *Consumer) processMessage(ctx context.Context, d delivery) {
	msg := d.msg
	defer d.stopLease()

	spanCtx, endSpan := c.tracer.Start(ctx, msg)

	handlerCtx := spanCtx
	if c.handlerTimeout > 0 {
		var cancel context.CancelFunc
		handlerCtx, cancel = context.WithTimeout(spanCtx, c.handlerTimeout)
		defer cancel()
	}

	start := time.Now()
	err := c.safeHandle(handlerCtx, msg)
	c.metrics.ObserveProcessingDuration(time.Since(start))
	d.stopLease()

	if err == nil {
		if ackErr := c.acknowledger.Ack(spanCtx, msg); ackErr != nil {
			c.metrics.IncSettleFailed(SettleAck)
			c.logger.Warn("ack failed", "message_id", msg.ID, "error", ackErr)
		} else {
			c.metrics.IncMessagesProcessed()
			c.logger.Debug("message acked", "message_id", msg.ID)
		}
		endSpan(nil)
		return
	}
	endSpan(err)

	c.logger.Warn("handler error", "message_id", msg.ID, "error", err)

	action, resolveErr := c.errorResolver.Resolve(spanCtx, msg, err)
	if resolveErr != nil {
		c.logger.Warn("error resolver failed, leaving message in queue",
			"message_id", msg.ID, "error", resolveErr)
		return
	}

	c.metrics.IncMessagesFailed(action)
	c.executeErrorAction(spanCtx, msg, action)
}

func (c *Consumer) receiveBackoff(consecutiveErrors int) time.Duration {
	return ExponentialBackoff(time.Second, 30*time.Second)(consecutiveErrors)
}

func (c *Consumer) safeHandle(ctx context.Context, msg *Message) (err error) {
	defer func() {
		if r := recover(); r != nil {
			buf := make([]byte, 4096)
			n := runtime.Stack(buf, false)
			err = &PanicError{Value: r, Stack: buf[:n]}
			c.logger.Error("handler panicked", "message_id", msg.ID, "panic", r, "stack", string(buf[:n]))
		}
	}()
	return c.handler.Handle(ctx, msg)
}

func (c *Consumer) executeErrorAction(ctx context.Context, msg *Message, action ErrorAction) {
	switch action {
	case Retry:
		timeout := c.retryBackoff(msg.ReceiveCount)
		if err := c.acknowledger.ChangeVisibility(ctx, msg, timeout); err != nil {
			c.metrics.IncSettleFailed(SettleChangeVisibility)
			c.logger.Warn("change visibility failed", "message_id", msg.ID, "error", err)
		}
	case Nack:
		if err := c.acknowledger.Nack(ctx, msg); err != nil {
			c.metrics.IncSettleFailed(SettleNack)
			c.logger.Warn("nack failed", "message_id", msg.ID, "error", err)
		}
	case Skip:
		if err := c.acknowledger.Ack(ctx, msg); err != nil {
			c.metrics.IncSettleFailed(SettleAck)
			c.logger.Warn("ack (skip) failed", "message_id", msg.ID, "error", err)
		}
	case DeadLetter:
		c.logger.Warn("message left for dead-letter policy", "message_id", msg.ID)
	}
}
