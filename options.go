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

// WithWaitTime sets how long a single receive call may wait for messages
// to arrive (the SQS long-poll duration, or the Pub/Sub pull deadline).
// Adapters clamp it to what their provider accepts. Default: 20s.
func WithWaitTime(d time.Duration) Option {
	return func(c *Consumer) {
		c.waitTime = d
	}
}

// WithMaxMessages sets the maximum number of messages per receive call.
// Adapters clamp it to their provider's limit (10 for SQS, 1000 for
// Pub/Sub). Default: 10.
func WithMaxMessages(n int) Option {
	return func(c *Consumer) {
		if n > 0 {
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

// WithRetryBackoff sets how long a message stays hidden after the
// ErrorResolver returns Retry. Default: ExponentialBackoff(30s, 15m).
func WithRetryBackoff(b BackoffFunc) Option {
	return func(c *Consumer) {
		if b != nil {
			c.retryBackoff = b
		}
	}
}

// WithLeaseExtension keeps messages leased while they wait for a worker and
// while the handler runs. The lease is first extended by d as soon as the
// message is received, then again every d/2 until the message is settled.
// Use it when handlers can run longer than the queue's visibility timeout
// or the subscription's ack deadline. A value <= 0 disables it.
// Default: disabled (the Pub/Sub adapter enables it with 60s).
func WithLeaseExtension(d time.Duration) Option {
	return func(c *Consumer) {
		c.leaseDuration = d
	}
}

// WithHandlerTimeout cancels the handler's context after d. A value <= 0
// disables it. Default: disabled.
func WithHandlerTimeout(d time.Duration) Option {
	return func(c *Consumer) {
		c.handlerTimeout = d
	}
}

// WithMiddleware wraps the handler with the given middleware. It may be
// passed several times; middleware run in the order they were added.
func WithMiddleware(mws ...Middleware) Option {
	return func(c *Consumer) {
		for _, mw := range mws {
			if mw != nil {
				c.middleware = append(c.middleware, mw)
			}
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

// WithTracer sets the tracer for distributed tracing. Default: NoopTracer.
func WithTracer(t Tracer) Option {
	return func(c *Consumer) {
		if t != nil {
			c.tracer = t
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
