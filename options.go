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
