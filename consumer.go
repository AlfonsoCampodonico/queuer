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
