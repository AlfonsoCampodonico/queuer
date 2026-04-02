package queuer

import "context"

// Tracer creates spans for message processing. Implement this interface
// to integrate distributed tracing into the consumer pipeline.
type Tracer interface {
	// Start begins a processing span for the given message and returns
	// an enriched context containing the span. The caller must call the
	// returned function to end the span, passing nil on success or the
	// handler error on failure.
	Start(ctx context.Context, msg *Message) (context.Context, func(error))
}

// NoopTracer is a Tracer that does nothing.
type NoopTracer struct{}

func (n *NoopTracer) Start(ctx context.Context, _ *Message) (context.Context, func(error)) {
	return ctx, func(error) {}
}
