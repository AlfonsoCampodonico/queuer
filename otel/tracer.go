package otel

import (
	"context"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"

	"github.com/AlfonsoCampodonico/queuer"
)

// Tracer implements queuer.Tracer using OpenTelemetry.
type Tracer struct {
	tracer     trace.Tracer
	propagator propagation.TextMapPropagator
	queueURL   string
}

// TracerOption configures the OTel Tracer.
type TracerOption func(*Tracer)

// WithTracerQueueLabel adds a queue_url attribute to all spans.
func WithTracerQueueLabel(url string) TracerOption {
	return func(t *Tracer) {
		t.queueURL = url
	}
}

// WithPropagator sets the propagator used to extract trace context from
// message attributes. Defaults to a W3C TraceContext + Baggage propagator.
func WithPropagator(p propagation.TextMapPropagator) TracerOption {
	return func(t *Tracer) {
		t.propagator = p
	}
}

// NewTracer creates an OTel-backed Tracer.
func NewTracer(provider trace.TracerProvider, opts ...TracerOption) *Tracer {
	t := &Tracer{
		propagator: propagation.NewCompositeTextMapPropagator(
			propagation.TraceContext{},
			propagation.Baggage{},
		),
	}
	for _, opt := range opts {
		opt(t)
	}
	t.tracer = provider.Tracer("queuer")
	return t
}

// messageCarrier adapts Message.Attributes to propagation.TextMapCarrier
// so trace context can be extracted from SQS message attributes.
type messageCarrier struct {
	attrs map[string]string
}

func (c messageCarrier) Get(key string) string {
	return c.attrs[key]
}

func (c messageCarrier) Set(key, value string) {
	c.attrs[key] = value
}

func (c messageCarrier) Keys() []string {
	keys := make([]string, 0, len(c.attrs))
	for k := range c.attrs {
		keys = append(keys, k)
	}
	return keys
}

// Start begins a span for processing the given message. If the message
// attributes contain W3C trace context (traceparent/tracestate), the span
// is created as a child of that remote parent.
func (t *Tracer) Start(ctx context.Context, msg *queuer.Message) (context.Context, func(error)) {
	if msg.Attributes != nil {
		ctx = t.propagator.Extract(ctx, messageCarrier{attrs: msg.Attributes})
	}

	attrs := []attribute.KeyValue{
		attribute.String("messaging.message.id", msg.ID),
		attribute.String("messaging.system", "aws_sqs"),
		attribute.String("messaging.operation.type", "process"),
	}
	if t.queueURL != "" {
		attrs = append(attrs, attribute.String("messaging.destination.name", t.queueURL))
	}
	if msg.ReceiveCount > 0 {
		attrs = append(attrs, attribute.Int("messaging.message.receive_count", msg.ReceiveCount))
	}

	ctx, span := t.tracer.Start(ctx, "queuer.process",
		trace.WithSpanKind(trace.SpanKindConsumer),
		trace.WithAttributes(attrs...),
	)

	return ctx, func(err error) {
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
		} else {
			span.SetStatus(codes.Ok, "")
		}
		span.End()
	}
}
