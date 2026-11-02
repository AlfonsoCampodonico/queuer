// Package otel provides OpenTelemetry metrics and tracing for queuer.
package otel

import (
	"context"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"

	"github.com/AlfonsoCampodonico/queuer"
)

const instrumentationName = "github.com/AlfonsoCampodonico/queuer/otel"

// Tracer implements queuer.Tracer using OpenTelemetry. Spans follow the
// OpenTelemetry messaging semantic conventions; the messaging system and
// destination come from Message.System and Message.Queue, which the
// adapters fill in.
type Tracer struct {
	tracer     trace.Tracer
	propagator propagation.TextMapPropagator
}

// TracerOption configures the OTel Tracer and Publisher.
type TracerOption func(*tracerConfig)

type tracerConfig struct {
	propagator propagation.TextMapPropagator
}

// WithPropagator sets the propagator used to extract trace context from, or
// inject it into, message attributes. Defaults to W3C TraceContext + Baggage.
func WithPropagator(p propagation.TextMapPropagator) TracerOption {
	return func(c *tracerConfig) {
		if p != nil {
			c.propagator = p
		}
	}
}

func newConfig(opts []TracerOption) tracerConfig {
	c := tracerConfig{
		propagator: propagation.NewCompositeTextMapPropagator(
			propagation.TraceContext{},
			propagation.Baggage{},
		),
	}
	for _, opt := range opts {
		opt(&c)
	}
	return c
}

// NewTracer creates an OTel-backed Tracer.
func NewTracer(provider trace.TracerProvider, opts ...TracerOption) *Tracer {
	c := newConfig(opts)
	return &Tracer{
		tracer:     provider.Tracer(instrumentationName),
		propagator: c.propagator,
	}
}

// Start begins a consumer span for processing msg. If the message
// attributes carry trace context (traceparent/tracestate), the span is
// created as a child of that remote parent.
func (t *Tracer) Start(ctx context.Context, msg *queuer.Message) (context.Context, func(error)) {
	if msg.Attributes != nil {
		ctx = t.propagator.Extract(ctx, propagation.MapCarrier(msg.Attributes))
	}

	attrs := []attribute.KeyValue{
		attribute.String("messaging.operation.type", "process"),
		attribute.String("messaging.operation.name", "process"),
		attribute.String("messaging.message.id", msg.ID),
		attribute.Int("messaging.message.body.size", len(msg.Body)),
	}
	if msg.System != "" {
		attrs = append(attrs, attribute.String("messaging.system", msg.System))
	}
	if msg.Queue != "" {
		attrs = append(attrs, attribute.String("messaging.destination.name", msg.Queue))
	}
	if msg.ReceiveCount > 0 {
		attrs = append(attrs, attribute.Int("messaging.message.delivery_count", msg.ReceiveCount))
	}

	ctx, span := t.tracer.Start(ctx, spanName("process", msg.Queue),
		trace.WithSpanKind(trace.SpanKindConsumer),
		trace.WithAttributes(attrs...),
	)
	return ctx, func(err error) {
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
		}
		span.End()
	}
}

func spanName(op, destination string) string {
	if destination == "" {
		return op
	}
	return op + " " + destination
}

// destinationDescriber is implemented by the adapters' publishers
// (sqs.Publisher, pubsub.Publisher).
type destinationDescriber interface {
	MessagingSystem() string
	DestinationName() string
}

// Publisher wraps a queuer.Publisher with a producer span and injects the
// span's trace context into the message attributes, so consumers using
// Tracer continue the same trace.
type Publisher struct {
	next        queuer.Publisher
	tracer      trace.Tracer
	propagator  propagation.TextMapPropagator
	system      string
	destination string
}

// NewPublisher wraps next. If next is one of the adapters' publishers, the
// messaging system and destination are added to the span.
func NewPublisher(next queuer.Publisher, provider trace.TracerProvider, opts ...TracerOption) *Publisher {
	c := newConfig(opts)
	p := &Publisher{
		next:       next,
		tracer:     provider.Tracer(instrumentationName),
		propagator: c.propagator,
	}
	if d, ok := next.(destinationDescriber); ok {
		p.system = d.MessagingSystem()
		p.destination = d.DestinationName()
	}
	return p
}

// Publish starts a producer span, injects its context into a copy of the
// message attributes and publishes the message. msg itself is not modified.
func (p *Publisher) Publish(ctx context.Context, msg *queuer.Message) (string, error) {
	attrs := []attribute.KeyValue{
		attribute.String("messaging.operation.type", "send"),
		attribute.String("messaging.operation.name", "send"),
		attribute.Int("messaging.message.body.size", len(msg.Body)),
	}
	if p.system != "" {
		attrs = append(attrs, attribute.String("messaging.system", p.system))
	}
	if p.destination != "" {
		attrs = append(attrs, attribute.String("messaging.destination.name", p.destination))
	}
	ctx, span := p.tracer.Start(ctx, spanName("send", p.destination),
		trace.WithSpanKind(trace.SpanKindProducer),
		trace.WithAttributes(attrs...),
	)
	defer span.End()

	out := *msg
	out.Attributes = make(map[string]string, len(msg.Attributes)+2)
	for k, v := range msg.Attributes {
		out.Attributes[k] = v
	}
	p.propagator.Inject(ctx, propagation.MapCarrier(out.Attributes))

	id, err := p.next.Publish(ctx, &out)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return "", err
	}
	span.SetAttributes(attribute.String("messaging.message.id", id))
	return id, nil
}
