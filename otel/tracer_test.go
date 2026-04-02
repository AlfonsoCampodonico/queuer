package otel_test

import (
	"context"
	"errors"
	"testing"

	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"

	"github.com/AlfonsoCampodonico/queuer"
	otelpkg "github.com/AlfonsoCampodonico/queuer/otel"
)

func setupTestTracer(t *testing.T) (*otelpkg.Tracer, *tracetest.InMemoryExporter) {
	t.Helper()
	exporter := tracetest.NewInMemoryExporter()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
	tracer := otelpkg.NewTracer(provider, otelpkg.WithTracerQueueLabel("test-queue"))
	return tracer, exporter
}

func TestTracer_Start_Success(t *testing.T) {
	tr, exporter := setupTestTracer(t)

	msg := &queuer.Message{ID: "msg-1", Body: "hello", Attributes: map[string]string{}}
	ctx, end := tr.Start(context.Background(), msg)
	end(nil)

	_ = ctx // span is in the context

	spans := exporter.GetSpans()
	if len(spans) != 1 {
		t.Fatalf("got %d spans, want 1", len(spans))
	}

	span := spans[0]
	if span.Name != "queuer.process" {
		t.Errorf("span name = %q, want %q", span.Name, "queuer.process")
	}
	if span.SpanKind != trace.SpanKindConsumer {
		t.Errorf("span kind = %v, want Consumer", span.SpanKind)
	}
	if span.Status.Code != codes.Ok {
		t.Errorf("span status = %v, want Ok", span.Status.Code)
	}
}

func TestTracer_Start_Error(t *testing.T) {
	tr, exporter := setupTestTracer(t)

	msg := &queuer.Message{ID: "msg-2", Body: "bad", Attributes: map[string]string{}}
	_, end := tr.Start(context.Background(), msg)
	end(errors.New("handler failed"))

	spans := exporter.GetSpans()
	if len(spans) != 1 {
		t.Fatalf("got %d spans, want 1", len(spans))
	}

	span := spans[0]
	if span.Status.Code != codes.Error {
		t.Errorf("span status = %v, want Error", span.Status.Code)
	}
	if len(span.Events) == 0 {
		t.Error("expected error event recorded on span")
	}
}

func TestTracer_Start_PropagatesParentContext(t *testing.T) {
	tr, exporter := setupTestTracer(t)

	// Create a parent span to generate a valid trace context
	parentProvider := sdktrace.NewTracerProvider()
	parentTracer := parentProvider.Tracer("test")
	parentCtx, parentSpan := parentTracer.Start(context.Background(), "parent")
	parentSpanCtx := parentSpan.SpanContext()
	parentSpan.End()

	// Inject trace context into message attributes (simulating a producer)
	attrs := map[string]string{}
	propagator := propagation.TraceContext{}
	propagator.Inject(parentCtx, propagation.MapCarrier(attrs))

	msg := &queuer.Message{ID: "msg-3", Body: "child", Attributes: attrs}
	_, end := tr.Start(context.Background(), msg)
	end(nil)

	spans := exporter.GetSpans()
	if len(spans) != 1 {
		t.Fatalf("got %d spans, want 1", len(spans))
	}

	// The child span should have the parent's trace ID
	childSpan := spans[0]
	if childSpan.SpanContext.TraceID() != parentSpanCtx.TraceID() {
		t.Errorf("child trace ID = %s, want %s", childSpan.SpanContext.TraceID(), parentSpanCtx.TraceID())
	}
}

func TestTracer_Start_Attributes(t *testing.T) {
	tr, exporter := setupTestTracer(t)

	msg := &queuer.Message{
		ID:           "msg-4",
		Body:         "test",
		ReceiveCount: 3,
		Attributes:   map[string]string{},
	}
	_, end := tr.Start(context.Background(), msg)
	end(nil)

	spans := exporter.GetSpans()
	if len(spans) != 1 {
		t.Fatalf("got %d spans, want 1", len(spans))
	}

	attrMap := make(map[string]interface{})
	for _, attr := range spans[0].Attributes {
		attrMap[string(attr.Key)] = attr.Value.AsInterface()
	}

	if attrMap["messaging.message.id"] != "msg-4" {
		t.Errorf("message.id = %v, want msg-4", attrMap["messaging.message.id"])
	}
	if attrMap["messaging.system"] != "aws_sqs" {
		t.Errorf("system = %v, want aws_sqs", attrMap["messaging.system"])
	}
	if attrMap["messaging.destination.name"] != "test-queue" {
		t.Errorf("destination = %v, want test-queue", attrMap["messaging.destination.name"])
	}
	if attrMap["messaging.message.receive_count"] != int64(3) {
		t.Errorf("receive_count = %v, want 3", attrMap["messaging.message.receive_count"])
	}
}

func TestTracer_Start_NilAttributes(t *testing.T) {
	tr, exporter := setupTestTracer(t)

	msg := &queuer.Message{ID: "msg-5", Body: "no-attrs"}
	_, end := tr.Start(context.Background(), msg)
	end(nil)

	spans := exporter.GetSpans()
	if len(spans) != 1 {
		t.Fatalf("got %d spans, want 1", len(spans))
	}
}
