package otel_test

import (
	"context"
	"errors"
	"testing"

	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"

	"github.com/AlfonsoCampodonico/queuer"
	otelpkg "github.com/AlfonsoCampodonico/queuer/otel"
)

func newProvider() (*sdktrace.TracerProvider, *tracetest.InMemoryExporter) {
	exporter := tracetest.NewInMemoryExporter()
	return sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter)), exporter
}

func attrMap(s tracetest.SpanStub) map[string]any {
	m := make(map[string]any)
	for _, kv := range s.Attributes {
		m[string(kv.Key)] = kv.Value.AsInterface()
	}
	return m
}

func TestTracer_SuccessSpan(t *testing.T) {
	provider, exporter := newProvider()
	tr := otelpkg.NewTracer(provider)

	msg := &queuer.Message{ID: "m1", Body: []byte("hello"), System: "gcp_pubsub", Queue: "orders-sub", ReceiveCount: 3}
	_, end := tr.Start(context.Background(), msg)
	end(nil)

	spans := exporter.GetSpans()
	if len(spans) != 1 {
		t.Fatalf("got %d spans, want 1", len(spans))
	}
	s := spans[0]
	if s.Name != "process orders-sub" || s.SpanKind != trace.SpanKindConsumer {
		t.Errorf("name=%q kind=%v", s.Name, s.SpanKind)
	}
	if s.Status.Code != codes.Unset {
		t.Errorf("status = %v, want Unset on success", s.Status.Code)
	}
	attrs := attrMap(s)
	want := map[string]any{
		"messaging.system":                 "gcp_pubsub",
		"messaging.destination.name":       "orders-sub",
		"messaging.message.id":             "m1",
		"messaging.operation.type":         "process",
		"messaging.message.delivery_count": int64(3),
		"messaging.message.body.size":      int64(5),
	}
	for k, v := range want {
		if attrs[k] != v {
			t.Errorf("%s = %v, want %v", k, attrs[k], v)
		}
	}
}

func TestTracer_ErrorSpan(t *testing.T) {
	provider, exporter := newProvider()
	_, end := otelpkg.NewTracer(provider).Start(context.Background(), &queuer.Message{ID: "m2"})
	end(errors.New("handler failed"))

	s := exporter.GetSpans()[0]
	if s.Status.Code != codes.Error || len(s.Events) == 0 {
		t.Errorf("status=%v events=%d, want Error with an exception event", s.Status.Code, len(s.Events))
	}
	if s.Name != "process" {
		t.Errorf("name = %q, want %q without a queue", s.Name, "process")
	}
	if _, ok := attrMap(s)["messaging.system"]; ok {
		t.Error("messaging.system set although Message.System is empty")
	}
}

func TestPublisher_PropagatesToConsumer(t *testing.T) {
	provider, exporter := newProvider()

	var published *queuer.Message
	inner := describedPublisher{fn: func(_ context.Context, msg *queuer.Message) (string, error) {
		published = msg
		return "id-1", nil
	}}
	pub := otelpkg.NewPublisher(inner, provider)

	original := &queuer.Message{Body: []byte("x"), Attributes: map[string]string{"k": "v"}}
	if _, err := pub.Publish(context.Background(), original); err != nil {
		t.Fatal(err)
	}
	if _, ok := original.Attributes["traceparent"]; ok {
		t.Error("Publish modified the caller's attributes")
	}
	if published.Attributes["traceparent"] == "" || published.Attributes["k"] != "v" {
		t.Fatalf("published attributes = %v", published.Attributes)
	}

	// The consumer side continues the producer's trace.
	received := &queuer.Message{ID: "id-1", Attributes: published.Attributes, System: "aws_sqs", Queue: "orders"}
	_, end := otelpkg.NewTracer(provider).Start(context.Background(), received)
	end(nil)

	spans := exporter.GetSpans()
	if len(spans) != 2 {
		t.Fatalf("got %d spans, want 2", len(spans))
	}
	producer, consumer := spans[0], spans[1]
	if producer.Name != "send orders" || producer.SpanKind != trace.SpanKindProducer {
		t.Errorf("producer name=%q kind=%v", producer.Name, producer.SpanKind)
	}
	if attrMap(producer)["messaging.message.id"] != "id-1" || attrMap(producer)["messaging.system"] != "aws_sqs" {
		t.Errorf("producer attributes = %v", attrMap(producer))
	}
	if consumer.SpanContext.TraceID() != producer.SpanContext.TraceID() {
		t.Error("consumer span is not in the producer's trace")
	}
	if consumer.Parent.SpanID() != producer.SpanContext.SpanID() {
		t.Error("consumer span is not a child of the producer span")
	}
}

func TestPublisher_Error(t *testing.T) {
	provider, exporter := newProvider()
	want := errors.New("throttled")
	pub := otelpkg.NewPublisher(queuer.PublisherFunc(func(context.Context, *queuer.Message) (string, error) {
		return "", want
	}), provider)
	if _, err := pub.Publish(context.Background(), &queuer.Message{}); !errors.Is(err, want) {
		t.Fatalf("err = %v, want %v", err, want)
	}
	if s := exporter.GetSpans()[0]; s.Status.Code != codes.Error || s.Name != "send" {
		t.Errorf("name=%q status=%v", s.Name, s.Status.Code)
	}
}

type describedPublisher struct {
	fn func(context.Context, *queuer.Message) (string, error)
}

func (d describedPublisher) Publish(ctx context.Context, msg *queuer.Message) (string, error) {
	return d.fn(ctx, msg)
}
func (describedPublisher) MessagingSystem() string { return "aws_sqs" }
func (describedPublisher) DestinationName() string { return "orders" }
