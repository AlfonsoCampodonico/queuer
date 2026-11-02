package otel_test

import (
	"context"
	"log"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"

	"github.com/AlfonsoCampodonico/queuer"
	qotel "github.com/AlfonsoCampodonico/queuer/otel"
)

func ExampleNewTracer() {
	metrics, err := qotel.NewMetrics(otel.GetMeterProvider(),
		qotel.WithAttributes(attribute.String("queue", "orders")),
	)
	if err != nil {
		log.Fatal(err)
	}

	opts := []queuer.Option{
		queuer.WithTracer(qotel.NewTracer(otel.GetTracerProvider())),
		queuer.WithMetrics(metrics),
	}
	_ = opts // pass to sqs.NewConsumer, pubsub.NewConsumer or queuer.New
}

func ExampleNewPublisher() {
	// Wrap any queuer.Publisher (sqs.Publisher, pubsub.Publisher) so every
	// message carries the current trace context to its consumer.
	var inner queuer.Publisher = queuer.PublisherFunc(func(context.Context, *queuer.Message) (string, error) {
		return "id-1", nil
	})
	pub := qotel.NewPublisher(inner, otel.GetTracerProvider())

	if _, err := pub.Publish(context.Background(), &queuer.Message{Body: []byte("hello")}); err != nil {
		log.Fatal(err)
	}
}
