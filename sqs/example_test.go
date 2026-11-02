package sqs_test

import (
	"context"
	"log"
	"os/signal"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"

	"github.com/AlfonsoCampodonico/queuer"
	"github.com/AlfonsoCampodonico/queuer/sqs"
)

func ExampleNewConsumer() {
	// In production, build the client with config.LoadDefaultConfig.
	client := awssqs.NewFromConfig(aws.Config{Region: "us-east-1"})
	queueURL := "https://sqs.us-east-1.amazonaws.com/123456789012/orders"

	handler := queuer.HandlerFunc(func(ctx context.Context, msg *queuer.Message) error {
		log.Printf("order %s: %s", msg.ID, msg.Body)
		return nil
	})

	consumer := sqs.NewConsumer(client, queueURL, handler,
		queuer.WithWorkers(10),
		queuer.WithLeaseExtension(2*time.Minute),
	)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	if err := consumer.Run(ctx); err != nil {
		log.Fatal(err)
	}
}

func ExampleWithBatching() {
	client := awssqs.NewFromConfig(aws.Config{Region: "us-east-1"})
	queueURL := "https://sqs.us-east-1.amazonaws.com/123456789012/orders"

	consumer := queuer.New(
		sqs.NewReceiver(client, queueURL),
		queuer.HandlerFunc(func(context.Context, *queuer.Message) error { return nil }),
		sqs.NewAcknowledger(client, queueURL, sqs.WithBatching(50*time.Millisecond)),
		queuer.WithWorkers(50),
	)
	_ = consumer
}

func ExampleNewPublisher() {
	client := awssqs.NewFromConfig(aws.Config{Region: "us-east-1"})
	pub := sqs.NewPublisher(client, "https://sqs.us-east-1.amazonaws.com/123456789012/orders")

	id, err := pub.Publish(context.Background(), &queuer.Message{
		Body:       []byte(`{"order_id":42}`),
		Attributes: map[string]string{"type": "order.created"},
	})
	if err != nil {
		log.Fatal(err)
	}
	log.Println("published", id)
}
