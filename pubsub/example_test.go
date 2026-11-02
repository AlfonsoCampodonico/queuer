package pubsub_test

import (
	"context"
	"log"
	"os/signal"
	"syscall"
	"time"

	apiv1 "cloud.google.com/go/pubsub/v2/apiv1"

	"github.com/AlfonsoCampodonico/queuer"
	"github.com/AlfonsoCampodonico/queuer/pubsub"
)

func ExampleNewConsumer() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Uses Application Default Credentials, or PUBSUB_EMULATOR_HOST if set.
	client, err := apiv1.NewSubscriptionAdminClient(ctx)
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = client.Close() }()

	handler := queuer.HandlerFunc(func(ctx context.Context, msg *queuer.Message) error {
		log.Printf("order %s: %s", msg.ID, msg.Body)
		return nil
	})

	consumer := pubsub.NewConsumer(client, pubsub.SubscriptionName("my-project", "orders-sub"), handler,
		queuer.WithWorkers(10),
		queuer.WithMaxMessages(100),
	)
	if err := consumer.Run(ctx); err != nil {
		log.Fatal(err)
	}
}

func ExampleWithBatching() {
	ctx := context.Background()
	client, err := apiv1.NewSubscriptionAdminClient(ctx)
	if err != nil {
		log.Fatal(err)
	}
	subscription := pubsub.SubscriptionName("my-project", "orders-sub")

	consumer := queuer.New(
		pubsub.NewReceiver(client, subscription),
		queuer.HandlerFunc(func(context.Context, *queuer.Message) error { return nil }),
		pubsub.NewAcknowledger(client, subscription, pubsub.WithBatching(50*time.Millisecond)),
		queuer.WithWorkers(50),
		queuer.WithMaxMessages(500),
		queuer.WithLeaseExtension(pubsub.DefaultLeaseExtension),
	)
	_ = consumer
}

func ExampleNewPublisher() {
	ctx := context.Background()
	client, err := apiv1.NewTopicAdminClient(ctx)
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = client.Close() }()

	pub := pubsub.NewPublisher(client, pubsub.TopicName("my-project", "orders"))
	id, err := pub.Publish(ctx, &queuer.Message{
		Body:       []byte(`{"order_id":42}`),
		Attributes: map[string]string{"type": "order.created"},
	})
	if err != nil {
		log.Fatal(err)
	}
	log.Println("published", id)
}
