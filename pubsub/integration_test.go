//go:build integration

// Integration tests against the Pub/Sub emulator:
//
//	docker run --rm -p 8085:8085 gcr.io/google.com/cloudsdktool/google-cloud-cli:emulators \
//	  gcloud beta emulators pubsub start --host-port=0.0.0.0:8085 --project=queuer-test
//	PUBSUB_EMULATOR_HOST=localhost:8085 go test -tags integration ./...
package pubsub_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	apiv1 "cloud.google.com/go/pubsub/v2/apiv1"
	"cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"

	"github.com/AlfonsoCampodonico/queuer"
	"github.com/AlfonsoCampodonico/queuer/pubsub"
)

const project = "queuer-test"

type env struct {
	sub    *apiv1.SubscriptionAdminClient
	topic  *apiv1.TopicAdminClient
	topicN string
	subN   string
}

func setup(t *testing.T, ackDeadline time.Duration) env {
	t.Helper()
	if os.Getenv("PUBSUB_EMULATOR_HOST") == "" {
		t.Skip("PUBSUB_EMULATOR_HOST not set")
	}
	ctx := context.Background()
	topicClient, err := apiv1.NewTopicAdminClient(ctx)
	if err != nil {
		t.Fatal(err)
	}
	subClient, err := apiv1.NewSubscriptionAdminClient(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = topicClient.Close()
		_ = subClient.Close()
	})

	id := fmt.Sprintf("queuer-it-%d", time.Now().UnixNano())
	e := env{sub: subClient, topic: topicClient, topicN: pubsub.TopicName(project, id), subN: pubsub.SubscriptionName(project, id)}
	if _, err := topicClient.CreateTopic(ctx, &pubsubpb.Topic{Name: e.topicN}); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	if _, err := subClient.CreateSubscription(ctx, &pubsubpb.Subscription{
		Name:               e.subN,
		Topic:              e.topicN,
		AckDeadlineSeconds: int32(ackDeadline.Seconds()),
	}); err != nil {
		t.Fatalf("CreateSubscription: %v", err)
	}
	return e
}

func quiet() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func runUntil(t *testing.T, c *queuer.Consumer, done <-chan struct{}, timeout time.Duration) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	errc := make(chan error, 1)
	go func() { errc <- c.Run(ctx) }()
	select {
	case <-done:
	case <-time.After(timeout):
		t.Error("timed out waiting for messages")
	}
	cancel()
	if err := <-errc; err != nil {
		t.Fatalf("Run: %v", err)
	}
}

func remaining(t *testing.T, e env) int {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	resp, err := e.sub.Pull(ctx, &pubsubpb.PullRequest{Subscription: e.subN, MaxMessages: 100})
	if err != nil {
		return 0 // deadline: nothing available
	}
	return len(resp.GetReceivedMessages())
}

func TestIntegration_PublishConsumeBatchedAck(t *testing.T) {
	e := setup(t, 30*time.Second)
	pub := pubsub.NewPublisher(e.topic, e.topicN)

	const n = 25
	for i := range n {
		if _, err := pub.Publish(context.Background(), &queuer.Message{
			Body:       []byte(fmt.Sprintf("msg-%d", i)),
			Attributes: map[string]string{"tenant": "acme"},
		}); err != nil {
			t.Fatalf("Publish: %v", err)
		}
	}

	var seen sync.Map
	var count atomic.Int64
	done := make(chan struct{})
	handler := queuer.HandlerFunc(func(_ context.Context, msg *queuer.Message) error {
		if msg.Attributes["tenant"] != "acme" || msg.System != "gcp_pubsub" || msg.SentAt.IsZero() {
			t.Errorf("unexpected message: %+v", msg)
		}
		if _, dup := seen.LoadOrStore(string(msg.Body), true); !dup && count.Add(1) == n {
			close(done)
		}
		return nil
	})

	c := queuer.New(pubsub.NewReceiver(e.sub, e.subN), handler,
		pubsub.NewAcknowledger(e.sub, e.subN, pubsub.WithBatching(50*time.Millisecond)),
		queuer.WithWorkers(10), queuer.WithMaxMessages(100), queuer.WithWaitTime(2*time.Second),
		queuer.WithLeaseExtension(pubsub.DefaultLeaseExtension), queuer.WithLogger(quiet()))
	runUntil(t, c, done, 60*time.Second)

	if left := remaining(t, e); left != 0 {
		t.Fatalf("%d messages left, want 0 (all acked)", left)
	}
}

func TestIntegration_NackRedelivers(t *testing.T) {
	e := setup(t, 30*time.Second)
	if _, err := pubsub.NewPublisher(e.topic, e.topicN).Publish(context.Background(), &queuer.Message{Body: []byte("x")}); err != nil {
		t.Fatal(err)
	}

	var calls atomic.Int64
	done := make(chan struct{})
	handler := queuer.HandlerFunc(func(context.Context, *queuer.Message) error {
		if calls.Add(1) == 1 {
			return errors.New("transient")
		}
		close(done)
		return nil
	})
	c := pubsub.NewConsumer(e.sub, e.subN, handler,
		queuer.WithErrorResolver(queuer.ErrorResolverFunc(func(context.Context, *queuer.Message, error) (queuer.ErrorAction, error) {
			return queuer.Nack, nil
		})),
		queuer.WithWaitTime(2*time.Second), queuer.WithLogger(quiet()))
	runUntil(t, c, done, 60*time.Second)

	if got := calls.Load(); got != 2 {
		t.Fatalf("handler called %d times, want 2", got)
	}
}

func TestIntegration_LeaseExtensionPreventsRedelivery(t *testing.T) {
	// 10s is the minimum ack deadline; the handler below runs for 15s.
	e := setup(t, 10*time.Second)
	if _, err := pubsub.NewPublisher(e.topic, e.topicN).Publish(context.Background(), &queuer.Message{Body: []byte("slow")}); err != nil {
		t.Fatal(err)
	}

	var calls atomic.Int64
	done := make(chan struct{})
	handler := queuer.HandlerFunc(func(context.Context, *queuer.Message) error {
		if calls.Add(1) == 1 {
			time.Sleep(15 * time.Second)
			close(done)
		}
		return nil
	})
	c := pubsub.NewConsumer(e.sub, e.subN, handler,
		queuer.WithWorkers(2),
		queuer.WithLeaseExtension(10*time.Second),
		queuer.WithWaitTime(2*time.Second), queuer.WithLogger(quiet()))
	runUntil(t, c, done, 60*time.Second)

	if got := calls.Load(); got != 1 {
		t.Fatalf("handler called %d times, want 1 (lease should prevent redelivery)", got)
	}
}
