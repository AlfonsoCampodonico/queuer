//go:build integration

// Integration tests against an SQS-compatible endpoint such as ElasticMQ:
//
//	docker run --rm -p 9324:9324 softwaremill/elasticmq-native
//	QUEUER_SQS_ENDPOINT=http://localhost:9324 go test -tags integration ./...
package sqs_test

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

	"github.com/aws/aws-sdk-go-v2/aws"
	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"

	"github.com/AlfonsoCampodonico/queuer"
	"github.com/AlfonsoCampodonico/queuer/sqs"
)

func newClient(t *testing.T) *awssqs.Client {
	t.Helper()
	endpoint := os.Getenv("QUEUER_SQS_ENDPOINT")
	if endpoint == "" {
		t.Skip("QUEUER_SQS_ENDPOINT not set")
	}
	return awssqs.NewFromConfig(aws.Config{
		Region: "us-east-1",
		Credentials: aws.CredentialsProviderFunc(func(context.Context) (aws.Credentials, error) {
			return aws.Credentials{AccessKeyID: "test", SecretAccessKey: "test"}, nil
		}),
	}, func(o *awssqs.Options) { o.BaseEndpoint = aws.String(endpoint) })
}

func createQueue(t *testing.T, client *awssqs.Client, visibility time.Duration) string {
	t.Helper()
	out, err := client.CreateQueue(context.Background(), &awssqs.CreateQueueInput{
		QueueName: aws.String(fmt.Sprintf("queuer-it-%d", time.Now().UnixNano())),
		Attributes: map[string]string{
			string(types.QueueAttributeNameVisibilityTimeout): fmt.Sprint(int(visibility.Seconds())),
		},
	})
	if err != nil {
		t.Fatalf("CreateQueue: %v", err)
	}
	url := aws.ToString(out.QueueUrl)
	t.Cleanup(func() {
		_, _ = client.DeleteQueue(context.Background(), &awssqs.DeleteQueueInput{QueueUrl: aws.String(url)})
	})
	return url
}

func quiet() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

// runUntil runs c until done is closed or the timeout passes.
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

func queueDepth(t *testing.T, client *awssqs.Client, url string) int {
	t.Helper()
	out, err := client.ReceiveMessage(context.Background(), &awssqs.ReceiveMessageInput{
		QueueUrl:            aws.String(url),
		MaxNumberOfMessages: 10,
		WaitTimeSeconds:     1,
		VisibilityTimeout:   0,
	})
	if err != nil {
		t.Fatalf("ReceiveMessage: %v", err)
	}
	return len(out.Messages)
}

func TestIntegration_PublishConsumeBatchedAck(t *testing.T) {
	client := newClient(t)
	url := createQueue(t, client, 30*time.Second)
	pub := sqs.NewPublisher(client, url)

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
		if msg.Attributes["tenant"] != "acme" || msg.System != "aws_sqs" || msg.ReceiveCount != 1 || msg.SentAt.IsZero() {
			t.Errorf("unexpected message: %+v", msg)
		}
		if _, dup := seen.LoadOrStore(string(msg.Body), true); !dup && count.Add(1) == n {
			close(done)
		}
		return nil
	})

	c := queuer.New(sqs.NewReceiver(client, url), handler,
		sqs.NewAcknowledger(client, url, sqs.WithBatching(50*time.Millisecond)),
		queuer.WithWorkers(10), queuer.WithWaitTime(time.Second), queuer.WithLogger(quiet()))
	runUntil(t, c, done, 30*time.Second)

	if depth := queueDepth(t, client, url); depth != 0 {
		t.Fatalf("%d messages left in queue, want 0 (all acked)", depth)
	}
}

func TestIntegration_NackRedelivers(t *testing.T) {
	client := newClient(t)
	url := createQueue(t, client, 30*time.Second)
	if _, err := sqs.NewPublisher(client, url).Publish(context.Background(), &queuer.Message{Body: []byte("x")}); err != nil {
		t.Fatal(err)
	}

	var counts []int
	var mu sync.Mutex
	done := make(chan struct{})
	handler := queuer.HandlerFunc(func(_ context.Context, msg *queuer.Message) error {
		mu.Lock()
		defer mu.Unlock()
		counts = append(counts, msg.ReceiveCount)
		if len(counts) == 1 {
			return errors.New("transient")
		}
		close(done)
		return nil
	})
	c := sqs.NewConsumer(client, url, handler,
		queuer.WithErrorResolver(queuer.ErrorResolverFunc(func(context.Context, *queuer.Message, error) (queuer.ErrorAction, error) {
			return queuer.Nack, nil
		})),
		queuer.WithWaitTime(time.Second), queuer.WithLogger(quiet()))
	runUntil(t, c, done, 30*time.Second)

	if len(counts) != 2 || counts[0] != 1 || counts[1] != 2 {
		t.Fatalf("receive counts = %v, want [1 2]", counts)
	}
}

func TestIntegration_LeaseExtensionPreventsRedelivery(t *testing.T) {
	client := newClient(t)
	// Visibility of 2s is far shorter than the 6s handler below.
	url := createQueue(t, client, 2*time.Second)
	if _, err := sqs.NewPublisher(client, url).Publish(context.Background(), &queuer.Message{Body: []byte("slow")}); err != nil {
		t.Fatal(err)
	}

	var calls atomic.Int64
	done := make(chan struct{})
	handler := queuer.HandlerFunc(func(context.Context, *queuer.Message) error {
		if calls.Add(1) == 1 {
			time.Sleep(6 * time.Second)
			close(done)
		}
		return nil
	})
	c := sqs.NewConsumer(client, url, handler,
		queuer.WithWorkers(2),
		queuer.WithLeaseExtension(2*time.Second),
		queuer.WithWaitTime(time.Second), queuer.WithLogger(quiet()))
	runUntil(t, c, done, 30*time.Second)

	if got := calls.Load(); got != 1 {
		t.Fatalf("handler called %d times, want 1 (lease should prevent redelivery)", got)
	}
	if depth := queueDepth(t, client, url); depth != 0 {
		t.Fatalf("%d messages left in queue, want 0", depth)
	}
}
