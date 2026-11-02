package pubsub

import (
	"context"
	"errors"
	"slices"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"github.com/googleapis/gax-go/v2"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/AlfonsoCampodonico/queuer"
)

const testSub = "projects/p/subscriptions/orders-sub"

type fakeClient struct {
	mu      sync.Mutex
	pull    func(context.Context, *pubsubpb.PullRequest) (*pubsubpb.PullResponse, error)
	acks    [][]string
	modacks []*pubsubpb.ModifyAckDeadlineRequest
	ackErr  error
}

func (f *fakeClient) Pull(ctx context.Context, req *pubsubpb.PullRequest, _ ...gax.CallOption) (*pubsubpb.PullResponse, error) {
	return f.pull(ctx, req)
}

func (f *fakeClient) Acknowledge(_ context.Context, req *pubsubpb.AcknowledgeRequest, _ ...gax.CallOption) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.acks = append(f.acks, slices.Clone(req.GetAckIds()))
	return f.ackErr
}

func (f *fakeClient) ModifyAckDeadline(_ context.Context, req *pubsubpb.ModifyAckDeadlineRequest, _ ...gax.CallOption) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.modacks = append(f.modacks, req)
	return nil
}

func TestReceiver_MapsMessages(t *testing.T) {
	published := time.Date(2026, 11, 2, 10, 0, 0, 0, time.UTC)
	client := &fakeClient{pull: func(_ context.Context, req *pubsubpb.PullRequest) (*pubsubpb.PullResponse, error) {
		if req.GetSubscription() != testSub || req.GetMaxMessages() != MaxPullMessages {
			t.Errorf("unexpected request: %v", req)
		}
		return &pubsubpb.PullResponse{ReceivedMessages: []*pubsubpb.ReceivedMessage{{
			AckId:           "ack-1",
			DeliveryAttempt: 2,
			Message: &pubsubpb.PubsubMessage{
				MessageId:   "m1",
				Data:        []byte("payload"),
				Attributes:  map[string]string{"traceparent": "00-abc-def-01"},
				PublishTime: timestamppb.New(published),
			},
		}, {
			AckId:   "ack-2",
			Message: &pubsubpb.PubsubMessage{MessageId: "m2"},
		}}}, nil
	}}

	msgs, err := NewReceiver(client, testSub).Receive(context.Background(), 5000, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if len(msgs) != 2 {
		t.Fatalf("got %d messages, want 2", len(msgs))
	}
	m := msgs[0]
	if m.ID != "m1" || string(m.Body) != "payload" || m.AckID != "ack-1" || m.ReceiveCount != 2 {
		t.Errorf("unexpected message: %+v", m)
	}
	if !m.SentAt.Equal(published) || m.System != System || m.Queue != "orders-sub" {
		t.Errorf("SentAt=%v System=%q Queue=%q", m.SentAt, m.System, m.Queue)
	}
	if m.Attributes["traceparent"] != "00-abc-def-01" {
		t.Errorf("Attributes = %v", m.Attributes)
	}
	if msgs[1].Attributes == nil {
		t.Error("Attributes is nil for a message without attributes")
	}
}

func TestReceiver_WaitTimeElapsedReturnsEmpty(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		client := &fakeClient{pull: func(ctx context.Context, _ *pubsubpb.PullRequest) (*pubsubpb.PullResponse, error) {
			<-ctx.Done()
			return nil, status.Error(codes.DeadlineExceeded, "deadline exceeded")
		}}
		start := time.Now()
		msgs, err := NewReceiver(client, testSub).Receive(context.Background(), 10, 5*time.Second)
		if err != nil || len(msgs) != 0 {
			t.Fatalf("Receive = %v, %v; want no messages and no error", msgs, err)
		}
		if waited := time.Since(start); waited != 5*time.Second {
			t.Errorf("waited %v, want 5s", waited)
		}
	})
}

func TestReceiver_ParentCancelledReturnsError(t *testing.T) {
	client := &fakeClient{pull: func(ctx context.Context, _ *pubsubpb.PullRequest) (*pubsubpb.PullResponse, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	}}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := NewReceiver(client, testSub).Receive(ctx, 10, time.Minute); !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
}

func TestReceiver_OtherErrorsPropagate(t *testing.T) {
	want := status.Error(codes.PermissionDenied, "denied")
	client := &fakeClient{pull: func(context.Context, *pubsubpb.PullRequest) (*pubsubpb.PullResponse, error) { return nil, want }}
	if _, err := NewReceiver(client, testSub).Receive(context.Background(), 10, time.Second); !errors.Is(err, want) {
		t.Fatalf("err = %v, want %v", err, want)
	}
}

func TestAcknowledger_Unbatched(t *testing.T) {
	client := &fakeClient{}
	a := NewAcknowledger(client, testSub)
	ctx := context.Background()

	if err := a.Ack(ctx, &queuer.Message{AckID: "a1"}); err != nil {
		t.Fatal(err)
	}
	if err := a.Nack(ctx, &queuer.Message{AckID: "a2"}); err != nil {
		t.Fatal(err)
	}
	if err := a.ChangeVisibility(ctx, &queuer.Message{AckID: "a3"}, time.Hour); err != nil {
		t.Fatal(err)
	}

	if len(client.acks) != 1 || !slices.Equal(client.acks[0], []string{"a1"}) {
		t.Errorf("acks = %v", client.acks)
	}
	if len(client.modacks) != 2 {
		t.Fatalf("modacks = %v", client.modacks)
	}
	if client.modacks[0].GetAckDeadlineSeconds() != 0 || client.modacks[0].GetAckIds()[0] != "a2" {
		t.Errorf("nack = %v", client.modacks[0])
	}
	if client.modacks[1].GetAckDeadlineSeconds() != 600 {
		t.Errorf("deadline = %d, want clamped to 600", client.modacks[1].GetAckDeadlineSeconds())
	}
}

func TestAcknowledger_BatchedGroupsByDeadline(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		client := &fakeClient{}
		a := NewAcknowledger(client, testSub, WithBatching(50*time.Millisecond))

		var wg sync.WaitGroup
		run := func(fn func() error) {
			wg.Add(1)
			go func() {
				defer wg.Done()
				if err := fn(); err != nil {
					t.Error(err)
				}
			}()
		}
		for _, id := range []string{"a1", "a2", "a3"} {
			run(func() error { return a.Ack(context.Background(), &queuer.Message{AckID: id}) })
		}
		for _, id := range []string{"b1", "b2"} {
			run(func() error {
				return a.ChangeVisibility(context.Background(), &queuer.Message{AckID: id}, 30*time.Second)
			})
		}
		run(func() error { return a.Nack(context.Background(), &queuer.Message{AckID: "c1"}) })
		wg.Wait()

		if len(client.acks) != 1 || len(client.acks[0]) != 3 {
			t.Errorf("acks = %v, want one request with 3 IDs", client.acks)
		}
		byDeadline := map[int32]int{}
		for _, m := range client.modacks {
			byDeadline[m.GetAckDeadlineSeconds()] += len(m.GetAckIds())
		}
		if len(client.modacks) != 2 || byDeadline[30] != 2 || byDeadline[0] != 1 {
			t.Errorf("modacks by deadline = %v over %d requests, want {30:2, 0:1} over 2", byDeadline, len(client.modacks))
		}
	})
}

func TestAcknowledger_ErrorReachesEveryCaller(t *testing.T) {
	want := status.Error(codes.Unavailable, "down")
	client := &fakeClient{ackErr: want}
	if err := NewAcknowledger(client, testSub).Ack(context.Background(), &queuer.Message{AckID: "a1"}); !errors.Is(err, want) {
		t.Fatalf("err = %v, want %v", err, want)
	}
}

type fakePublishClient struct {
	req *pubsubpb.PublishRequest
	ids []string
}

func (f *fakePublishClient) Publish(_ context.Context, req *pubsubpb.PublishRequest, _ ...gax.CallOption) (*pubsubpb.PublishResponse, error) {
	f.req = req
	return &pubsubpb.PublishResponse{MessageIds: f.ids}, nil
}

func TestPublisher(t *testing.T) {
	client := &fakePublishClient{ids: []string{"id-1"}}
	p := NewPublisher(client, TopicName("p", "orders"),
		WithOrderingKey(func(m *queuer.Message) string { return m.Attributes["customer"] }))

	id, err := p.Publish(context.Background(), &queuer.Message{Body: []byte("hi"), Attributes: map[string]string{"customer": "c1"}})
	if err != nil || id != "id-1" {
		t.Fatalf("Publish = %q, %v", id, err)
	}
	pm := client.req.GetMessages()[0]
	if client.req.GetTopic() != "projects/p/topics/orders" || string(pm.GetData()) != "hi" || pm.GetOrderingKey() != "c1" {
		t.Errorf("unexpected request: %v", client.req)
	}
	if p.MessagingSystem() != "gcp_pubsub" || p.DestinationName() != "orders" {
		t.Errorf("system=%q destination=%q", p.MessagingSystem(), p.DestinationName())
	}
}

func TestPublisher_NoMessageID(t *testing.T) {
	p := NewPublisher(&fakePublishClient{}, TopicName("p", "orders"))
	if _, err := p.Publish(context.Background(), &queuer.Message{}); !errors.Is(err, errNoMessageID) {
		t.Fatalf("err = %v, want errNoMessageID", err)
	}
}

func TestNewConsumer_EnablesLeaseExtension(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var once sync.Once
		client := &fakeClient{pull: func(ctx context.Context, _ *pubsubpb.PullRequest) (*pubsubpb.PullResponse, error) {
			var resp *pubsubpb.PullResponse
			once.Do(func() {
				resp = &pubsubpb.PullResponse{ReceivedMessages: []*pubsubpb.ReceivedMessage{{
					AckId: "a1", Message: &pubsubpb.PubsubMessage{MessageId: "m1"},
				}}}
			})
			if resp != nil {
				return resp, nil
			}
			<-ctx.Done()
			return nil, ctx.Err()
		}}
		handler := queuer.HandlerFunc(func(context.Context, *queuer.Message) error {
			time.Sleep(45 * time.Second)
			return nil
		})
		c := NewConsumer(client, testSub, handler)

		ctx, cancel := context.WithCancel(context.Background())
		errc := make(chan error, 1)
		go func() { errc <- c.Run(ctx) }()
		time.Sleep(time.Minute)
		cancel()
		if err := <-errc; err != nil {
			t.Fatal(err)
		}

		// Extended at 0s and 30s with the default 60s lease, then acked at 45s.
		if len(client.modacks) != 2 {
			t.Fatalf("modacks = %d, want 2", len(client.modacks))
		}
		for _, m := range client.modacks {
			if m.GetAckDeadlineSeconds() != 60 {
				t.Errorf("deadline = %d, want 60", m.GetAckDeadlineSeconds())
			}
		}
		if len(client.acks) != 1 {
			t.Errorf("acks = %v, want 1", client.acks)
		}
	})
}
