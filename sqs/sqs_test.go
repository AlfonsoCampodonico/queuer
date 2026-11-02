package sqs

import (
	"context"
	"errors"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"

	"github.com/AlfonsoCampodonico/queuer"
)

const testQueueURL = "https://sqs.us-east-1.amazonaws.com/123456789012/orders"

// fakeClient records calls made through the Client and SendClient interfaces.
type fakeClient struct {
	mu sync.Mutex

	receive func(*awssqs.ReceiveMessageInput) (*awssqs.ReceiveMessageOutput, error)

	deleted          []string
	deleteBatches    [][]string
	visibility       []types.ChangeMessageVisibilityBatchRequestEntry
	visibilityBatchN int
	failHandle       string
	sent             []*awssqs.SendMessageInput
}

func (f *fakeClient) ReceiveMessage(_ context.Context, in *awssqs.ReceiveMessageInput, _ ...func(*awssqs.Options)) (*awssqs.ReceiveMessageOutput, error) {
	return f.receive(in)
}

func (f *fakeClient) DeleteMessage(_ context.Context, in *awssqs.DeleteMessageInput, _ ...func(*awssqs.Options)) (*awssqs.DeleteMessageOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if aws.ToString(in.ReceiptHandle) == f.failHandle {
		return nil, errors.New("receipt handle is invalid")
	}
	f.deleted = append(f.deleted, aws.ToString(in.ReceiptHandle))
	return &awssqs.DeleteMessageOutput{}, nil
}

func (f *fakeClient) DeleteMessageBatch(_ context.Context, in *awssqs.DeleteMessageBatchInput, _ ...func(*awssqs.Options)) (*awssqs.DeleteMessageBatchOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := &awssqs.DeleteMessageBatchOutput{}
	var batch []string
	for _, e := range in.Entries {
		h := aws.ToString(e.ReceiptHandle)
		if h == f.failHandle {
			out.Failed = append(out.Failed, types.BatchResultErrorEntry{Id: e.Id, Code: aws.String("ReceiptHandleIsInvalid"), Message: aws.String("bad")})
			continue
		}
		batch = append(batch, h)
		f.deleted = append(f.deleted, h)
	}
	f.deleteBatches = append(f.deleteBatches, batch)
	return out, nil
}

func (f *fakeClient) ChangeMessageVisibility(_ context.Context, in *awssqs.ChangeMessageVisibilityInput, _ ...func(*awssqs.Options)) (*awssqs.ChangeMessageVisibilityOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.visibility = append(f.visibility, types.ChangeMessageVisibilityBatchRequestEntry{ReceiptHandle: in.ReceiptHandle, VisibilityTimeout: in.VisibilityTimeout})
	return &awssqs.ChangeMessageVisibilityOutput{}, nil
}

func (f *fakeClient) ChangeMessageVisibilityBatch(_ context.Context, in *awssqs.ChangeMessageVisibilityBatchInput, _ ...func(*awssqs.Options)) (*awssqs.ChangeMessageVisibilityBatchOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.visibilityBatchN++
	f.visibility = append(f.visibility, in.Entries...)
	return &awssqs.ChangeMessageVisibilityBatchOutput{}, nil
}

func (f *fakeClient) SendMessage(_ context.Context, in *awssqs.SendMessageInput, _ ...func(*awssqs.Options)) (*awssqs.SendMessageOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.sent = append(f.sent, in)
	return &awssqs.SendMessageOutput{MessageId: aws.String("sent-1")}, nil
}

func TestReceiver_MapsMessages(t *testing.T) {
	client := &fakeClient{receive: func(in *awssqs.ReceiveMessageInput) (*awssqs.ReceiveMessageOutput, error) {
		if in.MaxNumberOfMessages != 10 || in.WaitTimeSeconds != 20 {
			t.Errorf("max=%d wait=%d, want clamped to 10 and 20", in.MaxNumberOfMessages, in.WaitTimeSeconds)
		}
		return &awssqs.ReceiveMessageOutput{Messages: []types.Message{{
			MessageId:     aws.String("m1"),
			Body:          aws.String(`{"id":1}`),
			ReceiptHandle: aws.String("rh1"),
			Attributes: map[string]string{
				"ApproximateReceiveCount": "3",
				"SentTimestamp":           "1700000000123",
			},
			MessageAttributes: map[string]types.MessageAttributeValue{
				"traceparent": {DataType: aws.String("String"), StringValue: aws.String("00-abc-def-01")},
				"blob":        {DataType: aws.String("Binary"), BinaryValue: []byte{1}},
			},
		}}}, nil
	}}

	msgs, err := NewReceiver(client, testQueueURL).Receive(context.Background(), 50, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if len(msgs) != 1 {
		t.Fatalf("got %d messages, want 1", len(msgs))
	}
	m := msgs[0]
	if m.ID != "m1" || string(m.Body) != `{"id":1}` || m.AckID != "rh1" {
		t.Errorf("unexpected message: %+v", m)
	}
	if m.ReceiveCount != 3 {
		t.Errorf("ReceiveCount = %d, want 3", m.ReceiveCount)
	}
	if !m.SentAt.Equal(time.UnixMilli(1700000000123)) {
		t.Errorf("SentAt = %v", m.SentAt)
	}
	if m.System != System || m.Queue != "orders" {
		t.Errorf("System=%q Queue=%q", m.System, m.Queue)
	}
	if len(m.Attributes) != 1 || m.Attributes["traceparent"] != "00-abc-def-01" {
		t.Errorf("Attributes = %v, want only the string attribute", m.Attributes)
	}
}

func TestReceiver_BadReceiveCountIsIgnored(t *testing.T) {
	client := &fakeClient{receive: func(*awssqs.ReceiveMessageInput) (*awssqs.ReceiveMessageOutput, error) {
		return &awssqs.ReceiveMessageOutput{Messages: []types.Message{{
			MessageId:  aws.String("m1"),
			Attributes: map[string]string{"ApproximateReceiveCount": "3x"},
		}}}, nil
	}}
	msgs, err := NewReceiver(client, testQueueURL).Receive(context.Background(), 1, 0)
	if err != nil {
		t.Fatal(err)
	}
	if msgs[0].ReceiveCount != 0 {
		t.Fatalf("ReceiveCount = %d, want 0 for unparsable input", msgs[0].ReceiveCount)
	}
}

func TestReceiver_Error(t *testing.T) {
	want := errors.New("throttled")
	client := &fakeClient{receive: func(*awssqs.ReceiveMessageInput) (*awssqs.ReceiveMessageOutput, error) { return nil, want }}
	if _, err := NewReceiver(client, testQueueURL).Receive(context.Background(), 1, 0); !errors.Is(err, want) {
		t.Fatalf("err = %v, want %v", err, want)
	}
}

func TestAcknowledger_Unbatched(t *testing.T) {
	client := &fakeClient{}
	a := NewAcknowledger(client, testQueueURL)
	ctx := context.Background()

	if err := a.Ack(ctx, &queuer.Message{AckID: "rh1"}); err != nil {
		t.Fatal(err)
	}
	if err := a.Nack(ctx, &queuer.Message{AckID: "rh2"}); err != nil {
		t.Fatal(err)
	}
	if err := a.ChangeVisibility(ctx, &queuer.Message{AckID: "rh3"}, 48*time.Hour); err != nil {
		t.Fatal(err)
	}

	if len(client.deleted) != 1 || client.deleted[0] != "rh1" || len(client.deleteBatches) != 0 {
		t.Errorf("deleted=%v batches=%v", client.deleted, client.deleteBatches)
	}
	if len(client.visibility) != 2 || client.visibilityBatchN != 0 {
		t.Fatalf("visibility = %+v", client.visibility)
	}
	if client.visibility[0].VisibilityTimeout != 0 {
		t.Errorf("nack timeout = %d, want 0", client.visibility[0].VisibilityTimeout)
	}
	if client.visibility[1].VisibilityTimeout != 43200 {
		t.Errorf("visibility timeout = %d, want clamped to 43200", client.visibility[1].VisibilityTimeout)
	}
}

func TestAcknowledger_Batched(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		client := &fakeClient{failHandle: "rh-bad"}
		a := NewAcknowledger(client, testQueueURL, WithBatching(100*time.Millisecond))

		handles := []string{"rh1", "rh2", "rh-bad", "rh4"}
		errs := make([]error, len(handles))
		var wg sync.WaitGroup
		for i, h := range handles {
			wg.Add(1)
			go func() {
				defer wg.Done()
				errs[i] = a.Ack(context.Background(), &queuer.Message{AckID: h})
			}()
		}
		wg.Wait()

		if len(client.deleteBatches) != 1 || len(client.deleteBatches[0]) != 3 {
			t.Fatalf("delete batches = %v, want one batch with 3 successes", client.deleteBatches)
		}
		for i, err := range errs {
			var be *BatchEntryError
			if handles[i] == "rh-bad" {
				if !errors.As(err, &be) || be.Code != "ReceiptHandleIsInvalid" {
					t.Errorf("Ack(%s) = %v, want BatchEntryError", handles[i], err)
				}
			} else if err != nil {
				t.Errorf("Ack(%s) = %v", handles[i], err)
			}
		}
	})
}

func TestPublisher(t *testing.T) {
	client := &fakeClient{}
	p := NewPublisher(client, testQueueURL+".fifo",
		WithMessageGroupID(func(*queuer.Message) string { return "g1" }),
		WithDeduplicationID(func(m *queuer.Message) string { return m.Attributes["key"] }),
	)
	id, err := p.Publish(context.Background(), &queuer.Message{Body: []byte("hi"), Attributes: map[string]string{"key": "k1"}})
	if err != nil || id != "sent-1" {
		t.Fatalf("Publish = %q, %v", id, err)
	}
	in := client.sent[0]
	if aws.ToString(in.MessageBody) != "hi" || aws.ToString(in.MessageGroupId) != "g1" || aws.ToString(in.MessageDeduplicationId) != "k1" {
		t.Errorf("unexpected input: %+v", in)
	}
	if v := in.MessageAttributes["key"]; aws.ToString(v.DataType) != "String" || aws.ToString(v.StringValue) != "k1" {
		t.Errorf("attribute = %+v", v)
	}
	if p.MessagingSystem() != "aws_sqs" || p.DestinationName() != "orders.fifo" {
		t.Errorf("system=%q destination=%q", p.MessagingSystem(), p.DestinationName())
	}
}

func TestNewConsumer(t *testing.T) {
	c := NewConsumer(&fakeClient{}, testQueueURL, queuer.HandlerFunc(func(context.Context, *queuer.Message) error { return nil }))
	if c == nil {
		t.Fatal("NewConsumer returned nil")
	}
}
