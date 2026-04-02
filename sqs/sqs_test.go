package sqs

import (
	"context"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"

	"github.com/AlfonsoCampodonico/queuer"
)

// mockClient implements the Client interface for testing.
type mockClient struct {
	receiveMessageFunc          func(ctx context.Context, params *awssqs.ReceiveMessageInput, optFns ...func(*awssqs.Options)) (*awssqs.ReceiveMessageOutput, error)
	deleteMessageFunc           func(ctx context.Context, params *awssqs.DeleteMessageInput, optFns ...func(*awssqs.Options)) (*awssqs.DeleteMessageOutput, error)
	changeMessageVisibilityFunc func(ctx context.Context, params *awssqs.ChangeMessageVisibilityInput, optFns ...func(*awssqs.Options)) (*awssqs.ChangeMessageVisibilityOutput, error)
}

func (m *mockClient) ReceiveMessage(ctx context.Context, params *awssqs.ReceiveMessageInput, optFns ...func(*awssqs.Options)) (*awssqs.ReceiveMessageOutput, error) {
	return m.receiveMessageFunc(ctx, params, optFns...)
}

func (m *mockClient) DeleteMessage(ctx context.Context, params *awssqs.DeleteMessageInput, optFns ...func(*awssqs.Options)) (*awssqs.DeleteMessageOutput, error) {
	return m.deleteMessageFunc(ctx, params, optFns...)
}

func (m *mockClient) ChangeMessageVisibility(ctx context.Context, params *awssqs.ChangeMessageVisibilityInput, optFns ...func(*awssqs.Options)) (*awssqs.ChangeMessageVisibilityOutput, error) {
	return m.changeMessageVisibilityFunc(ctx, params, optFns...)
}

func TestReceiver_Receive(t *testing.T) {
	client := &mockClient{
		receiveMessageFunc: func(_ context.Context, params *awssqs.ReceiveMessageInput, _ ...func(*awssqs.Options)) (*awssqs.ReceiveMessageOutput, error) {
			if got := aws.ToString(params.QueueUrl); got != "https://sqs.us-east-1.amazonaws.com/123456789/test-queue" {
				t.Errorf("unexpected queue URL: %s", got)
			}
			if params.MaxNumberOfMessages != 5 {
				t.Errorf("expected MaxNumberOfMessages=5, got %d", params.MaxNumberOfMessages)
			}
			if params.WaitTimeSeconds != 10 {
				t.Errorf("expected WaitTimeSeconds=10, got %d", params.WaitTimeSeconds)
			}
			return &awssqs.ReceiveMessageOutput{
				Messages: []types.Message{
					{
						MessageId:     aws.String("msg-001"),
						Body:          aws.String(`{"event":"test"}`),
						ReceiptHandle: aws.String("receipt-abc"),
						MessageAttributes: map[string]types.MessageAttributeValue{
							"traceID": {StringValue: aws.String("trace-123")},
						},
						Attributes: map[string]string{
							string(types.MessageSystemAttributeNameApproximateReceiveCount): "3",
						},
					},
				},
			}, nil
		},
	}

	r := NewReceiver(client, "https://sqs.us-east-1.amazonaws.com/123456789/test-queue")
	msgs, err := r.Receive(context.Background(), 5, 10*time.Second)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(msgs) != 1 {
		t.Fatalf("expected 1 message, got %d", len(msgs))
	}

	msg := msgs[0]
	if msg.ID != "msg-001" {
		t.Errorf("expected ID=msg-001, got %s", msg.ID)
	}
	if msg.Body != `{"event":"test"}` {
		t.Errorf("unexpected body: %s", msg.Body)
	}
	if msg.ReceiptHandle != "receipt-abc" {
		t.Errorf("unexpected receipt handle: %s", msg.ReceiptHandle)
	}
	if msg.Attributes["traceID"] != "trace-123" {
		t.Errorf("unexpected attribute traceID: %s", msg.Attributes["traceID"])
	}
	if msg.ReceiveCount != 3 {
		t.Errorf("expected ReceiveCount=3, got %d", msg.ReceiveCount)
	}
}

func TestReceiver_Receive_Empty(t *testing.T) {
	client := &mockClient{
		receiveMessageFunc: func(_ context.Context, _ *awssqs.ReceiveMessageInput, _ ...func(*awssqs.Options)) (*awssqs.ReceiveMessageOutput, error) {
			return &awssqs.ReceiveMessageOutput{
				Messages: []types.Message{},
			}, nil
		},
	}

	r := NewReceiver(client, "https://sqs.us-east-1.amazonaws.com/123456789/test-queue")
	msgs, err := r.Receive(context.Background(), 10, 20*time.Second)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(msgs) != 0 {
		t.Errorf("expected 0 messages, got %d", len(msgs))
	}
}

func TestAcknowledger_Ack(t *testing.T) {
	var called bool
	client := &mockClient{
		deleteMessageFunc: func(_ context.Context, params *awssqs.DeleteMessageInput, _ ...func(*awssqs.Options)) (*awssqs.DeleteMessageOutput, error) {
			called = true
			if got := aws.ToString(params.QueueUrl); got != "https://sqs.us-east-1.amazonaws.com/123456789/test-queue" {
				t.Errorf("unexpected queue URL: %s", got)
			}
			if got := aws.ToString(params.ReceiptHandle); got != "receipt-xyz" {
				t.Errorf("unexpected receipt handle: %s", got)
			}
			return &awssqs.DeleteMessageOutput{}, nil
		},
	}

	a := NewAcknowledger(client, "https://sqs.us-east-1.amazonaws.com/123456789/test-queue")
	msg := &queuer.Message{ReceiptHandle: "receipt-xyz"}
	err := a.Ack(context.Background(), msg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !called {
		t.Error("DeleteMessage was not called")
	}
}

func TestAcknowledger_Nack(t *testing.T) {
	var called bool
	client := &mockClient{
		changeMessageVisibilityFunc: func(_ context.Context, params *awssqs.ChangeMessageVisibilityInput, _ ...func(*awssqs.Options)) (*awssqs.ChangeMessageVisibilityOutput, error) {
			called = true
			if got := aws.ToString(params.QueueUrl); got != "https://sqs.us-east-1.amazonaws.com/123456789/test-queue" {
				t.Errorf("unexpected queue URL: %s", got)
			}
			if got := aws.ToString(params.ReceiptHandle); got != "receipt-xyz" {
				t.Errorf("unexpected receipt handle: %s", got)
			}
			if params.VisibilityTimeout != 0 {
				t.Errorf("expected VisibilityTimeout=0, got %d", params.VisibilityTimeout)
			}
			return &awssqs.ChangeMessageVisibilityOutput{}, nil
		},
	}

	a := NewAcknowledger(client, "https://sqs.us-east-1.amazonaws.com/123456789/test-queue")
	msg := &queuer.Message{ReceiptHandle: "receipt-xyz"}
	err := a.Nack(context.Background(), msg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !called {
		t.Error("ChangeMessageVisibility was not called")
	}
}

func TestAcknowledger_ChangeVisibility(t *testing.T) {
	var called bool
	client := &mockClient{
		changeMessageVisibilityFunc: func(_ context.Context, params *awssqs.ChangeMessageVisibilityInput, _ ...func(*awssqs.Options)) (*awssqs.ChangeMessageVisibilityOutput, error) {
			called = true
			if got := aws.ToString(params.ReceiptHandle); got != "receipt-xyz" {
				t.Errorf("unexpected receipt handle: %s", got)
			}
			if params.VisibilityTimeout != 60 {
				t.Errorf("expected VisibilityTimeout=60, got %d", params.VisibilityTimeout)
			}
			return &awssqs.ChangeMessageVisibilityOutput{}, nil
		},
	}

	a := NewAcknowledger(client, "https://sqs.us-east-1.amazonaws.com/123456789/test-queue")
	msg := &queuer.Message{ReceiptHandle: "receipt-xyz"}
	err := a.ChangeVisibility(context.Background(), msg, 60*time.Second)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !called {
		t.Error("ChangeMessageVisibility was not called")
	}
}
