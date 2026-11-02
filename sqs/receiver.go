// Package sqs connects queuer to Amazon SQS.
package sqs

import (
	"context"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"

	"github.com/AlfonsoCampodonico/queuer"
)

// System is the OpenTelemetry messaging.system value for SQS.
const System = "aws_sqs"

// SQS limits.
const (
	MaxReceiveMessages   = 10
	MaxWaitTime          = 20 * time.Second
	MaxVisibilityTimeout = 12 * time.Hour
)

// Client is the subset of the SQS client used by this package.
// *github.com/aws/aws-sdk-go-v2/service/sqs.Client satisfies it.
type Client interface {
	ReceiveMessage(ctx context.Context, params *awssqs.ReceiveMessageInput, optFns ...func(*awssqs.Options)) (*awssqs.ReceiveMessageOutput, error)
	DeleteMessage(ctx context.Context, params *awssqs.DeleteMessageInput, optFns ...func(*awssqs.Options)) (*awssqs.DeleteMessageOutput, error)
	DeleteMessageBatch(ctx context.Context, params *awssqs.DeleteMessageBatchInput, optFns ...func(*awssqs.Options)) (*awssqs.DeleteMessageBatchOutput, error)
	ChangeMessageVisibility(ctx context.Context, params *awssqs.ChangeMessageVisibilityInput, optFns ...func(*awssqs.Options)) (*awssqs.ChangeMessageVisibilityOutput, error)
	ChangeMessageVisibilityBatch(ctx context.Context, params *awssqs.ChangeMessageVisibilityBatchInput, optFns ...func(*awssqs.Options)) (*awssqs.ChangeMessageVisibilityBatchOutput, error)
}

// Receiver polls an SQS queue for messages.
type Receiver struct {
	client    Client
	queueURL  string
	queueName string
}

// NewReceiver returns a Receiver for the queue at queueURL.
func NewReceiver(client Client, queueURL string) *Receiver {
	return &Receiver{client: client, queueURL: queueURL, queueName: QueueName(queueURL)}
}

// Receive long-polls the queue. maxMessages is clamped to 1..10 and
// waitTime to 0..20s.
func (r *Receiver) Receive(ctx context.Context, maxMessages int, waitTime time.Duration) ([]*queuer.Message, error) {
	out, err := r.client.ReceiveMessage(ctx, &awssqs.ReceiveMessageInput{
		QueueUrl:                    aws.String(r.queueURL),
		MaxNumberOfMessages:         int32(clamp(maxMessages, 1, MaxReceiveMessages)),
		WaitTimeSeconds:             int32(clamp(waitTime, 0, MaxWaitTime) / time.Second),
		MessageAttributeNames:       []string{"All"},
		MessageSystemAttributeNames: []types.MessageSystemAttributeName{types.MessageSystemAttributeNameAll},
	})
	if err != nil {
		return nil, err
	}

	msgs := make([]*queuer.Message, 0, len(out.Messages))
	for _, m := range out.Messages {
		msg := &queuer.Message{
			ID:         aws.ToString(m.MessageId),
			Body:       []byte(aws.ToString(m.Body)),
			AckID:      aws.ToString(m.ReceiptHandle),
			Attributes: make(map[string]string, len(m.MessageAttributes)),
			System:     System,
			Queue:      r.queueName,
		}
		for k, v := range m.MessageAttributes {
			if v.StringValue != nil {
				msg.Attributes[k] = *v.StringValue
			}
		}
		if n, err := strconv.Atoi(m.Attributes[string(types.MessageSystemAttributeNameApproximateReceiveCount)]); err == nil {
			msg.ReceiveCount = n
		}
		if ms, err := strconv.ParseInt(m.Attributes[string(types.MessageSystemAttributeNameSentTimestamp)], 10, 64); err == nil {
			msg.SentAt = time.UnixMilli(ms)
		}
		msgs = append(msgs, msg)
	}
	return msgs, nil
}

// QueueName returns the queue name from an SQS queue URL
// (https://sqs.<region>.amazonaws.com/<account>/<name>).
func QueueName(queueURL string) string {
	return queueURL[strings.LastIndex(queueURL, "/")+1:]
}

func clamp[T int | time.Duration](v, lo, hi T) T {
	return max(lo, min(v, hi))
}
