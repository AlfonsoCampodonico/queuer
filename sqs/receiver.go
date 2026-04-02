package sqs

import (
	"context"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"

	"github.com/AlfonsoCampodonico/queuer"
)

// Client is the subset of the SQS client used by this package.
type Client interface {
	ReceiveMessage(ctx context.Context, params *awssqs.ReceiveMessageInput, optFns ...func(*awssqs.Options)) (*awssqs.ReceiveMessageOutput, error)
	DeleteMessage(ctx context.Context, params *awssqs.DeleteMessageInput, optFns ...func(*awssqs.Options)) (*awssqs.DeleteMessageOutput, error)
	ChangeMessageVisibility(ctx context.Context, params *awssqs.ChangeMessageVisibilityInput, optFns ...func(*awssqs.Options)) (*awssqs.ChangeMessageVisibilityOutput, error)
}

// Receiver polls an SQS queue for messages.
type Receiver struct {
	client   Client
	queueURL string
}

func NewReceiver(client Client, queueURL string) *Receiver {
	return &Receiver{client: client, queueURL: queueURL}
}

func (r *Receiver) Receive(ctx context.Context, maxMessages int, waitTime time.Duration) ([]*queuer.Message, error) {
	out, err := r.client.ReceiveMessage(ctx, &awssqs.ReceiveMessageInput{
		QueueUrl:              aws.String(r.queueURL),
		MaxNumberOfMessages:   int32(maxMessages),
		WaitTimeSeconds:       int32(waitTime.Seconds()),
		MessageAttributeNames: []string{"All"},
		AttributeNames:        []types.QueueAttributeName{types.QueueAttributeNameAll},
	})
	if err != nil {
		return nil, err
	}

	msgs := make([]*queuer.Message, 0, len(out.Messages))
	for _, m := range out.Messages {
		msg := &queuer.Message{
			ID:            aws.ToString(m.MessageId),
			Body:          aws.ToString(m.Body),
			ReceiptHandle: aws.ToString(m.ReceiptHandle),
			Attributes:    make(map[string]string),
		}
		for k, v := range m.MessageAttributes {
			if v.StringValue != nil {
				msg.Attributes[k] = *v.StringValue
			}
		}
		if countStr, ok := m.Attributes["ApproximateReceiveCount"]; ok {
			var count int
			for _, ch := range countStr {
				count = count*10 + int(ch-'0')
			}
			msg.ReceiveCount = count
		}
		msgs = append(msgs, msg)
	}
	return msgs, nil
}
