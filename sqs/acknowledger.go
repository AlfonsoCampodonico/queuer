package sqs

import (
	"context"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"

	"github.com/AlfonsoCampodonico/queuer"
)

// Acknowledger manages SQS message lifecycle.
type Acknowledger struct {
	client   Client
	queueURL string
}

func NewAcknowledger(client Client, queueURL string) *Acknowledger {
	return &Acknowledger{client: client, queueURL: queueURL}
}

func (a *Acknowledger) Ack(ctx context.Context, msg *queuer.Message) error {
	_, err := a.client.DeleteMessage(ctx, &awssqs.DeleteMessageInput{
		QueueUrl:      aws.String(a.queueURL),
		ReceiptHandle: aws.String(msg.ReceiptHandle),
	})
	return err
}

func (a *Acknowledger) Nack(ctx context.Context, msg *queuer.Message) error {
	_, err := a.client.ChangeMessageVisibility(ctx, &awssqs.ChangeMessageVisibilityInput{
		QueueUrl:          aws.String(a.queueURL),
		ReceiptHandle:     aws.String(msg.ReceiptHandle),
		VisibilityTimeout: 0,
	})
	return err
}

func (a *Acknowledger) ChangeVisibility(ctx context.Context, msg *queuer.Message, timeout time.Duration) error {
	_, err := a.client.ChangeMessageVisibility(ctx, &awssqs.ChangeMessageVisibilityInput{
		QueueUrl:          aws.String(a.queueURL),
		ReceiptHandle:     aws.String(msg.ReceiptHandle),
		VisibilityTimeout: int32(timeout.Seconds()),
	})
	return err
}
