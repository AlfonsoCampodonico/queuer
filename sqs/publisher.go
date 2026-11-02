package sqs

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/aws"
	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"

	"github.com/AlfonsoCampodonico/queuer"
)

// SendClient is the subset of the SQS client used by Publisher.
// *github.com/aws/aws-sdk-go-v2/service/sqs.Client satisfies it.
type SendClient interface {
	SendMessage(ctx context.Context, params *awssqs.SendMessageInput, optFns ...func(*awssqs.Options)) (*awssqs.SendMessageOutput, error)
}

// Publisher sends messages to an SQS queue. Message attributes are sent as
// String message attributes (SQS allows at most 10 per message).
type Publisher struct {
	client    SendClient
	queueURL  string
	queueName string
	groupID   func(*queuer.Message) string
	dedupID   func(*queuer.Message) string
}

// PublisherOption configures a Publisher.
type PublisherOption func(*Publisher)

// WithMessageGroupID sets the MessageGroupId for each message. It is
// required for FIFO queues.
func WithMessageGroupID(fn func(*queuer.Message) string) PublisherOption {
	return func(p *Publisher) { p.groupID = fn }
}

// WithDeduplicationID sets the MessageDeduplicationId for each message, for
// FIFO queues without content-based deduplication.
func WithDeduplicationID(fn func(*queuer.Message) string) PublisherOption {
	return func(p *Publisher) { p.dedupID = fn }
}

// NewPublisher returns a Publisher for the queue at queueURL.
func NewPublisher(client SendClient, queueURL string, opts ...PublisherOption) *Publisher {
	p := &Publisher{client: client, queueURL: queueURL, queueName: QueueName(queueURL)}
	for _, opt := range opts {
		opt(p)
	}
	return p
}

// Publish sends msg and returns the SQS message ID.
func (p *Publisher) Publish(ctx context.Context, msg *queuer.Message) (string, error) {
	in := &awssqs.SendMessageInput{
		QueueUrl:    aws.String(p.queueURL),
		MessageBody: aws.String(string(msg.Body)),
	}
	if len(msg.Attributes) > 0 {
		in.MessageAttributes = make(map[string]types.MessageAttributeValue, len(msg.Attributes))
		for k, v := range msg.Attributes {
			in.MessageAttributes[k] = types.MessageAttributeValue{DataType: aws.String("String"), StringValue: aws.String(v)}
		}
	}
	if p.groupID != nil {
		in.MessageGroupId = aws.String(p.groupID(msg))
	}
	if p.dedupID != nil {
		in.MessageDeduplicationId = aws.String(p.dedupID(msg))
	}
	out, err := p.client.SendMessage(ctx, in)
	if err != nil {
		return "", err
	}
	return aws.ToString(out.MessageId), nil
}

// MessagingSystem returns "aws_sqs". The otel package uses it to describe
// producer spans.
func (p *Publisher) MessagingSystem() string { return System }

// DestinationName returns the queue name. The otel package uses it to
// describe producer spans.
func (p *Publisher) DestinationName() string { return p.queueName }
