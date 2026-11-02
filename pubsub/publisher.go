package pubsub

import (
	"context"
	"errors"

	"cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"github.com/googleapis/gax-go/v2"

	"github.com/AlfonsoCampodonico/queuer"
)

// PublishClient is the subset of the Pub/Sub publisher API used by Publisher.
// *cloud.google.com/go/pubsub/v2/apiv1.TopicAdminClient satisfies it.
type PublishClient interface {
	Publish(ctx context.Context, req *pubsubpb.PublishRequest, opts ...gax.CallOption) (*pubsubpb.PublishResponse, error)
}

// Publisher sends messages to a Pub/Sub topic, one Publish request per
// message. For high-throughput publishing with client-side batching, use the
// high-level cloud.google.com/go/pubsub/v2 Publisher instead.
type Publisher struct {
	client      PublishClient
	topic       string
	shortName   string
	orderingKey func(*queuer.Message) string
}

// PublisherOption configures a Publisher.
type PublisherOption func(*Publisher)

// WithOrderingKey sets each message's ordering key. The subscription must
// have message ordering enabled for the key to have an effect.
func WithOrderingKey(fn func(*queuer.Message) string) PublisherOption {
	return func(p *Publisher) { p.orderingKey = fn }
}

// NewPublisher returns a Publisher for topic, the full resource name
// "projects/<project>/topics/<id>" (see TopicName).
func NewPublisher(client PublishClient, topic string, opts ...PublisherOption) *Publisher {
	p := &Publisher{client: client, topic: topic, shortName: shortName(topic)}
	for _, opt := range opts {
		opt(p)
	}
	return p
}

var errNoMessageID = errors.New("pubsub: publish response has no message ID")

// Publish sends msg and returns the Pub/Sub message ID.
func (p *Publisher) Publish(ctx context.Context, msg *queuer.Message) (string, error) {
	pm := &pubsubpb.PubsubMessage{Data: msg.Body, Attributes: msg.Attributes}
	if p.orderingKey != nil {
		pm.OrderingKey = p.orderingKey(msg)
	}
	resp, err := p.client.Publish(ctx, &pubsubpb.PublishRequest{
		Topic:    p.topic,
		Messages: []*pubsubpb.PubsubMessage{pm},
	})
	if err != nil {
		return "", err
	}
	if len(resp.GetMessageIds()) == 0 {
		return "", errNoMessageID
	}
	return resp.GetMessageIds()[0], nil
}

// MessagingSystem returns "gcp_pubsub". The otel package uses it to
// describe producer spans.
func (p *Publisher) MessagingSystem() string { return System }

// DestinationName returns the topic ID. The otel package uses it to
// describe producer spans.
func (p *Publisher) DestinationName() string { return p.shortName }
