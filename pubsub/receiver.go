// Package pubsub connects queuer to Google Cloud Pub/Sub.
//
// It uses unary Pull against a subscription, so the queuer Consumer controls
// concurrency, flow control and lease extension the same way it does for SQS.
// Use the low-level clients from cloud.google.com/go/pubsub/v2/apiv1:
// *SubscriptionAdminClient satisfies Client and *TopicAdminClient satisfies
// PublishClient. Both honour the PUBSUB_EMULATOR_HOST environment variable.
package pubsub

import (
	"context"
	"errors"
	"maps"
	"strings"
	"time"

	"cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"github.com/googleapis/gax-go/v2"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/AlfonsoCampodonico/queuer"
)

// System is the OpenTelemetry messaging.system value for Pub/Sub.
const System = "gcp_pubsub"

// Pub/Sub limits.
const (
	MaxPullMessages = 1000
	MaxAckDeadline  = 600 * time.Second
)

// DefaultLeaseExtension is the lease extension NewConsumer enables. Pub/Sub
// subscriptions default to a 10s ack deadline, which most handlers exceed.
const DefaultLeaseExtension = 60 * time.Second

// Client is the subset of the Pub/Sub subscriber API used by this package.
// *cloud.google.com/go/pubsub/v2/apiv1.SubscriptionAdminClient satisfies it.
type Client interface {
	Pull(ctx context.Context, req *pubsubpb.PullRequest, opts ...gax.CallOption) (*pubsubpb.PullResponse, error)
	Acknowledge(ctx context.Context, req *pubsubpb.AcknowledgeRequest, opts ...gax.CallOption) error
	ModifyAckDeadline(ctx context.Context, req *pubsubpb.ModifyAckDeadlineRequest, opts ...gax.CallOption) error
}

// Receiver pulls messages from a Pub/Sub subscription.
type Receiver struct {
	client       Client
	subscription string
	shortName    string
}

// NewReceiver returns a Receiver for subscription, the full resource name
// "projects/<project>/subscriptions/<id>" (see SubscriptionName).
func NewReceiver(client Client, subscription string) *Receiver {
	return &Receiver{client: client, subscription: subscription, shortName: shortName(subscription)}
}

// Receive pulls up to maxMessages (clamped to 1..1000). The pull is given
// waitTime to return; if no messages arrive in that time Receive returns an
// empty slice and no error.
func (r *Receiver) Receive(ctx context.Context, maxMessages int, waitTime time.Duration) ([]*queuer.Message, error) {
	pullCtx := ctx
	if waitTime > 0 {
		var cancel context.CancelFunc
		pullCtx, cancel = context.WithTimeout(ctx, waitTime)
		defer cancel()
	}

	resp, err := r.client.Pull(pullCtx, &pubsubpb.PullRequest{
		Subscription: r.subscription,
		MaxMessages:  int32(max(1, min(maxMessages, MaxPullMessages))),
	})
	if err != nil {
		if ctx.Err() == nil && isDeadline(err) {
			return nil, nil
		}
		return nil, err
	}

	msgs := make([]*queuer.Message, 0, len(resp.GetReceivedMessages()))
	for _, rm := range resp.GetReceivedMessages() {
		pm := rm.GetMessage()
		msg := &queuer.Message{
			ID:           pm.GetMessageId(),
			Body:         pm.GetData(),
			Attributes:   maps.Clone(pm.GetAttributes()),
			AckID:        rm.GetAckId(),
			ReceiveCount: int(rm.GetDeliveryAttempt()),
			System:       System,
			Queue:        r.shortName,
		}
		if msg.Attributes == nil {
			msg.Attributes = map[string]string{}
		}
		if ts := pm.GetPublishTime(); ts != nil {
			msg.SentAt = ts.AsTime()
		}
		msgs = append(msgs, msg)
	}
	return msgs, nil
}

func isDeadline(err error) bool {
	return errors.Is(err, context.DeadlineExceeded) || status.Code(err) == codes.DeadlineExceeded
}

// SubscriptionName returns the full resource name of a subscription.
func SubscriptionName(project, id string) string {
	return "projects/" + project + "/subscriptions/" + id
}

// TopicName returns the full resource name of a topic.
func TopicName(project, id string) string {
	return "projects/" + project + "/topics/" + id
}

func shortName(resource string) string {
	return resource[strings.LastIndex(resource, "/")+1:]
}
