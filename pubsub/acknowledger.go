package pubsub

import (
	"context"
	"sync"
	"time"

	"cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"

	"github.com/AlfonsoCampodonico/queuer"
	"github.com/AlfonsoCampodonico/queuer/internal/batch"
)

// maxAckIDsPerRequest keeps Acknowledge and ModifyAckDeadline requests well
// under Pub/Sub's request size limit.
const maxAckIDsPerRequest = 1000

// Acknowledger settles Pub/Sub messages: Ack acknowledges the message, Nack
// sets its ack deadline to 0 for immediate redelivery, and ChangeVisibility
// sets its ack deadline (which also extends its lease).
type Acknowledger struct {
	client       Client
	subscription string
	batchWait    time.Duration

	acks *batch.Batcher[string]

	mu      sync.Mutex
	modacks map[int32]*batch.Batcher[string]
}

// AckOption configures an Acknowledger.
type AckOption func(*Acknowledger)

// WithBatching groups ack IDs into shared Acknowledge and ModifyAckDeadline
// requests. A batch is sent when it holds 1000 IDs or maxWait after its first
// ID. Each settle call waits for its batch.
//
// Batching cuts API calls but adds up to maxWait of latency per settle call,
// so it pays off when many workers settle messages concurrently. A
// maxWait <= 0 disables batching. Default: disabled.
func WithBatching(maxWait time.Duration) AckOption {
	return func(a *Acknowledger) {
		a.batchWait = maxWait
	}
}

// NewAcknowledger returns an Acknowledger for subscription, the full
// resource name "projects/<project>/subscriptions/<id>".
func NewAcknowledger(client Client, subscription string, opts ...AckOption) *Acknowledger {
	a := &Acknowledger{client: client, subscription: subscription, modacks: map[int32]*batch.Batcher[string]{}}
	for _, opt := range opts {
		opt(a)
	}
	a.acks = batch.New(a.batchSize(), a.batchWait, func(ctx context.Context, ids []string) []error {
		return batch.Fill(len(ids), a.client.Acknowledge(ctx, &pubsubpb.AcknowledgeRequest{
			Subscription: a.subscription,
			AckIds:       ids,
		}))
	})
	return a
}

func (a *Acknowledger) batchSize() int {
	if a.batchWait > 0 {
		return maxAckIDsPerRequest
	}
	return 1
}

// Ack acknowledges the message.
func (a *Acknowledger) Ack(ctx context.Context, msg *queuer.Message) error {
	return a.acks.Do(ctx, msg.AckID)
}

// Nack sets the message's ack deadline to 0 so it is redelivered immediately.
func (a *Acknowledger) Nack(ctx context.Context, msg *queuer.Message) error {
	return a.modack(0).Do(ctx, msg.AckID)
}

// ChangeVisibility sets the message's ack deadline to timeout, clamped to
// 0..600s and rounded down to whole seconds.
func (a *Acknowledger) ChangeVisibility(ctx context.Context, msg *queuer.Message, timeout time.Duration) error {
	secs := int32(max(0, min(timeout, MaxAckDeadline)) / time.Second)
	return a.modack(secs).Do(ctx, msg.AckID)
}

// modack returns the batcher for one deadline: ModifyAckDeadline takes a
// single deadline for all ack IDs in a request.
func (a *Acknowledger) modack(seconds int32) *batch.Batcher[string] {
	a.mu.Lock()
	defer a.mu.Unlock()
	b, ok := a.modacks[seconds]
	if !ok {
		b = batch.New(a.batchSize(), a.batchWait, func(ctx context.Context, ids []string) []error {
			return batch.Fill(len(ids), a.client.ModifyAckDeadline(ctx, &pubsubpb.ModifyAckDeadlineRequest{
				Subscription:       a.subscription,
				AckIds:             ids,
				AckDeadlineSeconds: seconds,
			}))
		})
		a.modacks[seconds] = b
	}
	return b
}
