package queuer

import (
	"context"
	"time"
)

// Message is a single message received from, or published to, a queue.
//
// Adapters fill in the provider-specific fields when receiving. When
// publishing, only Body and Attributes are used.
type Message struct {
	// ID is the provider-assigned message ID.
	ID string
	// Body is the message payload.
	Body []byte
	// Attributes holds string key/value metadata (SQS message attributes,
	// Pub/Sub attributes). Trace context is propagated through it.
	Attributes map[string]string
	// AckID is the provider token used to settle the message: the SQS
	// receipt handle or the Pub/Sub ack ID.
	AckID string
	// ReceiveCount is how many times the message has been delivered,
	// including this delivery. It is 0 when the provider does not report it
	// (Pub/Sub only reports it when the subscription has a dead-letter policy).
	ReceiveCount int
	// SentAt is when the message was originally published, if known.
	SentAt time.Time
	// System identifies the messaging system, using OpenTelemetry
	// semantic-convention values such as "aws_sqs" or "gcp_pubsub".
	System string
	// Queue is the short name of the queue or subscription the message
	// was received from.
	Queue string
}

// Handler processes a single message. Return nil to acknowledge
// the message, or an error to trigger the ErrorResolver.
type Handler interface {
	Handle(ctx context.Context, msg *Message) error
}

// HandlerFunc is an adapter to allow use of ordinary functions as Handlers.
type HandlerFunc func(ctx context.Context, msg *Message) error

// Handle calls f(ctx, msg).
func (f HandlerFunc) Handle(ctx context.Context, msg *Message) error {
	return f(ctx, msg)
}

// Middleware wraps a Handler with additional behaviour such as logging,
// idempotency checks or payload decoding.
type Middleware func(Handler) Handler

// Chain wraps h with the given middleware. The first middleware is the
// outermost one, so Chain(h, a, b) runs a, then b, then h.
func Chain(h Handler, mws ...Middleware) Handler {
	for i := len(mws) - 1; i >= 0; i-- {
		h = mws[i](h)
	}
	return h
}
