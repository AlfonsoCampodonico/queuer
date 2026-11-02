package queuer

import "context"

// Publisher sends messages to a queue or topic. Only msg.Body and
// msg.Attributes are used; the returned string is the provider-assigned
// message ID.
type Publisher interface {
	Publish(ctx context.Context, msg *Message) (string, error)
}

// PublisherFunc is an adapter to allow use of ordinary functions as Publishers.
type PublisherFunc func(ctx context.Context, msg *Message) (string, error)

// Publish calls f(ctx, msg).
func (f PublisherFunc) Publish(ctx context.Context, msg *Message) (string, error) {
	return f(ctx, msg)
}
