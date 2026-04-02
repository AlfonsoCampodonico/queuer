package queuer

import "context"

// Message represents a message received from a queue.
type Message struct {
	ID            string
	Body          string
	Attributes    map[string]string
	ReceiptHandle string
	ReceiveCount  int
}

// Handler processes a single message. Return nil to acknowledge
// the message, or an error to trigger the ErrorResolver.
type Handler interface {
	Handle(ctx context.Context, msg *Message) error
}

// HandlerFunc is an adapter to allow use of ordinary functions as Handlers.
type HandlerFunc func(ctx context.Context, msg *Message) error

func (f HandlerFunc) Handle(ctx context.Context, msg *Message) error {
	return f(ctx, msg)
}
