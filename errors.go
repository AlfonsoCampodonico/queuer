package queuer

import "context"

// ErrorAction tells the consumer what to do after a handler error.
type ErrorAction int

const (
	// Retry re-queues the message with a backoff (changes visibility timeout).
	Retry ErrorAction = iota
	// Nack sets visibility timeout to 0 for immediate retry.
	Nack
	// Skip deletes the message despite the error.
	Skip
	// DeadLetter leaves the message in the queue for SQS redrive policy to handle.
	DeadLetter
)

func (a ErrorAction) String() string {
	switch a {
	case Retry:
		return "retry"
	case Nack:
		return "nack"
	case Skip:
		return "skip"
	case DeadLetter:
		return "dead_letter"
	default:
		return "unknown"
	}
}

// ErrorResolver decides what action to take when a handler returns an error.
type ErrorResolver interface {
	Resolve(ctx context.Context, msg *Message, err error) (ErrorAction, error)
}

// DefaultErrorResolver returns Retry for all errors.
type DefaultErrorResolver struct{}

func (d *DefaultErrorResolver) Resolve(_ context.Context, _ *Message, _ error) (ErrorAction, error) {
	return Retry, nil
}
