package queuer

import (
	"context"
	"errors"
	"fmt"
)

// ErrorAction tells the consumer what to do after a handler error.
type ErrorAction int

const (
	// Retry hides the message for the duration returned by the retry
	// backoff (see WithRetryBackoff) and lets it be redelivered afterwards.
	Retry ErrorAction = iota
	// Nack makes the message available for redelivery immediately.
	Nack
	// Skip deletes (acknowledges) the message despite the error.
	Skip
	// DeadLetter leaves the message unsettled so the provider's dead-letter
	// policy (an SQS redrive policy or a Pub/Sub dead-letter topic) moves it
	// once its maximum delivery count is reached.
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

// ErrorResolverFunc is an adapter to allow use of ordinary functions as ErrorResolvers.
type ErrorResolverFunc func(ctx context.Context, msg *Message, err error) (ErrorAction, error)

// Resolve calls f(ctx, msg, err).
func (f ErrorResolverFunc) Resolve(ctx context.Context, msg *Message, err error) (ErrorAction, error) {
	return f(ctx, msg, err)
}

// DefaultErrorResolver returns Retry for all errors.
type DefaultErrorResolver struct{}

// Resolve always returns Retry.
func (d *DefaultErrorResolver) Resolve(_ context.Context, _ *Message, _ error) (ErrorAction, error) {
	return Retry, nil
}

// ErrShutdownTimeout is returned by Run when in-flight messages are not
// drained within the configured shutdown timeout.
var ErrShutdownTimeout = errors.New("queuer: shutdown timeout exceeded")

// ErrAlreadyRunning is returned by Run when the consumer is already running.
var ErrAlreadyRunning = errors.New("queuer: consumer is already running")

// PanicError wraps a value recovered from a handler panic.
type PanicError struct {
	Value any
	Stack []byte
}

func (e *PanicError) Error() string {
	return fmt.Sprintf("queuer: handler panicked: %v", e.Value)
}
