package queuer

import (
	"context"
	"time"
)

// Receiver polls for messages from a queue source.
type Receiver interface {
	Receive(ctx context.Context, maxMessages int, waitTime time.Duration) ([]*Message, error)
}
