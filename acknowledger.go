package queuer

import (
	"context"
	"time"
)

// Acknowledger manages message lifecycle after processing.
type Acknowledger interface {
	Ack(ctx context.Context, msg *Message) error
	Nack(ctx context.Context, msg *Message) error
	ChangeVisibility(ctx context.Context, msg *Message, timeout time.Duration) error
}
