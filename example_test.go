package queuer_test

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/AlfonsoCampodonico/queuer"
)

func ExampleHandlerFunc() {
	handler := queuer.HandlerFunc(func(ctx context.Context, msg *queuer.Message) error {
		fmt.Printf("processing: %s\n", msg.Body)
		return nil
	})

	var _ queuer.Handler = handler
	fmt.Println("HandlerFunc implements Handler")
	// Output: HandlerFunc implements Handler
}

func ExampleErrorResolver() {
	var errInvalidPayload = errors.New("invalid payload")

	resolver := &skipInvalidResolver{errInvalidPayload: errInvalidPayload}

	action, _ := resolver.Resolve(context.Background(), &queuer.Message{ID: "1"}, errInvalidPayload)
	fmt.Println("invalid payload:", action)

	action, _ = resolver.Resolve(context.Background(), &queuer.Message{ID: "2"}, errors.New("transient"))
	fmt.Println("transient error:", action)

	// Output:
	// invalid payload: skip
	// transient error: nack
}

type skipInvalidResolver struct {
	errInvalidPayload error
}

func (r *skipInvalidResolver) Resolve(_ context.Context, _ *queuer.Message, err error) (queuer.ErrorAction, error) {
	if errors.Is(err, r.errInvalidPayload) {
		return queuer.Skip, nil
	}
	return queuer.Nack, nil
}

func ExampleNew() {
	var recv queuer.Receiver = &stubReceiver{}
	var ack queuer.Acknowledger = &stubAcknowledger{}

	handler := queuer.HandlerFunc(func(_ context.Context, _ *queuer.Message) error {
		return nil
	})

	consumer := queuer.New(recv, handler, ack,
		queuer.WithWorkers(10),
		queuer.WithShutdownTimeout(60*time.Second),
		queuer.WithWaitTime(20*time.Second),
		queuer.WithMaxMessages(10),
		queuer.WithLogger(slog.Default()),
	)

	_ = consumer
	fmt.Println("consumer created")
	// Output: consumer created
}

type stubReceiver struct{}

func (s *stubReceiver) Receive(ctx context.Context, _ int, _ time.Duration) ([]*queuer.Message, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

type stubAcknowledger struct{}

func (s *stubAcknowledger) Ack(_ context.Context, _ *queuer.Message) error            { return nil }
func (s *stubAcknowledger) Nack(_ context.Context, _ *queuer.Message) error           { return nil }
func (s *stubAcknowledger) ChangeVisibility(_ context.Context, _ *queuer.Message, _ time.Duration) error {
	return nil
}
