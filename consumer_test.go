package queuer_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/AlfonsoCampodonico/queuer"
)

// --- Mock implementations ---

// mockReceiver returns messages on the first call and blocks on subsequent calls
// until the context is cancelled.
type mockReceiver struct {
	msgs     []*queuer.Message
	once     sync.Once
	returned chan struct{}
}

func newMockReceiver(msgs []*queuer.Message) *mockReceiver {
	return &mockReceiver{
		msgs:     msgs,
		returned: make(chan struct{}),
	}
}

func (m *mockReceiver) Receive(ctx context.Context, _ int, _ time.Duration) ([]*queuer.Message, error) {
	var msgs []*queuer.Message
	m.once.Do(func() {
		msgs = m.msgs
		close(m.returned)
	})
	if msgs != nil {
		return msgs, nil
	}
	// Block until context is cancelled to avoid busy-loop in tests.
	<-ctx.Done()
	return nil, ctx.Err()
}

// mockAcknowledger tracks acked message IDs.
type mockAcknowledger struct {
	mu      sync.Mutex
	acked   []string
	nacked  []string
	changed []string
}

func (m *mockAcknowledger) Ack(_ context.Context, msg *queuer.Message) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.acked = append(m.acked, msg.ID)
	return nil
}

func (m *mockAcknowledger) Nack(_ context.Context, msg *queuer.Message) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.nacked = append(m.nacked, msg.ID)
	return nil
}

func (m *mockAcknowledger) ChangeVisibility(_ context.Context, msg *queuer.Message, _ time.Duration) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.changed = append(m.changed, msg.ID)
	return nil
}

func (m *mockAcknowledger) ackedIDs() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	dst := make([]string, len(m.acked))
	copy(dst, m.acked)
	return dst
}

func (m *mockAcknowledger) nackedIDs() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	dst := make([]string, len(m.nacked))
	copy(dst, m.nacked)
	return dst
}

// mockErrorResolver returns a fixed action.
type mockErrorResolver struct {
	action queuer.ErrorAction
	err    error
}

func (m *mockErrorResolver) Resolve(_ context.Context, _ *queuer.Message, _ error) (queuer.ErrorAction, error) {
	return m.action, m.err
}

// --- Tests ---

func TestConsumer_ProcessesAndAcks(t *testing.T) {
	msgs := []*queuer.Message{
		{ID: "msg-1", Body: "hello"},
		{ID: "msg-2", Body: "world"},
	}

	var processed atomic.Int64
	handler := queuer.HandlerFunc(func(_ context.Context, _ *queuer.Message) error {
		processed.Add(1)
		return nil
	})

	recv := newMockReceiver(msgs)
	ack := &mockAcknowledger{}

	consumer := queuer.New(recv, handler, ack,
		queuer.WithWorkers(2),
		queuer.WithShutdownTimeout(2*time.Second),
	)

	ctx, cancel := context.WithCancel(context.Background())

	// Wait for the receiver to deliver the batch, then give workers time to process.
	go func() {
		<-recv.returned
		// Give workers time to process and ack.
		time.Sleep(100 * time.Millisecond)
		cancel()
	}()

	err := consumer.Run(ctx)
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}

	if got := processed.Load(); got != 2 {
		t.Fatalf("expected 2 processed, got %d", got)
	}

	ackedIDs := ack.ackedIDs()
	if len(ackedIDs) != 2 {
		t.Fatalf("expected 2 acked, got %d: %v", len(ackedIDs), ackedIDs)
	}
}

func TestConsumer_ErrorResolver_Nack(t *testing.T) {
	msgs := []*queuer.Message{
		{ID: "msg-1", Body: "fail"},
	}

	handler := queuer.HandlerFunc(func(_ context.Context, _ *queuer.Message) error {
		return errors.New("processing failed")
	})

	recv := newMockReceiver(msgs)
	ack := &mockAcknowledger{}
	resolver := &mockErrorResolver{action: queuer.Nack}

	consumer := queuer.New(recv, handler, ack,
		queuer.WithWorkers(1),
		queuer.WithErrorResolver(resolver),
		queuer.WithShutdownTimeout(2*time.Second),
	)

	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		<-recv.returned
		time.Sleep(100 * time.Millisecond)
		cancel()
	}()

	err := consumer.Run(ctx)
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}

	nackedIDs := ack.nackedIDs()
	if len(nackedIDs) != 1 {
		t.Fatalf("expected 1 nacked, got %d", len(nackedIDs))
	}
	if nackedIDs[0] != "msg-1" {
		t.Fatalf("expected nacked msg-1, got %s", nackedIDs[0])
	}
}

func TestConsumer_ErrorResolver_Skip(t *testing.T) {
	msgs := []*queuer.Message{
		{ID: "msg-1", Body: "skip-me"},
	}

	handler := queuer.HandlerFunc(func(_ context.Context, _ *queuer.Message) error {
		return errors.New("bad message")
	})

	recv := newMockReceiver(msgs)
	ack := &mockAcknowledger{}
	resolver := &mockErrorResolver{action: queuer.Skip}

	consumer := queuer.New(recv, handler, ack,
		queuer.WithWorkers(1),
		queuer.WithErrorResolver(resolver),
		queuer.WithShutdownTimeout(2*time.Second),
	)

	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		<-recv.returned
		time.Sleep(100 * time.Millisecond)
		cancel()
	}()

	err := consumer.Run(ctx)
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}

	// Skip action should call Ack (delete) on the message.
	ackedIDs := ack.ackedIDs()
	if len(ackedIDs) != 1 {
		t.Fatalf("expected 1 acked (skip), got %d", len(ackedIDs))
	}
	if ackedIDs[0] != "msg-1" {
		t.Fatalf("expected acked msg-1, got %s", ackedIDs[0])
	}
}

func TestConsumer_GracefulShutdown_WaitsForInFlight(t *testing.T) {
	msgs := []*queuer.Message{
		{ID: "msg-slow", Body: "slow"},
	}

	var finished atomic.Int64
	handler := queuer.HandlerFunc(func(_ context.Context, _ *queuer.Message) error {
		time.Sleep(300 * time.Millisecond)
		finished.Add(1)
		return nil
	})

	recv := newMockReceiver(msgs)
	ack := &mockAcknowledger{}

	consumer := queuer.New(recv, handler, ack,
		queuer.WithWorkers(1),
		queuer.WithShutdownTimeout(2*time.Second),
	)

	ctx, cancel := context.WithCancel(context.Background())

	// Cancel quickly after messages are dispatched, while handler is still sleeping.
	go func() {
		<-recv.returned
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()

	err := consumer.Run(ctx)
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}

	// The slow handler should have finished (graceful shutdown waited).
	if got := finished.Load(); got != 1 {
		t.Fatalf("expected slow handler to finish, got %d", got)
	}
}

func TestConsumer_GracefulShutdown_TimeoutExceeded(t *testing.T) {
	msgs := []*queuer.Message{
		{ID: "msg-stuck", Body: "stuck"},
	}

	handler := queuer.HandlerFunc(func(ctx context.Context, _ *queuer.Message) error {
		// Simulate a handler that takes much longer than the shutdown timeout.
		select {
		case <-time.After(5 * time.Second):
		case <-ctx.Done():
			// Even if context is cancelled, keep blocking to test timeout.
			<-time.After(5 * time.Second)
		}
		return nil
	})

	recv := newMockReceiver(msgs)
	ack := &mockAcknowledger{}

	consumer := queuer.New(recv, handler, ack,
		queuer.WithWorkers(1),
		queuer.WithShutdownTimeout(100*time.Millisecond),
	)

	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		<-recv.returned
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()

	err := consumer.Run(ctx)
	if err == nil {
		t.Fatal("expected timeout error, got nil")
	}
	if err.Error() != "queuer: shutdown timeout exceeded" {
		t.Fatalf("unexpected error: %v", err)
	}
}
