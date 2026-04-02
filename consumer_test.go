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
	if !errors.Is(err, queuer.ErrShutdownTimeout) {
		t.Fatalf("expected ErrShutdownTimeout, got %v", err)
	}
}

// --- Integration test ---

type trackingMetrics struct {
	received  atomic.Int64
	processed atomic.Int64
	failed    atomic.Int64
}

func (m *trackingMetrics) IncMessagesReceived(count int) { m.received.Add(int64(count)) }
func (m *trackingMetrics) IncMessagesProcessed()         { m.processed.Add(1) }
func (m *trackingMetrics) IncMessagesFailed(_ queuer.ErrorAction) {
	m.failed.Add(1)
}
func (m *trackingMetrics) ObserveProcessingDuration(_ time.Duration) {}
func (m *trackingMetrics) SetActiveWorkers(_ int)                    {}

func TestConsumer_Integration(t *testing.T) {
	msgs := []*queuer.Message{
		{ID: "1", Body: "ok", ReceiptHandle: "r1"},
		{ID: "2", Body: "fail", ReceiptHandle: "r2"},
		{ID: "3", Body: "ok", ReceiptHandle: "r3"},
	}

	recv := newMockReceiver(msgs)
	ack := &mockAcknowledger{}
	metrics := &trackingMetrics{}

	handler := queuer.HandlerFunc(func(_ context.Context, msg *queuer.Message) error {
		if msg.Body == "fail" {
			return errors.New("bad message")
		}
		return nil
	})

	resolver := &mockErrorResolver{action: queuer.Skip}

	c := queuer.New(recv, handler, ack,
		queuer.WithWorkers(3),
		queuer.WithErrorResolver(resolver),
		queuer.WithMetrics(metrics),
		queuer.WithShutdownTimeout(2*time.Second),
	)

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		<-recv.returned
		time.Sleep(100 * time.Millisecond)
		cancel()
	}()

	err := c.Run(ctx)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if got := metrics.received.Load(); got < 3 {
		t.Errorf("received = %d, want >= 3", got)
	}
	if got := metrics.processed.Load(); got != 2 {
		t.Errorf("processed = %d, want 2", got)
	}
	if got := metrics.failed.Load(); got != 1 {
		t.Errorf("failed = %d, want 1", got)
	}

	// "fail" message was Skip'd (acked), so all 3 should be acked
	ackedIDs := ack.ackedIDs()
	if len(ackedIDs) != 3 {
		t.Errorf("total acked = %d, want 3 (2 success + 1 skip), got %v", len(ackedIDs), ackedIDs)
	}
}

func TestNew_NilReceiver_Panics(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic for nil receiver")
		}
	}()
	queuer.New(nil, queuer.HandlerFunc(func(_ context.Context, _ *queuer.Message) error { return nil }), &mockAcknowledger{})
}

func TestNew_NilHandler_Panics(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic for nil handler")
		}
	}()
	recv := newMockReceiver(nil)
	queuer.New(recv, nil, &mockAcknowledger{})
}

func TestNew_NilAcknowledger_Panics(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic for nil acknowledger")
		}
	}()
	recv := newMockReceiver(nil)
	queuer.New(recv, queuer.HandlerFunc(func(_ context.Context, _ *queuer.Message) error { return nil }), nil)
}

type callbackErrorResolver struct {
	fn func(context.Context, *queuer.Message, error) (queuer.ErrorAction, error)
}

func (r *callbackErrorResolver) Resolve(ctx context.Context, msg *queuer.Message, err error) (queuer.ErrorAction, error) {
	return r.fn(ctx, msg, err)
}

func TestConsumer_PanicRecovery(t *testing.T) {
	msgs := []*queuer.Message{
		{ID: "msg-panic", Body: "boom"},
		{ID: "msg-ok", Body: "fine"},
	}

	var processed atomic.Int64
	handler := queuer.HandlerFunc(func(_ context.Context, msg *queuer.Message) error {
		if msg.Body == "boom" {
			panic("handler exploded")
		}
		processed.Add(1)
		return nil
	})

	recv := newMockReceiver(msgs)
	ack := &mockAcknowledger{}

	var resolvedErr atomic.Value
	resolver := &callbackErrorResolver{fn: func(_ context.Context, _ *queuer.Message, err error) (queuer.ErrorAction, error) {
		resolvedErr.Store(err)
		return queuer.Skip, nil
	}}

	c := queuer.New(recv, handler, ack,
		queuer.WithWorkers(1),
		queuer.WithErrorResolver(resolver),
		queuer.WithShutdownTimeout(2*time.Second),
	)

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		<-recv.returned
		time.Sleep(200 * time.Millisecond)
		cancel()
	}()

	err := c.Run(ctx)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	stored := resolvedErr.Load()
	if stored == nil {
		t.Fatal("expected error resolver to be called with panic error")
	}
	var pe *queuer.PanicError
	if !errors.As(stored.(error), &pe) {
		t.Fatalf("expected PanicError, got %T: %v", stored, stored)
	}
	if pe.Value != "handler exploded" {
		t.Errorf("panic value = %v, want %q", pe.Value, "handler exploded")
	}

	if got := processed.Load(); got != 1 {
		t.Errorf("processed = %d, want 1", got)
	}
}

type failingReceiver struct {
	failures  int
	callTimes []time.Time
	mu        sync.Mutex
	failCount int
}

func (r *failingReceiver) Receive(ctx context.Context, _ int, _ time.Duration) ([]*queuer.Message, error) {
	r.mu.Lock()
	r.callTimes = append(r.callTimes, time.Now())
	r.failCount++
	count := r.failCount
	r.mu.Unlock()

	if count <= r.failures {
		return nil, errors.New("throttled")
	}
	<-ctx.Done()
	return nil, ctx.Err()
}

func TestConsumer_ReceiveBackoff(t *testing.T) {
	recv := &failingReceiver{failures: 3}
	ack := &mockAcknowledger{}
	handler := queuer.HandlerFunc(func(_ context.Context, _ *queuer.Message) error { return nil })

	c := queuer.New(recv, handler, ack,
		queuer.WithWorkers(1),
		queuer.WithShutdownTimeout(100*time.Millisecond),
	)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = c.Run(ctx)

	recv.mu.Lock()
	defer recv.mu.Unlock()

	if len(recv.callTimes) < 4 {
		t.Fatalf("expected at least 4 receive calls, got %d", len(recv.callTimes))
	}

	gap1 := recv.callTimes[1].Sub(recv.callTimes[0])
	gap2 := recv.callTimes[2].Sub(recv.callTimes[1])

	if gap1 < 800*time.Millisecond {
		t.Errorf("first backoff gap = %v, want >= 800ms", gap1)
	}
	if gap2 < gap1 {
		t.Errorf("second backoff gap (%v) should be >= first (%v)", gap2, gap1)
	}
}

func TestConsumer_ShutdownContextNotCancelled(t *testing.T) {
	msgs := []*queuer.Message{
		{ID: "msg-1", Body: "check-ctx"},
	}

	var ctxWasCancelled atomic.Bool
	var handlerDone atomic.Bool

	handler := queuer.HandlerFunc(func(ctx context.Context, _ *queuer.Message) error {
		time.Sleep(200 * time.Millisecond)
		if ctx.Err() != nil {
			ctxWasCancelled.Store(true)
		}
		handlerDone.Store(true)
		return nil
	})

	recv := newMockReceiver(msgs)
	ack := &mockAcknowledger{}

	c := queuer.New(recv, handler, ack,
		queuer.WithWorkers(1),
		queuer.WithShutdownTimeout(2*time.Second),
	)

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		<-recv.returned
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()

	err := c.Run(ctx)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !handlerDone.Load() {
		t.Fatal("handler did not complete")
	}
	if ctxWasCancelled.Load() {
		t.Fatal("handler context was cancelled during shutdown — it should stay active until shutdown timeout")
	}
}
