package queuer_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/AlfonsoCampodonico/queuer"
)

// --- Mock implementations ---

// mockReceiver returns msgs on the first call and blocks on subsequent calls
// until the context is cancelled.
type mockReceiver struct {
	msgs []*queuer.Message
	once sync.Once
}

func (m *mockReceiver) Receive(ctx context.Context, _ int, _ time.Duration) ([]*queuer.Message, error) {
	var msgs []*queuer.Message
	m.once.Do(func() { msgs = m.msgs })
	if msgs != nil {
		return msgs, nil
	}
	<-ctx.Done()
	return nil, ctx.Err()
}

type visibilityCall struct {
	id      string
	timeout time.Duration
	at      time.Time
}

// mockAcknowledger records every settle call.
type mockAcknowledger struct {
	mu      sync.Mutex
	acked   []string
	nacked  []string
	changed []visibilityCall
	ackErr  error
}

func (m *mockAcknowledger) Ack(_ context.Context, msg *queuer.Message) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.ackErr != nil {
		return m.ackErr
	}
	m.acked = append(m.acked, msg.ID)
	return nil
}

func (m *mockAcknowledger) Nack(_ context.Context, msg *queuer.Message) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.nacked = append(m.nacked, msg.ID)
	return nil
}

func (m *mockAcknowledger) ChangeVisibility(_ context.Context, msg *queuer.Message, timeout time.Duration) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.changed = append(m.changed, visibilityCall{id: msg.ID, timeout: timeout, at: time.Now()})
	return nil
}

func (m *mockAcknowledger) snapshot() (acked, nacked []string, changed []visibilityCall) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return slices.Clone(m.acked), slices.Clone(m.nacked), slices.Clone(m.changed)
}

type trackingMetrics struct {
	received  atomic.Int64
	processed atomic.Int64
	failed    atomic.Int64

	mu           sync.Mutex
	settleFailed map[string]int64
}

func (m *trackingMetrics) IncMessagesReceived(count int)           { m.received.Add(int64(count)) }
func (m *trackingMetrics) IncMessagesProcessed()                   { m.processed.Add(1) }
func (m *trackingMetrics) IncMessagesFailed(_ queuer.ErrorAction)  { m.failed.Add(1) }
func (m *trackingMetrics) ObserveProcessingDuration(time.Duration) {}
func (m *trackingMetrics) SetActiveWorkers(int)                    {}
func (m *trackingMetrics) IncSettleFailed(op string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.settleFailed == nil {
		m.settleFailed = map[string]int64{}
	}
	m.settleFailed[op]++
}

func (m *trackingMetrics) settleFailures(op string) int64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.settleFailed[op]
}

func quietLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func okHandler() queuer.Handler {
	return queuer.HandlerFunc(func(context.Context, *queuer.Message) error { return nil })
}

func resolveTo(action queuer.ErrorAction) queuer.ErrorResolver {
	return queuer.ErrorResolverFunc(func(context.Context, *queuer.Message, error) (queuer.ErrorAction, error) {
		return action, nil
	})
}

// runUntilIdle runs c inside the synctest bubble, waits until every
// goroutine is blocked (all received messages handled), optionally runs
// during, then cancels and returns Run's error.
func runUntilIdle(t *testing.T, c *queuer.Consumer, during func()) error {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	errc := make(chan error, 1)
	go func() { errc <- c.Run(ctx) }()
	synctest.Wait()
	if during != nil {
		during()
	}
	cancel()
	return <-errc
}

func msgs(ids ...string) []*queuer.Message {
	out := make([]*queuer.Message, len(ids))
	for i, id := range ids {
		out[i] = &queuer.Message{ID: id, Body: []byte(id), AckID: "r-" + id}
	}
	return out
}

// --- Tests ---

func TestConsumer_ProcessesAndAcks(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var processed atomic.Int64
		handler := queuer.HandlerFunc(func(context.Context, *queuer.Message) error {
			processed.Add(1)
			return nil
		})
		ack := &mockAcknowledger{}
		c := queuer.New(&mockReceiver{msgs: msgs("1", "2")}, handler, ack,
			queuer.WithWorkers(2), queuer.WithLogger(quietLogger()))

		if err := runUntilIdle(t, c, nil); err != nil {
			t.Fatalf("Run = %v", err)
		}
		acked, _, _ := ack.snapshot()
		if processed.Load() != 2 || len(acked) != 2 {
			t.Fatalf("processed=%d acked=%v, want 2 and 2", processed.Load(), acked)
		}
	})
}

func TestConsumer_ErrorActions(t *testing.T) {
	failing := queuer.HandlerFunc(func(context.Context, *queuer.Message) error { return errors.New("boom") })

	tests := []struct {
		action      queuer.ErrorAction
		wantAcked   int
		wantNacked  int
		wantChanged int
	}{
		{queuer.Retry, 0, 0, 1},
		{queuer.Nack, 0, 1, 0},
		{queuer.Skip, 1, 0, 0},
		{queuer.DeadLetter, 0, 0, 0},
	}
	for _, tt := range tests {
		t.Run(tt.action.String(), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ack := &mockAcknowledger{}
				metrics := &trackingMetrics{}
				c := queuer.New(&mockReceiver{msgs: msgs("1")}, failing, ack,
					queuer.WithErrorResolver(resolveTo(tt.action)),
					queuer.WithMetrics(metrics),
					queuer.WithLogger(quietLogger()))

				if err := runUntilIdle(t, c, nil); err != nil {
					t.Fatalf("Run = %v", err)
				}
				acked, nacked, changed := ack.snapshot()
				if len(acked) != tt.wantAcked || len(nacked) != tt.wantNacked || len(changed) != tt.wantChanged {
					t.Fatalf("acked=%v nacked=%v changed=%v", acked, nacked, changed)
				}
				if metrics.failed.Load() != 1 || metrics.processed.Load() != 0 {
					t.Fatalf("failed=%d processed=%d, want 1 and 0", metrics.failed.Load(), metrics.processed.Load())
				}
			})
		})
	}
}

func TestConsumer_RetryUsesBackoff(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		m := msgs("1")
		m[0].ReceiveCount = 3
		ack := &mockAcknowledger{}
		c := queuer.New(&mockReceiver{msgs: m},
			queuer.HandlerFunc(func(context.Context, *queuer.Message) error { return errors.New("x") }), ack,
			queuer.WithRetryBackoff(queuer.ExponentialBackoff(10*time.Second, time.Hour)),
			queuer.WithLogger(quietLogger()))

		if err := runUntilIdle(t, c, nil); err != nil {
			t.Fatal(err)
		}
		_, _, changed := ack.snapshot()
		if len(changed) != 1 || changed[0].timeout != 40*time.Second {
			t.Fatalf("changed = %+v, want one call with 40s", changed)
		}
	})
}

func TestConsumer_AckFailureIsNotCountedAsProcessed(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ack := &mockAcknowledger{ackErr: errors.New("throttled")}
		metrics := &trackingMetrics{}
		c := queuer.New(&mockReceiver{msgs: msgs("1")}, okHandler(), ack,
			queuer.WithMetrics(metrics), queuer.WithLogger(quietLogger()))

		if err := runUntilIdle(t, c, nil); err != nil {
			t.Fatal(err)
		}
		if metrics.processed.Load() != 0 || metrics.settleFailures(queuer.SettleAck) != 1 {
			t.Fatalf("processed=%d ackFailures=%d, want 0 and 1",
				metrics.processed.Load(), metrics.settleFailures(queuer.SettleAck))
		}
	})
}

func TestConsumer_GracefulShutdown_WaitsForInFlight(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var finished atomic.Bool
		var ctxCancelled atomic.Bool
		handler := queuer.HandlerFunc(func(ctx context.Context, _ *queuer.Message) error {
			time.Sleep(300 * time.Millisecond)
			ctxCancelled.Store(ctx.Err() != nil)
			finished.Store(true)
			return nil
		})
		ack := &mockAcknowledger{}
		c := queuer.New(&mockReceiver{msgs: msgs("slow")}, handler, ack,
			queuer.WithWorkers(1),
			queuer.WithShutdownTimeout(2*time.Second),
			queuer.WithLogger(quietLogger()))

		// runUntilIdle cancels while the handler is still sleeping.
		if err := runUntilIdle(t, c, nil); err != nil {
			t.Fatalf("Run = %v", err)
		}
		if !finished.Load() {
			t.Fatal("handler did not finish before Run returned")
		}
		if ctxCancelled.Load() {
			t.Fatal("handler context was cancelled during the shutdown drain")
		}
		if acked, _, _ := ack.snapshot(); len(acked) != 1 {
			t.Fatalf("acked = %v, want 1", acked)
		}
	})
}

func TestConsumer_GracefulShutdown_TimeoutExceeded(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		handler := queuer.HandlerFunc(func(context.Context, *queuer.Message) error {
			time.Sleep(5 * time.Second) // ignores cancellation on purpose
			return nil
		})
		c := queuer.New(&mockReceiver{msgs: msgs("stuck")}, handler, &mockAcknowledger{},
			queuer.WithWorkers(1),
			queuer.WithShutdownTimeout(100*time.Millisecond),
			queuer.WithLogger(quietLogger()))

		if err := runUntilIdle(t, c, nil); !errors.Is(err, queuer.ErrShutdownTimeout) {
			t.Fatalf("Run = %v, want ErrShutdownTimeout", err)
		}
		// Run gave up on the stuck handler; let it finish so the bubble is empty.
		time.Sleep(5 * time.Second)
		synctest.Wait()
	})
}

func TestConsumer_ShutdownReleasesUndispatchedMessages(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		block := make(chan struct{})
		handler := queuer.HandlerFunc(func(context.Context, *queuer.Message) error {
			<-block
			return nil
		})
		ack := &mockAcknowledger{}
		// One worker with a channel buffer of one: message 1 is in the
		// handler, 2 is buffered, 3..5 are still held by the poll loop.
		c := queuer.New(&mockReceiver{msgs: msgs("1", "2", "3", "4", "5")}, handler, ack,
			queuer.WithWorkers(1),
			queuer.WithShutdownTimeout(time.Minute),
			queuer.WithLogger(quietLogger()))

		ctx, cancel := context.WithCancel(context.Background())
		errc := make(chan error, 1)
		go func() { errc <- c.Run(ctx) }()
		synctest.Wait()
		cancel()
		synctest.Wait()
		close(block)
		if err := <-errc; err != nil {
			t.Fatal(err)
		}

		acked, nacked, _ := ack.snapshot()
		if !slices.Equal(nacked, []string{"3", "4", "5"}) {
			t.Fatalf("nacked = %v, want [3 4 5]", nacked)
		}
		if !slices.Equal(acked, []string{"1", "2"}) {
			t.Fatalf("acked = %v, want [1 2]", acked)
		}
	})
}

func TestConsumer_LeaseExtension(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		handler := queuer.HandlerFunc(func(context.Context, *queuer.Message) error {
			time.Sleep(70 * time.Second)
			return nil
		})
		ack := &mockAcknowledger{}
		c := queuer.New(&mockReceiver{msgs: msgs("1")}, handler, ack,
			queuer.WithLeaseExtension(40*time.Second),
			queuer.WithShutdownTimeout(time.Minute),
			queuer.WithLogger(quietLogger()))

		start := time.Now()
		if err := runUntilIdle(t, c, func() { time.Sleep(2 * time.Minute) }); err != nil {
			t.Fatal(err)
		}

		acked, _, changed := ack.snapshot()
		if len(acked) != 1 {
			t.Fatalf("acked = %v, want 1", acked)
		}
		// Extended at 0s, 20s, 40s and 60s; stopped when the handler returned at 70s.
		var offsets []time.Duration
		for _, ch := range changed {
			if ch.timeout != 40*time.Second {
				t.Errorf("extension timeout = %v, want 40s", ch.timeout)
			}
			offsets = append(offsets, ch.at.Sub(start))
		}
		want := []time.Duration{0, 20 * time.Second, 40 * time.Second, 60 * time.Second}
		if !slices.Equal(offsets, want) {
			t.Fatalf("extensions at %v, want %v", offsets, want)
		}
	})
}

func TestConsumer_HandlerTimeout(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var gotErr atomic.Value
		handler := queuer.HandlerFunc(func(ctx context.Context, _ *queuer.Message) error {
			<-ctx.Done()
			return ctx.Err()
		})
		resolver := queuer.ErrorResolverFunc(func(_ context.Context, _ *queuer.Message, err error) (queuer.ErrorAction, error) {
			gotErr.Store(err)
			return queuer.Nack, nil
		})
		c := queuer.New(&mockReceiver{msgs: msgs("1")}, handler, &mockAcknowledger{},
			queuer.WithHandlerTimeout(time.Second),
			queuer.WithErrorResolver(resolver),
			queuer.WithLogger(quietLogger()))

		if err := runUntilIdle(t, c, func() { time.Sleep(2 * time.Second) }); err != nil {
			t.Fatal(err)
		}
		if err, _ := gotErr.Load().(error); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("handler error = %v, want DeadlineExceeded", err)
		}
	})
}

func TestConsumer_Middleware(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var mu sync.Mutex
		var order []string
		record := func(s string) {
			mu.Lock()
			order = append(order, s)
			mu.Unlock()
		}
		mw := func(name string) queuer.Middleware {
			return func(next queuer.Handler) queuer.Handler {
				return queuer.HandlerFunc(func(ctx context.Context, msg *queuer.Message) error {
					record(name)
					return next.Handle(ctx, msg)
				})
			}
		}
		handler := queuer.HandlerFunc(func(context.Context, *queuer.Message) error {
			record("handler")
			return nil
		})
		c := queuer.New(&mockReceiver{msgs: msgs("1")}, handler, &mockAcknowledger{},
			queuer.WithMiddleware(mw("a"), mw("b")),
			queuer.WithMiddleware(mw("c")),
			queuer.WithLogger(quietLogger()))

		if err := runUntilIdle(t, c, nil); err != nil {
			t.Fatal(err)
		}
		if want := []string{"a", "b", "c", "handler"}; !slices.Equal(order, want) {
			t.Fatalf("order = %v, want %v", order, want)
		}
	})
}

func TestConsumer_RunTwice(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := queuer.New(&mockReceiver{}, okHandler(), &mockAcknowledger{}, queuer.WithLogger(quietLogger()))
		ctx, cancel := context.WithCancel(context.Background())
		errc := make(chan error, 1)
		go func() { errc <- c.Run(ctx) }()
		synctest.Wait()

		if err := c.Run(ctx); !errors.Is(err, queuer.ErrAlreadyRunning) {
			t.Fatalf("second Run = %v, want ErrAlreadyRunning", err)
		}
		cancel()
		if err := <-errc; err != nil {
			t.Fatal(err)
		}
	})
}

func TestConsumer_PanicRecovery(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		handler := queuer.HandlerFunc(func(_ context.Context, msg *queuer.Message) error {
			if msg.ID == "boom" {
				panic("handler exploded")
			}
			return nil
		})
		var resolved atomic.Value
		resolver := queuer.ErrorResolverFunc(func(_ context.Context, _ *queuer.Message, err error) (queuer.ErrorAction, error) {
			resolved.Store(err)
			return queuer.Skip, nil
		})
		ack := &mockAcknowledger{}
		c := queuer.New(&mockReceiver{msgs: msgs("boom", "ok")}, handler, ack,
			queuer.WithWorkers(1),
			queuer.WithErrorResolver(resolver),
			queuer.WithLogger(quietLogger()))

		if err := runUntilIdle(t, c, nil); err != nil {
			t.Fatal(err)
		}
		var pe *queuer.PanicError
		if err, _ := resolved.Load().(error); !errors.As(err, &pe) || pe.Value != "handler exploded" {
			t.Fatalf("resolver got %v, want PanicError(handler exploded)", resolved.Load())
		}
		if acked, _, _ := ack.snapshot(); len(acked) != 2 {
			t.Fatalf("acked = %v, want both messages", acked)
		}
	})
}

type failingReceiver struct {
	failures  int
	mu        sync.Mutex
	callTimes []time.Time
}

func (r *failingReceiver) Receive(ctx context.Context, _ int, _ time.Duration) ([]*queuer.Message, error) {
	r.mu.Lock()
	r.callTimes = append(r.callTimes, time.Now())
	n := len(r.callTimes)
	r.mu.Unlock()
	if n <= r.failures {
		return nil, errors.New("throttled")
	}
	<-ctx.Done()
	return nil, ctx.Err()
}

func TestConsumer_ReceiveBackoff(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		recv := &failingReceiver{failures: 3}
		c := queuer.New(recv, okHandler(), &mockAcknowledger{}, queuer.WithLogger(quietLogger()))

		if err := runUntilIdle(t, c, func() { time.Sleep(time.Minute) }); err != nil {
			t.Fatal(err)
		}
		recv.mu.Lock()
		defer recv.mu.Unlock()
		var gaps []time.Duration
		for i := 1; i < len(recv.callTimes); i++ {
			gaps = append(gaps, recv.callTimes[i].Sub(recv.callTimes[i-1]))
		}
		if want := []time.Duration{time.Second, 2 * time.Second, 4 * time.Second}; !slices.Equal(gaps, want) {
			t.Fatalf("gaps = %v, want %v", gaps, want)
		}
	})
}

func TestNew_NilArguments_Panic(t *testing.T) {
	recv, h, ack := &mockReceiver{}, okHandler(), &mockAcknowledger{}
	cases := map[string]func(){
		"receiver":     func() { queuer.New(nil, h, ack) },
		"handler":      func() { queuer.New(recv, nil, ack) },
		"acknowledger": func() { queuer.New(recv, h, nil) },
	}
	for name, fn := range cases {
		t.Run(name, func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Fatalf("expected panic for nil %s", name)
				}
			}()
			fn()
		})
	}
}

func TestExponentialBackoff(t *testing.T) {
	b := queuer.ExponentialBackoff(10*time.Second, time.Minute)
	for attempt, want := range map[int]time.Duration{
		0:  10 * time.Second,
		1:  10 * time.Second,
		2:  20 * time.Second,
		3:  40 * time.Second,
		4:  time.Minute,
		99: time.Minute,
	} {
		if got := b(attempt); got != want {
			t.Errorf("attempt %d: got %v, want %v", attempt, got, want)
		}
	}
}
