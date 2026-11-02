package batch

import (
	"context"
	"errors"
	"sync"
	"testing"
	"testing/synctest"
	"time"
)

func TestBatcher_FlushesOnSize(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var mu sync.Mutex
		var calls [][]int
		b := New(3, time.Hour, func(_ context.Context, items []int) []error {
			mu.Lock()
			calls = append(calls, append([]int(nil), items...))
			mu.Unlock()
			return nil
		})

		var wg sync.WaitGroup
		for i := range 3 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				if err := b.Do(context.Background(), i); err != nil {
					t.Errorf("Do(%d) = %v", i, err)
				}
			}()
		}
		wg.Wait()

		if len(calls) != 1 || len(calls[0]) != 3 {
			t.Fatalf("calls = %v, want one batch of 3", calls)
		}
	})
}

func TestBatcher_FlushesOnTimer(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var calls int
		b := New(10, 50*time.Millisecond, func(_ context.Context, items []int) []error {
			calls++
			if len(items) != 1 {
				t.Errorf("items = %v, want 1", items)
			}
			return nil
		})

		start := time.Now()
		if err := b.Do(context.Background(), 1); err != nil {
			t.Fatal(err)
		}
		if waited := time.Since(start); waited != 50*time.Millisecond {
			t.Errorf("waited %v, want 50ms", waited)
		}
		if calls != 1 {
			t.Errorf("calls = %d, want 1", calls)
		}
	})
}

func TestBatcher_PerItemErrors(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		errBad := errors.New("bad")
		b := New(2, time.Hour, func(_ context.Context, items []int) []error {
			errs := make([]error, len(items))
			for i, v := range items {
				if v == 2 {
					errs[i] = errBad
				}
			}
			return errs
		})

		results := make(map[int]error)
		var mu sync.Mutex
		var wg sync.WaitGroup
		for _, v := range []int{1, 2} {
			wg.Add(1)
			go func() {
				defer wg.Done()
				err := b.Do(context.Background(), v)
				mu.Lock()
				results[v] = err
				mu.Unlock()
			}()
		}
		wg.Wait()

		if results[1] != nil || !errors.Is(results[2], errBad) {
			t.Fatalf("results = %v", results)
		}
	})
}

func TestBatcher_Unbatched(t *testing.T) {
	var calls int
	b := New(1, 0, func(_ context.Context, items []int) []error {
		calls++
		return Fill(len(items), nil)
	})
	for i := range 3 {
		if err := b.Do(context.Background(), i); err != nil {
			t.Fatal(err)
		}
	}
	if calls != 3 {
		t.Fatalf("calls = %d, want 3", calls)
	}
}

func TestBatcher_ContextCancelled(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		b := New(10, time.Second, func(context.Context, []int) []error { return nil })
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
		defer cancel()
		if err := b.Do(ctx, 1); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("err = %v, want DeadlineExceeded", err)
		}
		// Let the pending timer fire so the bubble has no live timers.
		time.Sleep(time.Second)
	})
}

func TestBatcher_WrongResultCount(t *testing.T) {
	b := New(1, 0, func(_ context.Context, _ []int) []error { return []error{nil, nil} })
	if err := b.Do(context.Background(), 1); !errors.Is(err, errShortResult) {
		t.Fatalf("err = %v, want errShortResult", err)
	}
}
