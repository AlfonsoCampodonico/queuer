// Package batch coalesces individual settle calls (acks, nacks, visibility
// changes) into provider batch requests. It is shared by the queuer adapters.
package batch

import (
	"context"
	"errors"
	"sync"
	"time"
)

// FlushFunc sends one batch. It must return either nil (every item
// succeeded) or a slice with one entry per item, nil meaning success.
type FlushFunc[T any] func(ctx context.Context, items []T) []error

// Batcher groups items until MaxSize is reached or MaxWait has passed since
// the first pending item, then flushes them with a single call. Do blocks
// until the item's batch has been flushed, so callers keep per-item errors.
type Batcher[T any] struct {
	maxSize int
	maxWait time.Duration
	flush   FlushFunc[T]

	mu      sync.Mutex
	pending []*item[T]
	timer   *time.Timer
}

type item[T any] struct {
	ctx  context.Context
	v    T
	done chan error
}

// New returns a Batcher. maxSize <= 1 or maxWait <= 0 flushes every item on
// its own, which keeps the call synchronous.
func New[T any](maxSize int, maxWait time.Duration, flush FlushFunc[T]) *Batcher[T] {
	if maxSize < 1 {
		maxSize = 1
	}
	return &Batcher[T]{maxSize: maxSize, maxWait: maxWait, flush: flush}
}

// Do adds v to the current batch and waits for that batch to be flushed.
// If ctx is done first, Do returns ctx.Err(); the item may still be sent.
func (b *Batcher[T]) Do(ctx context.Context, v T) error {
	it := &item[T]{ctx: ctx, v: v, done: make(chan error, 1)}

	b.mu.Lock()
	b.pending = append(b.pending, it)
	var ready []*item[T]
	switch {
	case len(b.pending) >= b.maxSize || b.maxWait <= 0:
		ready = b.takeLocked()
	case b.timer == nil:
		b.timer = time.AfterFunc(b.maxWait, b.flushPending)
	}
	b.mu.Unlock()

	if ready != nil {
		b.send(ready)
	}

	select {
	case err := <-it.done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (b *Batcher[T]) flushPending() {
	b.mu.Lock()
	ready := b.takeLocked()
	b.mu.Unlock()
	if len(ready) > 0 {
		b.send(ready)
	}
}

func (b *Batcher[T]) takeLocked() []*item[T] {
	if b.timer != nil {
		b.timer.Stop()
		b.timer = nil
	}
	ready := b.pending
	b.pending = nil
	return ready
}

var errShortResult = errors.New("batch: flush returned the wrong number of results")

func (b *Batcher[T]) send(items []*item[T]) {
	vals := make([]T, len(items))
	for i, it := range items {
		vals[i] = it.v
	}
	// A batch of several items is detached from the first caller's
	// cancellation because it carries other callers' items too; values
	// (trace context, credentials) are kept. A single item keeps its context.
	ctx := items[0].ctx
	if len(items) > 1 {
		ctx = context.WithoutCancel(ctx)
	}
	errs := b.flush(ctx, vals)
	for i, it := range items {
		var err error
		switch {
		case errs == nil:
		case len(errs) != len(items):
			err = errShortResult
		default:
			err = errs[i]
		}
		it.done <- err
	}
}

// Fill returns a result slice where every item failed with err, or nil if
// err is nil. It is a convenience for FlushFuncs whose request failed as a whole.
func Fill(n int, err error) []error {
	if err == nil {
		return nil
	}
	errs := make([]error, n)
	for i := range errs {
		errs[i] = err
	}
	return errs
}
