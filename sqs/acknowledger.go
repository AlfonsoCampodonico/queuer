package sqs

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"

	"github.com/AlfonsoCampodonico/queuer"
	"github.com/AlfonsoCampodonico/queuer/internal/batch"
)

// Acknowledger settles SQS messages: Ack deletes the message, Nack makes it
// visible again immediately, and ChangeVisibility hides it for a while.
type Acknowledger struct {
	client   Client
	queueURL string

	batchWait  time.Duration
	deletes    *batch.Batcher[string]
	visibility *batch.Batcher[visibilityChange]
}

type visibilityChange struct {
	receiptHandle string
	timeout       int32
}

// AckOption configures an Acknowledger.
type AckOption func(*Acknowledger)

// WithBatching groups deletes and visibility changes into DeleteMessageBatch
// and ChangeMessageVisibilityBatch calls of up to 10 entries. A batch is sent
// when it is full or maxWait after its first entry, and each settle call
// waits for its batch, so per-message errors are still reported.
//
// Batching cuts API calls (and cost) by up to 10x but adds up to maxWait of
// latency per settle call, so it pays off when many workers settle messages
// concurrently. A maxWait <= 0 disables batching. Default: disabled.
func WithBatching(maxWait time.Duration) AckOption {
	return func(a *Acknowledger) {
		a.batchWait = maxWait
	}
}

// NewAcknowledger returns an Acknowledger for the queue at queueURL.
func NewAcknowledger(client Client, queueURL string, opts ...AckOption) *Acknowledger {
	a := &Acknowledger{client: client, queueURL: queueURL}
	for _, opt := range opts {
		opt(a)
	}
	size := 1
	if a.batchWait > 0 {
		size = 10
	}
	a.deletes = batch.New(size, a.batchWait, a.flushDeletes)
	a.visibility = batch.New(size, a.batchWait, a.flushVisibility)
	return a
}

// Ack deletes the message from the queue.
func (a *Acknowledger) Ack(ctx context.Context, msg *queuer.Message) error {
	return a.deletes.Do(ctx, msg.AckID)
}

// Nack makes the message visible again immediately.
func (a *Acknowledger) Nack(ctx context.Context, msg *queuer.Message) error {
	return a.visibility.Do(ctx, visibilityChange{receiptHandle: msg.AckID})
}

// ChangeVisibility hides the message for timeout, clamped to 0..12h and
// rounded down to whole seconds.
func (a *Acknowledger) ChangeVisibility(ctx context.Context, msg *queuer.Message, timeout time.Duration) error {
	secs := int32(clamp(timeout, 0, MaxVisibilityTimeout) / time.Second)
	return a.visibility.Do(ctx, visibilityChange{receiptHandle: msg.AckID, timeout: secs})
}

func (a *Acknowledger) flushDeletes(ctx context.Context, handles []string) []error {
	if len(handles) == 1 {
		_, err := a.client.DeleteMessage(ctx, &awssqs.DeleteMessageInput{
			QueueUrl:      aws.String(a.queueURL),
			ReceiptHandle: aws.String(handles[0]),
		})
		return batch.Fill(1, err)
	}
	entries := make([]types.DeleteMessageBatchRequestEntry, len(handles))
	for i, h := range handles {
		entries[i] = types.DeleteMessageBatchRequestEntry{Id: aws.String(strconv.Itoa(i)), ReceiptHandle: aws.String(h)}
	}
	out, err := a.client.DeleteMessageBatch(ctx, &awssqs.DeleteMessageBatchInput{
		QueueUrl: aws.String(a.queueURL),
		Entries:  entries,
	})
	if err != nil {
		return batch.Fill(len(handles), err)
	}
	return batchErrors(len(handles), out.Failed)
}

func (a *Acknowledger) flushVisibility(ctx context.Context, changes []visibilityChange) []error {
	if len(changes) == 1 {
		_, err := a.client.ChangeMessageVisibility(ctx, &awssqs.ChangeMessageVisibilityInput{
			QueueUrl:          aws.String(a.queueURL),
			ReceiptHandle:     aws.String(changes[0].receiptHandle),
			VisibilityTimeout: changes[0].timeout,
		})
		return batch.Fill(1, err)
	}
	entries := make([]types.ChangeMessageVisibilityBatchRequestEntry, len(changes))
	for i, c := range changes {
		entries[i] = types.ChangeMessageVisibilityBatchRequestEntry{
			Id:                aws.String(strconv.Itoa(i)),
			ReceiptHandle:     aws.String(c.receiptHandle),
			VisibilityTimeout: c.timeout,
		}
	}
	out, err := a.client.ChangeMessageVisibilityBatch(ctx, &awssqs.ChangeMessageVisibilityBatchInput{
		QueueUrl: aws.String(a.queueURL),
		Entries:  entries,
	})
	if err != nil {
		return batch.Fill(len(changes), err)
	}
	return batchErrors(len(changes), out.Failed)
}

// BatchEntryError is the error for one entry of a failed batch request.
type BatchEntryError struct {
	Code        string
	Message     string
	SenderFault bool
}

func (e *BatchEntryError) Error() string {
	return fmt.Sprintf("sqs: batch entry failed: %s: %s", e.Code, e.Message)
}

func batchErrors(n int, failed []types.BatchResultErrorEntry) []error {
	if len(failed) == 0 {
		return nil
	}
	errs := make([]error, n)
	for _, f := range failed {
		i, err := strconv.Atoi(aws.ToString(f.Id))
		if err != nil || i < 0 || i >= n {
			continue
		}
		errs[i] = &BatchEntryError{Code: aws.ToString(f.Code), Message: aws.ToString(f.Message), SenderFault: f.SenderFault}
	}
	return errs
}
