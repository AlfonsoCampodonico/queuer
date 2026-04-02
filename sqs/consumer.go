package sqs

import "github.com/AlfonsoCampodonico/queuer"

// NewConsumer creates a queuer.Consumer wired to the given SQS queue.
func NewConsumer(client Client, queueURL string, handler queuer.Handler, opts ...queuer.Option) *queuer.Consumer {
	return queuer.New(
		NewReceiver(client, queueURL),
		handler,
		NewAcknowledger(client, queueURL),
		opts...,
	)
}
