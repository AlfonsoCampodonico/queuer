package sqs

import "github.com/AlfonsoCampodonico/queuer"

// NewConsumer creates a queuer.Consumer wired to the given SQS queue.
//
// To batch acknowledgements, build the consumer with queuer.New and
// NewAcknowledger(client, queueURL, WithBatching(...)) instead.
func NewConsumer(client Client, queueURL string, handler queuer.Handler, opts ...queuer.Option) *queuer.Consumer {
	return queuer.New(
		NewReceiver(client, queueURL),
		handler,
		NewAcknowledger(client, queueURL),
		opts...,
	)
}
