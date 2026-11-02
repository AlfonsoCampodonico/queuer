package pubsub

import "github.com/AlfonsoCampodonico/queuer"

// NewConsumer creates a queuer.Consumer wired to the given Pub/Sub
// subscription ("projects/<project>/subscriptions/<id>").
//
// Lease extension is enabled with DefaultLeaseExtension, because Pub/Sub's
// default 10s ack deadline is shorter than most handlers. Pass
// queuer.WithLeaseExtension in opts to change or disable it.
//
// To batch acknowledgements, build the consumer with queuer.New and
// NewAcknowledger(client, subscription, WithBatching(...)) instead.
func NewConsumer(client Client, subscription string, handler queuer.Handler, opts ...queuer.Option) *queuer.Consumer {
	opts = append([]queuer.Option{queuer.WithLeaseExtension(DefaultLeaseExtension)}, opts...)
	return queuer.New(
		NewReceiver(client, subscription),
		handler,
		NewAcknowledger(client, subscription),
		opts...,
	)
}
