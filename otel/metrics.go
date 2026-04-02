package otel

import (
	"context"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/AlfonsoCampodonico/queuer"
)

// Metrics implements queuer.MetricsCollector using OpenTelemetry instruments.
type Metrics struct {
	meterName string
	queueURL  string
	attrs     []attribute.KeyValue

	received  metric.Int64Counter
	processed metric.Int64Counter
	failed    metric.Int64Counter
	duration  metric.Float64Histogram
	active    metric.Int64UpDownCounter

	lastActive atomic.Int64
}

// NewMetrics creates a new Metrics instance backed by the given MeterProvider.
// It returns an error if any OTel instrument cannot be created.
func NewMetrics(provider metric.MeterProvider, opts ...MetricsOption) (*Metrics, error) {
	m := &Metrics{
		meterName: "queuer",
	}
	for _, opt := range opts {
		opt(m)
	}

	if m.queueURL != "" {
		m.attrs = append(m.attrs, attribute.String("queue_url", m.queueURL))
	}

	meter := provider.Meter(m.meterName)

	var err error

	m.received, err = meter.Int64Counter("queuer.messages.received",
		metric.WithDescription("Total number of messages received from the queue"),
	)
	if err != nil {
		return nil, err
	}

	m.processed, err = meter.Int64Counter("queuer.messages.processed",
		metric.WithDescription("Total number of messages successfully processed"),
	)
	if err != nil {
		return nil, err
	}

	m.failed, err = meter.Int64Counter("queuer.messages.failed",
		metric.WithDescription("Total number of messages that failed processing"),
	)
	if err != nil {
		return nil, err
	}

	m.duration, err = meter.Float64Histogram("queuer.messages.duration",
		metric.WithDescription("Duration of message processing in seconds"),
		metric.WithUnit("s"),
	)
	if err != nil {
		return nil, err
	}

	m.active, err = meter.Int64UpDownCounter("queuer.workers.active",
		metric.WithDescription("Current number of active workers"),
	)
	if err != nil {
		return nil, err
	}

	return m, nil
}

// IncMessagesReceived records that count messages were received.
func (m *Metrics) IncMessagesReceived(count int) {
	m.received.Add(context.Background(), int64(count), metric.WithAttributes(m.attrs...))
}

// IncMessagesProcessed records that a message was successfully processed.
func (m *Metrics) IncMessagesProcessed() {
	m.processed.Add(context.Background(), 1, metric.WithAttributes(m.attrs...))
}

// IncMessagesFailed records that a message failed with the given error action.
func (m *Metrics) IncMessagesFailed(action queuer.ErrorAction) {
	attrs := append([]attribute.KeyValue{attribute.String("action", action.String())}, m.attrs...)
	m.failed.Add(context.Background(), 1, metric.WithAttributes(attrs...))
}

// ObserveProcessingDuration records the time taken to process a message.
func (m *Metrics) ObserveProcessingDuration(duration time.Duration) {
	m.duration.Record(context.Background(), duration.Seconds(), metric.WithAttributes(m.attrs...))
}

// SetActiveWorkers updates the active worker gauge to the given absolute count.
// It computes the delta from the last known value since UpDownCounter requires deltas.
func (m *Metrics) SetActiveWorkers(count int) {
	delta := int64(count) - m.lastActive.Swap(int64(count))
	if delta != 0 {
		m.active.Add(context.Background(), delta, metric.WithAttributes(m.attrs...))
	}
}
