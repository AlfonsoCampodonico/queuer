package otel

import "go.opentelemetry.io/otel/attribute"

// MetricsOption configures a Metrics instance.
type MetricsOption func(*Metrics)

// WithMeterName overrides the default OTel meter name ("queuer").
func WithMeterName(name string) MetricsOption {
	return func(m *Metrics) {
		m.meterName = name
	}
}

// WithQueueLabel adds a queue_url attribute to every recorded metric.
func WithQueueLabel(url string) MetricsOption {
	return func(m *Metrics) {
		m.queueURL = url
	}
}

// WithAttributes adds constant attributes, such as the queue or service
// name, to every recorded metric.
func WithAttributes(attrs ...attribute.KeyValue) MetricsOption {
	return func(m *Metrics) {
		m.extra = append(m.extra, attrs...)
	}
}
