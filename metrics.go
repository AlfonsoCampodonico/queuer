package queuer

import "time"

// MetricsCollector records operational metrics for the consumer.
type MetricsCollector interface {
	IncMessagesReceived(count int)
	IncMessagesProcessed()
	IncMessagesFailed(action ErrorAction)
	ObserveProcessingDuration(duration time.Duration)
	SetActiveWorkers(count int)
}

// NoopMetrics is a MetricsCollector that does nothing.
type NoopMetrics struct{}

func (n *NoopMetrics) IncMessagesReceived(_ int)                 {}
func (n *NoopMetrics) IncMessagesProcessed()                     {}
func (n *NoopMetrics) IncMessagesFailed(_ ErrorAction)           {}
func (n *NoopMetrics) ObserveProcessingDuration(_ time.Duration) {}
func (n *NoopMetrics) SetActiveWorkers(_ int)                    {}
