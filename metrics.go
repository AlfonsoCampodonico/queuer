package queuer

import "time"

// Settle operations reported to MetricsCollector.IncSettleFailed.
const (
	SettleAck              = "ack"
	SettleNack             = "nack"
	SettleChangeVisibility = "change_visibility"
	SettleExtendLease      = "extend_lease"
)

// MetricsCollector records operational metrics for the consumer.
type MetricsCollector interface {
	IncMessagesReceived(count int)
	// IncMessagesProcessed is called when a handler succeeded and the
	// message was acknowledged.
	IncMessagesProcessed()
	IncMessagesFailed(action ErrorAction)
	// IncSettleFailed is called when acknowledging, nacking, changing the
	// visibility of, or extending the lease on a message fails. op is one of
	// the Settle* constants.
	IncSettleFailed(op string)
	ObserveProcessingDuration(duration time.Duration)
	SetActiveWorkers(count int)
}

// NoopMetrics is a MetricsCollector that does nothing.
type NoopMetrics struct{}

func (n *NoopMetrics) IncMessagesReceived(_ int)                 {}
func (n *NoopMetrics) IncMessagesProcessed()                     {}
func (n *NoopMetrics) IncMessagesFailed(_ ErrorAction)           {}
func (n *NoopMetrics) IncSettleFailed(_ string)                  {}
func (n *NoopMetrics) ObserveProcessingDuration(_ time.Duration) {}
func (n *NoopMetrics) SetActiveWorkers(_ int)                    {}
