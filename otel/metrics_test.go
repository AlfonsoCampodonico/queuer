package otel_test

import (
	"context"
	"testing"
	"time"

	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/AlfonsoCampodonico/queuer"
	otelmetrics "github.com/AlfonsoCampodonico/queuer/otel"
)

func collectMetrics(t *testing.T, reader *sdkmetric.ManualReader) metricdata.ResourceMetrics {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("collecting metrics: %v", err)
	}
	return rm
}

func findMetric(rm metricdata.ResourceMetrics, name string) *metricdata.Metrics {
	for _, scope := range rm.ScopeMetrics {
		for i := range scope.Metrics {
			if scope.Metrics[i].Name == name {
				return &scope.Metrics[i]
			}
		}
	}
	return nil
}

func TestIncMessagesReceived(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	defer func() { _ = provider.Shutdown(context.Background()) }()

	m, err := otelmetrics.NewMetrics(provider)
	if err != nil {
		t.Fatalf("NewMetrics: %v", err)
	}

	m.IncMessagesReceived(5)
	m.IncMessagesReceived(3)

	rm := collectMetrics(t, reader)
	met := findMetric(rm, "queuer.messages.received")
	if met == nil {
		t.Fatal("metric queuer.messages.received not found")
	}

	sum, ok := met.Data.(metricdata.Sum[int64])
	if !ok {
		t.Fatalf("expected Sum[int64], got %T", met.Data)
	}
	if len(sum.DataPoints) == 0 {
		t.Fatal("no data points")
	}
	if got := sum.DataPoints[0].Value; got != 8 {
		t.Errorf("expected 8, got %d", got)
	}
}

func TestIncMessagesProcessed(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	defer func() { _ = provider.Shutdown(context.Background()) }()

	m, err := otelmetrics.NewMetrics(provider)
	if err != nil {
		t.Fatalf("NewMetrics: %v", err)
	}

	m.IncMessagesProcessed()
	m.IncMessagesProcessed()

	rm := collectMetrics(t, reader)
	met := findMetric(rm, "queuer.messages.processed")
	if met == nil {
		t.Fatal("metric queuer.messages.processed not found")
	}

	sum, ok := met.Data.(metricdata.Sum[int64])
	if !ok {
		t.Fatalf("expected Sum[int64], got %T", met.Data)
	}
	if len(sum.DataPoints) == 0 {
		t.Fatal("no data points")
	}
	if got := sum.DataPoints[0].Value; got != 2 {
		t.Errorf("expected 2, got %d", got)
	}
}

func TestIncMessagesFailed(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	defer func() { _ = provider.Shutdown(context.Background()) }()

	m, err := otelmetrics.NewMetrics(provider)
	if err != nil {
		t.Fatalf("NewMetrics: %v", err)
	}

	m.IncMessagesFailed(queuer.Retry)
	m.IncMessagesFailed(queuer.DeadLetter)
	m.IncMessagesFailed(queuer.Retry)

	rm := collectMetrics(t, reader)
	met := findMetric(rm, "queuer.messages.failed")
	if met == nil {
		t.Fatal("metric queuer.messages.failed not found")
	}

	sum, ok := met.Data.(metricdata.Sum[int64])
	if !ok {
		t.Fatalf("expected Sum[int64], got %T", met.Data)
	}

	// Should have two distinct data points: one for retry (2) and one for dead_letter (1)
	if len(sum.DataPoints) < 2 {
		t.Fatalf("expected at least 2 data points, got %d", len(sum.DataPoints))
	}

	var retryCount, dlCount int64
	for _, dp := range sum.DataPoints {
		for _, kv := range dp.Attributes.ToSlice() {
			if string(kv.Key) == "action" {
				switch kv.Value.AsString() {
				case "retry":
					retryCount = dp.Value
				case "dead_letter":
					dlCount = dp.Value
				}
			}
		}
	}
	if retryCount != 2 {
		t.Errorf("expected retry count 2, got %d", retryCount)
	}
	if dlCount != 1 {
		t.Errorf("expected dead_letter count 1, got %d", dlCount)
	}
}

func TestObserveProcessingDuration(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	defer func() { _ = provider.Shutdown(context.Background()) }()

	m, err := otelmetrics.NewMetrics(provider)
	if err != nil {
		t.Fatalf("NewMetrics: %v", err)
	}

	m.ObserveProcessingDuration(150 * time.Millisecond)
	m.ObserveProcessingDuration(250 * time.Millisecond)

	rm := collectMetrics(t, reader)
	met := findMetric(rm, "queuer.messages.duration")
	if met == nil {
		t.Fatal("metric queuer.messages.duration not found")
	}

	hist, ok := met.Data.(metricdata.Histogram[float64])
	if !ok {
		t.Fatalf("expected Histogram[float64], got %T", met.Data)
	}
	if len(hist.DataPoints) == 0 {
		t.Fatal("no data points")
	}
	dp := hist.DataPoints[0]
	if dp.Count != 2 {
		t.Errorf("expected count 2, got %d", dp.Count)
	}
	// Sum should be 0.15 + 0.25 = 0.4
	if dp.Sum < 0.39 || dp.Sum > 0.41 {
		t.Errorf("expected sum ~0.4, got %f", dp.Sum)
	}
}

func TestSetActiveWorkers(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	defer func() { _ = provider.Shutdown(context.Background()) }()

	m, err := otelmetrics.NewMetrics(provider)
	if err != nil {
		t.Fatalf("NewMetrics: %v", err)
	}

	m.SetActiveWorkers(5)
	m.SetActiveWorkers(3)

	rm := collectMetrics(t, reader)
	met := findMetric(rm, "queuer.workers.active")
	if met == nil {
		t.Fatal("metric queuer.workers.active not found")
	}

	sum, ok := met.Data.(metricdata.Sum[int64])
	if !ok {
		t.Fatalf("expected Sum[int64], got %T", met.Data)
	}
	if len(sum.DataPoints) == 0 {
		t.Fatal("no data points")
	}
	// Delta: +5, then -2 (swap from 5 to 3) = net 3
	if got := sum.DataPoints[0].Value; got != 3 {
		t.Errorf("expected 3 active workers, got %d", got)
	}
}

func TestSetActiveWorkersSameValue(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	defer func() { _ = provider.Shutdown(context.Background()) }()

	m, err := otelmetrics.NewMetrics(provider)
	if err != nil {
		t.Fatalf("NewMetrics: %v", err)
	}

	m.SetActiveWorkers(5)
	m.SetActiveWorkers(5) // same value, delta=0, should be no-op

	rm := collectMetrics(t, reader)
	met := findMetric(rm, "queuer.workers.active")
	if met == nil {
		t.Fatal("metric queuer.workers.active not found")
	}

	sum, ok := met.Data.(metricdata.Sum[int64])
	if !ok {
		t.Fatalf("expected Sum[int64], got %T", met.Data)
	}
	if len(sum.DataPoints) == 0 {
		t.Fatal("no data points")
	}
	if got := sum.DataPoints[0].Value; got != 5 {
		t.Errorf("expected 5 active workers, got %d", got)
	}
}

func TestWithQueueLabel(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	defer func() { _ = provider.Shutdown(context.Background()) }()

	m, err := otelmetrics.NewMetrics(provider, otelmetrics.WithQueueLabel("https://sqs.us-east-1.amazonaws.com/123456789/my-queue"))
	if err != nil {
		t.Fatalf("NewMetrics: %v", err)
	}

	m.IncMessagesProcessed()

	rm := collectMetrics(t, reader)
	met := findMetric(rm, "queuer.messages.processed")
	if met == nil {
		t.Fatal("metric queuer.messages.processed not found")
	}

	sum, ok := met.Data.(metricdata.Sum[int64])
	if !ok {
		t.Fatalf("expected Sum[int64], got %T", met.Data)
	}
	if len(sum.DataPoints) == 0 {
		t.Fatal("no data points")
	}

	found := false
	for _, kv := range sum.DataPoints[0].Attributes.ToSlice() {
		if string(kv.Key) == "queue_url" {
			found = true
			if got := kv.Value.AsString(); got != "https://sqs.us-east-1.amazonaws.com/123456789/my-queue" {
				t.Errorf("unexpected queue_url: %s", got)
			}
		}
	}
	if !found {
		t.Error("queue_url attribute not found")
	}
}

func TestWithMeterName(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	defer func() { _ = provider.Shutdown(context.Background()) }()

	m, err := otelmetrics.NewMetrics(provider, otelmetrics.WithMeterName("custom-meter"))
	if err != nil {
		t.Fatalf("NewMetrics: %v", err)
	}

	m.IncMessagesProcessed()

	rm := collectMetrics(t, reader)

	found := false
	for _, scope := range rm.ScopeMetrics {
		if scope.Scope.Name == "custom-meter" {
			found = true
			break
		}
	}
	if !found {
		t.Error("expected scope with name 'custom-meter'")
	}
}

// Compile-time check that *Metrics satisfies queuer.MetricsCollector.
var _ queuer.MetricsCollector = (*otelmetrics.Metrics)(nil)
