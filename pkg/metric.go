package exporter

import (
	"net/netip"

	"github.com/prometheus/client_golang/prometheus"
)

// Handles the prometheus metrics export for HAProxy stick table data.
// It maintains a gauge vector metric for tracking client IP addresses and their associated values.
type StickTableExporter struct {
	// metric is the prometheus gauge vector for stick table data
	metric *prometheus.GaugeVec
	// stickData holds the current state of client IPs and their values
	stickData map[netip.Addr]int
	// tableName is the name of the HAProxy stick table
	tableName string
}

// UpdateMetrics updates the prometheus gauge vector with the current stick table data.
// For each IP address in stickData, it creates a metric with labels for client_ip,
// name, and type (fixed as "ip").
func (e *StickTableExporter) UpdateMetrics() {
	for ip, value := range e.stickData {
		e.metric.WithLabelValues(
			ip.String(),
			e.tableName,
			"ip",
		).Set(float64(value))
	}
}

// UpdateData updates the StickTableExporter's internal stick table data
func (e *StickTableExporter) UpdateData(newData map[netip.Addr]int) {
	e.stickData = newData
	e.UpdateMetrics()
}

// WriteMetricsToFile writes the current metrics to the specified file in Prometheus text format.
func (e *StickTableExporter) WriteMetricsToFile(filename string) error {
	// Create a new registry
	registry := prometheus.NewRegistry()
	registry.MustRegister(e.metric)

	return prometheus.WriteToTextfile(filename, registry)
}
