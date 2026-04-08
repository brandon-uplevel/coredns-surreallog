package surreallog

import (
	"github.com/coredns/coredns/plugin"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	loggedTotal = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: plugin.Namespace,
		Subsystem: "surreallog",
		Name:      "logged_total",
		Help:      "Total number of DNS queries sent to the log writer.",
	})

	droppedTotal = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: plugin.Namespace,
		Subsystem: "surreallog",
		Name:      "dropped_total",
		Help:      "Total number of DNS queries dropped because the write buffer was full.",
	})

	flushedTotal = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: plugin.Namespace,
		Subsystem: "surreallog",
		Name:      "flushed_total",
		Help:      "Total number of DNS queries successfully written to SurrealDB.",
	})

	errorsTotal = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: plugin.Namespace,
		Subsystem: "surreallog",
		Name:      "errors_total",
		Help:      "Total number of errors writing to SurrealDB.",
	})
)
