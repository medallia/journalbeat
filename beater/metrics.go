package beater

import (
	"context"
	"github.com/deathowl/go-metrics-prometheus"
	"github.com/elastic/beats/libbeat/logp"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/rcrowley/go-metrics"
	"net/http"
	"time"
)

const (
	metricPrefix string = "logging.journalbeat."
)

type JournalBeatMetrics struct {
	logMessagesPublished metrics.Counter
	logMessageDelay      metrics.Gauge

	httpServer *http.Server
}

// We use prometheus to expose metrics in a /metrics http endpoint
func startMetricsHttpServer(addr string, prometheusRegistry prometheus.Gatherer) *http.Server {
	srv := &http.Server{Addr: addr}

	http.Handle("/metrics", promhttp.HandlerFor(prometheusRegistry, promhttp.HandlerOpts{}))

	go func() {
		if err := srv.ListenAndServe(); err != nil {
			// cannot panic, because this probably is an intentional close
			logp.Info("Httpserver: ListenAndServe() stopped and returned: %s", err)
		}
	}()

	// returning reference so caller can call Shutdown()
	return srv
}

func (jbm *JournalBeatMetrics) start(httpAddr string) {
	registry := metrics.NewPrefixedRegistry(metricPrefix)
	jbm.logMessageDelay = metrics.NewRegisteredGauge("MessageConsumptionDelay", registry)
	jbm.logMessagesPublished = metrics.NewRegisteredCounter("MessagesPublished", registry)

	if httpAddr != "" {
		prometheusRegistry := prometheus.DefaultRegisterer
		jbm.httpServer = startMetricsHttpServer(httpAddr, prometheus.DefaultGatherer)
		pClient := prometheusmetrics.NewPrometheusProvider(registry, "", "", prometheusRegistry, 1*time.Second)
		go pClient.UpdatePrometheusMetrics()
	}
}

func (jbm *JournalBeatMetrics) close() {
	if jbm.httpServer != nil {
		ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
		if err := jbm.httpServer.Shutdown(ctx); err != nil {
			logp.Err("Could not close http server %v", err) // failure/timeout shutting down the server gracefully
		}
	}
	logp.Info("journalbeat extension stopped")
}