// Package observ holds logging, correlation IDs and Prometheus metrics.
package observ

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type ctxKey struct{}

func WithCorrelationID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, ctxKey{}, id)
}

func CorrelationID(ctx context.Context) string {
	id, _ := ctx.Value(ctxKey{}).(string)
	return id
}

func newID() string {
	b := make([]byte, 8)
	rand.Read(b)
	return hex.EncodeToString(b)
}

type Metrics struct {
	Registry     *prometheus.Registry
	Delivered    *prometheus.CounterVec   // channel, result: sent | retry | failed | dropped
	ProviderTime *prometheus.HistogramVec // channel
	HTTPRequests *prometheus.CounterVec   // method, route, code
	HTTPTime     *prometheus.HistogramVec // method, route
}

func NewMetrics() *Metrics {
	m := &Metrics{
		Registry: prometheus.NewRegistry(),
		Delivered: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "notifications_processed_total", Help: "Provider attempts by channel and result."},
			[]string{"channel", "result"}),
		ProviderTime: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "provider_request_duration_seconds", Help: "Latency of calls to the delivery provider.",
			Buckets: []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10}},
			[]string{"channel"}),
		HTTPRequests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "http_requests_total", Help: "API requests."}, []string{"method", "route", "code"}),
		HTTPTime: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "http_request_duration_seconds", Help: "API latency.",
			Buckets: prometheus.DefBuckets}, []string{"method", "route"}),
	}
	m.Registry.MustRegister(m.Delivered, m.ProviderTime, m.HTTPRequests, m.HTTPTime)
	return m
}

// QueueDepth reads the gauge from the database at scrape time, so it is never stale
// and no worker has to keep a counter in sync.
func (m *Metrics) RegisterQueueDepth(fn func(ctx context.Context) (map[[2]string]int, error)) {
	m.Registry.MustRegister(prometheus.CollectorFunc(func(ch chan<- prometheus.Metric) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		depth, err := fn(ctx)
		if err != nil {
			return
		}
		desc := prometheus.NewDesc("notification_queue_depth", "Notifications waiting or in flight.", []string{"channel", "status"}, nil)
		for k, v := range depth {
			ch <- prometheus.MustNewConstMetric(desc, prometheus.GaugeValue, float64(v), k[0], k[1])
		}
	}))
}

func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.Registry, promhttp.HandlerOpts{})
}

type statusWriter struct {
	http.ResponseWriter
	code int
}

func (w *statusWriter) WriteHeader(c int) { w.code = c; w.ResponseWriter.WriteHeader(c) }

// Middleware gives every request a correlation ID (taken from X-Correlation-ID or generated),
// puts it in the context and the response, then logs and counts the request.
func Middleware(log *slog.Logger, m *Metrics, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := r.Header.Get("X-Correlation-ID")
		if id == "" || len(id) > 100 {
			id = newID()
		}
		w.Header().Set("X-Correlation-ID", id)
		sw := &statusWriter{ResponseWriter: w, code: http.StatusOK}
		start := time.Now()
		next.ServeHTTP(sw, r.WithContext(WithCorrelationID(r.Context(), id)))

		route := r.Pattern // set by ServeMux; unknown paths have none, which keeps label cardinality bounded
		if route == "" {
			route = "unmatched"
		}
		d := time.Since(start)
		m.HTTPRequests.WithLabelValues(r.Method, route, strconv.Itoa(sw.code)).Inc()
		m.HTTPTime.WithLabelValues(r.Method, route).Observe(d.Seconds())
		if route != "GET /healthz" && route != "GET /metrics" {
			log.Info("request", "correlation_id", id, "method", r.Method, "path", r.URL.Path,
				"status", sw.code, "duration_ms", d.Milliseconds())
		}
	})
}
