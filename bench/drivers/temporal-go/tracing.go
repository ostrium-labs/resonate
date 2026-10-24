package main

import (
	"context"
	"net/url"
	"os"
	"sync"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

// W3C traceparent (+ baggage) into Temporal headers via the SDK's tracing
// interceptor.
var propagator = propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{})

// setupTracing exports spans over OTLP/gRPC to OTEL_EXPORTER_OTLP_ENDPOINT
// (default http://127.0.0.1:4317; an http:// endpoint is plaintext) with
// service.name from OTEL_SERVICE_NAME (default rbench-temporal-go). Export
// runs in the background batch processor: with no collector listening,
// spans are dropped, the run is not slowed or failed, and the error is
// logged once. The returned func flushes with a bounded wait.
func setupTracing(ctx context.Context) (*sdktrace.TracerProvider, func()) {
	if os.Getenv("OTEL_SERVICE_NAME") == "" {
		os.Setenv("OTEL_SERVICE_NAME", "rbench-temporal-go")
	}
	endpoint := envStr("OTEL_EXPORTER_OTLP_ENDPOINT", "http://127.0.0.1:4317")

	var once sync.Once
	otel.SetErrorHandler(otel.ErrorHandlerFunc(func(err error) {
		once.Do(func() {
			logf("otel: span export to %s failed (logged once; the run goes on): %v", endpoint, err)
		})
	}))
	otel.SetTextMapPropagator(propagator)

	opts := []otlptracegrpc.Option{
		otlptracegrpc.WithTimeout(3 * time.Second),
		otlptracegrpc.WithRetry(otlptracegrpc.RetryConfig{
			Enabled:         true,
			InitialInterval: 200 * time.Millisecond,
			MaxInterval:     1 * time.Second,
			MaxElapsedTime:  3 * time.Second,
		}),
	}
	if u, err := url.Parse(endpoint); err == nil && u.Scheme != "" {
		opts = append(opts, otlptracegrpc.WithEndpointURL(endpoint))
	} else {
		// host:port without a scheme: plaintext, as for http://.
		opts = append(opts, otlptracegrpc.WithEndpoint(endpoint), otlptracegrpc.WithInsecure())
	}
	// New does not dial eagerly (grpc.NewClient): it never blocks on a
	// missing collector.
	exp, err := otlptracegrpc.New(ctx, opts...)
	res, rerr := resource.Merge(resource.Default(), resource.Environment())
	if rerr != nil {
		res = resource.Default()
	}
	var tpOpts []sdktrace.TracerProviderOption
	tpOpts = append(tpOpts, sdktrace.WithResource(res))
	if err != nil {
		logf("otel: exporter for %s: %v (tracing off)", endpoint, err)
	} else {
		tpOpts = append(tpOpts, sdktrace.WithBatcher(exp, sdktrace.WithMaxQueueSize(65536)))
	}
	tp := sdktrace.NewTracerProvider(tpOpts...)
	otel.SetTracerProvider(tp)
	logf("otel: exporting spans to %s as service %s", endpoint, os.Getenv("OTEL_SERVICE_NAME"))
	return tp, func() {
		sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := tp.Shutdown(sctx); err != nil {
			once.Do(func() {
				logf("otel: flush on exit to %s failed: %v", endpoint, err)
			})
		}
	}
}
