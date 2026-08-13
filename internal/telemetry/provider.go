package telemetry

import (
	"context"
	"fmt"
	"os"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.21.0"
)

type Config struct {
	ServiceName string
	Endpoint    string  // otel-collector:4317
	SampleRate  float64 // 1.0 = 100%, 0.1 = 10%
}

func ConfigFromEnv() Config {
	endpoint := os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT")
	if endpoint == "" {
		endpoint = "otel-collector:4317"
	}
	name := os.Getenv("OTEL_SERVICE_NAME")
	if name == "" {
		name = "wallet-service"
	}
	return Config{
		ServiceName: name,
		Endpoint:    endpoint,
		SampleRate:  1.0,
	}
}

// InitTracer настраивает глобальный TracerProvider.
// Возвращает shutdown func — вызывай defer shutdown() в main.
func InitTracer(ctx context.Context, cfg Config) (func(), error) {
	exporter, err := otlptracegrpc.New(ctx,
		otlptracegrpc.WithEndpoint(cfg.Endpoint),
		otlptracegrpc.WithInsecure(),
	)
	if err != nil {
		return nil, fmt.Errorf("create otlp exporter: %w", err)
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithIDGenerator(requestIDGenerator{}),
		sdktrace.WithSampler(
			sdktrace.ParentBased(
				sdktrace.TraceIDRatioBased(cfg.SampleRate),
			),
		),
		sdktrace.WithResource(resource.NewWithAttributes(
			semconv.SchemaURL,
			attribute.String("service.name", cfg.ServiceName),
			attribute.String("service.version", "1.0.0"),
			attribute.String("deployment.environment", "dev"),
		)),
	)

	// Регистрируем глобально — otel.Tracer() работает везде в приложении
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.TraceContext{})

	return func() { _ = tp.Shutdown(ctx) }, nil
}
