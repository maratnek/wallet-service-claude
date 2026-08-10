// Package workerapp собирает и запускает wallet-worker — единственный
// консьюмер wallet.commands и единственный владелец хранилища кошельков.
// Вся конструкция зависимостей живёт здесь, а не в cmd/worker/main.go,
// чтобы main оставался чистой точкой входа (сигналы + вызов Run).
package workerapp

import (
	"context"
	"fmt"
	"log"
	"os"

	"go.opentelemetry.io/otel"

	"wallet-service/internal/async"
	"wallet-service/internal/repo"
	"wallet-service/internal/telemetry"
	"wallet-service/internal/wallet"
)

func Run(ctx context.Context) error {
	cfg := telemetry.ConfigFromEnv()
	if os.Getenv("OTEL_SERVICE_NAME") == "" {
		cfg.ServiceName = "wallet-worker"
	}
	shutdown, err := telemetry.InitTracer(ctx, cfg)
	if err != nil {
		return fmt.Errorf("init tracer: %w", err)
	}
	defer shutdown()

	// Спан на сам факт запуска закрываем сразу же, синхронно — если бы он
	// оставался открытым на всё время жизни воркера, каждое сообщение без
	// входящего traceparent становилось бы дочерним от этого никогда не
	// закрывающегося спана.
	func() {
		_, span := otel.Tracer("wallet-worker").Start(ctx, "wallet-worker.startup")
		defer span.End()
	}()

	broker := async.NewKafkaBroker(os.Getenv("KAFKA_BROKER"))
	defer broker.Close()

	storage := repo.NewInMemory()
	svc := wallet.NewService(storage)
	processor := async.NewWalletCommandProcessor(svc, broker)

	log.Println("wallet-worker started, consuming", async.CommandsTopic)
	if err := processor.Run(ctx); err != nil && ctx.Err() == nil {
		return fmt.Errorf("worker stopped: %w", err)
	}
	return nil
}
