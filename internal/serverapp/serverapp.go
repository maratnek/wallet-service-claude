// Package serverapp собирает и запускает gRPC proxy (wallet-service).
// Вся конструкция зависимостей живёт здесь, а не в cmd/server/main.go,
// чтобы main оставался чистой точкой входа (сигналы + вызов Run).
package serverapp

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"

	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	"wallet-service/internal/async"
	"wallet-service/internal/handler"
	"wallet-service/internal/telemetry"
	pb "wallet-service/proto"
)

func Run(ctx context.Context) error {
	cfg := telemetry.ConfigFromEnv()
	shutdown, err := telemetry.InitTracer(ctx, cfg)
	if err != nil {
		return fmt.Errorf("init tracer: %w", err)
	}
	defer shutdown()
	log.Printf("OTel tracer initialized: service=%s endpoint=%s", cfg.ServiceName, cfg.Endpoint)

	// Proxy — только продьюсер команд, без собственного хранилища. Единственный
	// консьюмер wallet.commands и владелец состояния — wallet-worker.
	broker := async.NewKafkaBroker(os.Getenv("KAFKA_BROKER"))
	defer broker.Close()
	asyncSvc := async.NewAsyncWalletService(broker)

	grpcServer := grpc.NewServer(
		grpc.StatsHandler(otelgrpc.NewServerHandler()),
	)
	pb.RegisterWalletServiceServer(grpcServer, handler.NewGRPCServer(asyncSvc))
	reflection.Register(grpcServer) // для grpcurl без proto файла

	addr := os.Getenv("GRPC_ADDR")
	if addr == "" {
		addr = ":50051"
	}
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("listen: %w", err)
	}

	errCh := make(chan error, 1)
	go func() {
		log.Printf("gRPC server listening on %s", addr)
		errCh <- grpcServer.Serve(lis)
	}()

	select {
	case <-ctx.Done():
		log.Println("Shutting down...")
		grpcServer.GracefulStop()
		return nil
	case err := <-errCh:
		return err
	}
}
