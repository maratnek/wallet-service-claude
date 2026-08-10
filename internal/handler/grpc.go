package handler

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	otelcodes "go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"wallet-service/internal/async"
	"wallet-service/internal/wallet"
	pb "wallet-service/proto"
)

// GRPCServer — тонкий продьюсер: только синтаксическая валидация входных
// полей и публикация команды в Kafka. Никакой бизнес-логики и никакого
// хранилища здесь нет — единственный владелец состояния кошельков это
// wallet-worker (см. internal/async и cmd/worker).
type GRPCServer struct {
	pb.UnimplementedWalletServiceServer
	asyncSvc *async.AsyncWalletService
	tracer   trace.Tracer
}

func NewGRPCServer(asyncSvc *async.AsyncWalletService) *GRPCServer {
	return &GRPCServer{asyncSvc: asyncSvc, tracer: otel.Tracer("wallet-service")}
}

func (s *GRPCServer) CreateWalletAsync(ctx context.Context, req *pb.CreateWalletRequest) (*pb.CreateWalletAcceptedResponse, error) {
	if err := s.validateSyntax(ctx, "CreateWalletAsync", func() error {
		if req.RequestId == "" {
			return fmt.Errorf("request_id is required")
		}
		if req.OwnerId == "" {
			return fmt.Errorf("owner_id is required")
		}
		if req.Currency == "" {
			return fmt.Errorf("currency is required")
		}
		return nil
	}); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	if err := s.asyncSvc.CreateWalletAsync(ctx, req.RequestId, wallet.CreateWalletInput{
		OwnerID:  req.OwnerId,
		Currency: req.Currency,
	}); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &pb.CreateWalletAcceptedResponse{RequestId: req.RequestId, Status: "accepted"}, nil
}

func (s *GRPCServer) GetWalletAsync(ctx context.Context, req *pb.GetWalletRequest) (*pb.GetWalletAcceptedResponse, error) {
	if err := s.validateSyntax(ctx, "GetWalletAsync", func() error {
		if req.RequestId == "" {
			return fmt.Errorf("request_id is required")
		}
		if req.WalletId == "" {
			return fmt.Errorf("wallet_id is required")
		}
		return nil
	}); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	if err := s.asyncSvc.GetWalletAsync(ctx, req.RequestId, req.WalletId); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &pb.GetWalletAcceptedResponse{RequestId: req.RequestId, Status: "accepted"}, nil
}

func (s *GRPCServer) TransferAsync(ctx context.Context, req *pb.TransferRequest) (*pb.TransferAcceptedResponse, error) {
	if err := s.validateSyntax(ctx, "TransferAsync", func() error {
		if req.RequestId == "" {
			return fmt.Errorf("request_id is required")
		}
		if req.FromWalletId == "" {
			return fmt.Errorf("from_wallet_id is required")
		}
		if req.ToWalletId == "" {
			return fmt.Errorf("to_wallet_id is required")
		}
		return nil
	}); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	if err := s.asyncSvc.TransferAsync(ctx, req.RequestId, wallet.TransferInput{
		FromID: req.FromWalletId,
		ToID:   req.ToWalletId,
		Amount: req.Amount,
	}); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &pb.TransferAcceptedResponse{RequestId: req.RequestId, Status: "accepted"}, nil
}

// validateSyntax оборачивает проверку полей в отдельный спан — в трейсе
// видно длительность и результат валидации отдельно от последующей
// публикации в Kafka. Проверяет только структурные вещи (обязательность
// полей), не бизнес-правила.
func (s *GRPCServer) validateSyntax(ctx context.Context, rpc string, check func() error) error {
	_, span := s.tracer.Start(ctx, "proxy.validate")
	defer span.End()
	span.SetAttributes(attribute.String("rpc.method", rpc))

	if err := check(); err != nil {
		span.RecordError(err)
		span.SetStatus(otelcodes.Error, err.Error())
		return err
	}
	span.SetStatus(otelcodes.Ok, "")
	return nil
}
