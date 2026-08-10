package wallet

import (
	"context"
	"fmt"
	"strings"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"wallet-service/internal/repo"
)

// Service — бизнес-логика. Не знает про gRPC, не знает про БД конкретную.
type Service struct {
	repo   repo.Repository
	tracer trace.Tracer
}

func NewService(r repo.Repository) *Service {
	return &Service{
		repo:   r,
		tracer: otel.Tracer("wallet-service/wallet"),
	}
}

type CreateWalletInput struct {
	OwnerID  string
	Currency string
}

type TransferInput struct {
	FromID string
	ToID   string
	Amount float64
}

func (s *Service) CreateWallet(ctx context.Context, in CreateWalletInput) (repo.Wallet, error) {
	ctx, span := s.tracer.Start(ctx, "wallet.CreateWallet",
		trace.WithAttributes(
			attribute.String("wallet.owner_id", in.OwnerID),
			attribute.String("wallet.currency", in.Currency),
		),
	)
	defer span.End()

	if in.OwnerID == "" {
		err := fmt.Errorf("owner_id is required")
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return repo.Wallet{}, err
	}
	if in.Currency == "" {
		err := fmt.Errorf("currency is required")
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return repo.Wallet{}, err
	}

	w := repo.Wallet{
		ID:       generateID(in.OwnerID),
		OwnerID:  in.OwnerID,
		Balance:  0,
		Currency: in.Currency,
	}

	// span передаётся через ctx — repo видит родительский спан
	if err := s.repo.Create(ctx, w); err != nil {
		if strings.Contains(err.Error(), "already exists") {
			existing, getErr := s.repo.Get(ctx, w.ID)
			if getErr != nil {
				span.RecordError(getErr)
				span.SetStatus(codes.Error, "repo.Get failed")
				return repo.Wallet{}, getErr
			}
			span.SetStatus(codes.Ok, "wallet exists")
			return existing, nil
		}

		span.RecordError(err)
		span.SetStatus(codes.Error, "repo.Create failed")
		return repo.Wallet{}, err
	}

	span.SetAttributes(attribute.String("wallet.id", w.ID))
	span.SetStatus(codes.Ok, "wallet created")
	return w, nil
}

func (s *Service) GetWallet(ctx context.Context, id string) (repo.Wallet, error) {
	ctx, span := s.tracer.Start(ctx, "wallet.GetWallet",
		trace.WithAttributes(attribute.String("wallet.id", id)),
	)
	defer span.End()

	w, err := s.repo.Get(ctx, id)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return repo.Wallet{}, err
	}

	span.SetStatus(codes.Ok, "")
	return w, nil
}

func (s *Service) Transfer(ctx context.Context, in TransferInput) (from, to repo.Wallet, err error) {
	ctx, span := s.tracer.Start(ctx, "wallet.Transfer",
		trace.WithAttributes(
			attribute.String("transfer.from", in.FromID),
			attribute.String("transfer.to", in.ToID),
			attribute.Float64("transfer.amount", in.Amount),
		),
	)
	defer span.End()

	// Валидация
	if in.Amount <= 0 {
		err = fmt.Errorf("amount must be positive, got %.2f", in.Amount)
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return
	}
	if in.FromID == in.ToID {
		err = fmt.Errorf("cannot transfer to the same wallet")
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return
	}

	// Получаем оба кошелька — каждый вызов repo создаёт дочерний спан
	from, err = s.repo.Get(ctx, in.FromID)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "from wallet not found")
		return
	}

	to, err = s.repo.Get(ctx, in.ToID)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "to wallet not found")
		return
	}

	// Проверяем баланс
	if from.Balance < in.Amount {
		err = fmt.Errorf("insufficient funds: have %.2f, need %.2f", from.Balance, in.Amount)
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return
	}

	// Проводим перевод
	from.Balance -= in.Amount
	to.Balance += in.Amount

	if err = s.repo.Update(ctx, from); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "update from wallet failed")
		return
	}
	if err = s.repo.Update(ctx, to); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "update to wallet failed")
		return
	}

	span.SetAttributes(
		attribute.Float64("transfer.from_balance_after", from.Balance),
		attribute.Float64("transfer.to_balance_after", to.Balance),
	)
	span.SetStatus(codes.Ok, "transfer complete")
	return
}

func generateID(ownerID string) string {
	// В реальном проекте — uuid.New()
	return fmt.Sprintf("wallet-%s-%d", ownerID, len(ownerID)*137)
}
