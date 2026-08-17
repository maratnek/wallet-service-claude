package repo

import (
	"context"
	"fmt"
	"sync"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

// Wallet — доменная модель
type Wallet struct {
	ID       string
	OwnerID  string
	Balance  uint64
	Currency string
}

// Repository — интерфейс, который видит сервисный слой.
// Реализация может быть postgres, in-memory, mock — сервис не знает.
type Repository interface {
	Create(ctx context.Context, w Wallet) error
	Get(ctx context.Context, id string) (Wallet, error)
	Update(ctx context.Context, w Wallet) error
}

// InMemory — простая потокобезопасная реализация для демо и тестов
type InMemory struct {
	mu      sync.RWMutex
	wallets map[string]Wallet
	tracer  trace.Tracer
}

func NewInMemory() *InMemory {
	return &InMemory{
		wallets: make(map[string]Wallet),
		tracer:  otel.Tracer("wallet-service/repo"),
	}
}

func (r *InMemory) Create(ctx context.Context, w Wallet) error {
	_, span := r.tracer.Start(ctx, "repo.Create",
		trace.WithAttributes(
			attribute.String("db.system", "in-memory"),
			attribute.String("db.operation", "INSERT"),
			attribute.String("wallet.id", w.ID),
		),
	)
	defer span.End()

	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.wallets[w.ID]; exists {
		err := fmt.Errorf("wallet %s already exists", w.ID)
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}

	r.wallets[w.ID] = w
	return nil
}

func (r *InMemory) Get(ctx context.Context, id string) (Wallet, error) {
	_, span := r.tracer.Start(ctx, "repo.Get",
		trace.WithAttributes(
			attribute.String("db.system", "in-memory"),
			attribute.String("db.operation", "SELECT"),
			attribute.String("wallet.id", id),
		),
	)
	defer span.End()

	r.mu.RLock()
	defer r.mu.RUnlock()

	w, ok := r.wallets[id]
	if !ok {
		err := fmt.Errorf("wallet %s not found", id)
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return Wallet{}, err
	}
	return w, nil
}

func (r *InMemory) Update(ctx context.Context, w Wallet) error {
	_, span := r.tracer.Start(ctx, "repo.Update",
		trace.WithAttributes(
			attribute.String("db.system", "in-memory"),
			attribute.String("db.operation", "UPDATE"),
			attribute.String("wallet.id", w.ID),
			attribute.Int64("wallet.balance", int64(w.Balance)),
		),
	)
	defer span.End()

	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.wallets[w.ID]; !exists {
		err := fmt.Errorf("wallet %s not found", w.ID)
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}

	r.wallets[w.ID] = w
	return nil
}
