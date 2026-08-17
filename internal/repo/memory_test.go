package repo_test

import (
	"context"
	"fmt"
	"testing"

	"wallet-service/internal/repo"
)

func TestCreate_Success(t *testing.T) {
	r := repo.NewInMemory()
	ctx := context.Background()

	w := repo.Wallet{ID: "w-1", OwnerID: "alice", Balance: 100, Currency: "USD"}
	if err := r.Create(ctx, w); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestCreate_Duplicate(t *testing.T) {
	r := repo.NewInMemory()
	ctx := context.Background()

	w := repo.Wallet{ID: "w-1", OwnerID: "alice", Balance: 100, Currency: "USD"}
	_ = r.Create(ctx, w)

	if err := r.Create(ctx, w); err == nil {
		t.Fatal("expected error for duplicate wallet ID, got nil")
	}
}

func TestGet_Success(t *testing.T) {
	r := repo.NewInMemory()
	ctx := context.Background()

	w := repo.Wallet{ID: "w-1", OwnerID: "alice", Balance: 250, Currency: "EUR"}
	_ = r.Create(ctx, w)

	got, err := r.Get(ctx, "w-1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.OwnerID != "alice" {
		t.Errorf("owner: want alice, got %s", got.OwnerID)
	}
	if got.Balance != 250 {
		t.Errorf("balance: want 250, got %d", got.Balance)
	}
	if got.Currency != "EUR" {
		t.Errorf("currency: want EUR, got %s", got.Currency)
	}
}

func TestGet_NotFound(t *testing.T) {
	r := repo.NewInMemory()

	_, err := r.Get(context.Background(), "nonexistent")
	if err == nil {
		t.Fatal("expected not found error, got nil")
	}
}

func TestUpdate_Success(t *testing.T) {
	r := repo.NewInMemory()
	ctx := context.Background()

	_ = r.Create(ctx, repo.Wallet{ID: "w-1", OwnerID: "alice", Balance: 100, Currency: "USD"})

	updated := repo.Wallet{ID: "w-1", OwnerID: "alice", Balance: 999, Currency: "USD"}
	if err := r.Update(ctx, updated); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	got, _ := r.Get(ctx, "w-1")
	if got.Balance != 999 {
		t.Errorf("balance after update: want 999, got %d", got.Balance)
	}
}

func TestUpdate_NotFound(t *testing.T) {
	r := repo.NewInMemory()

	err := r.Update(context.Background(), repo.Wallet{ID: "ghost"})
	if err == nil {
		t.Fatal("expected not found error on update, got nil")
	}
}

func TestConcurrentCreate(t *testing.T) {
	// Проверяет что RWMutex работает корректно под нагрузкой
	// Запускай с: make test-race
	r := repo.NewInMemory()
	ctx := context.Background()
	done := make(chan struct{}, 20)

	for i := range 20 {
		go func(n int) {
			_ = r.Create(ctx, repo.Wallet{
				ID:      fmt.Sprintf("w-%d", n),
				OwnerID: fmt.Sprintf("user-%d", n),
				Balance: uint64(n * 10),
			})
			done <- struct{}{}
		}(i)
	}

	for range 20 {
		<-done
	}
}
