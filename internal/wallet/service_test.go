package wallet_test

import (
	"context"
	"testing"

	"wallet-service/internal/repo"
	"wallet-service/internal/wallet"
)

// setupService создаёт сервис с in-memory репо — без OTel, без сети.
// otel.Tracer() вернёт no-op tracer если провайдер не инициализирован — тесты работают чисто.
func setupService() *wallet.Service {
	return wallet.NewService(repo.NewInMemory())
}

func TestCreateWallet_Success(t *testing.T) {
	svc := setupService()
	ctx := context.Background()

	w, err := svc.CreateWallet(ctx, wallet.CreateWalletInput{
		OwnerID:  "alice",
		Currency: "USD",
	})

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if w.ID == "" {
		t.Error("wallet ID should not be empty")
	}
	if w.OwnerID != "alice" {
		t.Errorf("owner_id: want alice, got %s", w.OwnerID)
	}
	if w.Currency != "USD" {
		t.Errorf("currency: want USD, got %s", w.Currency)
	}
	if w.Balance != 0 {
		t.Errorf("initial balance: want 0, got %d", w.Balance)
	}
}

func TestCreateWallet_WithInitialBalance(t *testing.T) {
	svc := setupService()

	w, err := svc.CreateWallet(context.Background(), wallet.CreateWalletInput{
		OwnerID:        "alice",
		Currency:       "USD",
		InitialBalance: 500,
	})

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if w.Balance != 500 {
		t.Errorf("initial balance: want 500, got %d", w.Balance)
	}
}

func TestCreateWallet_MissingOwner(t *testing.T) {
	svc := setupService()

	_, err := svc.CreateWallet(context.Background(), wallet.CreateWalletInput{
		Currency: "USD",
	})

	if err == nil {
		t.Fatal("expected error for missing owner_id, got nil")
	}
}

func TestCreateWallet_MissingCurrency(t *testing.T) {
	svc := setupService()

	_, err := svc.CreateWallet(context.Background(), wallet.CreateWalletInput{
		OwnerID: "alice",
	})

	if err == nil {
		t.Fatal("expected error for missing currency, got nil")
	}
}

func TestTransfer_Success(t *testing.T) {
	svc := setupService()
	ctx := context.Background()

	// Создаём кошельки напрямую через репо чтобы задать начальный баланс
	r := repo.NewInMemory()
	svc = wallet.NewService(r)

	_ = r.Create(ctx, repo.Wallet{ID: "w-alice", OwnerID: "alice", Balance: 500, Currency: "USD"})
	_ = r.Create(ctx, repo.Wallet{ID: "w-bob", OwnerID: "bob", Balance: 100, Currency: "USD"})

	from, to, err := svc.Transfer(ctx, wallet.TransferInput{
		FromID: "w-alice",
		ToID:   "w-bob",
		Amount: 200,
	})

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if from.Balance != 300 {
		t.Errorf("from balance: want 300, got %d", from.Balance)
	}
	if to.Balance != 300 {
		t.Errorf("to balance: want 300, got %d", to.Balance)
	}
}

func TestTransfer_InsufficientFunds(t *testing.T) {
	r := repo.NewInMemory()
	svc := wallet.NewService(r)
	ctx := context.Background()

	_ = r.Create(ctx, repo.Wallet{ID: "w-alice", OwnerID: "alice", Balance: 50, Currency: "USD"})
	_ = r.Create(ctx, repo.Wallet{ID: "w-bob", OwnerID: "bob", Balance: 0, Currency: "USD"})

	_, _, err := svc.Transfer(ctx, wallet.TransferInput{
		FromID: "w-alice",
		ToID:   "w-bob",
		Amount: 100,
	})

	if err == nil {
		t.Fatal("expected insufficient funds error, got nil")
	}
}

func TestTransfer_SameWallet(t *testing.T) {
	svc := setupService()

	_, _, err := svc.Transfer(context.Background(), wallet.TransferInput{
		FromID: "w-alice",
		ToID:   "w-alice",
		Amount: 10,
	})

	if err == nil {
		t.Fatal("expected error for same wallet transfer, got nil")
	}
}

func TestTransfer_ZeroAmount(t *testing.T) {
	svc := setupService()

	_, _, err := svc.Transfer(context.Background(), wallet.TransferInput{
		FromID: "w-alice",
		ToID:   "w-bob",
		Amount: 0,
	})

	if err == nil {
		t.Fatal("expected error for zero amount, got nil")
	}
}

func TestTransfer_WalletNotFound(t *testing.T) {
	svc := setupService()

	_, _, err := svc.Transfer(context.Background(), wallet.TransferInput{
		FromID: "nonexistent",
		ToID:   "also-nonexistent",
		Amount: 10,
	})

	if err == nil {
		t.Fatal("expected not found error, got nil")
	}
}

func TestGetWallet_NotFound(t *testing.T) {
	svc := setupService()

	_, err := svc.GetWallet(context.Background(), "nonexistent-id")

	if err == nil {
		t.Fatal("expected not found error, got nil")
	}
}
