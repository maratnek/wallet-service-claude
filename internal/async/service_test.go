package async_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"

	"wallet-service/internal/async"
	"wallet-service/internal/repo"
	"wallet-service/internal/wallet"
	pb "wallet-service/proto"
)

func TestAsyncWalletService_CreateWalletIsProcessed(t *testing.T) {
	storage := repo.NewInMemory()
	walletSvc := wallet.NewService(storage)
	broker := async.NewInMemoryBroker()

	// Proxy и worker — раздельные роли даже в тесте: proxy только
	// публикует, worker — единственный, кто читает команды и владеет
	// хранилищем.
	asyncSvc := async.NewAsyncWalletService(broker)
	processor := async.NewWalletCommandProcessor(walletSvc, broker)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := processor.Run(ctx); err != nil {
		t.Fatalf("start processor: %v", err)
	}

	requestID := uuid.NewString()
	if err := asyncSvc.CreateWalletAsync(ctx, requestID, wallet.CreateWalletInput{
		OwnerID:  "alice",
		Currency: "USD",
	}); err != nil {
		t.Fatalf("create wallet async: %v", err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		w, getErr := storage.Get(ctx, "wallet-alice-685")
		if getErr == nil && w.OwnerID == "alice" {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}

	t.Fatalf("wallet was not processed asynchronously")
}

func TestAsyncWalletService_GetWalletIsProcessed(t *testing.T) {
	storage := repo.NewInMemory()
	walletSvc := wallet.NewService(storage)
	broker := async.NewInMemoryBroker()

	asyncSvc := async.NewAsyncWalletService(broker)
	processor := async.NewWalletCommandProcessor(walletSvc, broker)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := processor.Run(ctx); err != nil {
		t.Fatalf("start processor: %v", err)
	}

	results := make(chan *pb.WalletResult, 1)
	if err := broker.Subscribe(ctx, async.ResultsTopic, func(_ context.Context, _ string, _ map[string]string, payload []byte) error {
		var res pb.WalletResult
		if err := proto.Unmarshal(payload, &res); err != nil {
			return err
		}
		results <- &res
		return nil
	}); err != nil {
		t.Fatalf("subscribe results: %v", err)
	}

	if err := storage.Create(ctx, repo.Wallet{ID: "wallet-1", OwnerID: "bob", Balance: 42, Currency: "USD"}); err != nil {
		t.Fatalf("seed wallet: %v", err)
	}

	requestID := uuid.NewString()
	if err := asyncSvc.GetWalletAsync(ctx, requestID, "wallet-1"); err != nil {
		t.Fatalf("get wallet async: %v", err)
	}

	select {
	case res := <-results:
		if res.RequestId != requestID {
			t.Fatalf("request_id: want %s, got %s", requestID, res.RequestId)
		}
		body, ok := res.Body.(*pb.WalletResult_GetWallet)
		if !ok {
			t.Fatalf("expected GetWallet result, got %T", res.Body)
		}
		if body.GetWallet.OwnerId != "bob" || body.GetWallet.Balance != 42 {
			t.Fatalf("unexpected wallet state: %+v", body.GetWallet)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("get wallet result was not delivered")
	}
}
