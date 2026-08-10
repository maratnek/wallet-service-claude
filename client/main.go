package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"

	"wallet-service/internal/async"
	"wallet-service/internal/telemetry"
	pb "wallet-service/proto"
)

// resultWaiter сопоставляет request_id с горутиной, ожидающей ответ из
// wallet.results. Записи никогда не удаляются по таймауту, если результат
// так и не пришёл — в демо-клиенте это осознанно опущено.
type resultWaiter struct {
	mu      sync.Mutex
	pending map[string]chan *pb.WalletResult
}

func newResultWaiter() *resultWaiter {
	return &resultWaiter{pending: make(map[string]chan *pb.WalletResult)}
}

func (w *resultWaiter) register(requestID string) chan *pb.WalletResult {
	ch := make(chan *pb.WalletResult, 1)
	w.mu.Lock()
	w.pending[requestID] = ch
	w.mu.Unlock()
	return ch
}

func (w *resultWaiter) deliver(requestID string, res *pb.WalletResult) {
	w.mu.Lock()
	ch, ok := w.pending[requestID]
	if ok {
		delete(w.pending, requestID)
	}
	w.mu.Unlock()
	if ok {
		ch <- res
	}
}

// walletClient связывает всё, что нужно сценариям, чтобы отправить async
// запрос и дождаться его результата из wallet.results.
type walletClient struct {
	rpc    pb.WalletServiceClient
	tracer trace.Tracer
	waiter *resultWaiter
}

func (c *walletClient) createWallet(ctx context.Context, ownerID, currency string) (*pb.CreateWalletResult, error) {
	reqID := uuid.NewString()
	reqCtx, span := c.tracer.Start(ctx, "client.create_wallet",
		trace.WithAttributes(attribute.String("wallet.request_id", reqID), attribute.String("wallet.owner_id", ownerID)))
	done := c.waiter.register(reqID)

	accepted, err := c.rpc.CreateWalletAsync(reqCtx, &pb.CreateWalletRequest{RequestId: reqID, OwnerId: ownerID, Currency: currency})
	if err != nil {
		span.End()
		return nil, fmt.Errorf("CreateWalletAsync: %w", err)
	}
	fmt.Printf("accepted: request_id=%s status=%s\n", accepted.RequestId, accepted.Status)

	select {
	case res := <-done:
		span.End()
		switch body := res.Body.(type) {
		case *pb.WalletResult_CreateWallet:
			return body.CreateWallet, nil
		case *pb.WalletResult_Error:
			return nil, fmt.Errorf("create wallet error: %s", body.Error.Message)
		default:
			return nil, fmt.Errorf("unexpected result type: %T", body)
		}
	case <-time.After(15 * time.Second):
		span.End()
		return nil, fmt.Errorf("timed out waiting for create result")
	}
}

func (c *walletClient) getWallet(ctx context.Context, walletID string) (*pb.GetWalletResult, error) {
	reqID := uuid.NewString()
	reqCtx, span := c.tracer.Start(ctx, "client.get_wallet",
		trace.WithAttributes(attribute.String("wallet.request_id", reqID), attribute.String("wallet.id", walletID)))
	done := c.waiter.register(reqID)

	accepted, err := c.rpc.GetWalletAsync(reqCtx, &pb.GetWalletRequest{RequestId: reqID, WalletId: walletID})
	if err != nil {
		span.End()
		return nil, fmt.Errorf("GetWalletAsync: %w", err)
	}
	fmt.Printf("accepted: request_id=%s status=%s\n", accepted.RequestId, accepted.Status)

	select {
	case res := <-done:
		span.End()
		switch body := res.Body.(type) {
		case *pb.WalletResult_GetWallet:
			return body.GetWallet, nil
		case *pb.WalletResult_Error:
			return nil, fmt.Errorf("get wallet error: %s", body.Error.Message)
		default:
			return nil, fmt.Errorf("unexpected result type: %T", body)
		}
	case <-time.After(15 * time.Second):
		span.End()
		return nil, fmt.Errorf("timed out waiting for get result")
	}
}

func main() {
	ctx := context.Background()

	cfg := telemetry.Config{ServiceName: "wallet-client", Endpoint: "otel-collector:4317", SampleRate: 1.0}
	shutdown, err := telemetry.InitTracer(ctx, cfg)
	if err != nil {
		log.Fatalf("init tracer: %v", err)
	}
	defer shutdown()

	conn, err := grpc.Dial("wallet-service:50051",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithStatsHandler(otelgrpc.NewClientHandler()),
	)
	if err != nil {
		log.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	// Свежая уникальная consumer group на каждый запуск + OffsetOldest
	// гарантируют, что клиент увидит результат, даже если worker успел его
	// опубликовать до того, как консьюмер клиента поднялся.
	waiter := newResultWaiter()
	resultsGroup := fmt.Sprintf("wallet-client-%d", time.Now().UnixNano())
	resultsBroker := async.NewKafkaBrokerWithGroup(os.Getenv("KAFKA_BROKER"), resultsGroup)
	tracer := otel.Tracer("wallet-client")

	consumeCtx, stopConsuming := context.WithCancel(ctx)
	defer stopConsuming()

	go func() {
		err := resultsBroker.Subscribe(consumeCtx, async.ResultsTopic, func(msgCtx context.Context, key string, headers map[string]string, payload []byte) error {
			var res pb.WalletResult
			if err := proto.Unmarshal(payload, &res); err != nil {
				return fmt.Errorf("unmarshal result: %w", err)
			}

			resultCtx := async.ExtractTraceContext(msgCtx, headers)
			_, span := tracer.Start(resultCtx, "wallet-client.consume-result",
				trace.WithAttributes(attribute.String("wallet.request_id", res.RequestId)))
			defer span.End()

			waiter.deliver(res.RequestId, &res)
			return nil
		})
		if err != nil && consumeCtx.Err() == nil {
			log.Printf("results consumer stopped: %v", err)
		}
	}()
	defer func() {
		stopConsuming()
		_ = resultsBroker.Close()
	}()

	client := &walletClient{rpc: pb.NewWalletServiceClient(conn), tracer: tracer, waiter: waiter}

	owners := []string{"alice", "bob", "carol"}

	// ── Сценарий 1: создать несколько кошельков асинхронно ──────────────────
	fmt.Println("=== Scenario 1: Create wallets (async) ===")

	walletIDs := make([]string, 0, len(owners))
	for _, owner := range owners {
		w, err := client.createWallet(ctx, owner, "USD")
		if err != nil {
			log.Fatalf("create wallet for %s: %v", owner, err)
		}
		fmt.Printf("wallet created: id=%s balance=%.2f %s\n", w.WalletId, w.Balance, w.Currency)
		walletIDs = append(walletIDs, w.WalletId)
	}

	// ── Сценарий 2: проверить состояние каждого кошелька асинхронно ─────────
	fmt.Println("\n=== Scenario 2: Get wallet state (async) ===")

	for _, id := range walletIDs {
		w, err := client.getWallet(ctx, id)
		if err != nil {
			log.Fatalf("get wallet %s: %v", id, err)
		}
		fmt.Printf("wallet state: id=%s owner=%s balance=%.2f %s\n", w.WalletId, w.OwnerId, w.Balance, w.Currency)
	}

	time.Sleep(3 * time.Second)
	fmt.Println("\nFlushing traces...")
	fmt.Println("Done. Open http://localhost:16686 — search service: wallet-client")
}
