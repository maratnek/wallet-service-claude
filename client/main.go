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

	client := pb.NewWalletServiceClient(conn)
	tracer := otel.Tracer("wallet-client")

	// Свежая уникальная consumer group на каждый запуск + OffsetOldest
	// гарантируют, что клиент увидит результат, даже если worker успел его
	// опубликовать до того, как консьюмер клиента поднялся.
	waiter := newResultWaiter()
	resultsGroup := fmt.Sprintf("wallet-client-%d", time.Now().UnixNano())
	resultsBroker := async.NewKafkaBrokerWithGroup(os.Getenv("KAFKA_BROKER"), resultsGroup)

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

	// ── Сценарий 1: создать кошелёк асинхронно, дождаться результата ────────
	fmt.Println("=== Scenario 1: Create wallet (async) ===")

	createReqID := uuid.NewString()
	createCtx, createSpan := tracer.Start(ctx, "client.create_wallet",
		trace.WithAttributes(attribute.String("wallet.request_id", createReqID)))
	createDone := waiter.register(createReqID)

	accepted, err := client.CreateWalletAsync(createCtx, &pb.CreateWalletRequest{
		RequestId: createReqID,
		OwnerId:   "alice",
		Currency:  "USD",
	})
	if err != nil {
		createSpan.End()
		log.Fatalf("CreateWalletAsync: %v", err)
	}
	fmt.Printf("accepted: request_id=%s status=%s\n", accepted.RequestId, accepted.Status)

	var walletID string
	select {
	case res := <-createDone:
		createSpan.End()
		switch body := res.Body.(type) {
		case *pb.WalletResult_CreateWallet:
			walletID = body.CreateWallet.WalletId
			fmt.Printf("wallet created: id=%s balance=%.2f %s\n",
				body.CreateWallet.WalletId, body.CreateWallet.Balance, body.CreateWallet.Currency)
		case *pb.WalletResult_Error:
			log.Fatalf("create wallet error: %s", body.Error.Message)
		default:
			log.Fatalf("unexpected result type: %T", body)
		}
	case <-time.After(15 * time.Second):
		createSpan.End()
		log.Fatal("timed out waiting for create result")
	}

	// ── Сценарий 2: проверить состояние кошелька асинхронно ─────────────────
	fmt.Println("\n=== Scenario 2: Get wallet state (async) ===")

	getReqID := uuid.NewString()
	getCtx, getSpan := tracer.Start(ctx, "client.get_wallet",
		trace.WithAttributes(attribute.String("wallet.request_id", getReqID)))
	getDone := waiter.register(getReqID)

	getAccepted, err := client.GetWalletAsync(getCtx, &pb.GetWalletRequest{
		RequestId: getReqID,
		WalletId:  walletID,
	})
	if err != nil {
		getSpan.End()
		log.Fatalf("GetWalletAsync: %v", err)
	}
	fmt.Printf("accepted: request_id=%s status=%s\n", getAccepted.RequestId, getAccepted.Status)

	select {
	case res := <-getDone:
		getSpan.End()
		switch body := res.Body.(type) {
		case *pb.WalletResult_GetWallet:
			fmt.Printf("wallet state: id=%s owner=%s balance=%.2f %s\n",
				body.GetWallet.WalletId, body.GetWallet.OwnerId, body.GetWallet.Balance, body.GetWallet.Currency)
		case *pb.WalletResult_Error:
			log.Fatalf("get wallet error: %s", body.Error.Message)
		default:
			log.Fatalf("unexpected result type: %T", body)
		}
	case <-time.After(15 * time.Second):
		getSpan.End()
		log.Fatal("timed out waiting for get result")
	}

	time.Sleep(3 * time.Second)
	fmt.Println("\nFlushing traces...")
	fmt.Println("Done. Open http://localhost:16686 — search service: wallet-client")
}
