package async

import (
	"context"
	"fmt"

	"wallet-service/internal/wallet"
	pb "wallet-service/proto"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/protobuf/proto"
)

// InjectTraceHeaders сериализует текущий trace-контекст (traceparent) в
// заголовки Kafka-сообщения. Заголовки — стандартный способ распространения
// trace-контекста через messaging-системы (в отличие от помещения его в
// тело сообщения, которое видит только бизнес-логика).
func InjectTraceHeaders(ctx context.Context) map[string]string {
	headers := map[string]string{}
	otel.GetTextMapPropagator().Inject(ctx, propagation.MapCarrier(headers))
	return headers
}

// ExtractTraceContext восстанавливает родительский trace-контекст из
// заголовков сообщения. Принципиально важно передавать сюда реальный ctx
// (с отменой/дедлайном вызывающего), а не context.Background() — иначе
// обработка сообщения перестаёт быть отменяемой при остановке процесса.
func ExtractTraceContext(ctx context.Context, headers map[string]string) context.Context {
	if len(headers) == 0 {
		return ctx
	}
	return otel.GetTextMapPropagator().Extract(ctx, propagation.MapCarrier(headers))
}

// AsyncWalletService — тонкий продьюсер на стороне proxy.
// Провалидированный запрос сериализуется в protobuf и публикуется в Kafka.
// Proxy не хранит результат и не ждёт его — обработку wallet.results в этой
// итерации делает сам client.
type AsyncWalletService struct {
	broker Broker
	tracer trace.Tracer
}

func NewAsyncWalletService(broker Broker) *AsyncWalletService {
	return &AsyncWalletService{broker: broker, tracer: otel.Tracer("wallet-service")}
}

func (s *AsyncWalletService) CreateWalletAsync(ctx context.Context, msgUUID string, in wallet.CreateWalletInput) error {
	cmd := &pb.WalletCommand{
		MsgUuid: msgUUID,
		Body: &pb.WalletCommand_CreateWallet{
			CreateWallet: &pb.CreateWalletCommand{OwnerId: in.OwnerID, Currency: in.Currency},
		},
	}
	return s.publishCommand(ctx, msgUUID, cmd)
}

func (s *AsyncWalletService) GetWalletAsync(ctx context.Context, msgUUID string, walletID string) error {
	cmd := &pb.WalletCommand{
		MsgUuid: msgUUID,
		Body: &pb.WalletCommand_GetWallet{
			GetWallet: &pb.GetWalletCommand{WalletId: walletID},
		},
	}
	return s.publishCommand(ctx, msgUUID, cmd)
}

func (s *AsyncWalletService) TransferAsync(ctx context.Context, msgUUID string, in wallet.TransferInput) error {
	cmd := &pb.WalletCommand{
		MsgUuid: msgUUID,
		Body: &pb.WalletCommand_Transfer{
			Transfer: &pb.TransferCommand{FromWalletId: in.FromID, ToWalletId: in.ToID, Amount: in.Amount},
		},
	}
	return s.publishCommand(ctx, msgUUID, cmd)
}

func (s *AsyncWalletService) publishCommand(ctx context.Context, msgUUID string, cmd *pb.WalletCommand) error {
	// Спан стартуем ДО того, как берём trace-контекст для заголовков —
	// так consumer в worker'е становится дочерним именно от этого
	// publish-спана, а не от родительского RPC-спана.
	publishCtx, span := s.tracer.Start(ctx, "wallet-service.publish-command",
		trace.WithAttributes(
			attribute.String("messaging.system", "kafka"),
			attribute.String("messaging.destination", CommandsTopic),
			attribute.String("messaging.operation", "publish"),
			attribute.String("wallet.msg_uuid", msgUUID),
		))
	defer span.End()

	payload, err := proto.Marshal(cmd)
	if err != nil {
		span.RecordError(err)
		return fmt.Errorf("marshal command: %w", err)
	}

	headers := InjectTraceHeaders(publishCtx)
	if err := s.broker.Publish(publishCtx, CommandsTopic, msgUUID, headers, payload); err != nil {
		span.RecordError(err)
		return err
	}
	return nil
}

// WalletCommandProcessor — единственный обработчик команд (worker).
// Владеет бизнес-логикой и единственным хранилищем кошельков.
type WalletCommandProcessor struct {
	walletSvc *wallet.Service
	broker    Broker
	tracer    trace.Tracer
}

func NewWalletCommandProcessor(walletSvc *wallet.Service, broker Broker) *WalletCommandProcessor {
	return &WalletCommandProcessor{walletSvc: walletSvc, broker: broker, tracer: otel.Tracer("wallet-worker")}
}

func (p *WalletCommandProcessor) Run(ctx context.Context) error {
	return p.broker.Subscribe(ctx, CommandsTopic, func(msgCtx context.Context, key string, headers map[string]string, payload []byte) error {
		// msgCtx приходит от Subscribe и несёт отмену/дедлайн вызывающего
		// процесса — используем именно его, а не context.Background().
		workerCtx := ExtractTraceContext(msgCtx, headers)

		var cmd pb.WalletCommand
		if err := proto.Unmarshal(payload, &cmd); err != nil {
			return fmt.Errorf("unmarshal command: %w", err)
		}

		workerCtx, span := p.tracer.Start(workerCtx, "wallet-worker.process-command",
			trace.WithAttributes(
				attribute.String("messaging.system", "kafka"),
				attribute.String("messaging.destination", CommandsTopic),
				attribute.String("messaging.operation", "process"),
				attribute.String("wallet.msg_uuid", cmd.MsgUuid),
			))
		defer span.End()

		switch body := cmd.Body.(type) {
		case *pb.WalletCommand_CreateWallet:
			created, err := p.walletSvc.CreateWallet(workerCtx, wallet.CreateWalletInput{
				OwnerID:  body.CreateWallet.OwnerId,
				Currency: body.CreateWallet.Currency,
			})
			if err != nil {
				span.RecordError(err)
				return p.publishResult(workerCtx, &pb.WalletResult{
					MsgUuid: cmd.MsgUuid,
					Body:      &pb.WalletResult_Error{Error: &pb.WalletResultError{Message: err.Error()}},
				})
			}
			return p.publishResult(workerCtx, &pb.WalletResult{
				MsgUuid: cmd.MsgUuid,
				Body: &pb.WalletResult_CreateWallet{
					CreateWallet: &pb.CreateWalletResult{WalletId: created.ID, Balance: created.Balance, Currency: created.Currency},
				},
			})

		case *pb.WalletCommand_Transfer:
			from, to, err := p.walletSvc.Transfer(workerCtx, wallet.TransferInput{
				FromID: body.Transfer.FromWalletId,
				ToID:   body.Transfer.ToWalletId,
				Amount: body.Transfer.Amount,
			})
			if err != nil {
				span.RecordError(err)
				return p.publishResult(workerCtx, &pb.WalletResult{
					MsgUuid: cmd.MsgUuid,
					Body:      &pb.WalletResult_Error{Error: &pb.WalletResultError{Message: err.Error()}},
				})
			}
			return p.publishResult(workerCtx, &pb.WalletResult{
				MsgUuid: cmd.MsgUuid,
				Body: &pb.WalletResult_Transfer{
					Transfer: &pb.TransferResult{FromBalance: from.Balance, ToBalance: to.Balance},
				},
			})

		case *pb.WalletCommand_GetWallet:
			w, err := p.walletSvc.GetWallet(workerCtx, body.GetWallet.WalletId)
			if err != nil {
				span.RecordError(err)
				return p.publishResult(workerCtx, &pb.WalletResult{
					MsgUuid: cmd.MsgUuid,
					Body:      &pb.WalletResult_Error{Error: &pb.WalletResultError{Message: err.Error()}},
				})
			}
			return p.publishResult(workerCtx, &pb.WalletResult{
				MsgUuid: cmd.MsgUuid,
				Body: &pb.WalletResult_GetWallet{
					GetWallet: &pb.GetWalletResult{WalletId: w.ID, OwnerId: w.OwnerID, Balance: w.Balance, Currency: w.Currency},
				},
			})

		default:
			err := fmt.Errorf("unknown command body: %T", body)
			span.RecordError(err)
			return err
		}
	})
}

func (p *WalletCommandProcessor) publishResult(ctx context.Context, res *pb.WalletResult) error {
	publishCtx, span := p.tracer.Start(ctx, "wallet-worker.publish-result",
		trace.WithAttributes(
			attribute.String("messaging.system", "kafka"),
			attribute.String("messaging.destination", ResultsTopic),
			attribute.String("messaging.operation", "publish"),
			attribute.String("wallet.msg_uuid", res.MsgUuid),
		))
	defer span.End()

	payload, err := proto.Marshal(res)
	if err != nil {
		span.RecordError(err)
		return fmt.Errorf("marshal result: %w", err)
	}

	headers := InjectTraceHeaders(publishCtx)
	if err := p.broker.Publish(publishCtx, ResultsTopic, res.MsgUuid, headers, payload); err != nil {
		span.RecordError(err)
		return err
	}
	return nil
}
