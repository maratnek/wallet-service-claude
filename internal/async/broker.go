package async

import "context"

// Топики Kafka. Именование не меняем относительно исходного варианта.
const (
	CommandsTopic = "wallet.commands"
	ResultsTopic  = "wallet.results"
)

// MessageHandler обрабатывает одно сообщение, прочитанное из топика.
// ctx приходит от Subscribe (с отменой родителя), headers — заголовки
// Kafka-сообщения (в них лежит W3C traceparent), payload — сырые байты
// protobuf-сообщения. Сериализацией и trace-контекстом занимается вызывающий
// код в internal/async/service.go, а не сам брокер.
type MessageHandler func(ctx context.Context, key string, headers map[string]string, payload []byte) error

// Broker описывает транспорт для асинхронной доставки сообщений.
// Не знает про protobuf и про OpenTelemetry — чистый транспортный контракт.
type Broker interface {
	Publish(ctx context.Context, topic, key string, headers map[string]string, payload []byte) error
	Subscribe(ctx context.Context, topic string, handler MessageHandler) error
}
