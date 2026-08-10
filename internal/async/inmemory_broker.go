package async

import (
	"context"
	"sync"
)

// InMemoryBroker — реализация Broker без реального Kafka: для юнит-тестов
// и локальной разработки. Publish синхронно регистрирует сообщение и
// асинхронно доставляет его всем подписчикам топика.
type InMemoryBroker struct {
	mu   sync.RWMutex
	subs map[string][]MessageHandler
}

func NewInMemoryBroker() *InMemoryBroker {
	return &InMemoryBroker{subs: make(map[string][]MessageHandler)}
}

func (b *InMemoryBroker) Publish(ctx context.Context, topic, key string, headers map[string]string, payload []byte) error {
	b.mu.RLock()
	handlers := append([]MessageHandler(nil), b.subs[topic]...)
	b.mu.RUnlock()

	for _, h := range handlers {
		go func(handler MessageHandler) {
			_ = handler(ctx, key, headers, payload)
		}(h)
	}
	return nil
}

func (b *InMemoryBroker) Subscribe(ctx context.Context, topic string, handler MessageHandler) error {
	b.mu.Lock()
	b.subs[topic] = append(b.subs[topic], handler)
	b.mu.Unlock()
	return nil
}
