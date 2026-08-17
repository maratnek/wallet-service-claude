package async

import (
	"context"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/IBM/sarama"
)

// KafkaBroker реализует Broker поверх Kafka.
//
// Держит один переиспользуемый SyncProducer на весь broker вместо того,
// чтобы открывать новое соединение на каждый Publish — так публикация не
// платит за handshake каждый раз.
type KafkaBroker struct {
	addr  string
	group string

	mu       sync.Mutex
	producer sarama.SyncProducer
}

// NewKafkaBroker создаёт broker с дефолтной consumer group.
// Годится для worker'а (там она и используется), для чистой публикации
// (proxy) group не участвует в работе вовсе.
func NewKafkaBroker(addr string) *KafkaBroker {
	return NewKafkaBrokerWithGroup(addr, "wallet-worker-group")
}

// NewKafkaBrokerWithGroup создаёт broker с явно заданной consumer group.
// Каждому независимому потребителю, которому нужно видеть ВСЕ сообщения
// топика (а не делить их с другими потребителями), нужна своя уникальная
// group — так теперь и работает client, читающий wallet.results.
func NewKafkaBrokerWithGroup(addr, group string) *KafkaBroker {
	if addr == "" {
		addr = "kafka:9092"
	}
	if group == "" {
		group = "wallet-worker-group"
	}
	return &KafkaBroker{addr: addr, group: group}
}

func (b *KafkaBroker) producerClient() (sarama.SyncProducer, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.producer != nil {
		return b.producer, nil
	}

	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.RequiredAcks = sarama.WaitForLocal

	producer, err := sarama.NewSyncProducer(strings.Split(b.addr, ","), config)
	if err != nil {
		return nil, err
	}
	b.producer = producer
	return producer, nil
}

// Close закрывает переиспользуемый producer. Вызывать при graceful shutdown.
func (b *KafkaBroker) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.producer == nil {
		return nil
	}
	err := b.producer.Close()
	b.producer = nil
	return err
}

func (b *KafkaBroker) Publish(ctx context.Context, topic, key string, headers map[string]string, payload []byte) error {
	producer, err := b.producerClient()
	if err != nil {
		return err
	}

	recordHeaders := make([]sarama.RecordHeader, 0, len(headers))
	for k, v := range headers {
		recordHeaders = append(recordHeaders, sarama.RecordHeader{Key: []byte(k), Value: []byte(v)})
	}

	msg := &sarama.ProducerMessage{
		Topic:   topic,
		Value:   sarama.ByteEncoder(payload),
		Headers: recordHeaders,
	}
	if key != "" {
		msg.Key = sarama.StringEncoder(key)
	}

	_, _, err = producer.SendMessage(msg)
	return err
}

func (b *KafkaBroker) Subscribe(ctx context.Context, topic string, handler MessageHandler) error {
	config := sarama.NewConfig()
	config.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRoundRobin
	config.Consumer.Offsets.Initial = sarama.OffsetOldest
	config.Consumer.Return.Errors = true

	consumerGroup, err := sarama.NewConsumerGroup(strings.Split(b.addr, ","), b.group, config)
	if err != nil {
		return err
	}
	defer consumerGroup.Close()

	for {
		if err := consumerGroup.Consume(ctx, []string{topic}, saramaConsumerGroupHandler{ctx: ctx, handler: handler}); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return err
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		time.Sleep(time.Second)
	}
}

type saramaConsumerGroupHandler struct {
	ctx     context.Context
	handler MessageHandler
}

func (h saramaConsumerGroupHandler) Setup(_ sarama.ConsumerGroupSession) error   { return nil }
func (h saramaConsumerGroupHandler) Cleanup(_ sarama.ConsumerGroupSession) error { return nil }
func (h saramaConsumerGroupHandler) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		headers := make(map[string]string, len(msg.Headers))
		for _, hd := range msg.Headers {
			headers[string(hd.Key)] = string(hd.Value)
		}
		if err := h.handler(h.ctx, string(msg.Key), headers, msg.Value); err != nil {
			log.Println("handle message failed:", err)
		}
		sess.MarkMessage(msg, "")
	}
	return nil
}
