package saramax

import (
	"context"
	"encoding/json"
	"github.com/IBM/sarama"
	"test/webook/pkg/logger"
	"time"
)

type BatchHandler[T any] struct {
	fn func(msg []*sarama.ConsumerMessage, evt []T) error
	l  logger.Logger
}

func (b *BatchHandler[T]) Setup(session sarama.ConsumerGroupSession) error {
	return nil
}

func (b *BatchHandler[T]) Cleanup(session sarama.ConsumerGroupSession) error {
	return nil
}

func (b *BatchHandler[T]) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	msgCh := claim.Messages()

	batchSize := 10
	for {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		messages := make([]*sarama.ConsumerMessage, 0, batchSize)
		evts := make([]T, 0, batchSize)
		done := false

		for i := 0; i < batchSize && !done; i++ {
			select {
			case <-ctx.Done():
				done = true
			case msg, ok := <-msgCh:
				if !ok {
					cancel()
					return nil
				}

				var evt T
				err := json.Unmarshal(msg.Value, &evt)
				if err != nil {
					b.l.Error("序列化失败", logger.Error(err))
					continue
				}
				messages = append(messages, msg)
				evts = append(evts, evt)
			}
		}
		cancel()
		if len(messages) == 0 {
			continue
		}

		err := b.fn(messages, evts)
		if err != nil {
			b.l.Error("消费失败",
				logger.Error(err),
			)
		}

		for _, msg := range messages {
			session.MarkMessage(msg, "")
		}
	}
}

func NewBatchHandler[T any](fn func(msg []*sarama.ConsumerMessage, evt []T) error, l logger.Logger) *BatchHandler[T] {
	return &BatchHandler[T]{l: l, fn: fn}
}
