package saramax

import (
	"encoding/json"
	"github.com/IBM/sarama"
	"test/webook/pkg/logger"
)

type Handler[T any] struct {
	fn func(msg *sarama.ConsumerMessage, evt T) error
	l  logger.Logger
}

func NewHandler[T any](fn func(msg *sarama.ConsumerMessage, evt T) error, l logger.Logger) *Handler[T] {
	return &Handler[T]{l: l, fn: fn}
}

func (h *Handler[T]) Setup(session sarama.ConsumerGroupSession) error {
	return nil
}

func (h *Handler[T]) Cleanup(session sarama.ConsumerGroupSession) error {
	return nil
}

func (h *Handler[T]) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	messages := claim.Messages()

	for msg := range messages {

		var t T
		err := json.Unmarshal(msg.Value, &t)
		if err != nil {
			h.l.Error("反序列化失败",
				logger.Any[string]("topic", msg.Topic),
				logger.Any[int32]("partition", msg.Partition),
				logger.Any[int64]("offset", msg.Offset),
				logger.Error(err),
			)
		}

		err = h.fn(msg, t)
		if err != nil {
			h.l.Error("消费失败",
				logger.Any[string]("topic", msg.Topic),
				logger.Any[int32]("partition", msg.Partition),
				logger.Any[int64]("offset", msg.Offset),
				logger.Error(err),
			)
		}

		session.MarkMessage(msg, "")
	}

	return nil
}
