package events

import (
	"context"
	"errors"
	"github.com/IBM/sarama"
	"gorm.io/gorm"
	"test/webook/pkg/logger"
	"test/webook/pkg/migrator"
	"test/webook/pkg/migrator/fixer"
	"test/webook/pkg/saramax"
	"time"
)

type SaramaConsumer[T migrator.Entity] struct {
	client   sarama.Client
	topic    string
	l        logger.Logger
	srtFixer *fixer.Fixer[T]
	dstFixer *fixer.Fixer[T]
}

func NewSaramaConsumer[T migrator.Entity](client sarama.Client, topic string, l logger.Logger, src *gorm.DB, dst *gorm.DB) (*SaramaConsumer[T], error) {
	srtFixer, err := fixer.NewFixer[T](src, dst)
	if err != nil {
		return nil, err
	}
	dstFixer, err := fixer.NewFixer[T](dst, src)
	if err != nil {
		return nil, err
	}
	return &SaramaConsumer[T]{client: client, topic: topic, l: l, srtFixer: srtFixer, dstFixer: dstFixer}, nil
}

func (s *SaramaConsumer[T]) Start() error {
	cg, err := sarama.NewConsumerGroupFromClient("migrator_fix", s.client)
	if err != nil {
		return err
	}

	go func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		//单个消费，尽量不影响线上服务正常运行
		er := cg.Consume(ctx, []string{s.topic}, saramax.NewHandler[InconsistentEvent](s.Consume, s.l))
		if er != nil {
			s.l.Error("退出数据修复消费者", logger.Error(err))
		}
	}()

	return nil
}

func (s *SaramaConsumer[T]) Consume(msg *sarama.ConsumerMessage, evt InconsistentEvent) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()

	switch evt.Direction {
	case "src":
		return s.srtFixer.Fix(ctx, evt.ID)
	case "dst":
		return s.dstFixer.Fix(ctx, evt.ID)
	default:
		return errors.New("未知 direction")
	}
}
