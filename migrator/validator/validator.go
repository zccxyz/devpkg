package validator

import (
	"context"
	"errors"
	"github.com/ecodeclub/ekit/slice"
	"golang.org/x/sync/errgroup"
	"gorm.io/gorm"
	"test/webook/pkg/logger"
	"test/webook/pkg/migrator"
	"test/webook/pkg/migrator/events"
)

type Validator[T migrator.Entity] struct {
	base      *gorm.DB
	target    *gorm.DB
	l         logger.Logger
	producer  *events.SaramaProducer
	direction string
	batchSize int
}

func (v *Validator[T]) Validate(ctx context.Context) error {
	var eg errgroup.Group
	eg.Go(func() error {
		return v.validateBaseToTarget(ctx)
	})
	eg.Go(func() error {
		return v.validateTargetToBase(ctx)
	})
	return eg.Wait()
}

func (v *Validator[T]) validateBaseToTarget(ctx context.Context) error {
	offset := -1
	for {
		offset++
		var src T
		err := v.base.WithContext(ctx).Order("id").Offset(offset).First(&src).Error
		switch {
		case errors.Is(err, gorm.ErrRecordNotFound):
			return nil
		case err == nil:
		default:
			v.l.Error("base -> target 查询base失败", logger.Error(err), logger.Any("offset", offset))
			continue
		}

		var dst T
		err = v.target.WithContext(ctx).Order("id").Offset(offset).First(&dst).Error
		switch {
		case errors.Is(err, gorm.ErrRecordNotFound):
			v.notify(ctx, src.ID(), events.InconsistentTargetMissing)
		case err == nil:
			if !src.CompareTo(dst) {
				v.notify(ctx, src.ID(), events.InconsistentNEQ)
			}
		default:
			v.l.Error("base -> target 查询target失败", logger.Error(err), logger.Any("offset", offset))
			continue
		}
	}
}

func (v *Validator[T]) validateTargetToBase(ctx context.Context) error {
	offset := -v.batchSize
	for {
		offset += v.batchSize
		var dsts []T
		err := v.target.WithContext(ctx).Select("id").Order("id").Offset(offset).Find(&dsts).Error
		switch {
		case err == nil:
		case errors.Is(err, gorm.ErrRecordNotFound):
			return nil
		default:
			v.l.Error("target -> base 查询target出错", logger.Error(err), logger.Any("offset", offset))
			continue
		}

		ids := slice.Map(dsts, func(idx int, src T) int64 {
			return src.ID()
		})
		var srcs []T
		err = v.base.WithContext(ctx).Where("id IN ?", ids).Find(&srcs).Error
		if errors.Is(err, gorm.ErrRecordNotFound) {
			v.notifyBatch(ctx, ids, events.InconsistentBaseMissing)
			continue
		}
		if err != nil {
			v.l.Error("target -> base 查询base出错", logger.Error(err), logger.Any("ids", ids))
			continue
		}
		srcIds := slice.Map(srcs, func(idx int, src T) int64 {
			return src.ID()
		})
		diff := slice.DiffSet(ids, srcIds)
		if len(diff) > 0 {
			v.notifyBatch(ctx, diff, events.InconsistentBaseMissing)
		}

		//查询完毕
		if len(ids) < v.batchSize {
			return nil
		}
	}
}

func (v *Validator[T]) notifyBatch(ctx context.Context, ids []int64, typ string) {
	for _, id := range ids {
		v.notify(ctx, id, typ)
	}
}

func (v *Validator[T]) notify(ctx context.Context, id int64, typ string) {
	err := v.producer.InconsistentEvent(ctx, events.InconsistentEvent{
		Typ:       typ,
		ID:        id,
		Direction: v.direction,
	})
	if err != nil {
		v.l.Error("通知失败", logger.Error(err), logger.Any("id", id),
			logger.Any("type", typ), logger.Any("direction", v.direction))
	}
}
