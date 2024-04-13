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
	"time"
)

// Validator uTime==0 && sleepInterval==0 全量校验，校验完毕后退出
// uTime==0 && sleepInterval>0 全量校验，校验完毕后继续增量校验
// uTime近期时间点 && sleepInterval>0 校验近期数据，后续保持增量校验
type Validator[T migrator.Entity] struct {
	base          *gorm.DB
	target        *gorm.DB
	l             logger.Logger
	producer      events.Producer
	direction     string
	batchSize     int
	uTime         int64
	sleepInterval time.Duration
	queryBase     func(ctx context.Context, offset int) (T, error)
}

func NewValidator[T migrator.Entity](base *gorm.DB, target *gorm.DB, l logger.Logger, producer events.Producer, direction string, batchSize int) *Validator[T] {
	return &Validator[T]{base: base, target: target, l: l, producer: producer, direction: direction, batchSize: batchSize}
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
	offset := 0
	for {
		src, err := v.queryBase(ctx, offset)
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
			v.l.Debug("base -> target 取消")
			return err
		}
		if errors.Is(err, gorm.ErrRecordNotFound) {
			if v.sleepInterval <= 0 {
				return nil
			}
			time.Sleep(v.sleepInterval)
			continue
		}
		if err != nil {
			v.l.Error("base -> target 查询base失败", logger.Error(err), logger.Any("offset", offset))
			offset++
			continue
		}

		var dst T
		err = v.target.WithContext(ctx).Where("id = ?", src.ID()).Offset(offset).First(&dst).Error
		switch {
		case errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled):
			v.l.Debug("base -> target 取消")
			return err
		case errors.Is(err, gorm.ErrRecordNotFound):
			v.notify(ctx, src.ID(), events.InconsistentTargetMissing)
		case err == nil:
			if !src.CompareTo(dst) {
				v.notify(ctx, src.ID(), events.InconsistentNEQ)
			}
		default:
			v.l.Error("base -> target 查询target失败", logger.Error(err), logger.Any("offset", offset))
		}
		offset++
	}
}

func (v *Validator[T]) validateTargetToBase(ctx context.Context) error {
	offset := 0
	for {
		var dsts []T
		err := v.target.WithContext(ctx).Select("id").Order("id").Limit(v.batchSize).Offset(offset).Find(&dsts).Error
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
			v.l.Debug("target -> base 取消")
			return err
		}
		if errors.Is(err, gorm.ErrRecordNotFound) {
			if v.sleepInterval <= 0 {
				return nil
			}
			time.Sleep(v.sleepInterval)
			continue
		}
		if err != nil {
			v.l.Error("target -> base 查询target出错", logger.Error(err), logger.Any("offset", offset))
			offset += len(dsts)
			continue
		}

		ids := slice.Map(dsts, func(idx int, src T) int64 {
			return src.ID()
		})
		var srcs []T
		err = v.base.WithContext(ctx).Where("id IN ?", ids).Find(&srcs).Error
		switch {
		case errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled):
			v.l.Debug("target -> base 取消")
			return err
		case errors.Is(err, gorm.ErrRecordNotFound):
			v.notifyBatch(ctx, ids, events.InconsistentBaseMissing)
		case err == nil:
			srcIds := slice.Map(srcs, func(idx int, src T) int64 {
				return src.ID()
			})
			diff := slice.DiffSet(ids, srcIds)
			if len(diff) > 0 {
				v.notifyBatch(ctx, diff, events.InconsistentBaseMissing)
			}
		default:
			v.l.Error("target -> base 查询base出错", logger.Error(err), logger.Any("ids", ids))
		}

		//查询完毕
		if len(ids) < v.batchSize {
			if v.sleepInterval <= 0 {
				return nil
			}
			time.Sleep(v.sleepInterval)
		}
		offset += len(dsts)
	}
}

func (v *Validator[T]) SetUTime(val int64) *Validator[T] {
	v.uTime = val
	return v
}

func (v *Validator[T]) SetSleepInterval(val time.Duration) *Validator[T] {
	v.sleepInterval = val
	return v
}

// SetFull 全量
func (v *Validator[T]) SetFull() *Validator[T] {
	v.queryBase = v.fullQuery
	return v
}

// SetIncr 增量
func (v *Validator[T]) SetIncr() *Validator[T] {
	v.queryBase = v.IncrQuery
	return v
}

func (v *Validator[T]) fullQuery(ctx context.Context, offset int) (T, error) {
	dbCtx, cancel := context.WithTimeout(ctx, time.Second*10)
	defer cancel()
	var src T
	err := v.base.WithContext(dbCtx).Order("id").Offset(offset).First(&src).Error
	return src, err
}

func (v *Validator[T]) IncrQuery(ctx context.Context, offset int) (T, error) {
	var src T
	ctx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	err := v.base.WithContext(ctx).Where("utime >= ?", v.uTime).Order("utime").Offset(offset).First(&src).Error
	return src, err
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
