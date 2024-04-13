package fixer

import (
	"context"
	"errors"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"test/webook/pkg/migrator"
)

type Fixer[T migrator.Entity] struct {
	base    *gorm.DB
	target  *gorm.DB
	columns []string
}

func NewFixer[T migrator.Entity](base *gorm.DB, target *gorm.DB) (*Fixer[T], error) {
	rows, err := base.Model(new(T)).Rows()
	if err != nil {
		return nil, err
	}
	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	return &Fixer[T]{base: base, target: target, columns: columns}, nil
}

func (f *Fixer[T]) Fix(ctx context.Context, id int64) error {
	var t T
	err := f.base.WithContext(ctx).Where("id = ?", id).First(&t).Error
	switch {
	case err == nil:
		return f.target.WithContext(ctx).Clauses(clause.OnConflict{
			DoUpdates: clause.AssignmentColumns(f.columns),
		}).Create(&t).Error
	case errors.Is(err, gorm.ErrRecordNotFound):
		return f.target.WithContext(ctx).Where("id = ?", id).Delete(&t).Error
	default:
		return err
	}
}
