package connpool

import (
	"context"
	"database/sql"
	"errors"
	"go.uber.org/atomic"
	"gorm.io/gorm"
	"test/webook/pkg/logger"
)

const (
	PatternSrcOnly  = "src_only"
	PatternSrcFirst = "src_first"
	PatternDstOnly  = "dst_only"
	PatternDstFirst = "dst_first"
)

var errUnknownPattern = errors.New("unknown pattern")

type DoubleWritePool struct {
	src     gorm.ConnPool
	dst     gorm.ConnPool
	pattern *atomic.String
	l       logger.Logger
}

func (d *DoubleWritePool) BeginTx(ctx context.Context, opts *sql.TxOptions) (gorm.ConnPool, error) {
	switch d.pattern.Load() {
	case PatternSrcOnly:
		src, err := d.src.(gorm.TxBeginner).BeginTx(ctx, opts)
		return &DoubleWritePoolTx{src: src, pattern: PatternSrcOnly, l: d.l}, err
	case PatternSrcFirst:
		src, err := d.src.(gorm.TxBeginner).BeginTx(ctx, opts)
		if err != nil {
			return nil, err
		}
		dst, er := d.dst.(gorm.TxBeginner).BeginTx(ctx, opts)
		if er != nil {
			d.l.Error("目标库开启事务失败", logger.Error(er))
		}
		return &DoubleWritePoolTx{src: src, pattern: PatternSrcFirst, dst: dst, l: d.l}, err
	case PatternDstFirst:
		dst, err := d.dst.(gorm.TxBeginner).BeginTx(ctx, opts)
		if err != nil {
			return nil, err
		}
		src, er := d.src.(gorm.TxBeginner).BeginTx(ctx, opts)
		if er != nil {
			d.l.Error("源库开启事务失败", logger.Error(er))
		}
		return &DoubleWritePoolTx{src: src, pattern: PatternDstFirst, dst: dst, l: d.l}, err
	case PatternDstOnly:
		dst, err := d.dst.(gorm.TxBeginner).BeginTx(ctx, opts)
		return &DoubleWritePoolTx{dst: dst, pattern: PatternDstOnly, l: d.l}, err
	default:
		return nil, errUnknownPattern
	}
}

func NewDoubleWritePool(src *gorm.DB, dst *gorm.DB, l logger.Logger) *DoubleWritePool {
	return &DoubleWritePool{src: src.ConnPool, dst: dst.ConnPool, l: l, pattern: atomic.NewString(PatternSrcOnly)}
}

func (d *DoubleWritePool) UpdatePattern(val string) error {
	switch val {
	case PatternSrcOnly, PatternSrcFirst, PatternDstOnly, PatternDstFirst:
		d.pattern.Store(val)
		return nil
	default:
		return errUnknownPattern
	}
}

func (d *DoubleWritePool) PrepareContext(ctx context.Context, query string) (*sql.Stmt, error) {
	panic("双写模式不支持 PrepareContext")
}

func (d *DoubleWritePool) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	switch d.pattern.Load() {
	case PatternSrcOnly:
		return d.src.ExecContext(ctx, query, args...)
	case PatternSrcFirst:
		result, err := d.src.ExecContext(ctx, query, args...)
		if err == nil {
			_, er := d.dst.ExecContext(ctx, query, args...)
			if er != nil {
				d.l.Error("写入dst失败", logger.Error(er), logger.Any("sql", query))
			}
		}
		return result, err
	case PatternDstFirst:
		result, err := d.dst.ExecContext(ctx, query, args...)
		if err == nil {
			_, er := d.src.ExecContext(ctx, query, args...)
			if er != nil {
				d.l.Error("写入src失败", logger.Error(er), logger.Any("sql", query))
			}
		}
		return result, err
	case PatternDstOnly:
		return d.dst.ExecContext(ctx, query, args...)
	default:
		return nil, errUnknownPattern
	}
}

func (d *DoubleWritePool) QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	switch d.pattern.Load() {
	case PatternSrcFirst, PatternSrcOnly:
		return d.src.QueryContext(ctx, query, args...)
	case PatternDstOnly, PatternDstFirst:
		return d.dst.QueryContext(ctx, query, args...)
	default:
		return nil, errUnknownPattern
	}
}

func (d *DoubleWritePool) QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row {
	switch d.pattern.Load() {
	case PatternSrcFirst, PatternSrcOnly:
		return d.src.QueryRowContext(ctx, query, args...)
	case PatternDstOnly, PatternDstFirst:
		return d.dst.QueryRowContext(ctx, query, args...)
	default:
		panic(errUnknownPattern)
	}
}

type DoubleWritePoolTx struct {
	src     *sql.Tx
	dst     *sql.Tx
	pattern string
	l       logger.Logger
}

func (d *DoubleWritePoolTx) Commit() error {
	switch d.pattern {
	case PatternSrcOnly:
		return d.src.Commit()
	case PatternSrcFirst:
		err := d.src.Commit()
		if err == nil && d.dst != nil {
			er := d.dst.Commit()
			if er != nil {
				d.l.Error("目标库提交失败", logger.Error(err))
			}
		}
		return err
	case PatternDstFirst:
		err := d.dst.Commit()
		if err == nil && d.src != nil {
			er := d.src.Commit()
			if er != nil {
				d.l.Error("源库提交失败", logger.Error(err))
			}
		}
		return err
	case PatternDstOnly:
		return d.dst.Commit()
	default:
		return errUnknownPattern
	}
}

func (d *DoubleWritePoolTx) Rollback() error {
	switch d.pattern {
	case PatternSrcOnly:
		return d.src.Rollback()
	case PatternSrcFirst:
		err := d.src.Rollback()
		if err == nil && d.dst != nil {
			er := d.dst.Rollback()
			if er != nil {
				d.l.Error("目标库回滚失败", logger.Error(err))
			}
		}
		return err
	case PatternDstFirst:
		err := d.dst.Rollback()
		if err == nil && d.src != nil {
			er := d.src.Rollback()
			if er != nil {
				d.l.Error("源库回滚失败", logger.Error(err))
			}
		}
		return err
	case PatternDstOnly:
		return d.dst.Rollback()
	default:
		return errUnknownPattern
	}
}

func (d *DoubleWritePoolTx) PrepareContext(ctx context.Context, query string) (*sql.Stmt, error) {
	panic("双写模式不支持 PrepareContext")
}

func (d *DoubleWritePoolTx) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	switch d.pattern {
	case PatternSrcOnly:
		return d.src.ExecContext(ctx, query, args...)
	case PatternSrcFirst:
		result, err := d.src.ExecContext(ctx, query, args...)
		if err == nil && d.dst != nil {
			_, er := d.dst.ExecContext(ctx, query, args...)
			if er != nil {
				d.l.Error("写入dst失败", logger.Error(er), logger.Any("sql", query))
			}
		}
		return result, err
	case PatternDstFirst:
		result, err := d.dst.ExecContext(ctx, query, args...)
		if err == nil && d.src != nil {
			_, er := d.src.ExecContext(ctx, query, args...)
			if er != nil {
				d.l.Error("写入src失败", logger.Error(er), logger.Any("sql", query))
			}
		}
		return result, err
	case PatternDstOnly:
		return d.dst.ExecContext(ctx, query, args...)
	default:
		return nil, errUnknownPattern
	}
}

func (d *DoubleWritePoolTx) QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	switch d.pattern {
	case PatternSrcFirst, PatternSrcOnly:
		return d.src.QueryContext(ctx, query, args...)
	case PatternDstOnly, PatternDstFirst:
		return d.dst.QueryContext(ctx, query, args...)
	default:
		return nil, errUnknownPattern
	}
}

func (d *DoubleWritePoolTx) QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row {
	switch d.pattern {
	case PatternSrcFirst, PatternSrcOnly:
		return d.src.QueryRowContext(ctx, query, args...)
	case PatternDstOnly, PatternDstFirst:
		return d.dst.QueryRowContext(ctx, query, args...)
	default:
		panic(errUnknownPattern)
	}
}
