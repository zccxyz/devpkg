package scheduler

import (
	"context"
	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
	"net/http"
	"sync"
	"test/webook/pkg/ginx"
	"test/webook/pkg/gormx/connpool"
	"test/webook/pkg/logger"
	"test/webook/pkg/migrator"
	"test/webook/pkg/migrator/events"
	"test/webook/pkg/migrator/validator"
	"time"
)

type Scheduler[T migrator.Entity] struct {
	pattern         string
	doubleWritePool *connpool.DoubleWritePool
	lock            sync.Mutex
	src             *gorm.DB
	dst             *gorm.DB
	l               logger.Logger
	producer        events.Producer
	cancelFull      func()
	cancelIncr      func()
}

func NewScheduler[T migrator.Entity](doubleWritePool *connpool.DoubleWritePool, src *gorm.DB, dst *gorm.DB, l logger.Logger, producer events.Producer) *Scheduler[T] {
	return &Scheduler[T]{
		doubleWritePool: doubleWritePool,
		src:             src,
		dst:             dst,
		l:               l,
		producer:        producer,
		pattern:         connpool.PatternSrcOnly,
		cancelFull: func() {

		},
		cancelIncr: func() {

		},
	}
}

func (s *Scheduler[T]) RegisterRoutes(server *gin.RouterGroup) {
	server.POST("/start/full", s.StartFull)
	server.POST("/stop/full", s.StopFull)
	server.POST("/start/incr", s.StartIncr)
	server.POST("/stop/incr", s.StopIncr)
	server.POST("/src_only", s.SrcOnly)
	server.POST("/src_first", s.SrcFirst)
	server.POST("/dst_first", s.DstFirst)
	server.POST("/dst_only", s.DstOnly)
}

func (s *Scheduler[T]) DstOnly(ctx *gin.Context) {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.pattern = connpool.PatternDstOnly
	err := s.doubleWritePool.UpdatePattern(s.pattern)
	if err != nil {
		ctx.JSON(http.StatusOK, ginx.Result{
			Code: 1,
			Msg:  err.Error(),
		})
	}
	ctx.JSON(http.StatusOK, ginx.Result{Msg: s.pattern + " OK"})
}

func (s *Scheduler[T]) DstFirst(ctx *gin.Context) {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.pattern = connpool.PatternDstFirst
	err := s.doubleWritePool.UpdatePattern(s.pattern)
	if err != nil {
		ctx.JSON(http.StatusOK, ginx.Result{
			Code: 1,
			Msg:  err.Error(),
		})
	}
	ctx.JSON(http.StatusOK, ginx.Result{Msg: s.pattern + " OK"})
}

func (s *Scheduler[T]) SrcFirst(ctx *gin.Context) {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.pattern = connpool.PatternSrcFirst
	err := s.doubleWritePool.UpdatePattern(s.pattern)
	if err != nil {
		ctx.JSON(http.StatusOK, ginx.Result{
			Code: 1,
			Msg:  err.Error(),
		})
	}
	ctx.JSON(http.StatusOK, ginx.Result{Msg: s.pattern + " OK"})
}

func (s *Scheduler[T]) SrcOnly(ctx *gin.Context) {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.pattern = connpool.PatternSrcOnly
	err := s.doubleWritePool.UpdatePattern(s.pattern)
	if err != nil {
		ctx.JSON(http.StatusOK, ginx.Result{
			Code: 1,
			Msg:  err.Error(),
		})
	}
	ctx.JSON(http.StatusOK, ginx.Result{Msg: s.pattern + " OK"})
}

func (s *Scheduler[T]) StopIncr(ctx *gin.Context) {
	s.lock.Lock()
	defer s.lock.Unlock()

	s.cancelIncr()
	ctx.JSON(http.StatusOK, ginx.Result{Msg: "增量校验取消成功"})
}

func (s *Scheduler[T]) StartIncr(ctx *gin.Context) {
	s.lock.Lock()
	defer s.lock.Unlock()

	v := s.newValidator()
	v.SetIncr().SetUTime(time.Now().UnixMilli()).SetSleepInterval(time.Second)

	cancel := s.cancelIncr
	newCtx, newCancel := context.WithCancel(context.Background())
	s.cancelFull = newCancel
	go func() {
		//取消之前的
		cancel()
		//开启增量校验
		err := v.Validate(newCtx)
		if err != nil {
			s.l.Error("增量校验退出", logger.Error(err))
		}
	}()
	ctx.JSON(http.StatusOK, ginx.Result{Msg: "开始增量校验"})
}

func (s *Scheduler[T]) StopFull(ctx *gin.Context) {
	s.lock.Lock()
	defer s.lock.Unlock()

	s.cancelFull()
	ctx.JSON(http.StatusOK, ginx.Result{Msg: "全量校验取消成功"})
}

func (s *Scheduler[T]) StartFull(ctx *gin.Context) {
	s.lock.Lock()
	defer s.lock.Unlock()

	v := s.newValidator()
	v.SetFull()

	cancel := s.cancelFull
	newCtx, newCancel := context.WithCancel(context.Background())
	s.cancelFull = newCancel
	go func() {
		//取消之前的
		cancel()
		//开启全量校验
		err := v.Validate(newCtx)
		if err != nil {
			s.l.Error("全量校验退出", logger.Error(err))
		}
	}()
	ctx.JSON(http.StatusOK, ginx.Result{Msg: "开始全量校验"})
}

func (s *Scheduler[T]) newValidator() *validator.Validator[T] {
	//判断方向
	switch s.pattern {
	case connpool.PatternSrcOnly, connpool.PatternSrcFirst:
		return validator.NewValidator[T](s.src, s.dst, s.l, s.producer, "src", 100)
	case connpool.PatternDstFirst, connpool.PatternDstOnly:
		return validator.NewValidator[T](s.dst, s.src, s.l, s.producer, "dst", 100)
	default:
		return nil
	}
}
