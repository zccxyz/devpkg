package ratelimit

import (
	_ "embed"
	"fmt"
	"github.com/gin-gonic/gin"
	"log"
	"net/http"
	"test/webook/pkg/limiter"
)

type MiddlewareBuilder struct {
	limiter limiter.Limiter
}

func NewMiddlewareBuilder(l limiter.Limiter) *MiddlewareBuilder {
	return &MiddlewareBuilder{
		limiter: l,
	}
}

func (m *MiddlewareBuilder) Builder() gin.HandlerFunc {
	return func(ctx *gin.Context) {
		limit, err := m.limiter.Limit(ctx, m.key())
		if err != nil {
			log.Println("请求频繁", err)
			//这里需要根据需求判断是否限流
			ctx.AbortWithStatus(http.StatusInternalServerError)
			return
		}

		if limit {
			ctx.AbortWithStatus(http.StatusTooManyRequests)
			return
		}
	}
}

func (m *MiddlewareBuilder) key() string {
	return fmt.Sprintf("rate_limit")
}
