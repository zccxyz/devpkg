package limiter

import (
	"context"
	_ "embed"
	"github.com/redis/go-redis/v9"
	"time"
)

//go:embed limit.lua
var limitLua string

type RedisSlideWindowLimiter struct {
	cmd       redis.Cmdable
	window    time.Duration
	threshold int64
}

func NewRedisSlideWindowLimiter(client redis.Cmdable, window time.Duration, threshold int64) *RedisSlideWindowLimiter {
	return &RedisSlideWindowLimiter{
		cmd:       client,
		window:    window,
		threshold: threshold,
	}
}

func (r *RedisSlideWindowLimiter) Limit(ctx context.Context, key string) (bool, error) {
	return r.cmd.Eval(
		ctx,
		limitLua,
		[]string{key},
		r.window.Milliseconds(), time.Now().UnixMilli(), r.threshold,
	).Bool()
}
