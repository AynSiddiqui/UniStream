package middleware

import (
	"context"
	"time"
	"unistream/pkg/core"

	"github.com/redis/go-redis/v9"
)

// Middleware helpers should arguably be in core or a common place if they return generic types.
// But following the previous pattern:

func NewIdempotencyMiddleware(rdb *redis.Client, ttl time.Duration) func(core.Handler) core.Handler {
	return func(next core.Handler) core.Handler {
		return func(ctx context.Context, msg *core.Message) error {
			key := "unistream:idempotency:" + msg.UUID
			
			exists, err := rdb.Exists(ctx, key).Result()
			if err != nil {
				return err
			}
			if exists > 0 {
				if err := msg.Ack(); err != nil {
					return err
				}
				return nil
			}

			if err := next(ctx, msg); err != nil {
				return err
			}

			success, err := rdb.SetNX(ctx, key, "1", ttl).Result()
			if err != nil {
				return err
			}
			if !success {
				return nil
			}

			return nil
		}
	}
}
