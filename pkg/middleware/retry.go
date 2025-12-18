package middleware

import (
	"context"
	"fmt"
	"time"
	"unistream/pkg/core"
)

func NewRetryMiddleware(maxRetries int, backoff time.Duration, dlqPublisher core.Publisher, dlqTopic string) func(core.Handler) core.Handler {
	return func(next core.Handler) core.Handler {
		return func(ctx context.Context, msg *core.Message) error {
			var err error
			for i := 0; i <= maxRetries; i++ {
				if i > 0 {
					select {
					case <-ctx.Done():
						return ctx.Err()
					case <-time.After(backoff * time.Duration(1<<uint(i-1))):
					}
				}

				err = next(ctx, msg)
				if err == nil {
					return nil
				}
			}

			if dlqPublisher != nil {
				if pubErr := dlqPublisher.Publish(ctx, dlqTopic, msg); pubErr != nil {
					return fmt.Errorf("failed to retry and failed to publish to DLQ: %v (orig: %v)", pubErr, err)
				}
				if err := msg.Ack(); err != nil {
					return  err
				}
				return nil
			}

			return fmt.Errorf("retries exhausted: %w", err)
		}
	}
}
