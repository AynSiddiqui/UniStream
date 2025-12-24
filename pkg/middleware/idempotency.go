package middleware

import (
	"context"
	"errors"
	"fmt"
	"time"
	"unistream/pkg/core"

	"github.com/redis/go-redis/v9"
)

var (
	// ErrDuplicateMessage is returned when a message has already been processed
	ErrDuplicateMessage = errors.New("duplicate message already processed")
)

// NewIdempotencyMiddleware creates a middleware that ensures messages are processed only once
// based on their UUID. It uses Redis to track processed messages with a TTL.
// 
// The middleware works as follows:
// 1. For messages without UUID, it processes them normally
// 2. For duplicate messages (already in Redis), it returns ErrDuplicateMessage
//    The subscriber should ack these messages without processing
// 3. For new messages, it acquires a lock in Redis and processes them
//    If processing fails, the lock remains to prevent reprocessing
func NewIdempotencyMiddleware(rdb *redis.Client, ttl time.Duration) func(core.Handler) core.Handler {
	return func(next core.Handler) core.Handler {
		return func(ctx context.Context, msg *core.Message) error {
			if msg.UUID == "" {
				// If no UUID, process normally (shouldn't happen in practice)
				return next(ctx, msg)
			}

			key := "unistream:idempotency:" + msg.UUID
			
			// Check if message was already processed
			exists, err := rdb.Exists(ctx, key).Result()
			if err != nil {
				return fmt.Errorf("idempotency check failed: %w", err)
			}
			
			if exists > 0 {
				// Message already processed, return special error to signal duplicate
				// The subscriber will ack this message without calling the handler
				return ErrDuplicateMessage
			}

			// Try to acquire lock using SETNX (set if not exists)
			// This prevents race conditions if the same message arrives concurrently
			acquired, err := rdb.SetNX(ctx, key, "1", ttl).Result()
			if err != nil {
				return fmt.Errorf("idempotency lock acquisition failed: %w", err)
			}
			
			if !acquired {
				// Another goroutine/process already acquired the lock (race condition)
				// Return duplicate error so subscriber can ack without processing
				return ErrDuplicateMessage
			}

			// Process the message
			if err := next(ctx, msg); err != nil {
				// On error, we keep the key in Redis to prevent reprocessing
				// This ensures true idempotency - even failed messages won't be reprocessed
				// The message will be nacked by the subscriber, but the lock remains
				return err
			}

			// Message processed successfully, the key remains in Redis with TTL
			// This ensures we don't reprocess even if ack fails
			return nil
		}
	}
}
