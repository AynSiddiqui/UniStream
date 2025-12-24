package middleware

import (
	"context"
	"fmt"
	"time"
	"unistream/pkg/core"
)

// ErrRetriesExhausted is returned when all retry attempts are exhausted
var ErrRetriesExhausted = fmt.Errorf("retries exhausted")

// ErrDLQPublishFailed is returned when DLQ publish fails
var ErrDLQPublishFailed = fmt.Errorf("DLQ publish failed")

// NewRetryMiddleware creates a retry middleware with exponential backoff and DLQ support.
// If maxRetries is 0, no retries are performed.
// If dlqPublisher is nil, messages are nacked after retries are exhausted.
func NewRetryMiddleware(maxRetries int, backoff time.Duration, dlqPublisher core.Publisher, dlqTopic string) func(core.Handler) core.Handler {
	return func(next core.Handler) core.Handler {
		return func(ctx context.Context, msg *core.Message) error {
			var lastErr error

			// Track retry attempts in metadata
			if msg.Metadata == nil {
				msg.Metadata = make(map[string]string)
			}

			for attempt := 0; attempt <= maxRetries; attempt++ {
				// Add retry attempt to metadata
				msg.Metadata["retry_attempt"] = fmt.Sprintf("%d", attempt)
				msg.Metadata["max_retries"] = fmt.Sprintf("%d", maxRetries)

				// Exponential backoff (skip for first attempt)
				if attempt > 0 {
					backoffDuration := backoff * time.Duration(1<<uint(attempt-1))
					select {
					case <-ctx.Done():
						return ctx.Err()
					case <-time.After(backoffDuration):
					}
				}

				lastErr = next(ctx, msg)
				if lastErr == nil {
					// Success - clear retry metadata
					if msg.Metadata != nil {
						delete(msg.Metadata, "retry_attempt")
						delete(msg.Metadata, "max_retries")
					}
					return nil
				}

				// If it's a duplicate message error, don't retry
				if lastErr == ErrDuplicateMessage {
					return lastErr
				}
			}

			// All retries exhausted
			if dlqPublisher != nil && dlqTopic != "" {
				// Add DLQ metadata
				msg.Metadata["dlq_reason"] = "retries_exhausted"
				msg.Metadata["dlq_timestamp"] = time.Now().Format(time.RFC3339)
				msg.Metadata["original_error"] = lastErr.Error()

				// Publish to DLQ
				if pubErr := dlqPublisher.Publish(ctx, dlqTopic, msg); pubErr != nil {
					return fmt.Errorf("%w: %v (original error: %v)", ErrDLQPublishFailed, pubErr, lastErr)
				}

				// Message sent to DLQ, ack the original message
				return nil
			}

			// No DLQ configured, return error (message will be nacked)
			return fmt.Errorf("%w: %v", ErrRetriesExhausted, lastErr)
		}
	}
}
