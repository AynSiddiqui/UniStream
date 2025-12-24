package middleware

import (
	"context"
	"fmt"
	"time"
	"unistream/pkg/core"
)

// ConsumerStatusCallback is called to report consumer status
type ConsumerStatusCallback func(status ConsumerStatus)

// ConsumerStatus represents the status of message consumption
type ConsumerStatus struct {
	UUID          string
	Status        string // "processing", "processed", "skipped", "retrying", "dlq", "error"
	Attempt       int
	MaxRetries    int
	IsDuplicate   bool
	Error         error
	Timestamp     time.Time
	ProcessingTime time.Duration
}

// NewConsumerStatusMiddleware creates middleware that reports consumer status
func NewConsumerStatusMiddleware(callback ConsumerStatusCallback) func(core.Handler) core.Handler {
	if callback == nil {
		// Default callback that logs to stdout
		callback = func(status ConsumerStatus) {
			timestamp := status.Timestamp.Format("2006-01-02 15:04:05.000")
			switch status.Status {
			case "processing":
				fmt.Printf("[%s] [CONSUMER] Processing UUID=%s\n", timestamp, status.UUID)
			case "processed":
				fmt.Printf("[%s] [CONSUMER] ✓ Processed UUID=%s (took %v)\n", timestamp, status.UUID, status.ProcessingTime)
			case "skipped":
				fmt.Printf("[%s] [CONSUMER] ⊘ Skipped UUID=%s (duplicate)\n", timestamp, status.UUID)
			case "retrying":
				fmt.Printf("[%s] [CONSUMER] ↻ Retrying UUID=%s (attempt %d/%d)\n", timestamp, status.UUID, status.Attempt, status.MaxRetries)
			case "dlq":
				fmt.Printf("[%s] [CONSUMER] ⚠ Sent to DLQ UUID=%s (retries exhausted)\n", timestamp, status.UUID)
			case "error":
				fmt.Printf("[%s] [CONSUMER] ✗ Error UUID=%s: %v\n", timestamp, status.UUID, status.Error)
			}
		}
	}

	return func(next core.Handler) core.Handler {
		return func(ctx context.Context, msg *core.Message) error {
			startTime := time.Now()
			
			// Report processing start
			callback(ConsumerStatus{
				UUID:      msg.UUID,
				Status:    "processing",
				Timestamp: startTime,
			})

			// Process message
			err := next(ctx, msg)
			processingTime := time.Since(startTime)

			if err != nil {
				if err == ErrDuplicateMessage {
					// Duplicate message - skipped by idempotency
					callback(ConsumerStatus{
						UUID:          msg.UUID,
						Status:        "skipped",
						IsDuplicate:   true,
						Timestamp:     time.Now(),
						ProcessingTime: processingTime,
					})
					return err
				}

				// Check if this is a retry attempt
				attempt := 0
				maxRetries := 0
				if msg.Metadata != nil {
					if a, ok := msg.Metadata["retry_attempt"]; ok {
						fmt.Sscanf(a, "%d", &attempt)
					}
					if m, ok := msg.Metadata["max_retries"]; ok {
						fmt.Sscanf(m, "%d", &maxRetries)
					}
				}

				if attempt > 0 {
					// Retry attempt
					callback(ConsumerStatus{
						UUID:          msg.UUID,
						Status:        "retrying",
						Attempt:        attempt,
						MaxRetries:     maxRetries,
						Error:          err,
						Timestamp:      time.Now(),
						ProcessingTime: processingTime,
					})
				} else {
					// First attempt error
					callback(ConsumerStatus{
						UUID:          msg.UUID,
						Status:        "error",
						Error:         err,
						Timestamp:     time.Now(),
						ProcessingTime: processingTime,
					})
				}
				return err
			}

			// Check if sent to DLQ (metadata added by retry middleware)
			if msg.Metadata != nil {
				if _, ok := msg.Metadata["dlq_reason"]; ok {
					callback(ConsumerStatus{
						UUID:          msg.UUID,
						Status:        "dlq",
						Timestamp:     time.Now(),
						ProcessingTime: processingTime,
					})
					return nil
				}
			}

			// Success
			callback(ConsumerStatus{
				UUID:          msg.UUID,
				Status:        "processed",
				Timestamp:     time.Now(),
				ProcessingTime: processingTime,
			})

			return nil
		}
	}
}

