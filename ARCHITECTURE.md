# UniStream Architecture

## Overview

UniStream is designed as an extensible, unified streaming adapter that provides a consistent interface across different messaging systems (Kafka, Pulsar, and future frameworks like RabbitMQ).

## Core Principles

1. **Unified Interface**: Same API for all streaming frameworks
2. **Middleware Pattern**: Composable middleware for cross-cutting concerns
3. **Extensibility**: Easy to add new streaming frameworks
4. **Type Safety**: Strong typing with Go interfaces
5. **Configuration**: Flexible configuration via options pattern

## Architecture Layers

```
┌─────────────────────────────────────────┐
│         Application Layer               │
│  (Examples, User Code)                  │
└─────────────────────────────────────────┘
                    │
┌─────────────────────────────────────────┐
│         UniStream API                    │
│  (unistream package)                     │
└─────────────────────────────────────────┘
                    │
┌─────────────────────────────────────────┐
│         Core Interfaces                  │
│  (Broker, Publisher, Subscriber)        │
└─────────────────────────────────────────┘
                    │
┌─────────────────────────────────────────┐
│         Middleware Layer                 │
│  (Idempotency, Retry, Schema, etc.)     │
└─────────────────────────────────────────┘
                    │
┌─────────────────────────────────────────┐
│         Driver Layer                     │
│  (Kafka, Pulsar, RabbitMQ*)             │
└─────────────────────────────────────────┘
                    │
┌─────────────────────────────────────────┐
│         Native Clients                   │
│  (segmentio/kafka-go, pulsar-client-go)  │
└─────────────────────────────────────────┘
```

## Adding New Streaming Frameworks

To add a new streaming framework (e.g., RabbitMQ):

### 1. Create Driver Package

Create `internal/drivers/rabbitmq/adapter.go`:

```go
package rabbitmq

import (
    "context"
    "unistream/pkg/core"
    // RabbitMQ client library
)

func init() {
    core.RegisterDriver(core.DriverRabbitMQ, func(addr, user, pass string, extra map[string]interface{}) (core.Broker, error) {
        return NewBroker(addr, user, pass, extra)
    })
}

// Implement core.Broker interface
type Broker struct {
    // RabbitMQ connection
}

func (b *Broker) CreateTopic(ctx context.Context, name string, partitions int) error {
    // Create exchange/queue
}

func (b *Broker) NewPublisher(topic string, opts ...core.PublishOption) (core.Publisher, error) {
    // Return Publisher implementation
}

func (b *Broker) NewSubscriber(topic string, groupID string, opts ...core.SubscribeOption) (core.Subscriber, error) {
    // Return Subscriber implementation
}

func (b *Broker) Close() error {
    // Close connections
}
```

### 2. Add Driver Type

In `pkg/core/interfaces.go`:

```go
const (
    DriverKafka   DriverType = "kafka"
    DriverPulsar  DriverType = "pulsar"
    DriverRabbitMQ DriverType = "rabbitmq"  // Add new driver
)
```

### 3. Export in unistream package

In `pkg/unistream/unistream.go`:

```go
const (
    DriverKafka   = core.DriverKafka
    DriverPulsar  = core.DriverPulsar
    DriverRabbitMQ = core.DriverRabbitMQ  // Export new driver
)
```

### 4. Import in Application

```go
import (
    _ "unistream/internal/drivers/rabbitmq"  // Register driver
    "unistream/pkg/unistream"
)

cfg := unistream.Config{
    Driver: unistream.DriverRabbitMQ,
    Addr:   "localhost:5672",
}
```

## Middleware System

Middleware is applied in a chain:

```
Handler → ConsumerStatus → Idempotency → Retry → UserHandler
```

### Middleware Order

1. **ConsumerStatus** (outermost): Reports all status changes
2. **Idempotency**: Checks for duplicates (after retry to avoid retrying duplicates)
3. **Retry**: Retries failed messages
4. **User Handler**: Your business logic

### Adding Custom Middleware

```go
func MyCustomMiddleware(next core.Handler) core.Handler {
    return func(ctx context.Context, msg *core.Message) error {
        // Pre-processing
        err := next(ctx, msg)
        // Post-processing
        return err
    }
}

// Apply in adapter
finalHandler = MyCustomMiddleware(finalHandler)
```

## Configuration

### Subscribe Options

- `WithIdempotency(enabled, redisAddr)` - Enable idempotency
- `WithDLQ(topic)` - Enable DLQ with retry (default: 3 retries)
- `WithRetry(maxRetries, backoff)` - Custom retry configuration

### Publish Options

- `WithSchemaValidator(validator)` - Enable schema validation

## Message Flow

### Publishing

1. Schema validation (if enabled)
2. Publish to broker
3. Return error if failed

### Consuming

1. Fetch message from broker
2. ConsumerStatus: Report "processing"
3. Idempotency: Check if duplicate
   - If duplicate: Report "skipped", ack, return
4. Retry: Attempt processing
   - On error: Retry with backoff
   - After max retries: Send to DLQ (if configured)
5. User Handler: Process message
6. ConsumerStatus: Report "processed" or "error"
7. Ack message

## Error Handling

- **Duplicate Messages**: Acked without processing
- **Processing Errors**: Retried (if retry enabled)
- **Retry Exhausted**: Sent to DLQ (if DLQ enabled), otherwise nacked
- **DLQ Publish Failed**: Error returned (message not acked)

## Best Practices

1. **Always set UUID**: Required for idempotency
2. **Use DLQ**: Configure DLQ for production
3. **Schema Validation**: Validate messages at publish time
4. **Monitor Status**: Use consumer status callbacks for monitoring
5. **Graceful Shutdown**: Handle context cancellation properly

## Extensibility Points

1. **New Drivers**: Add new streaming frameworks
2. **Custom Middleware**: Add domain-specific middleware
3. **Schema Validators**: Implement custom validation logic
4. **Status Callbacks**: Custom monitoring/observability

## Future Enhancements

- RabbitMQ driver
- NATS driver
- AWS SQS/SNS driver
- Rate limiting middleware
- Circuit breaker middleware
- Metrics/observability middleware

