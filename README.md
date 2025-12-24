# UniStream

A unified, extensible streaming adapter library for Go that provides a consistent interface across multiple messaging systems (Kafka, Pulsar, and more).

## Features

- ✅ **Unified API**: Same interface for Kafka, Pulsar, and future frameworks
- ✅ **Idempotency**: Automatic duplicate message detection using Redis
- ✅ **Retry & DLQ**: Configurable retry with exponential backoff and Dead Letter Queue
- ✅ **Schema Validation**: Validate messages before publishing
- ✅ **Consumer Status**: Real-time status reporting (processing, processed, skipped, retrying, DLQ)
- ✅ **Extensible**: Easy to add new streaming frameworks
- ✅ **Production Ready**: Comprehensive error handling and observability

## Quick Start

### Installation

```bash
go get unistream
```

### Basic Usage

```go
package main

import (
    "context"
    "unistream/pkg/unistream"
    _ "unistream/internal/drivers/kafka" // Register driver
)

func main() {
    // Connect to broker
    broker, err := unistream.Connect(ctx, unistream.Config{
        Driver: unistream.DriverKafka,
        Addr:   "localhost:9092",
    })
    if err != nil {
        log.Fatal(err)
    }
    defer broker.Close()

    // Create publisher
    pub, _ := broker.NewPublisher("my-topic")
    defer pub.Close()

    // Publish message
    pub.Publish(ctx, "my-topic", &unistream.Message{
        UUID:    "msg-123",
        Payload: []byte("Hello World"),
    })

    // Create subscriber with idempotency
    sub, _ := broker.NewSubscriber("my-topic", "my-group",
        unistream.WithIdempotency(true, "localhost:6379"),
    )
    defer sub.Close()

    // Subscribe
    sub.Subscribe(ctx, "my-topic", func(ctx context.Context, msg *unistream.Message) error {
        fmt.Printf("Received: %s\n", string(msg.Payload))
        return msg.Ack()
    })
}
```

## Features in Detail

### Idempotency

Prevents duplicate message processing using Redis:

```go
sub, _ := broker.NewSubscriber("orders", "order-processor",
    unistream.WithIdempotency(true, "localhost:6379"),
)
```

**How it works:**
- Messages are tracked by UUID in Redis
- Duplicate messages are automatically skipped
- Status is reported: `⊘ Skipped UUID=xxx (duplicate)`

### Retry & DLQ

Automatic retry with exponential backoff and DLQ routing:

```go
sub, _ := broker.NewSubscriber("orders", "order-processor",
    unistream.WithDLQ("orders-dlq"), // Enables retry + DLQ
    // Or customize:
    unistream.WithRetry(5, 2*time.Second), // 5 retries, 2s initial backoff
)
```

**How it works:**
- Failed messages are retried with exponential backoff
- After max retries, messages are sent to DLQ
- Status is reported: `↻ Retrying` and `⚠ Sent to DLQ`

### Schema Validation

Validate messages before publishing:

```go
validator := middleware.NewJSONSchemaValidator()
validator.RegisterSchema("orders", func(payload []byte) error {
    var order Order
    if err := json.Unmarshal(payload, &order); err != nil {
        return err
    }
    if order.ID == "" {
        return fmt.Errorf("order ID required")
    }
    return nil
})

pub, _ := broker.NewPublisher("orders",
    unistream.WithSchemaValidator(validator),
)
```

### Consumer Status

Automatic status reporting for all messages:

```
[2025-12-24 16:30:00.123] [CONSUMER] Processing UUID=order-123
[2025-12-24 16:30:00.456] [CONSUMER] ✓ Processed UUID=order-123 (took 333ms)
[2025-12-24 16:30:01.000] [CONSUMER] ⊘ Skipped UUID=order-123 (duplicate)
[2025-12-24 16:30:02.000] [CONSUMER] ↻ Retrying UUID=order-456 (attempt 1/3)
[2025-12-24 16:30:05.000] [CONSUMER] ⚠ Sent to DLQ UUID=order-456 (retries exhausted)
```

## Supported Brokers

- ✅ **Apache Kafka** (via Redpanda or Kafka)
- ✅ **Apache Pulsar**
- 🔜 **RabbitMQ** (architecture ready, implementation pending)
- 🔜 **NATS** (architecture ready, implementation pending)

## Examples

See `cmd/` directory for complete examples:

- `cmd/example_kafka/main.go` - Kafka with idempotency
- `cmd/example_pulsar/main.go` - Pulsar with idempotency
- `cmd/example_complete/main.go` - All features (idempotency, retry, DLQ, schema)

## Architecture

UniStream uses a layered architecture:

1. **Application Layer**: Your code
2. **UniStream API**: Unified interface
3. **Core Interfaces**: Broker, Publisher, Subscriber
4. **Middleware Layer**: Idempotency, Retry, Schema, Status
5. **Driver Layer**: Kafka, Pulsar implementations
6. **Native Clients**: Underlying client libraries

See [ARCHITECTURE.md](ARCHITECTURE.md) for details on extending UniStream.

## Configuration

### Subscribe Options

- `WithIdempotency(enabled, redisAddr)` - Enable idempotency
- `WithDLQ(topic)` - Enable retry + DLQ (default: 3 retries, 1s backoff)
- `WithRetry(maxRetries, backoff)` - Custom retry configuration

### Publish Options

- `WithSchemaValidator(validator)` - Enable schema validation

## Requirements

- Go 1.24+
- Redis (for idempotency)
- Kafka/Redpanda or Pulsar broker

## Development

### Running Examples

```bash
# Start infrastructure
cd deploy
docker-compose up -d

# Run Kafka example
go run cmd/example_kafka/main.go

# Run Pulsar example
go run cmd/example_pulsar/main.go

# Run complete example (all features)
go run cmd/example_complete/main.go
```

### Adding New Drivers

See [ARCHITECTURE.md](ARCHITECTURE.md) for instructions on adding new streaming frameworks.

## License

[Your License Here]

## Contributing

[Contributing Guidelines]

