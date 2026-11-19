# UniStream

## Status

This library is a prototype with known message-loss defects. It cannot be imported from outside this repository. Do not use it in production. A successor Kafka-only library is being written and will be linked here.

### Known defects

This incomplete list covers message-loss and install defects only; these findings are from code reading, not yet reproduced by a test, except the external import failure.

- Kafka Nack does nothing, and a later offset can be committed past a failed message (`internal/drivers/kafka/adapter.go:220`, `internal/drivers/kafka/adapter.go:241`).
- An idempotency claim remains after failure or a crash, so redelivery can be acknowledged without processing (`pkg/middleware/idempotency.go:49`).
- A handler can acknowledge before failing while the driver also acknowledges success (`pkg/core/interfaces.go:23`, `internal/drivers/kafka/adapter.go:245`).
- The module path is not normally fetchable and external code cannot import the internal drivers needed for registration (`go.mod:1`, `pkg/unistream/unistream.go:38`).
- The Kafka writer does not set RequiredAcks, so Publish can succeed without the broker storing the message (`internal/drivers/kafka/adapter.go:81`).
- The Kafka key is used as the message ID, so idempotency can drop distinct messages sharing a key (`internal/drivers/kafka/adapter.go:135`, `internal/drivers/kafka/adapter.go:214`).

A unified, extensible streaming adapter library for Go that provides a consistent interface across multiple messaging systems (Kafka, Pulsar, and more).

## Features

- ✅ **Unified API**: Same interface for Kafka, Pulsar, and future frameworks
- ✅ **Idempotency**: Automatic duplicate message detection using Redis
- ✅ **Retry & DLQ**: Configurable retry with exponential backoff and Dead Letter Queue
- ✅ **Schema Validation**: Validate messages before publishing
- ✅ **Consumer Status**: Real-time status reporting (processing, processed, skipped, retrying, DLQ)
- ✅ **Extensible**: Easy to add new streaming frameworks

## Quick Start

### Installation

The module is named `unistream` and is not published to a Go module proxy, so `go get unistream` will not work. Clone the repository and point your module at it with a `replace` directive:

```bash
git clone https://github.com/AynSiddiqui/UniStream.git ../UniStream
```

Run this from your project directory so the clone sits next to it. Then, in your own project's `go.mod`:

```
require unistream v0.0.0

replace unistream => ../UniStream
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

### Local Infrastructure

`deploy/docker-compose.yaml` starts everything the examples need:

| Service | Image | Ports |
| --- | --- | --- |
| Redpanda (Kafka API) | `redpandadata/redpanda:v24.1.5` | 9092, 8081, 8082 |
| Redis (idempotency) | `redis:7-alpine` | 6379 |
| Pulsar | `apachepulsar/pulsar:3.2.0` | 6650, 8080 |

Stop and remove them with `docker-compose down` from the `deploy` directory.

### Adding New Drivers

See [ARCHITECTURE.md](ARCHITECTURE.md) for instructions on adding new streaming frameworks.

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.

## Contributing

Contributions are welcome. Open an issue to discuss a change, then send a pull request from a feature branch. Run `go test ./...` before submitting.

