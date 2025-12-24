# UniStream Examples

This directory contains example applications demonstrating how to use UniStream with idempotency support for both Kafka and Pulsar.

## Prerequisites

1. **Docker and Docker Compose** - Required to run the infrastructure services
2. **Go 1.24+** - Required to build and run the examples
3. **Redis** - Running via Docker Compose (for idempotency)

## Quick Start

### 1. Start Infrastructure Services

From the project root, start all required services:

```bash
cd deploy
docker-compose up -d
```

This will start:
- **Redpanda** (Kafka-compatible) on port `9092`
- **Apache Pulsar** on port `6650`
- **Redis** on port `6379`

Wait about 10-15 seconds for all services to be ready.

### 2. Run Examples

#### Example 1: Basic Kafka Example with Idempotency

```bash
go run cmd/example/main.go
```

**What it does:**
- Connects to Kafka (Redpanda)
- Creates a topic called "orders"
- Subscribes with idempotency enabled
- Publishes the same message 4 times (order-123)
- Publishes one unique message (order-456)
- **Expected Result:** Only 2 messages should be processed (order-123 once, order-456 once)

#### Example 2: Kafka Example (Detailed)

```bash
go run cmd/example_kafka/main.go
```

**What it does:**
- Connects to Kafka (Redpanda)
- Creates a topic called "my-kafka-topic"
- Subscribes with idempotency enabled
- Publishes 3 unique messages (ord-1, ord-2, ord-3)
- Publishes 3 duplicate messages (all with UUID ord-1)
- **Expected Result:** Only 3 messages should be processed (ord-1, ord-2, ord-3, each once)

#### Example 3: Pulsar Example

```bash
go run cmd/example_pulsar/main.go
```

**What it does:**
- Connects to Apache Pulsar
- Creates a topic called "my-pulsar-topic"
- Subscribes with idempotency enabled
- Publishes 3 unique messages (msg-1, msg-2, msg-3)
- Publishes 3 duplicate messages (all with UUID msg-1)
- **Expected Result:** Only 3 messages should be processed (msg-1, msg-2, msg-3, each once)

## Understanding Idempotency

All examples demonstrate **idempotency** - the ability to process the same message only once, even if it arrives multiple times.

### How It Works

1. **Message UUID**: Each message must have a unique `UUID` field
2. **Redis Tracking**: The middleware uses Redis to track processed message UUIDs
3. **Duplicate Detection**: When a message with a known UUID arrives:
   - The middleware checks Redis
   - If found, it returns `ErrDuplicateMessage`
   - The subscriber acks the message without processing
   - The handler is never called

### Key Features

- **Automatic Deduplication**: Duplicate messages are automatically skipped
- **Race Condition Safe**: Uses Redis SETNX to prevent concurrent processing
- **TTL Support**: Processed message UUIDs expire after 24 hours (configurable)
- **Works with Both Kafka and Pulsar**: Same middleware works for both brokers

## Expected Output

When running any example, you should see:

```
=== UniStream Kafka Example ===
Connecting to Kafka...
✓ Connected to Kafka
Creating topic 'orders'...
✓ Topic 'orders' created

Starting subscriber with idempotency enabled...
✓ Subscriber created with idempotency middleware

=== Publishing Messages ===
[TEST] Publishing the same message 3 times with UUID: order-123
  → Publishing attempt 1 with UUID: order-123
  → Publishing attempt 2 with UUID: order-123
  → Publishing attempt 3 with UUID: order-123

[PROCESSED] UUID=order-123 | Payload={"id": "order-123", ...} | Total processed: 1 | This UUID processed: 1 time(s)

[TEST] Publishing a new unique message with UUID: order-456
[PROCESSED] UUID=order-456 | Payload={"id": "order-456", ...} | Total processed: 2 | This UUID processed: 1 time(s)

=== Final Results ===
Total messages processed: 2
Expected: 2 (order-123 once, order-456 once)

Processing summary by UUID:
  - order-123: 1 time(s)
  - order-456: 1 time(s)

✓ SUCCESS: Idempotency is working correctly!
```

## Troubleshooting

### Services Not Starting

If Docker services fail to start:
```bash
cd deploy
docker-compose down
docker-compose up -d
```

Check service status:
```bash
docker-compose ps
```

### Connection Errors

- **Kafka/Redpanda**: Ensure Redpanda is running on `localhost:9092`
- **Pulsar**: Ensure Pulsar is running on `localhost:6650`
- **Redis**: Ensure Redis is running on `localhost:6379`

### Idempotency Not Working

1. Check Redis is running: `docker ps | grep redis`
2. Check Redis connection: `redis-cli ping` (should return `PONG`)
3. Verify message UUIDs are set correctly
4. Check Redis keys: `redis-cli KEYS "unistream:idempotency:*"`

## Building Binaries

You can also build the examples as standalone binaries:

```bash
# Build all examples
go build -o bin/example.exe cmd/example/main.go
go build -o bin/example_kafka.exe cmd/example_kafka/main.go
go build -o bin/example_pulsar.exe cmd/example_pulsar/main.go

# Run the binaries
./bin/example.exe
./bin/example_kafka.exe
./bin/example_pulsar.exe
```

## Code Structure

Each example follows this pattern:

1. **Configuration**: Set up broker connection config
2. **Connect**: Establish connection to the broker
3. **Create Topic**: Create the topic (if needed)
4. **Subscribe**: Create subscriber with idempotency middleware
5. **Publish**: Send messages (including duplicates to test)
6. **Verify**: Check that duplicates were skipped

## Next Steps

- Modify the examples to test different scenarios
- Add your own message processing logic
- Experiment with different TTL values for idempotency
- Test with multiple subscribers to see how idempotency works across instances

