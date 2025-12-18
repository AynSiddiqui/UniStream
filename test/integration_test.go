//go:build integration
// +build integration

package test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"unistream/pkg/unistream"

	"github.com/redis/go-redis/v9"
)

func TestIntegration(t *testing.T) {
	// 1. Setup
	time.Sleep(5 * time.Second)

	cfg := unistream.Config{
		Driver: unistream.DriverKafka,
		Addr:   "localhost:9092",
		Extra:  map[string]interface{}{},
	}
	
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	broker, err := unistream.Connect(ctx, cfg)
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	defer broker.Close()

	topic := "test-topic-" + fmt.Sprint(time.Now().Unix())
	groupID := "test-group-" + fmt.Sprint(time.Now().Unix())

	if err := broker.CreateTopic(ctx, topic, 1); err != nil {
		t.Logf("Create topic warning: %v", err)
	}

	// 2. Initialize Redis for verification (not used by driver directly in this test, but good check)
	rdb := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})
	if err := rdb.Ping(ctx).Err(); err != nil {
		t.Fatalf("Failed to connect to Redis: %v", err)
	}

	// 3. Publish
	pub, err := broker.NewPublisher(topic)
	if err != nil {
		t.Fatalf("NewPublisher failed: %v", err)
	}
	defer pub.Close()

	// 4. Subscribe (with options if we had them fully implemented)
	sub, err := broker.NewSubscriber(topic, groupID, unistream.WithIdempotency(true))
	if err != nil {
		t.Fatalf("NewSubscriber failed: %v", err)
	}
	defer sub.Close()

	processedCount := 0
	handler := func(ctx context.Context, msg *unistream.Message) error {
		processedCount++
		fmt.Printf("Processing message: %s\n", msg.UUID)
		_ = msg.Ack()
		return nil
	}

	go sub.Subscribe(ctx, topic, handler)

	// 6. Produce Message
	msgID := "msg-123"
	payload := []byte("hello unistream")
	
	err = pub.Publish(ctx, topic, &unistream.Message{
		UUID: msgID,
		Payload: payload,
	})
	if err != nil {
		t.Fatalf("Publish failed: %v", err)
	}

	// 7. Verify Consumption
	time.Sleep(5 * time.Second)
	if processedCount != 1 {
		t.Errorf("Expected 1 processed message, got %d", processedCount)
	}
	// Note: Idempotency is mocked/placeholder in adapter for now, so we verify basic flow only.
}
