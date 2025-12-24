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

	// 4. Subscribe with idempotency enabled
	sub, err := broker.NewSubscriber(topic, groupID, 
		unistream.WithIdempotency(true, "localhost:6379"),
	)
	if err != nil {
		t.Fatalf("NewSubscriber failed: %v", err)
	}
	defer sub.Close()

	processedCount := 0
	handler := func(ctx context.Context, msg *unistream.Message) error {
		processedCount++
		fmt.Printf("Processing message: %s\n", msg.UUID)
		// Ack is handled by the subscriber after successful processing
		return nil
	}

	go sub.Subscribe(ctx, topic, handler)

	// 5. Produce unique messages
	msgID1 := "msg-123"
	msgID2 := "msg-456"
	payload := []byte("hello unistream")
	
	// Publish first message
	err = pub.Publish(ctx, topic, &unistream.Message{
		UUID: msgID1,
		Payload: payload,
	})
	if err != nil {
		t.Fatalf("Publish failed: %v", err)
	}

	// Wait a bit for processing
	time.Sleep(2 * time.Second)

	// Publish duplicate message (should be skipped)
	err = pub.Publish(ctx, topic, &unistream.Message{
		UUID: msgID1,
		Payload: []byte("duplicate message"),
	})
	if err != nil {
		t.Fatalf("Publish failed: %v", err)
	}

	// Publish second unique message
	err = pub.Publish(ctx, topic, &unistream.Message{
		UUID: msgID2,
		Payload: payload,
	})
	if err != nil {
		t.Fatalf("Publish failed: %v", err)
	}

	// 6. Verify Consumption
	time.Sleep(5 * time.Second)
	if processedCount != 2 {
		t.Errorf("Expected 2 processed messages (msg-123 and msg-456), got %d", processedCount)
	}

	// 7. Verify idempotency: Publish duplicate again
	err = pub.Publish(ctx, topic, &unistream.Message{
		UUID: msgID1,
		Payload: []byte("another duplicate"),
	})
	if err != nil {
		t.Fatalf("Publish failed: %v", err)
	}

	time.Sleep(2 * time.Second)
	// Count should still be 2 (duplicate should be skipped)
	if processedCount != 2 {
		t.Errorf("Expected 2 processed messages after duplicate, got %d", processedCount)
	}
}
