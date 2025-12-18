package main

import (
	"context"
	"fmt"
	"log"
	"time"
	_ "unistream/internal/drivers/memory" // Register memory driver
	"unistream/pkg/unistream"
)

func main() {
	// 1. Configuration
	// User only interacts with unistream package
	cfg := unistream.Config{
		Driver: unistream.DriverMemory, // Use Memory for local testing without CGO/Docker reqs
		Addr:   "memory://local",
		Extra: map[string]interface{}{
			"retries": 5,
		},
	}

	// 2. Connect
	ctx := context.Background()
	broker, err := unistream.Connect(ctx, cfg)
	if err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}
	defer broker.Close()

	// 3. Create Topic
	if err := broker.CreateTopic(ctx, "orders", 4); err != nil {
		log.Printf("Topic creation warning: %v", err)
	}

	// 4. Publish
	pub, err := broker.NewPublisher("orders")
	if err != nil {
		log.Fatal(err)
	}
	defer pub.Close()

	if err := pub.Publish(ctx, "orders", &unistream.Message{
		UUID:    "order-123",
		Payload: []byte(`{"id": "order-123", "amount": 100}`),
	}); err != nil {
		log.Fatal(err)
	}
	fmt.Println("Message published!")

	// 5. Subscribe with Features (Idempotency)
	sub, err := broker.NewSubscriber("orders", "billing-service", 
		unistream.WithIdempotency(true),
		unistream.WithDLQ("orders-dlq"),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer sub.Close()

	// 6. Handle
	fmt.Println("Starting subscriber...")
	go sub.Subscribe(ctx, "orders", func(c context.Context, msg *unistream.Message) error {
		fmt.Printf("Received: %s\n", string(msg.Payload))
		// Idempotency middleware would handle dedup here if fully wired
		return msg.Ack()
	})

	// Keep alive
	time.Sleep(10 * time.Second)
}
