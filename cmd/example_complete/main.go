package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"
	_ "unistream/internal/drivers/kafka"
	"unistream/pkg/middleware"
	"unistream/pkg/unistream"
)

// Order represents a sample message schema
type Order struct {
	ID     string  `json:"id"`
	Amount float64 `json:"amount"`
	Status string  `json:"status"`
}

func main() {
	fmt.Println("=== UniStream Complete Example ===")
	fmt.Println("Features: Idempotency, Retry, DLQ, Schema Validation, Consumer Status")
	fmt.Println()

	cfg := unistream.Config{
		Driver: unistream.DriverKafka,
		Addr:   "localhost:9092",
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	broker, err := unistream.Connect(ctx, cfg)
	if err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}
	defer broker.Close()

	topic := "orders"
	dlqTopic := "orders-dlq"
	groupID := fmt.Sprintf("order-processor-%d", time.Now().Unix())

	// Create topics
	broker.CreateTopic(ctx, topic, 1)
	broker.CreateTopic(ctx, dlqTopic, 1)

	// Create schema validator
	validator := middleware.NewJSONSchemaValidator()
	validator.RegisterSchema(topic, func(payload []byte) error {
		var order Order
		if err := json.Unmarshal(payload, &order); err != nil {
			return fmt.Errorf("invalid JSON: %w", err)
		}
		if order.ID == "" {
			return fmt.Errorf("order ID is required")
		}
		if order.Amount <= 0 {
			return fmt.Errorf("order amount must be positive")
		}
		return nil
	})

	// Subscribe with all features enabled
	sub, err := broker.NewSubscriber(topic, groupID,
		unistream.WithIdempotency(true, "localhost:6379"),
		unistream.WithDLQ(dlqTopic),
	)
	if err != nil {
		log.Fatalf("Failed to create subscriber: %v", err)
	}
	defer sub.Close()

	// Publisher with schema validation
	pub, err := broker.NewPublisher(topic,
		unistream.WithSchemaValidator(validator),
	)
	if err != nil {
		log.Fatalf("Failed to create publisher: %v", err)
	}
	defer pub.Close()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Start consumer
	go func() {
		err := sub.Subscribe(ctx, topic, func(ctx context.Context, msg *unistream.Message) error {
			var order Order
			if err := json.Unmarshal(msg.Payload, &order); err != nil {
				return fmt.Errorf("failed to parse order: %w", err)
			}

			// Simulate processing
			fmt.Printf("Processing order: %s, amount: %.2f\n", order.ID, order.Amount)

			// Simulate occasional failures for retry demonstration
			if order.ID == "order-fail" {
				return fmt.Errorf("simulated processing failure")
			}

			return nil
		})
		if err != nil {
			log.Printf("Subscribe error: %v", err)
		}
	}()

	time.Sleep(3 * time.Second)

	// Publish messages
	go func() {
		// Valid message
		order1 := Order{ID: "order-123", Amount: 100.0, Status: "pending"}
		payload1, _ := json.Marshal(order1)
		pub.Publish(ctx, topic, &unistream.Message{
			UUID:    order1.ID,
			Payload: payload1,
		})

		time.Sleep(1 * time.Second)

		// Duplicate (should be skipped)
		pub.Publish(ctx, topic, &unistream.Message{
			UUID:    order1.ID,
			Payload: payload1,
		})

		time.Sleep(1 * time.Second)

		// Message that will fail (will retry then go to DLQ)
		order2 := Order{ID: "order-fail", Amount: 200.0, Status: "pending"}
		payload2, _ := json.Marshal(order2)
		pub.Publish(ctx, topic, &unistream.Message{
			UUID:    order2.ID,
			Payload: payload2,
		})
	}()

	fmt.Println("\nService running. Watch consumer status output above.")
	fmt.Println("Press Ctrl+C to stop.")
	<-sigChan
	fmt.Println("\nShutting down...")
	cancel()
	time.Sleep(1 * time.Second)
}
