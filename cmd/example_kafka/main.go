package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"
	_ "unistream/internal/drivers/kafka" // Register kafka driver
	"unistream/pkg/unistream"
)

func main() {
	// 1. Configuration for Kafka
	// The Broker instance represents the persistent connection handle.
	// It should be kept alive for the lifecycle of the application.
	cfg := unistream.Config{
		Driver: unistream.DriverKafka,
		Addr:   "localhost:9092",
		Extra: map[string]interface{}{
			// Kafka specific options
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 2. Connect
	fmt.Println("Connecting to Kafka...")
	broker, err := unistream.Connect(ctx, cfg)
	if err != nil {
		log.Fatalf("Failed to connect to Kafka: %v", err)
	}
	// Close the broker only when the application exits.
	// This closes any underlying connection pools or resources.
	defer broker.Close()

	topic := "my-kafka-topic"
	groupID := "my-kafka-group"

	// 3. Create Topic
	if err := broker.CreateTopic(ctx, topic, 1); err != nil {
		log.Printf("CreateTopic warning (might exist): %v", err)
	}

	// 4. Subscribe (Long Running)
	fmt.Println("Starting subscriber...")
	sub, err := broker.NewSubscriber(topic, groupID)
	if err != nil {
		log.Fatalf("Failed to create subscriber: %v", err)
	}
	defer sub.Close()

	// Handle graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		err := sub.Subscribe(ctx, topic, func(ctx context.Context, msg *unistream.Message) error {
			fmt.Printf(">> Received: ID=%s. Processing...", msg.UUID)
			
			// Simulate processing time
			// The stream is blocked for this consumer until we return (unless async)
			// But 'Commit' effectively happens when we Ack.
			time.Sleep(500 * time.Millisecond) 
			
			fmt.Println(" Done. Committing.")
			return msg.Ack() // This removes the message from the stream (advances offset)
		})
		if err != nil {
			log.Printf("Subscribe error: %v", err)
		}
	}()

	// 5. Publish a few messages to test
	go func() {
		time.Sleep(5 * time.Second) // Wait longer for sub to start and rebalance
		pub, err := broker.NewPublisher(topic)
		if err != nil {
			log.Printf("Pub create fail: %v", err)
			return
		}
		defer pub.Close()

		for i := 1; i <= 3; i++ {
			msgID := fmt.Sprintf("ord-%d", i)
			fmt.Printf("<< Publishing %s\n", msgID)
			_ = pub.Publish(ctx, topic, &unistream.Message{
				UUID:    msgID,
				Payload: []byte(fmt.Sprintf("Order %d", i)),
			})
			time.Sleep(5 * time.Second)
		}
	}()

	fmt.Println("Service running. Press Ctrl+C to stop.")
	<-sigChan
	fmt.Println("\nShutdown signal received. Exiting...")
	cancel()
	time.Sleep(1 * time.Second) // Allow cleanup
}
