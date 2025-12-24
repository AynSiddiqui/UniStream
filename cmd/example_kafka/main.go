package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"
	_ "unistream/internal/drivers/kafka"
	"unistream/pkg/unistream"
)

func main() {
	fmt.Println("=== UniStream Kafka Example ===")
	fmt.Println("Features: Idempotency, Consumer Status Output")
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

	topic := "my-kafka-topic"
	groupID := fmt.Sprintf("my-kafka-group-%d", time.Now().Unix())

	if err := broker.CreateTopic(ctx, topic, 1); err != nil {
		log.Printf("CreateTopic warning: %v", err)
	}

	// Subscribe with idempotency
	sub, err := broker.NewSubscriber(topic, groupID,
		unistream.WithIdempotency(true, "localhost:6379"),
	)
	if err != nil {
		log.Fatalf("Failed to create subscriber: %v", err)
	}
	defer sub.Close()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		err := sub.Subscribe(ctx, topic, func(ctx context.Context, msg *unistream.Message) error {
			// Consumer status is automatically logged by middleware
			time.Sleep(100 * time.Millisecond)
			return nil
		})
		if err != nil {
			log.Printf("Subscribe error: %v", err)
		}
	}()

	time.Sleep(5 * time.Second)

	// Publish messages
	go func() {
		pub, err := broker.NewPublisher(topic)
		if err != nil {
			log.Printf("Failed to create publisher: %v", err)
			return
		}
		defer pub.Close()

		// Unique messages
		for i := 1; i <= 3; i++ {
			msgID := fmt.Sprintf("ord-%d", i)
			pub.Publish(ctx, topic, &unistream.Message{
				UUID:    msgID,
				Payload: []byte(fmt.Sprintf("Order %d", i)),
			})
			time.Sleep(1 * time.Second)
		}

		// Duplicates (will be skipped)
		time.Sleep(2 * time.Second)
		for i := 1; i <= 3; i++ {
			pub.Publish(ctx, topic, &unistream.Message{
				UUID:    "ord-1",
				Payload: []byte("Order 1 (duplicate)"),
			})
			time.Sleep(1 * time.Second)
		}
	}()

	fmt.Println("\nService running. Watch consumer status output above.")
	fmt.Println("Press Ctrl+C to stop.")
	<-sigChan
	cancel()
	time.Sleep(1 * time.Second)
}
