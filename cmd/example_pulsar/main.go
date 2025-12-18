package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"
	_ "unistream/internal/drivers/pulsar" // Register pulsar driver
	"unistream/pkg/unistream"
)

func main() {
	// 1. Configuration for Pulsar
	cfg := unistream.Config{
		Driver: unistream.DriverPulsar,
		Addr:   "localhost:6650", // Standard Pulsar port
		Extra: map[string]interface{}{
			// Pulsar specific options could go here if implemented
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 2. Connect
	fmt.Println("Connecting to Pulsar...")
	broker, err := unistream.Connect(ctx, cfg)
	if err != nil {
		log.Fatalf("Failed to connect to Pulsar: %v", err)
	}
	defer broker.Close()

	topic := "my-pulsar-topic"
	groupID := "my-pulsar-sub"

	// 3. Create Topic (Pulsar auto-creates, but calling it valid API usage)
	if err := broker.CreateTopic(ctx, topic, 1); err != nil {
		log.Printf("CreateTopic warning: %v", err)
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
			time.Sleep(500 * time.Millisecond) 
			
			fmt.Println(" Done. Committing.")
			return msg.Ack() 
		})
		if err != nil {
			log.Printf("Subscribe error: %v", err)
		}
	}()

	// 5. Publish
	go func() {
		time.Sleep(2 * time.Second)
		fmt.Println("Creating publisher...")
		pub, err := broker.NewPublisher(topic)
		if err != nil {
			log.Printf("Failed to create publisher: %v", err)
			return
		}
		defer pub.Close()

		for i := 1; i <= 3; i++ {
			msgID := fmt.Sprintf("msg-%d", time.Now().Unix())
			fmt.Printf("<< Publishing %s\n", msgID)
			err = pub.Publish(ctx, topic, &unistream.Message{
				UUID:    msgID,
				Payload: []byte(fmt.Sprintf("Msg %d", i)),
				Metadata: map[string]string{"source": "unistream"},
			})
			if err != nil {
				log.Printf("Publish failed: %v", err)
			}
			time.Sleep(1 * time.Second)
		}
	}()

	fmt.Println("Service running. Press Ctrl+C to stop.")
	<-sigChan
	fmt.Println("\nShutdown signal received. Exiting...")
	cancel()
	time.Sleep(1 * time.Second)
}
