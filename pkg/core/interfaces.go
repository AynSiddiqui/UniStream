package core

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// Message is the central data carrier.
type Message struct {
	UUID     string
	Payload  []byte
	Metadata map[string]string
	Context  context.Context
	
	ackFunc  func() error
	nackFunc func() error
}

func (m *Message) SetAck(f func() error) { m.ackFunc = f }
func (m *Message) SetNack(f func() error) { m.nackFunc = f }
func (m *Message) Ack() error {
	if m.ackFunc != nil {
		return m.ackFunc()
	}
	return nil
}
func (m *Message) Nack() error {
	if m.nackFunc != nil {
		return m.nackFunc()
	}
	return nil
}

// Handler is a function that processes a single message.
type Handler func(ctx context.Context, msg *Message) error

// Publisher defines the interface for sending messages to a topic.
type Publisher interface {
	Publish(ctx context.Context, topic string, msg *Message) error
	Close() error
}

// Subscriber defines the interface for consuming messages from a topic.
type Subscriber interface {
	Subscribe(ctx context.Context, topic string, handler Handler) error
	Close() error
}

// Broker is the generic interface for interacting with any messaging system.
type Broker interface {
	CreateTopic(ctx context.Context, name string, partitions int) error
	NewPublisher(topic string, opts ...PublishOption) (Publisher, error)
	NewSubscriber(topic string, groupID string, opts ...SubscribeOption) (Subscriber, error)
	Close() error
}

// Options/Config

// PublishConfig holds configuration for publishers.
type PublishConfig struct {}

// SubscribeConfig holds configuration for subscribers.
type SubscribeConfig struct {
	IdempotencyEnabled bool
	IdempotencyTTL     time.Duration
	DLQTopic           string
	MaxRetries         int
}

type PublishOption func(*PublishConfig)
type SubscribeOption func(*SubscribeConfig)

// DriverType constants
type DriverType string

const (
	DriverKafka  DriverType = "kafka"
	DriverPulsar DriverType = "pulsar"
	DriverMemory DriverType = "memory"
)

// Config holds configuration for connecting to a broker.
type Config struct {
	Driver   DriverType
	Addr     string
	Username string
	Password string
	Extra    map[string]interface{}
}

// Feature Toggle Constructors (Must be here if Options are here)
// WithIdempotency enables idempotency middleware.
func WithIdempotency(enabled bool) SubscribeOption {
	return func(c *SubscribeConfig) {
		c.IdempotencyEnabled = enabled
		if c.IdempotencyTTL == 0 {
			c.IdempotencyTTL = 24 * time.Hour
		}
	}
}

// DriverFactory creates a Broker.
type Factory func(addr, user, pass string, extra map[string]interface{}) (Broker, error)

// WithDLQ enables DLQ rerouting on failure.
func WithDLQ(topic string) SubscribeOption {
	return func(c *SubscribeConfig) {
		c.DLQTopic = topic
		if c.MaxRetries == 0 {
			c.MaxRetries = 3
		}
	}
}

var (
	drivers   = make(map[DriverType]Factory)
	driversMu sync.RWMutex
)

// RegisterDriver registers a driver factory.
func RegisterDriver(name DriverType, factory Factory) {
	driversMu.Lock()
	defer driversMu.Unlock()
	if factory == nil {
		panic("unistream: Register driver is nil")
	}
	drivers[name] = factory
}

// GetDriver returns a driver factory.
func GetDriver(name DriverType) (Factory, error) {
	driversMu.RLock()
	defer driversMu.RUnlock()
	f, ok := drivers[name]
	if !ok {
		return nil, fmt.Errorf("unistream: unknown driver %q (forgotten import?)", name)
	}
	return f, nil
}
