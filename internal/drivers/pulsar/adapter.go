package pulsar

import (
	"context"
	"errors"
	"fmt"
	"time"
	"unistream/pkg/core"
	"unistream/pkg/middleware"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/redis/go-redis/v9"
)

func init() {
	core.RegisterDriver(core.DriverPulsar, func(addr, user, pass string, extra map[string]interface{}) (core.Broker, error) {
		return NewBroker(addr, user, pass, extra)
	})
}

type Broker struct {
	client pulsar.Client
}

func NewBroker(addr, user, pass string, extra map[string]interface{}) (*Broker, error) {
	opts := pulsar.ClientOptions{
		URL:               "pulsar://" + addr,
		OperationTimeout:  30 * time.Second,
		ConnectionTimeout: 30 * time.Second,
	}
	// Auth handling would go here (Token/Oauth2) using user/pass

	client, err := pulsar.NewClient(opts)
	if err != nil {
		return nil, err
	}
	return &Broker{client: client}, nil
}

func (b *Broker) CreateTopic(ctx context.Context, name string, partitions int) error {
	// Pulsar topics allow auto-creation.
	return nil
}

func (b *Broker) NewPublisher(topic string, opts ...core.PublishOption) (core.Publisher, error) {
	cfg := &core.PublishConfig{}
	for _, o := range opts {
		o(cfg)
	}

	p, err := b.client.CreateProducer(pulsar.ProducerOptions{
		Topic: topic,
	})
	if err != nil {
		return nil, err
	}
	
	pub := core.Publisher(&Publisher{p: p})
	
	// Apply schema validation if configured
	if cfg.SchemaValidator != nil {
		if validator, ok := cfg.SchemaValidator.(middleware.SchemaValidator); ok {
			schemaMiddleware := middleware.NewSchemaValidationMiddleware(validator)
			pub = schemaMiddleware(pub)
		}
	}
	
	return pub, nil
}

func (b *Broker) NewSubscriber(topic string, groupID string, opts ...core.SubscribeOption) (core.Subscriber, error) {
	cfg := &core.SubscribeConfig{}
	for _, o := range opts {
		o(cfg)
	}
	
	return &Subscriber{
		client: b.client,
		topic:  topic,
		group:  groupID,
		cfg:    cfg,
		broker: b,
	}, nil
}

func (b *Broker) Close() error {
	b.client.Close()
	return nil
}

type Publisher struct {
	p pulsar.Producer
}

func (p *Publisher) Publish(ctx context.Context, topic string, msg *core.Message) error {
	_, err := p.p.Send(ctx, &pulsar.ProducerMessage{
		Key:        msg.UUID,
		Payload:    msg.Payload,
		Properties: msg.Metadata,
	})
	return err
}
func (p *Publisher) Close() error { p.p.Close(); return nil }

type Subscriber struct {
	client pulsar.Client
	topic  string
	group  string
	cfg    *core.SubscribeConfig
	broker *Broker // Store broker reference for DLQ publisher creation
}

func (s *Subscriber) Subscribe(ctx context.Context, topic string, handler core.Handler) error {
	consumer, err := s.client.Subscribe(pulsar.ConsumerOptions{
		Topic:            topic,
		SubscriptionName: s.group,
		Type:             pulsar.Shared,
	})
	if err != nil {
		return err
	}
	defer consumer.Close()

	// Build middleware chain
	finalHandler := handler

	// Setup DLQ publisher if configured
	var dlqPublisher core.Publisher
	if s.cfg != nil && s.cfg.DLQTopic != "" {
		var err error
		dlqPublisher, err = s.broker.NewPublisher(s.cfg.DLQTopic)
		if err != nil {
			return fmt.Errorf("failed to create DLQ publisher: %w", err)
		}
		defer dlqPublisher.Close()
	}

	// Apply retry middleware if enabled (must be before idempotency)
	if s.cfg != nil && s.cfg.EnableRetry {
		maxRetries := s.cfg.MaxRetries
		if maxRetries == 0 {
			maxRetries = 3
		}
		backoff := s.cfg.RetryBackoff
		if backoff == 0 {
			backoff = 1 * time.Second
		}
		retryMiddleware := middleware.NewRetryMiddleware(maxRetries, backoff, dlqPublisher, s.cfg.DLQTopic)
		finalHandler = retryMiddleware(finalHandler)
	}

	// Apply idempotency middleware if enabled (must be after retry)
	if s.cfg != nil && s.cfg.IdempotencyEnabled && s.cfg.IdempotencyRedisAddr != "" {
		rdb := redis.NewClient(&redis.Options{
			Addr: s.cfg.IdempotencyRedisAddr,
		})
		if err := rdb.Ping(ctx).Err(); err != nil {
			return fmt.Errorf("failed to connect to Redis for idempotency: %w", err)
		}

		ttl := s.cfg.IdempotencyTTL
		if ttl == 0 {
			ttl = 24 * time.Hour
		}

		idempotencyMiddleware := middleware.NewIdempotencyMiddleware(rdb, ttl)
		finalHandler = idempotencyMiddleware(finalHandler)
	}

	// Apply consumer status middleware (outermost - reports all status)
	statusMiddleware := middleware.NewConsumerStatusMiddleware(nil)
	finalHandler = statusMiddleware(finalHandler)

	for {
		msg, err := consumer.Receive(ctx)
		if err != nil {
			return err
		}

		uMsg := &core.Message{
			UUID:     msg.Key(),
			Payload:  msg.Payload(),
			Metadata: msg.Properties(),
			Context:  ctx,
		}
		uMsg.SetAck(func() error { consumer.Ack(msg); return nil })
		uMsg.SetNack(func() error { consumer.Nack(msg); return nil })

		// Process message with middleware chain
		err = finalHandler(ctx, uMsg)
		
		if err != nil {
			// Check for duplicate message (idempotency)
			if errors.Is(err, middleware.ErrDuplicateMessage) {
				// Duplicate - ack and continue
				if ackErr := uMsg.Ack(); ackErr != nil {
					return fmt.Errorf("failed to ack duplicate message: %w", ackErr)
				}
				continue
			}
			
			// Check if message was sent to DLQ (retry middleware returns nil after DLQ)
			// If retry middleware successfully sent to DLQ, it returns nil
			// Otherwise, nack for retry
			if nackErr := uMsg.Nack(); nackErr != nil {
				return fmt.Errorf("failed to nack message: %w (original error: %v)", nackErr, err)
			}
			continue
		}
		
		// Success - ack the message
		if err := uMsg.Ack(); err != nil {
			return fmt.Errorf("failed to ack message: %w", err)
		}
	}
}
func (s *Subscriber) Close() error { return nil }
