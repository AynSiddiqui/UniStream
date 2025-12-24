package kafka

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strconv"
	"time"
	"unistream/pkg/core"
	"unistream/pkg/middleware"

	"github.com/redis/go-redis/v9"
	segmentio "github.com/segmentio/kafka-go"
)

func init() {
	core.RegisterDriver(core.DriverKafka, func(addr, user, pass string, extra map[string]interface{}) (core.Broker, error) {
		return NewBroker(addr, user, pass, extra)
	})
}

type Broker struct {
	addr    string
	dialer  *segmentio.Dialer
	brokers []string
}

func NewBroker(addr, user, pass string, extra map[string]interface{}) (*Broker, error) {
	dialer := &segmentio.Dialer{
		Timeout:   10 * segmentio.DefaultDialer.Timeout,
		KeepAlive: segmentio.DefaultDialer.KeepAlive,
	}

	return &Broker{
		addr:    addr,
		dialer:  dialer,
		brokers: []string{addr},
	}, nil
}

func (b *Broker) CreateTopic(ctx context.Context, name string, partitions int) error {
	conn, err := b.dialer.DialContext(ctx, "tcp", b.addr)
	if err != nil {
		return fmt.Errorf("failed to dial kafka: %w", err)
	}
	defer conn.Close()

	controller, err := conn.Controller()
	if err != nil {
		return fmt.Errorf("failed to get controller: %w", err)
	}

	controllerConn, err := b.dialer.DialContext(ctx, "tcp", net.JoinHostPort(controller.Host, strconv.Itoa(controller.Port)))
	if err != nil {
		return fmt.Errorf("failed to dial controller: %w", err)
	}
	defer controllerConn.Close()

	topicConfigs := []segmentio.TopicConfig{
		{
			Topic:             name,
			NumPartitions:     partitions,
			ReplicationFactor: 1,
		},
	}

	err = controllerConn.CreateTopics(topicConfigs...)
	if err != nil {
		return fmt.Errorf("failed to create topic: %w", err)
	}
	return nil
}

func (b *Broker) NewPublisher(topic string, opts ...core.PublishOption) (core.Publisher, error) {
	cfg := &core.PublishConfig{}
	for _, o := range opts {
		o(cfg)
	}

	w := &segmentio.Writer{
		Addr:     segmentio.TCP(b.brokers...),
		Topic:    topic,
		Balancer: &segmentio.LeastBytes{},
	}

	pub := core.Publisher(&Publisher{w: w})

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

	sub := &Subscriber{
		reader: segmentio.NewReader(segmentio.ReaderConfig{
			Brokers:        b.brokers,
			GroupID:        groupID,
			Topic:          topic,
			StartOffset:    segmentio.FirstOffset, // Start from earliest to catch pending messages
			CommitInterval: 0,                     // Disable auto-commit to ensure manual Ack() works
		}),
		cfg:    cfg,
		broker: b,
	}
	return sub, nil
}

func (b *Broker) Close() error {
	return nil
}

// Publisher Wrapper
type Publisher struct {
	w *segmentio.Writer
}

func (p *Publisher) Publish(ctx context.Context, topic string, msg *core.Message) error {
	headers := make([]segmentio.Header, 0, len(msg.Metadata))
	for k, v := range msg.Metadata {
		headers = append(headers, segmentio.Header{Key: k, Value: []byte(v)})
	}
	return p.w.WriteMessages(ctx, segmentio.Message{
		Key:     []byte(msg.UUID),
		Value:   msg.Payload,
		Headers: headers,
	})
}
func (p *Publisher) Close() error { return p.w.Close() }

// Subscriber Wrapper
type Subscriber struct {
	reader *segmentio.Reader
	cfg    *core.SubscribeConfig
	broker *Broker // Store broker reference for DLQ publisher creation
}

func (s *Subscriber) Subscribe(ctx context.Context, topic string, handler core.Handler) error {
	defer s.reader.Close()

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
		m, err := s.reader.FetchMessage(ctx)
		if err != nil {
			return err
		}

		meta := make(map[string]string)
		for _, h := range m.Headers {
			meta[string(h.Key)] = string(h.Value)
		}

		uMsg := &core.Message{
			UUID:     string(m.Key),
			Payload:  m.Value,
			Metadata: meta,
			Context:  ctx,
		}
		uMsg.SetAck(func() error { return s.reader.CommitMessages(ctx, m) })
		uMsg.SetNack(func() error { return nil })

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
			if err == nil {
				// Message sent to DLQ, already acked by retry middleware
				continue
			}

			// Other errors - don't commit (will be retried on next fetch)
			continue
		}

		// Success - ack the message
		if err := uMsg.Ack(); err != nil {
			return fmt.Errorf("failed to ack message: %w", err)
		}
	}
}

func (s *Subscriber) Close() error { return s.reader.Close() }
