package kafka

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"unistream/pkg/core"

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
	w := &segmentio.Writer{
		Addr:     segmentio.TCP(b.brokers...),
		Topic:    topic,
		Balancer: &segmentio.LeastBytes{},
	}
	return &Publisher{w: w}, nil
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
		cfg: cfg,
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
}

func (s *Subscriber) Subscribe(ctx context.Context, topic string, handler core.Handler) error {
	defer s.reader.Close()
	
	// Middleware application would go here using s.cfg
	
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
			UUID: string(m.Key),
			Payload: m.Value,
			Metadata: meta,
			Context: ctx,
		}
		// Fix Ack definition
		uMsg.SetAck(func() error { return s.reader.CommitMessages(ctx, m) })

		if err := handler(ctx, uMsg); err != nil {
			// Handle error
		} 
	}
}

func (s *Subscriber) Close() error { return s.reader.Close() }
