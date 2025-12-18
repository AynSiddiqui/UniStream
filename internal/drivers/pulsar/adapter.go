package pulsar

import (
	"context"
	"fmt"
	"time"
	"unistream/pkg/core"

	"github.com/apache/pulsar-client-go/pulsar"
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
	p, err := b.client.CreateProducer(pulsar.ProducerOptions{
		Topic: topic,
	})
	if err != nil {
		return nil, err
	}
	return &Publisher{p: p}, nil
}

func (b *Broker) NewSubscriber(topic string, groupID string, opts ...core.SubscribeOption) (core.Subscriber, error) {
	return &Subscriber{
		client: b.client,
		topic:  topic,
		group:  groupID,
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

		if err := handler(ctx, uMsg); err != nil {
			err = uMsg.Nack()
			if err != nil {
				fmt.Printf("Error nacking: %v\n", err)
			}
		} else {
			err = uMsg.Ack()
			if err != nil {
				fmt.Printf("Error acking: %v\n", err)
			}
		}
	}
}
func (s *Subscriber) Close() error { return nil }
