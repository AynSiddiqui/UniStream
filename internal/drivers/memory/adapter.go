package memory

import (
	"context"
	"sync"
	"unistream/pkg/core"
)

type Broker struct {
	topics map[string]chan *core.Message
	mu     sync.RWMutex
}

func init() {
	core.RegisterDriver(core.DriverMemory, func(addr, user, pass string, extra map[string]interface{}) (core.Broker, error) {
		return NewBroker(), nil
	})
}

func NewBroker() *Broker {
	return &Broker{
		topics: make(map[string]chan *core.Message),
	}
}

func (b *Broker) CreateTopic(ctx context.Context, name string, partitions int) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if _, ok := b.topics[name]; !ok {
		b.topics[name] = make(chan *core.Message, 1000)
	}
	return nil
}

func (b *Broker) NewPublisher(topic string, opts ...core.PublishOption) (core.Publisher, error) {
	b.CreateTopic(context.Background(), topic, 1) // Auto create
	return &Publisher{b: b, topic: topic}, nil
}

func (b *Broker) NewSubscriber(topic string, groupID string, opts ...core.SubscribeOption) (core.Subscriber, error) {
	b.CreateTopic(context.Background(), topic, 1)
	return &Subscriber{b: b, topic: topic}, nil
}

func (b *Broker) Close() error { return nil }

type Publisher struct {
	b     *Broker
	topic string
}

func (p *Publisher) Publish(ctx context.Context, topic string, msg *core.Message) error {
	p.b.mu.RLock()
	ch, ok := p.b.topics[topic]
	p.b.mu.RUnlock()
	if ok {
		ch <- msg
	}
	return nil
}
func (p *Publisher) Close() error { return nil }

type Subscriber struct {
	b     *Broker
	topic string
}

func (s *Subscriber) Subscribe(ctx context.Context, topic string, handler core.Handler) error {
	s.b.mu.RLock()
	ch, ok := s.b.topics[topic]
	s.b.mu.RUnlock()
	
	if !ok {
		return nil
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case msg := <-ch:
			handler(ctx, msg)
		}
	}
}
func (s *Subscriber) Close() error { return nil }
