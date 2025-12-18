package unistream

import (
	"context"
	"unistream/pkg/core"
)

// Type Aliases to expose them nicely to the user
type Config = core.Config
type Broker = core.Broker
type Publisher = core.Publisher
type Subscriber = core.Subscriber
type Message = core.Message
type Handler = core.Handler
type DriverType = core.DriverType
type PublishOption = core.PublishOption
type SubscribeOption = core.SubscribeOption

const (
	DriverKafka  = core.DriverKafka
	DriverPulsar = core.DriverPulsar
	DriverMemory = core.DriverMemory
)

// Helper options
var WithIdempotency = core.WithIdempotency
var WithDLQ = core.WithDLQ

// Connect establishes a connection.
func Connect(ctx context.Context, cfg Config) (Broker, error) {
	factory, err := core.GetDriver(cfg.Driver)
	if err != nil {
		return nil, err
	}
	return factory(cfg.Addr, cfg.Username, cfg.Password, cfg.Extra)
}
