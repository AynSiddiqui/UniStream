package middleware

import (
	"context"
	"unistream/pkg/core"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

func NewObservabilitySubscribeMiddleware(tracer trace.Tracer) func(core.Handler) core.Handler {
	return func(next core.Handler) core.Handler {
		return func(ctx context.Context, msg *core.Message) error {
			propagator := otel.GetTextMapPropagator()
			carrier := propagation.MapCarrier(msg.Metadata)
			ctx = propagator.Extract(ctx, carrier)

			ctx, span := tracer.Start(ctx, "unistream.consume")
			defer span.End()

			span.AddEvent("processing_started")
			err := next(ctx, msg)
			if err != nil {
				span.RecordError(err)
			}
			span.AddEvent("processing_finished")
			return err
		}
	}
}

// Ensure Publisher wrapper type is not exported or used incorrectly if generic middleware isn't fully supported for publisher yet.
// For now, simpler wrapper:

type PublishMiddleware func(core.Publisher) core.Publisher

func NewObservabilityPublishMiddleware(tracer trace.Tracer) PublishMiddleware {
	return func(next core.Publisher) core.Publisher {
		return &observabilityPublisher{
			next:   next,
			tracer: tracer,
		}
	}
}

type observabilityPublisher struct {
	next   core.Publisher
	tracer trace.Tracer
}

func (p *observabilityPublisher) Publish(ctx context.Context, topic string, msg *core.Message) error {
	ctx, span := p.tracer.Start(ctx, "unistream.publish")
	defer span.End()

	if msg.Metadata == nil {
		msg.Metadata = make(map[string]string)
	}
	propagator := otel.GetTextMapPropagator()
	carrier := propagation.MapCarrier(msg.Metadata)
	propagator.Inject(ctx, carrier)

	return p.next.Publish(ctx, topic, msg)
}

func (p *observabilityPublisher) Close() error {
	return p.next.Close()
}
