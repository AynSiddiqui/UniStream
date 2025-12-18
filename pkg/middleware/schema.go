package middleware

import (
	"context"
	"fmt"
	"unistream/pkg/core"
)

type SchemaValidator interface {
	Validate(topic string, payload []byte) error
}

func NewSchemaValidationMiddleware(validator SchemaValidator) func(core.Publisher) core.Publisher {
	return func(next core.Publisher) core.Publisher {
		return &schemaValidationPublisher{
			next:      next,
			validator: validator,
		}
	}
}

type schemaValidationPublisher struct {
	next      core.Publisher
	validator SchemaValidator
}

func (s *schemaValidationPublisher) Publish(ctx context.Context, topic string, msg *core.Message) error {
	if err := s.validator.Validate(topic, msg.Payload); err != nil {
		return fmt.Errorf("schema validation failed: %w", err)
	}
	return s.next.Publish(ctx, topic, msg)
}

func (s *schemaValidationPublisher) Close() error {
	return s.next.Close()
}
