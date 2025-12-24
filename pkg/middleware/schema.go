package middleware

import (
	"context"
	"encoding/json"
	"fmt"
	"unistream/pkg/core"
)

// SchemaValidator defines the interface for schema validation
type SchemaValidator interface {
	Validate(topic string, payload []byte) error
}

// JSONSchemaValidator is a simple JSON schema validator
type JSONSchemaValidator struct {
	schemas map[string]func([]byte) error
}

// NewJSONSchemaValidator creates a new JSON schema validator
func NewJSONSchemaValidator() *JSONSchemaValidator {
	return &JSONSchemaValidator{
		schemas: make(map[string]func([]byte) error),
	}
}

// RegisterSchema registers a validation function for a topic
func (v *JSONSchemaValidator) RegisterSchema(topic string, validator func([]byte) error) {
	v.schemas[topic] = validator
}

// Validate validates a payload against the registered schema for the topic
func (v *JSONSchemaValidator) Validate(topic string, payload []byte) error {
	validator, exists := v.schemas[topic]
	if !exists {
		// No schema registered for this topic, allow it
		return nil
	}
	return validator(payload)
}

// ValidateJSON validates that payload is valid JSON
func ValidateJSON(payload []byte) error {
	var v interface{}
	return json.Unmarshal(payload, &v)
}

// NewSchemaValidationMiddleware creates a middleware that validates messages before publishing
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
