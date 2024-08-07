package notification

import (
	"context"
	"encoding/json"

	"github.com/pkg/errors"
)

// Provider is an interface implemented by all notification methods.
type Provider interface {
	Send(ctx context.Context, msg Message) error
	Summary() string
}

// ProviderFactory is a function that creates a new instance of a notification provider with a
// given context and options.
type ProviderFactory[T any] func(ctx context.Context, options T) (Provider, error)

//nolint:gochecknoglobals
var (
	allProviders   = map[MethodType]ProviderFactory[any]{}
	defaultOptions = map[MethodType]any{}
)

// GetProvider returns a new instance of a provider with a given name and options.
func GetProvider(ctx context.Context, name MethodType, jsonOptions any) (Provider, error) {
	factory := allProviders[name]
	if factory == nil {
		return nil, errors.Errorf("unknown provider: %v", name)
	}

	return factory(ctx, jsonOptions)
}

// RegisterProvider registers a new provider with a given name and factory function.
func RegisterProvider[T any](name MethodType, p ProviderFactory[*T]) {
	var defT T

	defaultOptions[name] = defT

	allProviders[name] = func(ctx context.Context, jsonOptions any) (Provider, error) {
		typedOptions := defT

		v, err := json.Marshal(jsonOptions)
		if err != nil {
			return nil, errors.Wrap(err, "unable to marshal options")
		}

		if err := json.Unmarshal(v, &typedOptions); err != nil {
			return nil, errors.Wrap(err, "unable to unmarshal options")
		}

		return p(ctx, &typedOptions)
	}
}
