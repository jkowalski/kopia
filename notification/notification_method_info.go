package notification

import (
	"context"
	"encoding/json"

	"github.com/pkg/errors"
)

// MethodType represents the type of the notification method.
type MethodType string

// MethodInfo represents JSON-serializable configuration of a notification method and parameters.
type MethodInfo struct {
	Type   MethodType
	Config any
}

// UnmarshalJSON parses the JSON-encoded notification method configuration into MethodInfo.
func (c *MethodInfo) UnmarshalJSON(b []byte) error {
	raw := struct {
		Type MethodType      `json:"type"`
		Data json.RawMessage `json:"config"`
	}{}

	if err := json.Unmarshal(b, &raw); err != nil {
		return errors.Wrap(err, "error unmarshaling connection info JSON")
	}

	c.Type = raw.Type

	if f := allProviders[raw.Type]; f == nil {
		return errors.Errorf("provider type '%v' not registered", raw.Type)
	}

	c.Config = defaultOptions[raw.Type]
	if err := json.Unmarshal(raw.Data, &c.Config); err != nil {
		return errors.Wrap(err, "unable to unmarshal config")
	}

	return nil
}

// MarshalJSON returns JSON-encoded notification method configuration.
func (c MethodInfo) MarshalJSON() ([]byte, error) {
	//nolint:wrapcheck
	return json.Marshal(struct {
		Type MethodType  `json:"type"`
		Data interface{} `json:"config"`
	}{
		Type: c.Type,
		Data: c.Config,
	})
}

// Provider returns a new instance of the notification provider.
func (c MethodInfo) Provider(ctx context.Context) (Provider, error) {
	return GetProvider(ctx, c.Type, c.Config)
}
