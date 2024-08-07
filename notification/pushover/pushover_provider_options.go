package pushover

import (
	"context"

	"github.com/pkg/errors"
)

// Options defines Pushover notification provider options.
type Options struct {
	AppToken string `json:"appToken"`
	UserKey  string `json:"userKey"`

	Endpoint string `json:"-"`
}

// ApplyDefaultsAndValidate applies default values and validates the configuration.
func (o *Options) ApplyDefaultsAndValidate(ctx context.Context) error {
	if o.AppToken == "" {
		return errors.Errorf("App Token must be provided")
	}

	if o.UserKey == "" {
		return errors.Errorf("User Key must be provided")
	}

	return nil
}

// MergeOptions updates the destination options with the source options.
func MergeOptions(src Options, dst *Options, isUpdate bool) {
	copyOrMerge(&dst.AppToken, src.AppToken, isUpdate)
	copyOrMerge(&dst.UserKey, src.UserKey, isUpdate)
}

func copyOrMerge[T comparable](dst *T, src T, isUpdate bool) {
	var defaultT T

	if !isUpdate || src != defaultT {
		*dst = src
	}
}
