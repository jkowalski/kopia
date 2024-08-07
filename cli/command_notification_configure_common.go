package cli

import (
	"context"

	"github.com/alecthomas/kingpin/v2"
	"github.com/pkg/errors"

	"github.com/kopia/kopia/notification"
	"github.com/kopia/kopia/repo"
)

// commonNotificationOptions is a common configuration for notification methods.
type commonNotificationOptions struct {
	profileName          string
	sendTestNotification bool
}

func (c *commonNotificationOptions) setup(cmd *kingpin.CmdClause) {
	cmd.Flag("profile", "Profile name").Required().StringVar(&c.profileName)
	cmd.Flag("send-test-notification", "Test the notification").BoolVar(&c.sendTestNotification)
}

// configureNotificationAction is a helper function that creates a Kingpin action that
// configures a notification method.
// it will read the existing profile, merge the provided options, and save the profile back
// or send a test notification based on the flags.
func configureNotificationAction[T any](
	svc appServices,
	c *commonNotificationOptions,
	methodName notification.MethodType,
	opt *T,
	merge func(src T, dst *T, isUpdate bool),
) func(ctx *kingpin.ParseContext) error {
	return svc.directRepositoryWriteAction(func(ctx context.Context, rep repo.DirectRepositoryWriter) error {
		var options *T

		// read the existing profile, if any.
		oldProfile, exists, err := notification.GetProfile(ctx, rep, c.profileName)
		if err != nil {
			return errors.Wrap(err, "unable to get notification profile")
		}

		if exists {
			if oldProfile.Method.Type != methodName {
				return errors.Errorf("profile %q already exists but is not of type %q", c.profileName, methodName)
			}

			var ok bool

			options, ok = oldProfile.Method.Config.(*T)
			if !ok {
				return errors.Errorf("profile %q already exists but is not of type %q 2", c.profileName, methodName)
			}
		} else {
			var defaultT T

			options = &defaultT
		}

		merge(*opt, options, exists)

		np, err := notification.GetProvider(ctx, methodName, options)
		if err != nil {
			return errors.Wrap(err, "unable to get notification provider")
		}

		if c.sendTestNotification {
			return notification.SendTestNotification(ctx, np)
		}

		return notification.SaveProfile(ctx, rep, notification.ProfileConfig{
			Profile: c.profileName,
			Method: notification.MethodInfo{
				Type:   methodName,
				Config: options,
			},
		})
	})
}
