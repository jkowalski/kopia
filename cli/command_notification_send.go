package cli

import (
	"context"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/notification"
	"github.com/kopia/kopia/repo"
)

type commandNotificationSendTestMessage struct {
	profileName string
}

func (c *commandNotificationSendTestMessage) setup(svc appServices, parent commandParent) {
	cmd := parent.Command("send", "Send test notification")

	cmd.Flag("profile", "Profile name").Required().StringVar(&c.profileName)

	cmd.Action(svc.repositoryReaderAction(c.run))
}

func (c *commandNotificationSendTestMessage) run(ctx context.Context, rep repo.Repository) error {
	p, ok, err := notification.GetProfile(ctx, rep, c.profileName)
	if err != nil {
		return errors.Wrap(err, "unable to get notification profile")
	}

	if !ok {
		return errors.Errorf("notification profile %q not found", c.profileName)
	}

	prov, err := p.Method.Provider(ctx)
	if err != nil {
		return errors.Wrap(err, "unable to get notification provider")
	}

	if err := notification.SendTestNotification(ctx, prov); err != nil {
		return errors.Wrap(err, "unable to send test notification")
	}

	log(ctx).Info("Test notification sent successfully")

	return nil
}
