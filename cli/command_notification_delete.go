package cli

import (
	"context"

	"github.com/kopia/kopia/notification"
	"github.com/kopia/kopia/repo"
)

type commandNotificationDelete struct {
	profileName string
}

func (c *commandNotificationDelete) setup(svc appServices, parent commandParent) {
	cmd := parent.Command("delete", "Delete notification profile").Alias("rm")

	cmd.Flag("profile", "Profile name").Required().StringVar(&c.profileName)

	cmd.Action(svc.repositoryWriterAction(c.run))
}

func (c *commandNotificationDelete) run(ctx context.Context, rep repo.RepositoryWriter) error {
	//nolint:wrapcheck
	return notification.DeleteProfile(ctx, rep, c.profileName)
}
