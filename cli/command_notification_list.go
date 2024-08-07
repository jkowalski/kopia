package cli

import (
	"context"
	"fmt"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/notification"
	"github.com/kopia/kopia/repo"
)

type commandNotificationList struct {
	out textOutput
}

func (c *commandNotificationList) setup(svc appServices, parent commandParent) {
	cmd := parent.Command("list", "List notification profiles").Alias("ls")

	c.out.setup(svc)

	cmd.Action(svc.repositoryReaderAction(c.run))
}

func (c *commandNotificationList) run(ctx context.Context, rep repo.Repository) error {
	profileConfigs, err := notification.ListProfiles(ctx, rep)
	if err != nil {
		return errors.Wrap(err, "unable to list notification profiles")
	}

	for i, pc := range profileConfigs {
		var summary string

		if prov, err := pc.Method.Provider(ctx); err == nil {
			summary = prov.Summary()
		} else {
			summary = fmt.Sprintf("%v - invalid", pc.Method.Type)
		}

		if i > 0 {
			c.out.printStdout("\n")
		}

		c.out.printStdout("Profile %q:\n  %v\n", pc.Profile, summary)
	}

	return nil
}
