package cli

import (
	"context"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/repo"
)

type commandContentDelete struct {
	ids []string
}

func (c *commandContentDelete) setup(parent commandParent) {
	cmd := parent.Command("delete", "Remove content").Alias("remove").Alias("rm")
	cmd.Arg("id", "IDs of content to remove").Required().StringsVar(&c.ids)
	cmd.Action(directRepositoryWriteAction(c.run))
}

func (c *commandContentDelete) run(ctx context.Context, rep repo.DirectRepositoryWriter) error {
	advancedCommand(ctx)

	for _, contentID := range toContentIDs(c.ids) {
		if err := rep.ContentManager().DeleteContent(ctx, contentID); err != nil {
			return errors.Wrapf(err, "error deleting content %v", contentID)
		}
	}

	return nil
}
