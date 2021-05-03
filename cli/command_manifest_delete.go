package cli

import (
	"context"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/repo"
)

type commandManifestDelete struct {
	manifestRemoveItems []string
}

func (c *commandManifestDelete) setup(svc appServices, parent commandParent) {
	cmd := parent.Command("delete", "Remove manifest items").Alias("remove").Alias("rm")
	cmd.Arg("item", "Items to remove").Required().StringsVar(&c.manifestRemoveItems)
	cmd.Action(svc.repositoryWriterAction(c.run))
}

func (c *commandManifestDelete) run(ctx context.Context, rep repo.RepositoryWriter) error {
	advancedCommand(ctx)

	for _, it := range toManifestIDs(c.manifestRemoveItems) {
		if err := rep.DeleteManifest(ctx, it); err != nil {
			return errors.Wrapf(err, "unable to delete manifest %v", it)
		}
	}

	return nil
}
