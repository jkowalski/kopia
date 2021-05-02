package cli

import (
	"context"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/blob"
)

type commandBlobDelete struct {
	blobIDs []string
}

func (c *commandBlobDelete) setup(app appServices, parent commandParent) {
	cmd := parent.Command("delete", "Delete blobs by ID").Alias("remove").Alias("rm")
	cmd.Arg("blobIDs", "Blob IDs").Required().StringsVar(&c.blobIDs)
	cmd.Action(app.directRepositoryWriteAction(c.run))
}

func (c *commandBlobDelete) run(ctx context.Context, rep repo.DirectRepositoryWriter) error {
	advancedCommand(ctx)

	for _, b := range c.blobIDs {
		err := rep.BlobStorage().DeleteBlob(ctx, blob.ID(b))
		if err != nil {
			return errors.Wrapf(err, "error deleting %v", b)
		}
	}

	return nil
}
