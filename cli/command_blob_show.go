package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"os"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/iocopy"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/blob"
)

type commandBlobShow struct {
	blobShowDecrypt bool
	blobShowIDs     []string
}

func (c *commandBlobShow) setup(app appServices, parent commandParent) {
	cmd := parent.Command("show", "Show contents of BLOBs").Alias("cat")
	cmd.Flag("decrypt", "Decrypt blob if possible").BoolVar(&c.blobShowDecrypt)
	cmd.Arg("blobID", "Blob IDs").Required().StringsVar(&c.blobShowIDs)
	cmd.Action(app.directRepositoryReadAction(c.run))
}

func (c *commandBlobShow) run(ctx context.Context, rep repo.DirectRepository) error {
	for _, blobID := range c.blobShowIDs {
		if err := c.maybeDecryptBlob(ctx, os.Stdout, rep, blob.ID(blobID)); err != nil {
			return errors.Wrap(err, "error presenting blob")
		}
	}

	return nil
}

func (c *commandBlobShow) maybeDecryptBlob(ctx context.Context, w io.Writer, rep repo.DirectRepository, blobID blob.ID) error {
	var (
		d   []byte
		err error
	)

	if c.blobShowDecrypt && canDecryptBlob(blobID) {
		d, err = rep.IndexBlobReader().DecryptBlob(ctx, blobID)

		if isJSONBlob(blobID) && err == nil {
			var b bytes.Buffer

			if err = json.Indent(&b, d, "", "  "); err != nil {
				return errors.Wrap(err, "invalid JSON")
			}

			d = b.Bytes()
		}
	} else {
		d, err = rep.BlobReader().GetBlob(ctx, blobID, 0, -1)
	}

	if err != nil {
		return errors.Wrapf(err, "error getting %v", blobID)
	}

	if _, err := iocopy.Copy(w, bytes.NewReader(d)); err != nil {
		return errors.Wrap(err, "error copying data")
	}

	return nil
}

func canDecryptBlob(b blob.ID) bool {
	switch b[0] {
	case 'n', 'm', 'l':
		return true
	default:
		return false
	}
}

func isJSONBlob(b blob.ID) bool {
	switch b[0] {
	case 'm', 'l':
		return true
	default:
		return false
	}
}
