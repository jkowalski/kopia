package cli

import (
	"context"

	"github.com/kopia/kopia/internal/apiclient"
	"github.com/kopia/kopia/internal/serverapi"
)

type commandServerCancel struct {
	sf serverClientFlags
}

func (c *commandServerCancel) setup(app appServices, parent commandParent) {
	cmd := parent.Command("cancel", "Cancels in-progress uploads for one or more sources")
	c.sf.setup(cmd)
	cmd.Action(app.serverAction(&c.sf, c.runServerCancelUpload))
}

func (c *commandServerCancel) runServerCancelUpload(ctx context.Context, cli *apiclient.KopiaAPIClient) error {
	return cli.Post(ctx, "sources/cancel", &serverapi.Empty{}, &serverapi.Empty{})
}
