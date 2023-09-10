package cli

import (
	"context"

	"github.com/kopia/kopia/internal/osservice"
)

type commandServerUninstallService struct {
	serviceName string
}

func (c *commandServerUninstallService) setup(svc advancedAppServices, parent commandParent) {
	cmd := parent.Command("uninstall-service", "Uninstall Kopia service")
	cmd.Flag(serviceFlagPrefix+"name", "Service name").Default(defaultServiceName).StringVar(&c.serviceName)

	cmd.Action(svc.baseActionWithContext(c.run))
}

func (c *commandServerUninstallService) run(ctx context.Context) error {
	return osservice.Unistall(ctx, c.serviceName)
}
