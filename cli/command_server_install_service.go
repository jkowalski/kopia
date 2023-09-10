package cli

import (
	"context"
	"os"
	"os/user"
	"path/filepath"
	"strings"

	"github.com/alecthomas/kingpin/v2"
	"github.com/kopia/kopia/internal/osservice"
	"github.com/pkg/errors"
)

const (
	defaultServiceName = "kopia"
	serviceFlagPrefix  = "service-"
)

type commandServerInstallService struct {
	serviceName      string
	serviceUsername  string
	servicePassword  string
	serviceReinstall bool

	commonServerStartInstallFlags
}

func (c *commandServerInstallService) setup(svc advancedAppServices, parent commandParent) {
	cmd := parent.Command("install-service", "Install Kopia as a service")

	c.commonServerStartInstallFlags.setup(svc, cmd)

	cmd.Flag(serviceFlagPrefix+"name", "Service name").Default(defaultServiceName).StringVar(&c.serviceName)
	cmd.Flag(serviceFlagPrefix+"username", "Service username").StringVar(&c.serviceUsername)
	cmd.Flag(serviceFlagPrefix+"password", "Service password").StringVar(&c.servicePassword)
	cmd.Flag(serviceFlagPrefix+"reinstall", "Reinstall service if already exists").BoolVar(&c.serviceReinstall)

	cmd.Action(func(pc *kingpin.ParseContext) error {
		// copy most args from the current invocation and build `kopia server start` invocation.
		args := []string{
			"server",
			"start",
			"--config-file",
			svc.repositoryConfigFileName(),
		}

		for _, e := range pc.Elements {
			switch cl := e.Clause.(type) {
			case *kingpin.FlagClause:
				switch {
				case strings.HasPrefix(cl.Model().Name, serviceFlagPrefix):
					// flags starting with 'service-' have special meaning, skip those.

				case cl.Model().Name == "config-file":
					// skip config file, we will always pass resolved version of this

				default:
					args = append(args, "--"+cl.Model().Name)
					if e.Value != nil {
						args = append(args, *e.Value)
					}
				}
			}
		}

		executable, err := os.Executable()
		if err != nil {
			executable = "kopia"
		}

		installOptions := osservice.InstallOptions{
			DisplayName: "Kopia Server - " + filepath.Base(svc.repositoryConfigFileName()),
			Description: "Fast And Secure Open Source Backup.\nServer using config: " + svc.repositoryConfigFileName(),
			Reinstall:   c.serviceReinstall,
		}

		u, err := user.Current()
		if err != nil {
			return errors.Wrap(err, "current user")
		}

		installOptions.Username = u.Username
		installOptions.GetPasswordFunc = func() (string, error) {
			return askForServicePassword(os.Stdout)
		}

		return svc.baseActionWithContext(func(ctx context.Context) error {
			return osservice.Install(ctx, c.serviceName, executable, args, installOptions)
		})(pc)
	})
}
