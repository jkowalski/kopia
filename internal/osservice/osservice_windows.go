package osservice

import (
	"context"

	"github.com/kopia/kopia/repo/logging"
	"github.com/pkg/errors"
	"golang.org/x/sys/windows"
	"golang.org/x/sys/windows/svc/mgr"
)

var log = logging.Module("osservice")

func Install(ctx context.Context, serviceName, exepath string, args []string, opts InstallOptions) error {

	var startType uint32

	if opts.AutoStart {
		startType = mgr.StartAutomatic
	} else {
		startType = mgr.StartManual
	}

	cfg := mgr.Config{
		StartType:        startType,
		Description:      opts.Description,
		DisplayName:      opts.DisplayName,
		ServiceStartName: opts.Username,
	}

	if opts.GetPasswordFunc != nil && opts.Username != "" {
		p, err := opts.GetPasswordFunc()
		if err != nil {
			return errors.Wrap(err, "get password")
		}

		cfg.Password = p
	}

	err := installInternal(ctx, serviceName, exepath, args, cfg)
	if err == nil {
		return nil
	}

	if errors.Is(err, windows.ERROR_SERVICE_EXISTS) && opts.Reinstall {
		log(ctx).Infof("Service already exists - reinstalling...")

		if uerr := Unistall(ctx, serviceName); uerr != nil {
			return errors.Wrap(uerr, "uninstall service")
		}

		err = installInternal(ctx, serviceName, exepath, args, cfg)
	}

	return errors.Wrap(err, "install service")
}

func installInternal(ctx context.Context, serviceName, exepath string, args []string, cfg mgr.Config) error {
	m, err := mgr.Connect()
	if err != nil {
		return errors.Wrap(err, "connect to service control manager")
	}
	defer m.Disconnect()

	log(ctx).Infow("Installing service", "serviceName", serviceName, "displayName", cfg.DisplayName, "user", cfg.ServiceStartName)

	s, err := m.CreateService(serviceName, exepath, cfg, args...)
	if err != nil {
		return err
	}

	defer s.Close()

	return nil
}

func Unistall(ctx context.Context, serviceName string) error {
	m, err := mgr.Connect()
	if err != nil {
		return errors.Wrap(err, "connect to service control manager")
	}
	defer m.Disconnect()

	s, err := m.OpenService(serviceName)
	if err != nil {
		return errors.Wrap(err, "OpenService")
	}

	defer s.Close()

	log(ctx).Infow("Uninstalling service", "serviceName", serviceName)
	return errors.Wrap(s.Delete(), "DeleteService")
}
