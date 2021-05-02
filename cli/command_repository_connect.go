package cli

import (
	"context"
	"time"

	"github.com/alecthomas/kingpin"
	"github.com/pkg/errors"

	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/content"
)

type commandRepositoryConnect struct {
	co connectOptions

	server commandRepositoryConnectServer
}

func (c *commandRepositoryConnect) setup(parent commandParent) {
	cmd := parent.Command("connect", "Connect to a repository.")

	c.co.setup(cmd)
	c.server.setup(cmd, &c.co)

	for _, prov := range storageProviders {
		// Set up 'connect' subcommand
		f := prov.newFlags()
		cc := cmd.Command(prov.name, "Connect to repository in "+prov.description)
		f.setup(cc)
		cc.Action(func(_ *kingpin.ParseContext) error {
			ctx := rootContext()
			st, err := f.connect(ctx, false)
			if err != nil {
				return errors.Wrap(err, "can't connect to storage")
			}

			return runConnectCommandWithStorage(ctx, &c.co, st)
		})
	}
}

type connectOptions struct {
	connectPersistCredentials     bool
	connectCacheDirectory         string
	connectMaxCacheSizeMB         int64
	connectMaxMetadataCacheSizeMB int64
	connectMaxListCacheDuration   time.Duration
	connectHostname               string
	connectUsername               string
	connectCheckForUpdates        bool
	connectReadonly               bool
	connectDescription            string
	connectEnableActions          bool
}

func (c *connectOptions) setup(cmd *kingpin.CmdClause) {
	// Set up flags shared between 'create' and 'connect'. Note that because those flags are used by both command
	// we must use *Var() methods, otherwise one of the commands would always get default flag values.
	cmd.Flag("persist-credentials", "Persist credentials").Default("true").Envar("KOPIA_PERSIST_CREDENTIALS_ON_CONNECT").BoolVar(&c.connectPersistCredentials)
	cmd.Flag("cache-directory", "Cache directory").PlaceHolder("PATH").Envar("KOPIA_CACHE_DIRECTORY").StringVar(&c.connectCacheDirectory)
	cmd.Flag("content-cache-size-mb", "Size of local content cache").PlaceHolder("MB").Default("5000").Int64Var(&c.connectMaxCacheSizeMB)
	cmd.Flag("metadata-cache-size-mb", "Size of local metadata cache").PlaceHolder("MB").Default("5000").Int64Var(&c.connectMaxMetadataCacheSizeMB)
	cmd.Flag("max-list-cache-duration", "Duration of index cache").Default("30s").Hidden().DurationVar(&c.connectMaxListCacheDuration)
	cmd.Flag("override-hostname", "Override hostname used by this repository connection").Hidden().StringVar(&c.connectHostname)
	cmd.Flag("override-username", "Override username used by this repository connection").Hidden().StringVar(&c.connectUsername)
	cmd.Flag("check-for-updates", "Periodically check for Kopia updates on GitHub").Default("true").Envar(checkForUpdatesEnvar).BoolVar(&c.connectCheckForUpdates)
	cmd.Flag("readonly", "Make repository read-only to avoid accidental changes").BoolVar(&c.connectReadonly)
	cmd.Flag("description", "Human-readable description of the repository").StringVar(&c.connectDescription)
	cmd.Flag("enable-actions", "Allow snapshot actions").BoolVar(&c.connectEnableActions)
}

func (c *connectOptions) toRepoConnectOptions() *repo.ConnectOptions {
	return &repo.ConnectOptions{
		PersistCredentials: c.connectPersistCredentials,
		CachingOptions: content.CachingOptions{
			CacheDirectory:            c.connectCacheDirectory,
			MaxCacheSizeBytes:         c.connectMaxCacheSizeMB << 20,         //nolint:gomnd
			MaxMetadataCacheSizeBytes: c.connectMaxMetadataCacheSizeMB << 20, //nolint:gomnd
			MaxListCacheDurationSec:   int(c.connectMaxListCacheDuration.Seconds()),
		},
		ClientOptions: repo.ClientOptions{
			Hostname:      c.connectHostname,
			Username:      c.connectUsername,
			ReadOnly:      c.connectReadonly,
			Description:   c.connectDescription,
			EnableActions: c.connectEnableActions,
		},
	}
}

func runConnectCommandWithStorage(ctx context.Context, co *connectOptions, st blob.Storage) error {
	password, err := getPasswordFromFlags(ctx, false, false)
	if err != nil {
		return errors.Wrap(err, "getting password")
	}

	return runConnectCommandWithStorageAndPassword(ctx, co, st, password)
}

func runConnectCommandWithStorageAndPassword(ctx context.Context, co *connectOptions, st blob.Storage, password string) error {
	configFile := repositoryConfigFileName()
	if err := repo.Connect(ctx, configFile, st, password, co.toRepoConnectOptions()); err != nil {
		return errors.Wrap(err, "error connecting to repository")
	}

	log(ctx).Infof("Connected to repository.")
	co.maybeInitializeUpdateCheck(ctx)

	return nil
}
