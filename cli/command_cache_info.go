package cli

import (
	"context"
	"fmt"
	"io/ioutil"
	"path/filepath"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/units"
	"github.com/kopia/kopia/repo"
)

type commandCacheInfo struct {
	onlyShowPath bool

	app appServices
}

func (c *commandCacheInfo) setup(app appServices, parent commandParent) {
	cmd := parent.Command("info", "Displays cache information and statistics").Default()
	cmd.Flag("path", "Only display cache path").BoolVar(&c.onlyShowPath)
	cmd.Action(app.repositoryReaderAction(c.run))

	c.app = app
}

func (c *commandCacheInfo) run(ctx context.Context, rep repo.Repository) error {
	opts, err := repo.GetCachingOptions(ctx, c.app.repositoryConfigFileName())
	if err != nil {
		return errors.Wrap(err, "error getting cache options")
	}

	if c.onlyShowPath {
		fmt.Println(opts.CacheDirectory)
		return nil
	}

	entries, err := ioutil.ReadDir(opts.CacheDirectory)
	if err != nil {
		return errors.Wrap(err, "unable to scan cache directory")
	}

	path2Limit := map[string]int64{
		"contents":        opts.MaxCacheSizeBytes,
		"metadata":        opts.MaxMetadataCacheSizeBytes,
		"server-contents": opts.MaxCacheSizeBytes,
	}

	for _, ent := range entries {
		if !ent.IsDir() {
			continue
		}

		subdir := filepath.Join(opts.CacheDirectory, ent.Name())

		fileCount, totalFileSize, err := scanCacheDir(subdir)
		if err != nil {
			return err
		}

		maybeLimit := ""
		if l, ok := path2Limit[ent.Name()]; ok {
			maybeLimit = fmt.Sprintf(" (limit %v)", units.BytesStringBase10(l))
		}

		if ent.Name() == "blob-list" {
			maybeLimit = fmt.Sprintf(" (duration %vs)", opts.MaxListCacheDurationSec)
		}

		fmt.Printf("%v: %v files %v%v\n", subdir, fileCount, units.BytesStringBase10(totalFileSize), maybeLimit)
	}

	printStderr("To adjust cache sizes use 'kopia cache set'.\n")
	printStderr("To clear caches use 'kopia cache clear'.\n")

	return nil
}
