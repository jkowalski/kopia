package restore

import (
	"context"
	"path"
	"sync/atomic"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/logging"
)

var log = logging.GetContextLoggerFunc("restore")

// Output encapsulates output for restore operation.
type Output interface {
	BeginDirectory(ctx context.Context, relativePath string, e fs.Directory) error
	FinishDirectory(ctx context.Context, relativePath string, e fs.Directory) error
	WriteFile(ctx context.Context, relativePath string, e fs.File) error
	CreateSymlink(ctx context.Context, relativePath string, e fs.Symlink) error
	Close(ctx context.Context) error
}

// Stats represents restore statistics.
type Stats struct {
	TotalFileSize int64
	FileCount     int32
	DirCount      int32
	SymlinkCount  int32
}

// Entry walks a snapshot root with given root entry and restores it to the provided output.
func Entry(ctx context.Context, rep repo.Repository, output Output, rootEntry fs.Entry) (Stats, error) {
	return copyToOutput(ctx, output, rootEntry)
}

func copyToOutput(ctx context.Context, output Output, rootEntry fs.Entry) (Stats, error) {
	c := copier{output: output}

	if err := c.copyEntry(ctx, rootEntry, ""); err != nil {
		return Stats{}, errors.Wrap(err, "error copying")
	}

	if err := c.output.Close(ctx); err != nil {
		return Stats{}, errors.Wrap(err, "error closing output")
	}

	return c.stats, nil
}

type copier struct {
	stats  Stats
	output Output
}

func (c *copier) copyEntry(ctx context.Context, e fs.Entry, targetPath string) error {
	switch e := e.(type) {
	case fs.Directory:
		log(ctx).Debugf("dir: '%v'", targetPath)
		return c.copyDirectory(ctx, e, targetPath)
	case fs.File:
		log(ctx).Debugf("file: '%v'", targetPath)

		atomic.AddInt32(&c.stats.FileCount, 1)
		atomic.AddInt64(&c.stats.TotalFileSize, e.Size())

		return c.output.WriteFile(ctx, targetPath, e)
	case fs.Symlink:
		atomic.AddInt32(&c.stats.SymlinkCount, 1)
		log(ctx).Debugf("symlink: '%v'", targetPath)

		return c.output.CreateSymlink(ctx, targetPath, e)
	default:
		return errors.Errorf("invalid FS entry type for %q: %#v", targetPath, e)
	}
}

func (c *copier) copyDirectory(ctx context.Context, d fs.Directory, targetPath string) error {
	atomic.AddInt32(&c.stats.DirCount, 1)

	if err := c.output.BeginDirectory(ctx, targetPath, d); err != nil {
		return errors.Wrap(err, "create directory")
	}

	if err := c.copyDirectoryContent(ctx, d, targetPath); err != nil {
		return errors.Wrap(err, "copy directory contents")
	}

	if err := c.output.FinishDirectory(ctx, targetPath, d); err != nil {
		return errors.Wrap(err, "finish directory")
	}

	return nil
}

func (c *copier) copyDirectoryContent(ctx context.Context, d fs.Directory, targetPath string) error {
	entries, err := d.Readdir(ctx)
	if err != nil {
		return err
	}

	for _, e := range entries {
		if err := c.copyEntry(ctx, e, path.Join(targetPath, e.Name())); err != nil {
			return err
		}
	}

	return nil
}
