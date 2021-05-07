package cli

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/alecthomas/kingpin"
	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"

	"github.com/kopia/kopia/internal/gather"
	"github.com/kopia/kopia/internal/stats"
	"github.com/kopia/kopia/internal/timetrack"
	"github.com/kopia/kopia/internal/units"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/blob"
)

type commandRepositorySyncTo struct {
	repositorySyncUpdate               bool
	repositorySyncDelete               bool
	repositorySyncDryRun               bool
	repositorySyncParallelism          int
	repositorySyncDestinationMustExist bool
	repositorySyncTimes                bool

	lastSyncProgress       string
	syncProgressMutex      sync.Mutex
	nextSyncOutputTime     timetrack.Throttle
	setTimeUnsupportedOnce sync.Once

	out textOutput
}

func (c *commandRepositorySyncTo) setup(svc advancedAppServices, parent commandParent) {
	cmd := parent.Command("sync-to", "Synchronizes contents of this repository to another location")
	cmd.Flag("update", "Whether to update blobs present in destination and source if the source is newer.").Default("true").BoolVar(&c.repositorySyncUpdate)
	cmd.Flag("delete", "Whether to delete blobs present in destination but not source.").BoolVar(&c.repositorySyncDelete)
	cmd.Flag("dry-run", "Do not perform copying.").Short('n').BoolVar(&c.repositorySyncDryRun)
	cmd.Flag("parallel", "Copy parallelism.").Default("1").IntVar(&c.repositorySyncParallelism)
	cmd.Flag("must-exist", "Fail if destination does not have repository format blob.").BoolVar(&c.repositorySyncDestinationMustExist)
	cmd.Flag("times", "Synchronize blob times if supported.").BoolVar(&c.repositorySyncTimes)

	c.out.setup(svc)

	for _, prov := range storageProviders {
		// Set up 'sync-to' subcommand
		f := prov.newFlags()
		cc := cmd.Command(prov.name, "Synchronize repository data to another repository in "+prov.description)
		f.setup(svc, cc)
		cc.Action(func(_ *kingpin.ParseContext) error {
			ctx := rootContext()
			st, err := f.connect(ctx, false)
			if err != nil {
				return errors.Wrap(err, "can't connect to storage")
			}

			rep, err := svc.openRepository(ctx, true)
			if err != nil {
				return errors.Wrap(err, "open repository")
			}

			dr, ok := rep.(repo.DirectRepository)
			if !ok {
				return errors.Errorf("sync only supports directly-connected repositories")
			}

			return c.runSyncWithStorage(ctx, dr.BlobReader(), st)
		})
	}
}

const syncProgressInterval = 300 * time.Millisecond

func (c *commandRepositorySyncTo) runSyncWithStorage(ctx context.Context, src blob.Reader, dst blob.Storage) error {
	log(ctx).Infof("Synchronizing repositories:")
	log(ctx).Infof("  Source:      %v", src.DisplayName())
	log(ctx).Infof("  Destination: %v", dst.DisplayName())

	if !c.repositorySyncDelete {
		log(ctx).Infof("NOTE: By default no BLOBs are deleted, pass --delete to allow it.")
	}

	if err := c.ensureRepositoriesHaveSameFormatBlob(ctx, src, dst); err != nil {
		return err
	}

	log(ctx).Infof("Looking for BLOBs to synchronize...")

	var (
		inSyncBlobs int
		inSyncBytes int64

		blobsToCopy    []blob.Metadata
		totalCopyBytes int64

		blobsToDelete    []blob.Metadata
		totalDeleteBytes int64

		srcBlobs     int
		totalSrcSize int64
	)

	dstMetadata, err := c.listDestinationBlobs(ctx, dst)
	if err != nil {
		return err
	}

	c.beginSyncProgress()

	if err := src.ListBlobs(ctx, "", func(srcmd blob.Metadata) error {
		totalSrcSize += srcmd.Length

		dstmd, exists := dstMetadata[srcmd.BlobID]
		delete(dstMetadata, srcmd.BlobID)

		switch {
		case !exists:
			blobsToCopy = append(blobsToCopy, srcmd)
			totalCopyBytes += srcmd.Length
		case srcmd.Timestamp.After(dstmd.Timestamp) && c.repositorySyncUpdate:
			blobsToCopy = append(blobsToCopy, srcmd)
			totalCopyBytes += srcmd.Length
		default:
			inSyncBlobs++
			inSyncBytes += srcmd.Length
		}

		srcBlobs++
		c.outputSyncProgress(fmt.Sprintf("  Found %v BLOBs (%v) in the source repository, %v (%v) to copy", srcBlobs, units.BytesStringBase10(totalSrcSize), len(blobsToCopy), units.BytesStringBase10(totalCopyBytes)))

		return nil
	}); err != nil {
		return errors.Wrap(err, "error listing blobs")
	}

	c.finishSyncProcess()

	if c.repositorySyncDelete {
		for _, dstmd := range dstMetadata {
			// found in dst, not in src since we were deleting from dst as we found a match.
			blobsToDelete = append(blobsToDelete, dstmd)
			totalDeleteBytes += dstmd.Length
		}
	}

	log(ctx).Infof(
		"  Found %v BLOBs to delete (%v), %v in sync (%v)",
		len(blobsToDelete), units.BytesStringBase10(totalDeleteBytes),
		inSyncBlobs, units.BytesStringBase10(inSyncBytes),
	)

	if c.repositorySyncDryRun {
		return nil
	}

	log(ctx).Infof("Copying...")

	c.beginSyncProgress()

	finalErr := c.runSyncBlobs(ctx, src, dst, blobsToCopy, blobsToDelete, totalCopyBytes)

	c.finishSyncProcess()

	return finalErr
}

func (c *commandRepositorySyncTo) listDestinationBlobs(ctx context.Context, dst blob.Storage) (map[blob.ID]blob.Metadata, error) {
	dstTotalBytes := int64(0)
	dstMetadata := map[blob.ID]blob.Metadata{}

	c.beginSyncProgress()

	if err := dst.ListBlobs(ctx, "", func(bm blob.Metadata) error {
		dstMetadata[bm.BlobID] = bm
		dstTotalBytes += bm.Length
		c.outputSyncProgress(fmt.Sprintf("  Found %v BLOBs in the destination repository (%v)", len(dstMetadata), units.BytesStringBase10(dstTotalBytes)))
		return nil
	}); err != nil {
		return nil, errors.Wrap(err, "error listing BLOBs in destination repository")
	}

	c.finishSyncProcess()

	return dstMetadata, nil
}

func (c *commandRepositorySyncTo) beginSyncProgress() {
	c.lastSyncProgress = ""

	c.nextSyncOutputTime.Reset()
}

func (c *commandRepositorySyncTo) outputSyncProgress(s string) {
	c.syncProgressMutex.Lock()
	defer c.syncProgressMutex.Unlock()

	if len(s) < len(c.lastSyncProgress) {
		s += strings.Repeat(" ", len(c.lastSyncProgress)-len(s))
	}

	if c.nextSyncOutputTime.ShouldOutput(syncProgressInterval) {
		c.out.printStderr("\r%v", s)
	}

	c.lastSyncProgress = s
}

func (c *commandRepositorySyncTo) finishSyncProcess() {
	c.out.printStderr("\r%v\n", c.lastSyncProgress)
}

func (c *commandRepositorySyncTo) runSyncBlobs(ctx context.Context, src blob.Reader, dst blob.Storage, blobsToCopy, blobsToDelete []blob.Metadata, totalBytes int64) error {
	eg, ctx := errgroup.WithContext(ctx)
	copyCh := sliceToChannel(ctx, blobsToCopy)
	deleteCh := sliceToChannel(ctx, blobsToDelete)

	var progressMutex sync.Mutex

	var totalCopied stats.CountSum

	tt := timetrack.Start()

	for i := 0; i < c.repositorySyncParallelism; i++ {
		workerID := i

		eg.Go(func() error {
			for m := range copyCh {
				log(ctx).Debugf("[%v] Copying %v (%v bytes)...\n", workerID, m.BlobID, m.Length)
				if err := c.syncCopyBlob(ctx, m, src, dst); err != nil {
					return errors.Wrapf(err, "error copying %v", m.BlobID)
				}

				numBlobs, bytesCopied := totalCopied.Add(m.Length)
				progressMutex.Lock()
				eta := "unknown"
				speed := "-"

				if est, ok := tt.Estimate(float64(bytesCopied), float64(totalBytes)); ok {
					eta = fmt.Sprintf("%v (%v)", est.Remaining, formatTimestamp(est.EstimatedEndTime))
					speed = units.BitsPerSecondsString(est.SpeedPerSecond * 8) //nolint:gomnd
				}

				c.outputSyncProgress(
					fmt.Sprintf("  Copied %v blobs (%v), Speed: %v, ETA: %v",
						numBlobs, units.BytesStringBase10(bytesCopied), speed, eta))
				progressMutex.Unlock()
			}

			for m := range deleteCh {
				log(ctx).Debugf("[%v] Deleting %v (%v bytes)...\n", workerID, m.BlobID, m.Length)
				if err := syncDeleteBlob(ctx, m, dst); err != nil {
					return errors.Wrapf(err, "error deleting %v", m.BlobID)
				}
			}
			return nil
		})
	}

	if err := eg.Wait(); err != nil {
		return errors.Wrap(err, "error copying blobs")
	}

	return nil
}

func sliceToChannel(ctx context.Context, md []blob.Metadata) chan blob.Metadata {
	ch := make(chan blob.Metadata)

	go func() {
		defer close(ch)

		for _, it := range md {
			select {
			case ch <- it:
			case <-ctx.Done():
				return
			}
		}
	}()

	return ch
}

func (c *commandRepositorySyncTo) syncCopyBlob(ctx context.Context, m blob.Metadata, src blob.Reader, dst blob.Storage) error {
	data, err := src.GetBlob(ctx, m.BlobID, 0, -1)
	if err != nil {
		if errors.Is(err, blob.ErrBlobNotFound) {
			log(ctx).Infof("ignoring BLOB not found: %v", m.BlobID)
			return nil
		}

		return errors.Wrapf(err, "error reading blob '%v' from source", m.BlobID)
	}

	if err := dst.PutBlob(ctx, m.BlobID, gather.FromSlice(data)); err != nil {
		return errors.Wrapf(err, "error writing blob '%v' to destination", m.BlobID)
	}

	if c.repositorySyncTimes {
		if err := dst.SetTime(ctx, m.BlobID, m.Timestamp); err != nil {
			if errors.Is(err, blob.ErrSetTimeUnsupported) {
				c.setTimeUnsupportedOnce.Do(func() {
					log(ctx).Infof("destination repository does not support setting time")
				})
			}

			return errors.Wrapf(err, "error setting time on destination '%v'", m.BlobID)
		}
	}

	return nil
}

func syncDeleteBlob(ctx context.Context, m blob.Metadata, dst blob.Storage) error {
	err := dst.DeleteBlob(ctx, m.BlobID)

	if errors.Is(err, blob.ErrBlobNotFound) {
		return nil
	}

	return errors.Wrap(err, "error deleting blob")
}

func (c *commandRepositorySyncTo) ensureRepositoriesHaveSameFormatBlob(ctx context.Context, src blob.Reader, dst blob.Storage) error {
	srcData, err := src.GetBlob(ctx, repo.FormatBlobID, 0, -1)
	if err != nil {
		return errors.Wrap(err, "error reading format blob")
	}

	dstData, err := dst.GetBlob(ctx, repo.FormatBlobID, 0, -1)
	if err != nil {
		// target does not have format blob, save it there first.
		if errors.Is(err, blob.ErrBlobNotFound) {
			if c.repositorySyncDestinationMustExist {
				return errors.Errorf("destination repository does not have a format blob")
			}

			return dst.PutBlob(ctx, repo.FormatBlobID, gather.FromSlice(srcData))
		}

		return errors.Wrap(err, "error reading destination repository format blob")
	}

	if bytes.Equal(srcData, dstData) {
		return nil
	}

	return errors.Errorf("destination repository contains incompatible data")
}
