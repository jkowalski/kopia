// Package snapshotmaintenance provides helpers to run snapshot GC and low-level repository snapshotmaintenance.
package snapshotmaintenance

import (
	"context"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/maintenance"
	"github.com/kopia/kopia/snapshot/snapshotgc"
)

// Run runs the complete snapshot and repository maintenance.
func Run(ctx context.Context, dr repo.DirectRepositoryWriter, mode maintenance.Mode, force bool) error {
	return maintenance.RunExclusive(ctx, dr, mode, force,
		func(runParams maintenance.RunParameters) error {
			// run snapshot GC before full maintenance
			if runParams.Mode == maintenance.ModeFull {
				if _, err := snapshotgc.Run(ctx, dr, runParams.Params.SnapshotGC, true); err != nil {
					return errors.Wrap(err, "snapshot GC failure")
				}
			}

			return maintenance.Run(ctx, runParams)
		})
}
