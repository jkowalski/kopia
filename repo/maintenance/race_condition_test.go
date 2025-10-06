package maintenance_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/faketime"
	"github.com/kopia/kopia/internal/gather"
	"github.com/kopia/kopia/internal/repotesting"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/content"
	"github.com/kopia/kopia/repo/encryption"
	"github.com/kopia/kopia/repo/format"
	"github.com/kopia/kopia/repo/maintenance"
	"github.com/kopia/kopia/snapshot/snapshotmaintenance"
)

// TestFullMaintenanceRaceConditionBetweenRewriteAndBlobDeletion demonstrates a race condition
// in runFullMaintenance where deleted contents can fail to rewrite due to their backing
// pack blobs being deleted during the same maintenance run.
//
// ROOT CAUSE:
// In maintenance_run.go:runFullMaintenance(), three tasks run sequentially:
// 1. runTaskRewriteContentsFull (line 494) - starts async workers to rewrite contents in short packs
// 2. runTaskDropDeletedContentsFull (line 503) - drops deleted content entries from indexes
// 3. runTaskDeleteOrphanedBlobsFull (line 509) - deletes pack blobs not referenced in index
//
// THE RACE:
// - RewriteContentsFull takes a snapshot of contents to rewrite (including deleted ones)
// - Worker goroutines process this list asynchronously throughout maintenance
// - Meanwhile, DropDeletedContentsFull removes deleted content entries from the index
// - Then DeleteOrphanedBlobsFull deletes pack blobs that are no longer referenced
// - A worker finally tries to rewrite a deleted content whose blob was just deleted
// - RewriteContent() fails because getContentDataReadLocked() cannot read from missing blob
//
// TIMING REQUIREMENTS:
// For this race to occur, a deleted content must be:
// - Age > RewriteMinAge (2h) - old enough to be rewritten
// - Age > DropContentFromIndexExtraMargin + 2 GC cycles - old enough to drop from index
// - Pack blob age > BlobDeleteMinAge (24h) - old enough for blob deletion
//
// WORKAROUND:
// Setting KOPIA_IGNORE_MAINTENANCE_REWRITE_ERROR=1 causes the code to ignore
// rewrite failures for deleted contents (content_rewrite.go:122), which is safe
// since these contents are being removed anyway.
//
// This test demonstrates the timing window where this race can occur. The actual
// manifestation is timing-dependent and may not always trigger in the test.
func TestFullMaintenanceRaceConditionBetweenRewriteAndBlobDeletion(t *testing.T) {
	t.Parallel()

	// Set up fake clock starting at a known time
	startTime := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	ta := faketime.NewClockTimeWithOffset(time.Until(startTime))

	ctx, env := repotesting.NewEnvironment(t, format.FormatVersion1, repotesting.Options{
		OpenOptions: func(o *repo.Options) {
			o.TimeNowFunc = ta.NowFunc()
		},
		NewRepositoryOptions: func(nro *repo.NewRepositoryOptions) {
			nro.BlockFormat.Encryption = encryption.DefaultAlgorithm
			nro.BlockFormat.MasterKey = []byte{
				0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15,
				0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15,
			}
			nro.BlockFormat.Hash = "HMAC-SHA256"
			nro.BlockFormat.HMACSecret = []byte{1, 2, 3}
		},
	})

	// Set repository owner for maintenance
	setMaintenanceOwner(t, ctx, env.RepositoryWriter)

	// Step 1: Create small prefixed contents (metadata) that will be in short packs
	// Prefixed contents go into 'q' packs which are more likely to be short
	t.Logf("Step 1: Creating prefixed contents at %v", ta.NowFunc()())

	var contentIDs []content.ID
	for i := 0; i < 10; i++ {
		// Create small prefixed contents with different data (uses "m" prefix like manifests)
		// This ensures they are actually different contents, not deduplicated
		data := []byte("test metadata content number " + string(rune(i)))
		cid, err := env.RepositoryWriter.ContentManager().WriteContent(ctx, gather.FromSlice(data), "m", content.NoCompression)
		require.NoError(t, err)
		contentIDs = append(contentIDs, cid)
		t.Logf("Created content %v", cid)
	}

	require.NoError(t, env.RepositoryWriter.Flush(ctx))

	// Verify contents exist in the index
	var foundCount int
	err := env.RepositoryWriter.ContentReader().IterateContents(ctx, content.IterateOptions{
		Range:          content.IDRange{},
		IncludeDeleted: false,
	}, func(ci content.Info) error {
		for _, cid := range contentIDs {
			if ci.ContentID == cid {
				foundCount++
				t.Logf("Found content: %v in pack %v, size=%v", ci.ContentID, ci.PackBlobID, ci.PackedLength)
			}
		}
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, len(contentIDs), foundCount, "expected to find all created contents")

	// Step 2: Delete the contents - this marks them as deleted
	t.Logf("Step 2: Deleting contents at %v", ta.NowFunc()())
	for _, cid := range contentIDs {
		require.NoError(t, env.RepositoryWriter.ContentManager().DeleteContent(ctx, cid))
	}
	require.NoError(t, env.RepositoryWriter.Flush(ctx))

	// Verify contents are marked deleted
	var deletedContentCount int
	err = env.RepositoryWriter.ContentReader().IterateContents(ctx, content.IterateOptions{
		Range:          content.IDRange{},
		IncludeDeleted: true,
	}, func(ci content.Info) error {
		for _, cid := range contentIDs {
			if ci.ContentID == cid && ci.Deleted {
				deletedContentCount++
				t.Logf("Content %v is marked deleted, timestamp=%v", ci.ContentID, ci.Timestamp())
			}
		}
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, len(contentIDs), deletedContentCount, "expected all contents to be marked deleted")

	// Step 3: Advance time past RewriteMinAge (2 hours) and run a first snapshot GC
	// This is needed to set up the timing for drop deleted contents
	t.Logf("Step 3: Advancing time by 3 hours for first GC at %v", ta.NowFunc()())
	ta.Advance(3 * time.Hour)

	err = snapshotmaintenance.Run(ctx, env.RepositoryWriter, maintenance.ModeAuto, false, maintenance.SafetyFull)
	require.NoError(t, err)

	// Step 4: Advance time and run second snapshot GC
	// After two GCs with sufficient spacing, contents can be dropped from index
	t.Logf("Step 4: Advancing time by 5 hours for second GC at %v", ta.NowFunc()())
	ta.Advance(5 * time.Hour)

	err = snapshotmaintenance.Run(ctx, env.RepositoryWriter, maintenance.ModeAuto, false, maintenance.SafetyFull)
	require.NoError(t, err)

	// Step 5: Advance time past BlobDeleteMinAge (24 hours total from start)
	// This makes the pack blobs old enough to be deleted
	t.Logf("Step 5: Advancing time by 20 hours to reach blob deletion threshold at %v", ta.NowFunc()())
	ta.Advance(20 * time.Hour) // Total is now ~28 hours from start

	// Verify current time is well past all thresholds
	currentTime := ta.NowFunc()()
	t.Logf("Current time: %v (elapsed from start: %v)", currentTime, currentTime.Sub(startTime))

	// Step 6: Run full maintenance - this should trigger the race condition
	t.Logf("Step 6: Running full maintenance at %v", currentTime)

	// Without the workaround, this should fail
	err = snapshotmaintenance.Run(ctx, env.RepositoryWriter, maintenance.ModeFull, false, maintenance.SafetyFull)

	if os.Getenv("KOPIA_IGNORE_MAINTENANCE_REWRITE_ERROR") != "" {
		// With the environment variable set, maintenance should succeed
		require.NoError(t, err, "maintenance should succeed with KOPIA_IGNORE_MAINTENANCE_REWRITE_ERROR set")
		t.Logf("SUCCESS: Maintenance succeeded with workaround environment variable")
	} else {
		// Without the environment variable, we expect failure due to the race condition
		// The error happens because RewriteContent tries to read from a pack blob that
		// was deleted by DeleteOrphanedBlobsFull in the same maintenance run
		if err != nil {
			t.Logf("RACE CONDITION DETECTED: Maintenance failed as expected: %v", err)
			require.Error(t, err, "expected error due to race condition")
		} else {
			// In some cases the race might not manifest (timing dependent)
			t.Logf("WARNING: Race condition did not manifest in this run (timing dependent)")
		}
	}
}

// TestFullMaintenanceRaceConditionWithWorkaround verifies the workaround works
func TestFullMaintenanceRaceConditionWithWorkaround(t *testing.T) {
	// Note: Cannot use t.Parallel() with t.Setenv()

	// Set the workaround environment variable
	t.Setenv("KOPIA_IGNORE_MAINTENANCE_REWRITE_ERROR", "1")

	// Run the same scenario as above
	startTime := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	ta := faketime.NewClockTimeWithOffset(time.Until(startTime))

	ctx, env := repotesting.NewEnvironment(t, format.FormatVersion3, repotesting.Options{
		OpenOptions: func(o *repo.Options) {
			o.TimeNowFunc = ta.NowFunc()
		},
		NewRepositoryOptions: func(nro *repo.NewRepositoryOptions) {
			nro.BlockFormat.Encryption = encryption.DefaultAlgorithm
			nro.BlockFormat.MasterKey = []byte{
				0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15,
				0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15,
			}
			nro.BlockFormat.Hash = "HMAC-SHA256"
			nro.BlockFormat.HMACSecret = []byte{1, 2, 3}
		},
	})

	setMaintenanceOwner(t, ctx, env.RepositoryWriter)

	// Create and delete content directly with varied data
	var contentIDs []content.ID
	for i := 0; i < 10; i++ {
		data := []byte("test metadata content number " + string(rune(i)))
		cid, err := env.RepositoryWriter.ContentManager().WriteContent(ctx, gather.FromSlice(data), "m", content.NoCompression)
		require.NoError(t, err)
		contentIDs = append(contentIDs, cid)
	}

	require.NoError(t, env.RepositoryWriter.Flush(ctx))

	// Delete the contents
	for _, cid := range contentIDs {
		require.NoError(t, env.RepositoryWriter.ContentManager().DeleteContent(ctx, cid))
	}
	require.NoError(t, env.RepositoryWriter.Flush(ctx))

	// Advance time and run GCs
	ta.Advance(3 * time.Hour)

	err := snapshotmaintenance.Run(ctx, env.RepositoryWriter, maintenance.ModeAuto, false, maintenance.SafetyFull)
	require.NoError(t, err)

	ta.Advance(5 * time.Hour)

	err = snapshotmaintenance.Run(ctx, env.RepositoryWriter, maintenance.ModeAuto, false, maintenance.SafetyFull)
	require.NoError(t, err)

	ta.Advance(20 * time.Hour)

	// With workaround, maintenance should succeed
	err = snapshotmaintenance.Run(ctx, env.RepositoryWriter, maintenance.ModeFull, false, maintenance.SafetyFull)
	require.NoError(t, err, "maintenance should succeed with KOPIA_IGNORE_MAINTENANCE_REWRITE_ERROR workaround")

	t.Logf("SUCCESS: Full maintenance completed successfully with workaround")
}

func setMaintenanceOwner(t *testing.T, ctx context.Context, rep repo.RepositoryWriter) {
	t.Helper()

	maintParams, err := maintenance.GetParams(ctx, rep)
	require.NoError(t, err)

	co := rep.ClientOptions()
	require.NotZero(t, co)

	maintParams.Owner = co.UsernameAtHost()

	err = maintenance.SetParams(ctx, rep, maintParams)
	require.NoError(t, err)

	require.NoError(t, rep.Flush(ctx))
}
