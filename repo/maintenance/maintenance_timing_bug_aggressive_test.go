package maintenance_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/faketime"
	"github.com/kopia/kopia/internal/repotesting"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/maintenance"
	"github.com/kopia/kopia/repo/object"
	"github.com/kopia/kopia/snapshot/snapshotmaintenance"
)

// TestMaintenanceTimingBug_RapidCycles - Try to break safety with rapid maintenance cycles
func (s *formatSpecificTestSuite) TestMaintenanceTimingBug_RapidCycles(t *testing.T) {
	ft := faketime.NewClockTimeWithOffset(0)

	ctx, env := repotesting.NewEnvironment(t, s.formatVersion, repotesting.Options{
		OpenOptions: func(o *repo.Options) {
			o.TimeNowFunc = ft.NowFunc()
		},
	})

	slowClient := env.MustConnectOpenAnother(t, func(o *repo.Options) {
		o.TimeNowFunc = ft.NowFunc()
	})

	// Create content
	var objectID object.ID
	require.NoError(t, repo.WriteSession(ctx, env.Repository, repo.WriteSessionOptions{}, func(ctx context.Context, w repo.RepositoryWriter) error {
		ow := w.NewObjectWriter(ctx, object.WriterOptions{Prefix: "y"})
		fmt.Fprintf(ow, "test content")
		var err error
		objectID, err = ow.Result()
		return err
	}))

	require.NoError(t, slowClient.Refresh(ctx))
	verifyObjectReadable(ctx, t, slowClient, objectID)

	t.Logf("==== Baseline maintenance ====")
	require.NoError(t, snapshotmaintenance.Run(ctx, env.RepositoryWriter, maintenance.ModeFull, true, maintenance.SafetyFull))

	// Age content to make it GC eligible
	ft.Advance(25 * time.Hour)

	t.Logf("==== Mark content as deleted ====")
	require.NoError(t, snapshotmaintenance.Run(ctx, env.RepositoryWriter, maintenance.ModeFull, true, maintenance.SafetyFull))
	require.NoError(t, env.Repository.Refresh(ctx))
	// Slow client does NOT refresh

	// Now do RAPID maintenance cycles with minimal time between them
	// This might cause timing calculations to fail
	for i := 0; i < 10; i++ {
		// Advance just past the minimum required (MarginBetweenSnapshotGC = 4h)
		ft.Advance(4*time.Hour + 5*time.Minute)

		t.Logf("==== Rapid maintenance cycle %d at T+%v ====", i, ft.NowFunc()().Sub(time.Time{}))
		require.NoError(t, snapshotmaintenance.Run(ctx, env.RepositoryWriter, maintenance.ModeFull, true, maintenance.SafetyFull))
		require.NoError(t, env.Repository.Refresh(ctx))

		// Slow client STILL hasn't refreshed - simulating offline client
	}

	// Try to read with slow client
	t.Logf("==== Attempting to read with slow client ====")
	require.NoError(t, slowClient.Refresh(ctx))

	err := repo.WriteSession(ctx, slowClient, repo.WriteSessionOptions{}, func(ctx context.Context, w repo.RepositoryWriter) error {
		r, err := w.OpenObject(ctx, objectID)
		if err != nil {
			t.Logf("*** ERROR DETECTED ***: Cannot read object %v: %v", objectID, err)
			t.Logf("Error details: %+v", err)
			t.Logf("Checking error chain...")

			// Check if this wraps the actual BLOB not found error
			if errors.Is(err, blob.ErrBlobNotFound) {
				t.Logf("✅ CONFIRMED: Error chain contains blob.ErrBlobNotFound")
				t.Logf("*** BUG REPRODUCED ***: BLOB actually deleted from storage")
			} else if errors.Is(err, object.ErrObjectNotFound) {
				t.Logf("⚠️  Error is object.ErrObjectNotFound but NOT blob.ErrBlobNotFound")
				t.Logf("This might be a different issue (index problem, not blob deletion)")
			}
			return err
		}
		r.Close()
		return nil
	})

	if err != nil {
		// CRITICAL: Must be blob.ErrBlobNotFound to match issue #4769
		if !errors.Is(err, blob.ErrBlobNotFound) {
			t.Fatalf("WRONG ERROR TYPE: Got %v, but need blob.ErrBlobNotFound to match #4769", err)
		}
		t.Fatalf("BUG REPRODUCED: BLOB deleted prematurely after %d rapid cycles: %v", 10, err)
	}
}

// TestMaintenanceTimingBug_BoundaryCondition - Test exact boundary of safety margins
func (s *formatSpecificTestSuite) TestMaintenanceTimingBug_BoundaryCondition(t *testing.T) {
	ft := faketime.NewClockTimeWithOffset(0)

	ctx, env := repotesting.NewEnvironment(t, s.formatVersion, repotesting.Options{
		OpenOptions: func(o *repo.Options) {
			o.TimeNowFunc = ft.NowFunc()
		},
	})

	slowClient := env.MustConnectOpenAnother(t, func(o *repo.Options) {
		o.TimeNowFunc = ft.NowFunc()
	})

	var objectID object.ID
	require.NoError(t, repo.WriteSession(ctx, env.Repository, repo.WriteSessionOptions{}, func(ctx context.Context, w repo.RepositoryWriter) error {
		ow := w.NewObjectWriter(ctx, object.WriterOptions{Prefix: "y"})
		fmt.Fprintf(ow, "boundary test")
		var err error
		objectID, err = ow.Result()
		return err
	}))

	require.NoError(t, slowClient.Refresh(ctx))

	t.Logf("==== Initial maintenance ====")
	require.NoError(t, snapshotmaintenance.Run(ctx, env.RepositoryWriter, maintenance.ModeFull, true, maintenance.SafetyFull))

	// Advance exactly 24 hours (BlobDeleteMinAge)
	ft.Advance(24 * time.Hour)

	t.Logf("==== Maintenance at exactly 24h mark (BlobDeleteMinAge boundary) ====")
	require.NoError(t, snapshotmaintenance.Run(ctx, env.RepositoryWriter, maintenance.ModeFull, true, maintenance.SafetyFull))
	require.NoError(t, env.Repository.Refresh(ctx))

	// Advance exactly 4 hours (MarginBetweenSnapshotGC)
	ft.Advance(4 * time.Hour)

	t.Logf("==== Maintenance at exactly 4h after previous (MarginBetweenSnapshotGC boundary) ====")
	require.NoError(t, snapshotmaintenance.Run(ctx, env.RepositoryWriter, maintenance.ModeFull, true, maintenance.SafetyFull))
	require.NoError(t, env.Repository.Refresh(ctx))

	// One more cycle
	ft.Advance(4 * time.Hour)
	require.NoError(t, snapshotmaintenance.Run(ctx, env.RepositoryWriter, maintenance.ModeFull, true, maintenance.SafetyFull))

	// Slow client tries to read
	require.NoError(t, slowClient.Refresh(ctx))
	err := repo.WriteSession(ctx, slowClient, repo.WriteSessionOptions{}, func(ctx context.Context, w repo.RepositoryWriter) error {
		_, err := w.OpenObject(ctx, objectID)
		if err != nil {
			t.Logf("*** ERROR DETECTED ***: Blob deleted at exact boundary: %v", err)
			t.Logf("Error type: %T", err)

			// Check for the actual BLOB not found error
			if errors.Is(err, blob.ErrBlobNotFound) {
				t.Logf("✅ CONFIRMED: blob.ErrBlobNotFound - BLOB actually deleted")
			} else {
				t.Logf("⚠️  Different error type - not blob deletion")
			}
		}
		return err
	})

	if err != nil {
		require.ErrorIs(t, err, blob.ErrBlobNotFound, "MUST be blob.ErrBlobNotFound to match #4769")
		t.Fatalf("BUG REPRODUCED at boundary condition: %v", err)
	}
}

// TestMaintenanceTimingBug_MaintenanceSpansMultipleSafetyWindows - Maintenance that spans 30+ hours
func (s *formatSpecificTestSuite) TestMaintenanceTimingBug_MaintenanceSpansMultipleSafetyWindows(t *testing.T) {
	ft := faketime.NewClockTimeWithOffset(0)

	ctx, env := repotesting.NewEnvironment(t, s.formatVersion, repotesting.Options{
		OpenOptions: func(o *repo.Options) {
			o.TimeNowFunc = ft.NowFunc()
		},
	})

	slowClient := env.MustConnectOpenAnother(t, func(o *repo.Options) {
		o.TimeNowFunc = ft.NowFunc()
	})

	// Create multiple objects
	var objectIDs []object.ID
	for i := 0; i < 5; i++ {
		var oid object.ID
		require.NoError(t, repo.WriteSession(ctx, env.Repository, repo.WriteSessionOptions{}, func(ctx context.Context, w repo.RepositoryWriter) error {
			ow := w.NewObjectWriter(ctx, object.WriterOptions{Prefix: "y"})
			fmt.Fprintf(ow, "multi-window test %d", i)
			var err error
			oid, err = ow.Result()
			return err
		}))
		objectIDs = append(objectIDs, oid)
	}

	require.NoError(t, slowClient.Refresh(ctx))

	t.Logf("==== Initial maintenance ====")
	require.NoError(t, snapshotmaintenance.Run(ctx, env.RepositoryWriter, maintenance.ModeFull, true, maintenance.SafetyFull))

	// Age content significantly
	ft.Advance(25 * time.Hour)

	t.Logf("==== Long maintenance session that spans BEYOND BlobDeleteMinAge (30 hours) ====")
	maintenanceStart := ft.NowFunc()()

	// Start maintenance
	require.NoError(t, snapshotmaintenance.Run(ctx, env.RepositoryWriter, maintenance.ModeFull, true, maintenance.SafetyFull))

	// Simulate maintenance taking 30 hours (longer than BlobDeleteMinAge of 24h!)
	ft.Advance(30 * time.Hour)
	maintenanceEnd := ft.NowFunc()()

	t.Logf("Simulated maintenance duration: %v (exceeds BlobDeleteMinAge of 24h)", maintenanceEnd.Sub(maintenanceStart))

	require.NoError(t, env.Repository.Refresh(ctx))

	// Another maintenance right after
	ft.Advance(1 * time.Hour)

	t.Logf("==== Maintenance immediately after long session ====")
	require.NoError(t, snapshotmaintenance.Run(ctx, env.RepositoryWriter, maintenance.ModeFull, true, maintenance.SafetyFull))
	require.NoError(t, env.Repository.Refresh(ctx))

	// Try reading without refreshing slow client yet
	t.Logf("==== Slow client attempts read ====")
	require.NoError(t, slowClient.Refresh(ctx))

	for i, oid := range objectIDs {
		err := repo.WriteSession(ctx, slowClient, repo.WriteSessionOptions{}, func(ctx context.Context, w repo.RepositoryWriter) error {
			_, err := w.OpenObject(ctx, oid)
			if err != nil {
				t.Logf("*** BUG REPRODUCED ***: Object %d deleted after maintenance spanned 30h: %v", i, err)
			}
			return err
		})

		if err != nil {
			t.Fatalf("BUG REPRODUCED: Object %d lost after 30h maintenance span: %v", i, err)
		}
	}
}

// TestMaintenanceTimingBug_ContentCreatedDuringLongMaintenance - Create content DURING a long maintenance
func (s *formatSpecificTestSuite) TestMaintenanceTimingBug_ContentCreatedDuringLongMaintenance(t *testing.T) {
	ft := faketime.NewClockTimeWithOffset(0)

	ctx, env := repotesting.NewEnvironment(t, s.formatVersion, repotesting.Options{
		OpenOptions: func(o *repo.Options) {
			o.TimeNowFunc = ft.NowFunc()
		},
	})

	slowClient := env.MustConnectOpenAnother(t, func(o *repo.Options) {
		o.TimeNowFunc = ft.NowFunc()
	})

	t.Logf("==== Initial maintenance ====")
	require.NoError(t, snapshotmaintenance.Run(ctx, env.RepositoryWriter, maintenance.ModeFull, true, maintenance.SafetyFull))

	ft.Advance(25 * time.Hour)

	t.Logf("==== Start long maintenance ====")
	maintenanceStartTime := ft.NowFunc()()
	require.NoError(t, snapshotmaintenance.Run(ctx, env.RepositoryWriter, maintenance.ModeFull, true, maintenance.SafetyFull))

	// Advance time to simulate maintenance is still running
	ft.Advance(10 * time.Hour)

	// Create content DURING the simulated long maintenance window
	t.Logf("==== Creating content at T+10h into maintenance ====")
	var objectID object.ID
	require.NoError(t, repo.WriteSession(ctx, env.Repository, repo.WriteSessionOptions{}, func(ctx context.Context, w repo.RepositoryWriter) error {
		ow := w.NewObjectWriter(ctx, object.WriterOptions{Prefix: "y"})
		fmt.Fprintf(ow, "created during maintenance")
		var err error
		objectID, err = ow.Result()
		return err
	}))

	contentCreationTime := ft.NowFunc()()
	t.Logf("Content created at: %v (maintenance started at: %v, delta: %v)",
		contentCreationTime, maintenanceStartTime, contentCreationTime.Sub(maintenanceStartTime))

	require.NoError(t, slowClient.Refresh(ctx))
	verifyObjectReadable(ctx, t, slowClient, objectID)

	// Continue time advancement
	ft.Advance(5 * time.Hour)
	require.NoError(t, env.Repository.Refresh(ctx))

	// Next maintenance cycle - this is where it might delete the blob prematurely
	ft.Advance(10 * time.Hour)
	t.Logf("==== Next maintenance cycle at T+25h from content creation ====")
	require.NoError(t, snapshotmaintenance.Run(ctx, env.RepositoryWriter, maintenance.ModeFull, true, maintenance.SafetyFull))
	require.NoError(t, env.Repository.Refresh(ctx))

	// Slow client reads
	require.NoError(t, slowClient.Refresh(ctx))
	err := repo.WriteSession(ctx, slowClient, repo.WriteSessionOptions{}, func(ctx context.Context, w repo.RepositoryWriter) error {
		_, err := w.OpenObject(ctx, objectID)
		if err != nil {
			t.Logf("*** BUG REPRODUCED ***: Content created during maintenance window was deleted: %v", err)
		}
		return err
	})

	if err != nil {
		t.Fatalf("BUG REPRODUCED: Content created during long maintenance deleted: %v", err)
	}
}

// TestMaintenanceTimingBug_VerifyBlobActuallyDeleted - Verify blob is actually deleted from storage
func (s *formatSpecificTestSuite) TestMaintenanceTimingBug_VerifyBlobActuallyDeleted(t *testing.T) {
	ft := faketime.NewClockTimeWithOffset(0)

	ctx, env := repotesting.NewEnvironment(t, s.formatVersion, repotesting.Options{
		OpenOptions: func(o *repo.Options) {
			o.TimeNowFunc = ft.NowFunc()
		},
	})

	var objectID object.ID

	require.NoError(t, repo.WriteSession(ctx, env.Repository, repo.WriteSessionOptions{}, func(ctx context.Context, w repo.RepositoryWriter) error {
		ow := w.NewObjectWriter(ctx, object.WriterOptions{Prefix: "y"})
		fmt.Fprintf(ow, "verify blob deletion test")
		var err error
		objectID, err = ow.Result()

		if err == nil {
			cid, _, _ := objectID.ContentID()
			t.Logf("Created object %v with content %v", objectID, cid)
		}

		return err
	}))

	t.Logf("==== Initial maintenance ====")
	require.NoError(t, snapshotmaintenance.Run(ctx, env.RepositoryWriter, maintenance.ModeFull, true, maintenance.SafetyFull))

	// Age to GC eligibility
	ft.Advance(25 * time.Hour)

	t.Logf("==== Mark deleted ====")
	require.NoError(t, snapshotmaintenance.Run(ctx, env.RepositoryWriter, maintenance.ModeFull, true, maintenance.SafetyFull))

	// Minimal spacing
	ft.Advance(4 * time.Hour)
	t.Logf("==== GC cycle that should trigger deletion ====")
	require.NoError(t, snapshotmaintenance.Run(ctx, env.RepositoryWriter, maintenance.ModeFull, true, maintenance.SafetyFull))

	ft.Advance(4 * time.Hour)
	require.NoError(t, snapshotmaintenance.Run(ctx, env.RepositoryWriter, maintenance.ModeFull, true, maintenance.SafetyFull))

	// Try to read - this should fail because blob was deleted
	require.NoError(t, env.Repository.Refresh(ctx))
	err := repo.WriteSession(ctx, env.Repository, repo.WriteSessionOptions{}, func(ctx context.Context, w repo.RepositoryWriter) error {
		_, err := w.OpenObject(ctx, objectID)
		if err != nil {
			t.Logf("*** BUG REPRODUCED - VERIFIED BLOB DELETION ***")
			t.Logf("Object %v cannot be read: %v", objectID, err)
			t.Logf("This confirms the underlying blob was actually deleted from storage")

			// Verify this is the actual BLOB not found error
			if errors.Is(err, blob.ErrBlobNotFound) {
				t.Logf("✅ VERIFIED: blob.ErrBlobNotFound in error chain")
			} else {
				t.Logf("⚠️  NOT blob.ErrBlobNotFound - this is a different issue")
			}

			// Try to check if content still exists in index
			cid, _, _ := objectID.ContentID()
			contentInfo, err2 := w.ContentInfo(ctx, cid)
			if err2 != nil {
				t.Logf("Content info lookup failed: %v", err2)
			} else {
				t.Logf("Content still in index but deleted=%v: %+v", contentInfo.Deleted, contentInfo)
			}

			require.ErrorIs(t, err, blob.ErrBlobNotFound, "MUST be blob.ErrBlobNotFound")
		}
		return err
	})

	if err != nil {
		t.Fatalf("BUG REPRODUCED AND VERIFIED: Blob actually deleted from storage: %v", err)
	}
}

// TestMaintenanceTimingBug_MinimalDelayBetweenGCCycles - Try to trigger with minimal delays
func (s *formatSpecificTestSuite) TestMaintenanceTimingBug_MinimalDelayBetweenGCCycles(t *testing.T) {
	ft := faketime.NewClockTimeWithOffset(0)

	ctx, env := repotesting.NewEnvironment(t, s.formatVersion, repotesting.Options{
		OpenOptions: func(o *repo.Options) {
			o.TimeNowFunc = ft.NowFunc()
		},
	})

	slowClient := env.MustConnectOpenAnother(t, func(o *repo.Options) {
		o.TimeNowFunc = ft.NowFunc()
	})

	var objectID object.ID
	require.NoError(t, repo.WriteSession(ctx, env.Repository, repo.WriteSessionOptions{}, func(ctx context.Context, w repo.RepositoryWriter) error {
		ow := w.NewObjectWriter(ctx, object.WriterOptions{Prefix: "y"})
		fmt.Fprintf(ow, "minimal delay test")
		var err error
		objectID, err = ow.Result()
		return err
	}))

	require.NoError(t, slowClient.Refresh(ctx))

	t.Logf("==== GC #1 ====")
	require.NoError(t, snapshotmaintenance.Run(ctx, env.RepositoryWriter, maintenance.ModeFull, true, maintenance.SafetyFull))

	// Age to make eligible for GC
	ft.Advance(25 * time.Hour)

	t.Logf("==== GC #2 - mark deleted ====")
	require.NoError(t, snapshotmaintenance.Run(ctx, env.RepositoryWriter, maintenance.ModeFull, true, maintenance.SafetyFull))
	require.NoError(t, env.Repository.Refresh(ctx))

	// Wait EXACTLY MarginBetweenSnapshotGC (4 hours) - no more
	ft.Advance(4 * time.Hour)

	t.Logf("==== GC #3 - exactly at margin ====")
	require.NoError(t, snapshotmaintenance.Run(ctx, env.RepositoryWriter, maintenance.ModeFull, true, maintenance.SafetyFull))
	require.NoError(t, env.Repository.Refresh(ctx))

	// One more at minimum interval
	ft.Advance(4 * time.Hour)

	t.Logf("==== GC #4 - might drop from index ====")
	require.NoError(t, snapshotmaintenance.Run(ctx, env.RepositoryWriter, maintenance.ModeFull, true, maintenance.SafetyFull))
	require.NoError(t, env.Repository.Refresh(ctx))

	// Slow client tries to read
	require.NoError(t, slowClient.Refresh(ctx))
	err := repo.WriteSession(ctx, slowClient, repo.WriteSessionOptions{}, func(ctx context.Context, w repo.RepositoryWriter) error {
		_, err := w.OpenObject(ctx, objectID)
		if err != nil {
			t.Logf("*** ERROR DETECTED ***: Object deleted with minimal GC delays: %v", err)
			if errors.Is(err, blob.ErrBlobNotFound) {
				t.Logf("✅ CONFIRMED: blob.ErrBlobNotFound")
			}
		}
		return err
	})

	if err != nil {
		require.ErrorIs(t, err, blob.ErrBlobNotFound, "MUST be blob.ErrBlobNotFound")
		t.Fatalf("BUG REPRODUCED with minimal delays: %v", err)
	}
}
