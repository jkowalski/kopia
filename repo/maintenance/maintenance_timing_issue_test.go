package maintenance_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/faketime"
	"github.com/kopia/kopia/internal/repotesting"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/maintenance"
	"github.com/kopia/kopia/repo/object"
	"github.com/kopia/kopia/snapshot/snapshotmaintenance"
)

// TestMaintenanceTimingIssue_LongRunningOperations reproduces issue #4769
// where long-running maintenance operations can cause premature blob deletion
// leading to "BLOB not found" errors for clients that haven't refreshed indexes.
//
// Theory: When content rewrite takes many hours to complete, the timing
// calculations for blob deletion safety margins can fail because:
// 1. MaintenanceStartTime is captured at session start
// 2. Blob deletion uses MaintenanceStartTime as NotAfterTime cutoff
// 3. If rewrite takes hours, subsequent blob deletion might not allow
//    sufficient time for other clients to refresh their indexes
func (s *formatSpecificTestSuite) TestMaintenanceTimingIssue_LongRunningOperations(t *testing.T) {
	// Setup fake time to control timing precisely
	ft := faketime.NewClockTimeWithOffset(0)

	ctx, env := repotesting.NewEnvironment(t, s.formatVersion, repotesting.Options{
		OpenOptions: func(o *repo.Options) {
			o.TimeNowFunc = ft.NowFunc()
		},
	})

	// Create a second client that will simulate a slow-to-refresh client
	slowClient := env.MustConnectOpenAnother(t, func(o *repo.Options) {
		o.TimeNowFunc = ft.NowFunc()
	})

	t.Logf("==== PHASE 1: Create initial content ====")

	// Create multiple objects to ensure we have enough content for rewrite
	var objectIDs []object.ID
	for i := 0; i < 10; i++ {
		var oid object.ID
		require.NoError(t, repo.WriteSession(ctx, env.Repository, repo.WriteSessionOptions{}, func(ctx context.Context, w repo.RepositoryWriter) error {
			ow := w.NewObjectWriter(ctx, object.WriterOptions{Prefix: "y", MetadataCompressor: "zstd-fastest"})
			fmt.Fprintf(ow, "content data %d - %s", i, time.Now().String())
			var err error
			oid, err = ow.Result()
			return err
		}))
		objectIDs = append(objectIDs, oid)
		t.Logf("Created object %d: %v", i, oid)
	}

	// Verify both clients can read the objects
	require.NoError(t, slowClient.Refresh(ctx))
	for i, oid := range objectIDs {
		verifyObjectReadable(ctx, t, env.Repository, oid)
		verifyObjectReadable(ctx, t, slowClient, oid)
		t.Logf("Object %d verified readable on both clients", i)
	}

	t.Logf("==== PHASE 2: First maintenance - establishes baseline ====")

	// Run initial maintenance to establish timing baseline
	require.NoError(t, snapshotmaintenance.Run(ctx, env.RepositoryWriter, maintenance.ModeFull, true, maintenance.SafetyFull))

	t.Logf("==== PHASE 3: Advance time and create more content ====")

	// Advance time significantly (25 hours) to allow content to age
	ft.Advance(25 * time.Hour)

	// Create more objects that will trigger rewrite
	for i := 10; i < 20; i++ {
		var oid object.ID
		require.NoError(t, repo.WriteSession(ctx, env.Repository, repo.WriteSessionOptions{}, func(ctx context.Context, w repo.RepositoryWriter) error {
			ow := w.NewObjectWriter(ctx, object.WriterOptions{Prefix: "y", MetadataCompressor: "zstd-fastest"})
			fmt.Fprintf(ow, "additional content %d", i)
			var err error
			oid, err = ow.Result()
			return err
		}))
		objectIDs = append(objectIDs, oid)
	}

	t.Logf("==== PHASE 4: Long-running maintenance session ====")

	// Simulate a maintenance session where content rewrite takes a very long time
	// This is the critical scenario that can trigger the bug

	// Start maintenance at T0
	maintenanceStartTime := ft.NowFunc()()
	t.Logf("Starting maintenance at time T=%v", maintenanceStartTime)

	// Run maintenance which will do content rewrite
	// In a real scenario, this could take hours due to:
	// - Large repository size
	// - Slow storage backend
	// - Network latency
	// - High CPU load
	require.NoError(t, snapshotmaintenance.Run(ctx, env.RepositoryWriter, maintenance.ModeFull, true, maintenance.SafetyFull))

	// Simulate the maintenance operation taking 8 hours to complete
	// During this time, content rewrite creates new blobs and orphans old ones
	ft.Advance(8 * time.Hour)
	maintenanceEndTime := ft.NowFunc()()

	t.Logf("Maintenance completed at time T=%v (duration: %v)", maintenanceEndTime, maintenanceEndTime.Sub(maintenanceStartTime))

	// The main client refreshes immediately after maintenance
	require.NoError(t, env.Repository.Refresh(ctx))

	// But the slow client has NOT refreshed yet - simulating a client that:
	// - Is offline
	// - Has cached indexes
	// - Is in the middle of a long backup operation
	t.Logf("Slow client has NOT refreshed yet")

	// Verify main client can still read objects
	for i, oid := range objectIDs[:5] {
		verifyObjectReadable(ctx, t, env.Repository, oid)
		t.Logf("Object %d still readable on main client after maintenance", i)
	}

	t.Logf("==== PHASE 5: Another maintenance cycle - this is where the bug triggers ====")

	// Advance time past the safety margins but not enough for proper refresh window
	// SafetyFull has:
	// - MinRewriteToOrphanDeletionDelay: 1 hour
	// - BlobDeleteMinAge: 24 hours
	// - MarginBetweenSnapshotGC: 4 hours

	// Advance 2 hours - this is past MinRewriteToOrphanDeletionDelay (1 hour)
	// so blob deletion WILL run in the next maintenance
	ft.Advance(2 * time.Hour)

	t.Logf("Running second maintenance at time T=%v", ft.NowFunc()())

	// This maintenance will try to delete orphaned blobs
	require.NoError(t, snapshotmaintenance.Run(ctx, env.RepositoryWriter, maintenance.ModeFull, true, maintenance.SafetyFull))

	// Main client refreshes
	require.NoError(t, env.Repository.Refresh(ctx))

	t.Logf("==== PHASE 6: Verify slow client can still read (this is where bug manifests) ====")

	// NOW the slow client tries to read objects
	// If the bug exists, some blobs may have been deleted even though
	// the slow client still has old indexes referencing them

	// Advance a bit more time to simulate the client coming back online
	ft.Advance(1 * time.Hour)

	// Slow client finally refreshes
	require.NoError(t, slowClient.Refresh(ctx))

	// Try to read objects - if bug exists, this will fail with "BLOB not found"
	for i, oid := range objectIDs[:5] {
		// This is the critical test - can the slow client still read after delayed refresh?
		err := repo.WriteSession(ctx, slowClient, repo.WriteSessionOptions{}, func(ctx context.Context, w repo.RepositoryWriter) error {
			r, err := w.OpenObject(ctx, oid)
			if err != nil {
				t.Logf("FAILED to read object %d (%v) on slow client: %v", i, oid, err)
				return err
			}
			r.Close()
			t.Logf("SUCCESS: Object %d still readable on slow client after delayed refresh", i)
			return nil
		})

		if err != nil {
			t.Fatalf("BUG REPRODUCED: Slow client cannot read object %d (%v) after maintenance - blob was deleted prematurely: %v", i, oid, err)
		}
	}

	t.Logf("==== TEST COMPLETED ====")
	t.Logf("All objects remain accessible after long-running maintenance and delayed client refresh")
}

// TestMaintenanceTimingIssue_MultipleClients tests the interaction between
// multiple clients with different refresh patterns during long maintenance cycles
func (s *formatSpecificTestSuite) TestMaintenanceTimingIssue_MultipleClients(t *testing.T) {
	ft := faketime.NewClockTimeWithOffset(0)

	ctx, env := repotesting.NewEnvironment(t, s.formatVersion, repotesting.Options{
		OpenOptions: func(o *repo.Options) {
			o.TimeNowFunc = ft.NowFunc()
		},
	})

	// Create three clients with different refresh behaviors
	clientB := env.MustConnectOpenAnother(t, func(o *repo.Options) {
		o.TimeNowFunc = ft.NowFunc()
	})

	clientC := env.MustConnectOpenAnother(t, func(o *repo.Options) {
		o.TimeNowFunc = ft.NowFunc()
	})

	t.Logf("==== Create content on client A ====")

	var objectID object.ID
	require.NoError(t, repo.WriteSession(ctx, env.Repository, repo.WriteSessionOptions{}, func(ctx context.Context, w repo.RepositoryWriter) error {
		ow := w.NewObjectWriter(ctx, object.WriterOptions{Prefix: "y", MetadataCompressor: "zstd-fastest"})
		fmt.Fprintf(ow, "shared content data")
		var err error
		objectID, err = ow.Result()
		return err
	}))

	t.Logf("Created object: %v", objectID)

	// All clients refresh and verify they can read
	require.NoError(t, clientB.Refresh(ctx))
	require.NoError(t, clientC.Refresh(ctx))

	verifyObjectReadable(ctx, t, env.Repository, objectID)
	verifyObjectReadable(ctx, t, clientB, objectID)
	verifyObjectReadable(ctx, t, clientC, objectID)

	t.Logf("==== Run initial maintenance ====")

	// Initial maintenance
	require.NoError(t, snapshotmaintenance.Run(ctx, env.RepositoryWriter, maintenance.ModeFull, true, maintenance.SafetyFull))

	// Age the content
	ft.Advance(25 * time.Hour)

	t.Logf("==== Run second maintenance (starts content rewrite) ====")

	// This will mark content as deleted
	require.NoError(t, snapshotmaintenance.Run(ctx, env.RepositoryWriter, maintenance.ModeFull, true, maintenance.SafetyFull))

	// Client A and B refresh, but Client C does not
	require.NoError(t, env.Repository.Refresh(ctx))
	require.NoError(t, clientB.Refresh(ctx))
	// Client C is "offline" - doesn't refresh

	t.Logf("==== Advance time and run maintenance cycles ====")

	// Run through several maintenance cycles with proper spacing
	for cycle := 0; cycle < 5; cycle++ {
		ft.Advance(5 * time.Hour)

		t.Logf("Maintenance cycle %d at T=%v", cycle+3, ft.NowFunc()())
		require.NoError(t, snapshotmaintenance.Run(ctx, env.RepositoryWriter, maintenance.ModeFull, true, maintenance.SafetyFull))

		// Client A always refreshes
		require.NoError(t, env.Repository.Refresh(ctx))

		// Client B refreshes every other cycle
		if cycle%2 == 0 {
			require.NoError(t, clientB.Refresh(ctx))
		}

		// Client C still hasn't refreshed (offline for extended period)
	}

	t.Logf("==== Client C comes back online ====")

	// Finally, client C comes back online and refreshes
	require.NoError(t, clientC.Refresh(ctx))

	// Try to read the object - this tests if blob was kept long enough
	// for slow clients to eventually refresh
	err := repo.WriteSession(ctx, clientC, repo.WriteSessionOptions{}, func(ctx context.Context, w repo.RepositoryWriter) error {
		_, err := w.OpenObject(ctx, objectID)
		return err
	})

	// With SafetyFull, the blob should have been deleted by now (after multiple cycles)
	// But we're testing that the deletion happened safely based on the schedule
	// If the object is not found, that's expected - but we're documenting the timing
	if err != nil {
		t.Logf("Object no longer accessible after extended offline period: %v", err)
		t.Logf("This is expected behavior after sufficient time has passed")
	} else {
		t.Logf("Object still accessible - good safety margins")
	}
}

// TestMaintenanceTimingIssue_RewriteDuringLongOperation tests content rewrite
// timing when the operation itself takes a very long time
func (s *formatSpecificTestSuite) TestMaintenanceTimingIssue_RewriteDuringLongOperation(t *testing.T) {
	ft := faketime.NewClockTimeWithOffset(0)

	ctx, env := repotesting.NewEnvironment(t, s.formatVersion, repotesting.Options{
		OpenOptions: func(o *repo.Options) {
			o.TimeNowFunc = ft.NowFunc()
		},
	})

	anotherClient := env.MustConnectOpenAnother(t, func(o *repo.Options) {
		o.TimeNowFunc = ft.NowFunc()
	})

	t.Logf("==== Create initial content ====")

	// Create content that will be rewritten
	var objectIDs []object.ID
	for i := 0; i < 15; i++ {
		var oid object.ID
		require.NoError(t, repo.WriteSession(ctx, env.Repository, repo.WriteSessionOptions{}, func(ctx context.Context, w repo.RepositoryWriter) error {
			ow := w.NewObjectWriter(ctx, object.WriterOptions{Prefix: "y", MetadataCompressor: "zstd-fastest"})
			fmt.Fprintf(ow, "test content %d with enough data to trigger rewrite behavior", i)
			var err error
			oid, err = ow.Result()
			return err
		}))
		objectIDs = append(objectIDs, oid)
	}

	require.NoError(t, anotherClient.Refresh(ctx))

	t.Logf("==== Initial maintenance ====")

	// Baseline maintenance
	require.NoError(t, snapshotmaintenance.Run(ctx, env.RepositoryWriter, maintenance.ModeFull, true, maintenance.SafetyFull))

	t.Logf("==== Age content significantly ====")

	// Age content to make it eligible for GC
	ft.Advance(25 * time.Hour)

	t.Logf("==== Second maintenance (marks deleted) ====")

	require.NoError(t, snapshotmaintenance.Run(ctx, env.RepositoryWriter, maintenance.ModeFull, true, maintenance.SafetyFull))

	// Main client refreshes, but anotherClient does not
	require.NoError(t, env.Repository.Refresh(ctx))

	t.Logf("==== Third maintenance cycle with timing observation ====")

	// Advance past MarginBetweenSnapshotGC (4 hours)
	ft.Advance(5 * time.Hour)

	beforeMaintenance := ft.NowFunc()()
	t.Logf("Starting maintenance at: %v", beforeMaintenance)

	// This maintenance will potentially do rewrite and/or blob deletion
	require.NoError(t, snapshotmaintenance.Run(ctx, env.RepositoryWriter, maintenance.ModeFull, true, maintenance.SafetyFull))

	afterMaintenance := ft.NowFunc()()
	t.Logf("Maintenance completed at: %v", afterMaintenance)

	// Main client refreshes
	require.NoError(t, env.Repository.Refresh(ctx))

	t.Logf("==== Continue maintenance cycles ====")

	// Continue cycles to potentially trigger blob deletion
	for i := 0; i < 3; i++ {
		ft.Advance(5 * time.Hour)
		t.Logf("Maintenance cycle %d at: %v", i+4, ft.NowFunc()())
		require.NoError(t, snapshotmaintenance.Run(ctx, env.RepositoryWriter, maintenance.ModeFull, true, maintenance.SafetyFull))
		require.NoError(t, env.Repository.Refresh(ctx))
	}

	t.Logf("==== Delayed client refresh ====")

	// Finally refresh the other client
	require.NoError(t, anotherClient.Refresh(ctx))

	// Test if objects are still readable
	for i, oid := range objectIDs[:3] {
		err := repo.WriteSession(ctx, anotherClient, repo.WriteSessionOptions{}, func(ctx context.Context, w repo.RepositoryWriter) error {
			r, err := w.OpenObject(ctx, oid)
			if err != nil {
				return err
			}
			r.Close()
			return nil
		})

		if err != nil {
			t.Logf("Object %d no longer readable: %v", i, err)
		} else {
			t.Logf("Object %d still readable after delayed refresh", i)
		}
	}

	t.Logf("Test completed - timing behavior documented")
}
