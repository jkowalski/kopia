package maintenance_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/faketime"
	"github.com/kopia/kopia/internal/gather"
	"github.com/kopia/kopia/internal/repotesting"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/content"
	"github.com/kopia/kopia/repo/maintenance"
	"github.com/kopia/kopia/repo/object"
	"github.com/kopia/kopia/snapshot/snapshotmaintenance"
)

// TestBlobDeletion_RewriteAndDelete focuses on BLOB deletion after content rewrite
func (s *formatSpecificTestSuite) TestBlobDeletion_RewriteAndDelete(t *testing.T) {
	ft := faketime.NewClockTimeWithOffset(0)

	ctx, env := repotesting.NewEnvironment(t, s.formatVersion, repotesting.Options{
		OpenOptions: func(o *repo.Options) {
			o.TimeNowFunc = ft.NowFunc()
		},
	})

	slowClient := env.MustConnectOpenAnother(t, func(o *repo.Options) {
		o.TimeNowFunc = ft.NowFunc()
	})

	// Create multiple small objects to trigger rewrite
	var objectIDs []object.ID
	var originalBlobs []blob.ID

	t.Logf("==== Creating multiple objects ====")
	for i := 0; i < 5; i++ {
		var oid object.ID
		require.NoError(t, repo.WriteSession(ctx, env.Repository, repo.WriteSessionOptions{}, func(ctx context.Context, w repo.RepositoryWriter) error {
			ow := w.NewObjectWriter(ctx, object.WriterOptions{Prefix: "y"})
			fmt.Fprintf(ow, "blob deletion test object %d", i)
			var err error
			oid, err = ow.Result()

			if err == nil {
				cid, _, _ := oid.ContentID()
				info, err2 := w.ContentInfo(ctx, cid)
				if err2 == nil {
					originalBlobs = append(originalBlobs, info.PackBlobID)
					t.Logf("Object %d in blob: %v", i, info.PackBlobID)
				}
			}
			return err
		}))
		objectIDs = append(objectIDs, oid)
	}

	require.NoError(t, slowClient.Refresh(ctx))

	// Verify all objects readable
	for i, oid := range objectIDs {
		verifyObjectReadable(ctx, t, slowClient, oid)
		t.Logf("Object %d verified on slow client", i)
	}

	t.Logf("==== Baseline maintenance ====")
	require.NoError(t, snapshotmaintenance.Run(ctx, env.RepositoryWriter, maintenance.ModeFull, true, maintenance.SafetyFull))

	// Age content significantly to make it eligible for rewrite
	ft.Advance(25 * time.Hour)

	t.Logf("==== Maintenance with content rewrite ====")
	// This should trigger content rewrite which orphans old blobs
	require.NoError(t, snapshotmaintenance.Run(ctx, env.RepositoryWriter, maintenance.ModeFull, true, maintenance.SafetyFull))

	// Main client refreshes, slow client does NOT
	require.NoError(t, env.Repository.Refresh(ctx))
	t.Logf("Main client refreshed, slow client has OLD indexes")

	// Get new blob locations after rewrite
	var newBlobs []blob.ID
	require.NoError(t, repo.WriteSession(ctx, env.Repository, repo.WriteSessionOptions{}, func(ctx context.Context, w repo.RepositoryWriter) error {
		for i, oid := range objectIDs {
			cid, _, _ := oid.ContentID()
			info, err := w.ContentInfo(ctx, cid)
			if err == nil {
				newBlobs = append(newBlobs, info.PackBlobID)
				t.Logf("Object %d now in blob: %v (was: %v)", i, info.PackBlobID, originalBlobs[i])
			}
		}
		return nil
	}))

	// Check if any blobs were rewritten
	rewritten := false
	for i := range originalBlobs {
		if i < len(newBlobs) && originalBlobs[i] != newBlobs[i] {
			t.Logf("REWRITE DETECTED: Object %d moved from %v to %v", i, originalBlobs[i], newBlobs[i])
			rewritten = true
		}
	}

	if !rewritten {
		t.Logf("No rewrite occurred in this cycle")
	}

	// Advance past MinRewriteToOrphanDeletionDelay (1h)
	ft.Advance(2 * time.Hour)

	t.Logf("==== Maintenance that may delete orphaned blobs ====")
	require.NoError(t, snapshotmaintenance.Run(ctx, env.RepositoryWriter, maintenance.ModeFull, true, maintenance.SafetyFull))
	require.NoError(t, env.Repository.Refresh(ctx))

	// Continue advancing time to reach blob deletion window
	// BlobDeleteMinAge is 24h, so we need to advance time
	for cycle := 0; cycle < 5; cycle++ {
		ft.Advance(6 * time.Hour)
		t.Logf("==== Maintenance cycle %d (checking for blob deletion) ====", cycle+1)
		require.NoError(t, snapshotmaintenance.Run(ctx, env.RepositoryWriter, maintenance.ModeFull, true, maintenance.SafetyFull))
		require.NoError(t, env.Repository.Refresh(ctx))

		// Try reading with slow client (still has old indexes)
		for i, oid := range objectIDs {
			err := repo.WriteSession(ctx, slowClient, repo.WriteSessionOptions{}, func(ctx context.Context, w repo.RepositoryWriter) error {
				_, err := w.OpenObject(ctx, oid)
				if err != nil {
					t.Logf("CYCLE %d: Object %d ERROR: %v", cycle+1, i, err)

					// Check if this is actual BLOB not found
					if errors.Is(err, blob.ErrBlobNotFound) {
						t.Logf("✅ FOUND IT: blob.ErrBlobNotFound for object %d", i)
						t.Logf("This means the underlying BLOB was deleted from storage")

						// Verify the blob really doesn't exist
						if i < len(originalBlobs) {
							var tmp gather.WriteBuffer
							defer tmp.Close()
							blobErr := env.RepositoryWriter.BlobStorage().GetBlob(ctx, originalBlobs[i], 0, -1, &tmp)
							if errors.Is(blobErr, blob.ErrBlobNotFound) {
								t.Logf("✅ CONFIRMED: Original blob %v deleted from storage", originalBlobs[i])
							}
						}
					}
				}
				return err
			})

			if err != nil && errors.Is(err, blob.ErrBlobNotFound) {
				t.Fatalf("🔴 BUG REPRODUCED: BLOB NOT FOUND 🔴\n\n"+
					"Object %d cannot be read by slow client\n"+
					"Error contains blob.ErrBlobNotFound\n"+
					"This means the blob was DELETED from storage while slow client still needs it\n\n"+
					"Cycle: %d\n"+
					"Error: %v\n", i, cycle+1, err)
			}
		}
	}

	t.Logf("No blob.ErrBlobNotFound detected in this scenario")
}

// TestBlobDeletion_LongMaintenanceWithBlobDeletion tests blob deletion when maintenance spans long time
func (s *formatSpecificTestSuite) TestBlobDeletion_LongMaintenanceWithBlobDeletion(t *testing.T) {
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
	var originalBlobID blob.ID

	require.NoError(t, repo.WriteSession(ctx, env.Repository, repo.WriteSessionOptions{}, func(ctx context.Context, w repo.RepositoryWriter) error {
		ow := w.NewObjectWriter(ctx, object.WriterOptions{Prefix: "y"})
		fmt.Fprintf(ow, "long maintenance blob test")
		var err error
		objectID, err = ow.Result()

		if err == nil {
			cid, _, _ := objectID.ContentID()
			info, err2 := w.ContentInfo(ctx, cid)
			if err2 == nil {
				originalBlobID = info.PackBlobID
				t.Logf("Content in blob: %v", originalBlobID)
			}
		}
		return err
	}))

	require.NoError(t, slowClient.Refresh(ctx))
	verifyObjectReadable(ctx, t, slowClient, objectID)

	t.Logf("==== Initial maintenance ====")
	require.NoError(t, snapshotmaintenance.Run(ctx, env.RepositoryWriter, maintenance.ModeFull, true, maintenance.SafetyFull))

	// Age content
	ft.Advance(25 * time.Hour)

	t.Logf("==== Maintenance session starts ====")
	maintenanceStartTime := ft.NowFunc()()
	t.Logf("Maintenance start time: %v", maintenanceStartTime)

	// Run maintenance - this could trigger rewrite
	require.NoError(t, snapshotmaintenance.Run(ctx, env.RepositoryWriter, maintenance.ModeFull, true, maintenance.SafetyFull))

	// Simulate maintenance taking many hours (this is the key!)
	t.Logf("==== Simulating maintenance took 10 hours ====")
	ft.Advance(10 * time.Hour)
	actualTime := ft.NowFunc()()
	t.Logf("Actual time after long maintenance: %v", actualTime)

	// Main client refreshes
	require.NoError(t, env.Repository.Refresh(ctx))

	// The issue: blob deletion uses MaintenanceStartTime, not actual completion time
	// So blobs created during the 10-hour window might be deleted prematurely

	t.Logf("==== Run more maintenance cycles to trigger blob deletion ====")
	for cycle := 0; cycle < 10; cycle++ {
		ft.Advance(3 * time.Hour)
		t.Logf("Cycle %d at time: %v", cycle+1, ft.NowFunc()())

		require.NoError(t, snapshotmaintenance.Run(ctx, env.RepositoryWriter, maintenance.ModeFull, true, maintenance.SafetyFull))
		require.NoError(t, env.Repository.Refresh(ctx))

		// Try to read with slow client
		err := repo.WriteSession(ctx, slowClient, repo.WriteSessionOptions{}, func(ctx context.Context, w repo.RepositoryWriter) error {
			_, err := w.OpenObject(ctx, objectID)
			if err != nil && errors.Is(err, blob.ErrBlobNotFound) {
				t.Logf("🔴 BLOB NOT FOUND at cycle %d", cycle+1)
				t.Logf("Error: %v", err)

				// Check if original blob was deleted
				var tmp gather.WriteBuffer
				defer tmp.Close()
				blobErr := env.RepositoryWriter.BlobStorage().GetBlob(ctx, originalBlobID, 0, -1, &tmp)
				if errors.Is(blobErr, blob.ErrBlobNotFound) {
					t.Logf("✅ Original blob %v was deleted", originalBlobID)
				}
			}
			return err
		})

		if err != nil && errors.Is(err, blob.ErrBlobNotFound) {
			timeSinceMaintenanceStart := ft.NowFunc()().Sub(maintenanceStartTime)
			t.Fatalf("🔴 BUG REPRODUCED: BLOB NOT FOUND 🔴\n\n"+
				"Blob was deleted while slow client still needs it\n"+
				"Time since maintenance start: %v\n"+
				"This may be due to MaintenanceStartTime being used for blob deletion cutoff\n"+
				"instead of actual operation completion time\n\n"+
				"Error: %v\n", timeSinceMaintenanceStart, err)
		}
	}

	t.Logf("No blob.ErrBlobNotFound in this scenario")
}

// TestBlobDeletion_VerifyBlobVsContentError distinguishes blob deletion from content dropping
func (s *formatSpecificTestSuite) TestBlobDeletion_VerifyBlobVsContentError(t *testing.T) {
	ft := faketime.NewClockTimeWithOffset(0)

	ctx, env := repotesting.NewEnvironment(t, s.formatVersion, repotesting.Options{
		OpenOptions: func(o *repo.Options) {
			o.TimeNowFunc = ft.NowFunc()
		},
	})

	var objectID object.ID
	var contentID content.ID
	var packBlobID blob.ID

	// Create content
	require.NoError(t, repo.WriteSession(ctx, env.Repository, repo.WriteSessionOptions{}, func(ctx context.Context, w repo.RepositoryWriter) error {
		ow := w.NewObjectWriter(ctx, object.WriterOptions{Prefix: "y"})
		fmt.Fprintf(ow, "error type verification")
		var err error
		objectID, err = ow.Result()

		if err == nil {
			contentID, _, _ = objectID.ContentID()
			info, err2 := w.ContentInfo(ctx, contentID)
			if err2 == nil {
				packBlobID = info.PackBlobID
			}
		}
		return err
	}))

	t.Logf("Created: object=%v, content=%v, blob=%v", objectID, contentID, packBlobID)

	// Run aggressive maintenance to trigger SOMETHING
	t.Logf("==== Baseline ====")
	require.NoError(t, snapshotmaintenance.Run(ctx, env.RepositoryWriter, maintenance.ModeFull, true, maintenance.SafetyFull))

	ft.Advance(25 * time.Hour)

	for cycle := 0; cycle < 10; cycle++ {
		ft.Advance(4 * time.Hour)
		t.Logf("==== Cycle %d ====", cycle+1)
		require.NoError(t, snapshotmaintenance.Run(ctx, env.RepositoryWriter, maintenance.ModeFull, true, maintenance.SafetyFull))
		require.NoError(t, env.Repository.Refresh(ctx))

		// Try to read and diagnose error type
		err := repo.WriteSession(ctx, env.Repository, repo.WriteSessionOptions{}, func(ctx context.Context, w repo.RepositoryWriter) error {
			_, err := w.OpenObject(ctx, objectID)
			if err != nil {
				t.Logf("\n=== ERROR DETECTED at cycle %d ===", cycle+1)
				t.Logf("Error: %v", err)

				// Check error types
				hasContentNotFound := errors.Is(err, content.ErrContentNotFound)
				hasBlobNotFound := errors.Is(err, blob.ErrBlobNotFound)
				hasObjectNotFound := errors.Is(err, object.ErrObjectNotFound)

				t.Logf("Error chain contains:")
				t.Logf("  - content.ErrContentNotFound: %v", hasContentNotFound)
				t.Logf("  - blob.ErrBlobNotFound: %v", hasBlobNotFound)
				t.Logf("  - object.ErrObjectNotFound: %v", hasObjectNotFound)

				if hasBlobNotFound {
					t.Logf("🔴 THIS IS BLOB DELETION! 🔴")

					// Verify blob is really gone
					var tmp gather.WriteBuffer
					defer tmp.Close()
					blobErr := env.RepositoryWriter.BlobStorage().GetBlob(ctx, packBlobID, 0, -1, &tmp)
					t.Logf("Direct blob check: %v", blobErr)
					if errors.Is(blobErr, blob.ErrBlobNotFound) {
						t.Logf("✅ Blob %v DELETED from storage", packBlobID)
					} else if blobErr == nil {
						t.Logf("⚠️  Blob still exists but can't be read?")
					}
				} else if hasContentNotFound {
					t.Logf("ℹ️  This is content index dropping (not blob deletion)")

					// Check if blob still exists
					var tmp gather.WriteBuffer
					defer tmp.Close()
					blobErr := env.RepositoryWriter.BlobStorage().GetBlob(ctx, packBlobID, 0, -1, &tmp)
					if blobErr == nil {
						t.Logf("ℹ️  Blob %v still EXISTS in storage (content just dropped from index)", packBlobID)
					}
				}
			}
			return err
		})

		if err != nil && errors.Is(err, blob.ErrBlobNotFound) {
			t.Fatalf("🔴 BLOB DELETION BUG FOUND 🔴\n"+
				"Cycle: %d\n"+
				"Error: %v\n", cycle+1, err)
		}
	}
}
