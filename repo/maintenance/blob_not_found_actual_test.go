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

// TestActualBlobNotFound_AfterRewrite
//
// This test targets ACTUAL blob.ErrBlobNotFound after content rewrite.
//
// Scenario:
// 1. Content rewrite moves content to new blob, orphaning old blob
// 2. Old blob gets deleted after safety margins
// 3. Client with stale index (pointing to old blob) tries to read
// 4. Should get blob.ErrBlobNotFound
func (s *formatSpecificTestSuite) TestActualBlobNotFound_AfterRewrite(t *testing.T) {
	ft := faketime.NewClockTimeWithOffset(0)

	ctx, env := repotesting.NewEnvironment(t, s.formatVersion, repotesting.Options{
		OpenOptions: func(o *repo.Options) {
			o.TimeNowFunc = ft.NowFunc()
		},
	})

	// Client that will NOT refresh and keep stale index
	staleClient := env.MustConnectOpenAnother(t, func(o *repo.Options) {
		o.TimeNowFunc = ft.NowFunc()
	})

	// Create multiple pieces of content to create fragmentation and trigger rewrite
	var objectID object.ID
	var originalBlobID blob.ID

	t.Logf("==== Phase 1: Create content ====")

	// Create multiple objects in separate sessions to create multiple small packs
	// This creates fragmentation which triggers rewrite
	for i := 0; i < 10; i++ {
		require.NoError(t, repo.WriteSession(ctx, env.Repository, repo.WriteSessionOptions{}, func(ctx context.Context, w repo.RepositoryWriter) error {
			ow := w.NewObjectWriter(ctx, object.WriterOptions{Prefix: "y"})
			// Write larger content to make packs more substantial
			for j := 0; j < 100; j++ {
				fmt.Fprintf(ow, "content that will be rewritten - iteration %d line %d\n", i, j)
			}
			var err error
			oid, err := ow.Result()

			// Track the first object for testing
			if i == 0 {
				objectID = oid
				if err == nil {
					cid, _, _ := objectID.ContentID()
					info, err2 := w.ContentInfo(ctx, cid)
					if err2 == nil {
						originalBlobID = info.PackBlobID
						t.Logf("Content %v in original blob: %v", cid, originalBlobID)
					}
				}
			}
			return err
		}))
	}

	// Both clients refresh and can read
	require.NoError(t, env.Repository.Refresh(ctx))
	require.NoError(t, staleClient.Refresh(ctx))
	verifyObjectReadable(ctx, t, staleClient, objectID)

	t.Logf("==== Phase 2: Initial maintenance ====")
	require.NoError(t, snapshotmaintenance.Run(ctx, env.RepositoryWriter, maintenance.ModeFull, true, maintenance.SafetyFull))

	// Age content significantly to trigger rewrite
	// RewriteMinAge is typically 2 hours, so advance well past that
	ft.Advance(48 * time.Hour)  // Advance 2 days to ensure content is old enough

	t.Logf("==== Phase 3: Maintenance with content rewrite ====")
	maintenanceStart := ft.NowFunc()()
	require.NoError(t, snapshotmaintenance.Run(ctx, env.RepositoryWriter, maintenance.ModeFull, true, maintenance.SafetyFull))

	// Main client refreshes - gets new index with new blob location
	require.NoError(t, env.Repository.Refresh(ctx))

	// Stale client does NOT refresh - still has old index pointing to old blob
	t.Logf("Stale client has NOT refreshed - still points to old blob")

	// Check if content was rewritten OR if it went through GC path
	var newBlobID blob.ID
	var wasRewritten bool
	require.NoError(t, repo.WriteSession(ctx, env.Repository, repo.WriteSessionOptions{}, func(ctx context.Context, w repo.RepositoryWriter) error {
		cid, _, _ := objectID.ContentID()
		info, err := w.ContentInfo(ctx, cid)
		if err == nil {
			newBlobID = info.PackBlobID
			if newBlobID != originalBlobID {
				wasRewritten = true
				t.Logf("✅ Content rewritten: %v → %v", originalBlobID, newBlobID)
			} else {
				if info.Deleted {
					t.Logf("Content marked as deleted (in %v), will be dropped soon", originalBlobID)
				} else {
					t.Logf("Content NOT rewritten and not deleted yet (still in %v)", originalBlobID)
				}
			}
		} else {
			t.Logf("Content dropped from index (GC path)")
		}
		return nil
	}))

	// We can test blob deletion through either path:
	// 1. Rewrite path: content rewritten, old blob becomes orphaned
	// 2. GC path: content deleted and dropped, blob becomes orphaned
	// Both lead to blob deletion, so continue the test
	if wasRewritten {
		t.Logf("Testing blob deletion after content rewrite")
	} else {
		t.Logf("Testing blob deletion through GC path (content will be deleted)")
	}

	// Verify original blob still exists
	var tmp gather.WriteBuffer
	defer tmp.Close()
	err := env.RepositoryWriter.BlobStorage().GetBlob(ctx, originalBlobID, 0, -1, &tmp)
	require.NoError(t, err, "Original blob should still exist")
	t.Logf("Original blob %v still exists (%d bytes)", originalBlobID, tmp.Length())

	t.Logf("==== Phase 4: Wait for blob deletion window ====")
	// MinRewriteToOrphanDeletionDelay = 1h
	// Advance past that
	ft.Advance(2 * time.Hour)

	t.Logf("==== Phase 5: Maintenance cycles to delete orphaned blob ====")
	for cycle := 1; cycle <= 30; cycle++ {
		ft.Advance(2 * time.Hour)
		timeSinceRewrite := ft.NowFunc()().Sub(maintenanceStart)

		t.Logf("Cycle %d (T+%v since rewrite)", cycle, timeSinceRewrite)
		require.NoError(t, snapshotmaintenance.Run(ctx, env.RepositoryWriter, maintenance.ModeFull, true, maintenance.SafetyFull))
		require.NoError(t, env.Repository.Refresh(ctx))

		// Check if original blob still exists
		tmp.Reset()
		blobErr := env.RepositoryWriter.BlobStorage().GetBlob(ctx, originalBlobID, 0, -1, &tmp)

		if errors.Is(blobErr, blob.ErrBlobNotFound) {
			t.Logf("🔴 Original blob %v DELETED at cycle %d (T+%v)", originalBlobID, cycle, timeSinceRewrite)

			// NOW try to read with stale client
			t.Logf("==== Attempting read with stale client ====")
			readErr := repo.WriteSession(ctx, staleClient, repo.WriteSessionOptions{}, func(ctx context.Context, w repo.RepositoryWriter) error {
				_, err := w.OpenObject(ctx, objectID)
				if err != nil {
					t.Logf("❌ Read FAILED with error: %v", err)
					t.Logf("Error type: %T", err)

					// Check error chain
					hasContent := errors.Is(err, content.ErrContentNotFound)
					hasBlob := errors.Is(err, blob.ErrBlobNotFound)
					hasObject := errors.Is(err, object.ErrObjectNotFound)

					t.Logf("Error chain:")
					t.Logf("  - content.ErrContentNotFound: %v", hasContent)
					t.Logf("  - blob.ErrBlobNotFound: %v", hasBlob)
					t.Logf("  - object.ErrObjectNotFound: %v", hasObject)

					if hasBlob {
						t.Logf("✅✅✅ FOUND IT: blob.ErrBlobNotFound ✅✅✅")
						t.Logf("This is the ACTUAL BLOB NOT FOUND error we're looking for!")
					}
				} else {
					t.Logf("✅ Read SUCCEEDED - no error (this is unexpected!)")
				}
				return err
			})

			t.Logf("After read attempt, readErr = %v", readErr)

			if readErr != nil && errors.Is(readErr, blob.ErrBlobNotFound) {
				blobInfo := fmt.Sprintf("Original blob: %v\n", originalBlobID)
				if wasRewritten && newBlobID != "" {
					blobInfo += fmt.Sprintf("New blob: %v\n", newBlobID)
				}

				t.Fatalf("\n"+
					"🎯🎯🎯 BUG REPRODUCED: ACTUAL BLOB NOT FOUND 🎯🎯🎯\n\n"+
					"Blob was deleted from storage while stale client still has index pointing to it\n\n"+
					"Timeline:\n"+
					"  - Maintenance started at: T+0\n"+
					"  - Blob deleted at: T+%v (cycle %d)\n"+
					"  - Stale client tried to read and got blob.ErrBlobNotFound\n\n"+
					"%s"+
					"This proves blobs are deleted before all clients have refreshed their indexes.\n\n"+
					"Error: %v\n",
					timeSinceRewrite, cycle, blobInfo, readErr)
			} else if readErr != nil {
				t.Logf("Got error but NOT blob.ErrBlobNotFound: %v", readErr)
				// Continue to see if we get blob.ErrBlobNotFound in later cycles
			}

			break // Original blob was deleted, no need to continue
		} else if blobErr != nil {
			t.Fatalf("Unexpected error checking blob: %v", blobErr)
		}

		// Blob still exists
		if cycle%5 == 0 {
			t.Logf("Original blob still exists after %d cycles", cycle)
		}
	}

	t.Logf("Test completed - original blob was not deleted within 30 cycles (~60 hours)")
}
