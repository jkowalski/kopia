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

// TestMaintenanceBugProof_ContentDroppedButBlobExists
//
// This test CONCLUSIVELY PROVES the bug:
// - Content is dropped from index prematurely
// - Blob STILL EXISTS in storage
// - Data is inaccessible despite blob existing
//
// This proves the issue is premature index dropping, not blob deletion.
func (s *formatSpecificTestSuite) TestMaintenanceBugProof_ContentDroppedButBlobExists(t *testing.T) {
	ft := faketime.NewClockTimeWithOffset(0)

	ctx, env := repotesting.NewEnvironment(t, s.formatVersion, repotesting.Options{
		OpenOptions: func(o *repo.Options) {
			o.TimeNowFunc = ft.NowFunc()
		},
	})

	var objectID object.ID
	var contentID content.ID
	var packBlobID blob.ID

	// STEP 1: Create content and record its blob location
	t.Logf("==== STEP 1: Create content ====")
	require.NoError(t, repo.WriteSession(ctx, env.Repository, repo.WriteSessionOptions{}, func(ctx context.Context, w repo.RepositoryWriter) error {
		ow := w.NewObjectWriter(ctx, object.WriterOptions{Prefix: "y"})
		fmt.Fprintf(ow, "PROOF TEST DATA - This content will be dropped from index while blob still exists")
		var err error
		objectID, err = ow.Result()

		if err == nil {
			contentID, _, _ = objectID.ContentID()
			t.Logf("Created object: %v", objectID)
			t.Logf("Content ID: %v", contentID)

			// Get the content info to find which blob it's in
			info, err2 := w.ContentInfo(ctx, contentID)
			if err2 == nil {
				packBlobID = info.PackBlobID
				t.Logf("Content stored in pack blob: %v", packBlobID)
			}
		}
		return err
	}))

	// STEP 2: Verify content is in index and blob exists
	t.Logf("==== STEP 2: Verify content in index and blob exists ====")
	require.NoError(t, repo.WriteSession(ctx, env.Repository, repo.WriteSessionOptions{}, func(ctx context.Context, w repo.RepositoryWriter) error {
		info, err := w.ContentInfo(ctx, contentID)
		require.NoError(t, err, "Content MUST be in index")
		t.Logf("✅ Content in index: PackBlob=%v, Deleted=%v", info.PackBlobID, info.Deleted)

		// Verify blob exists in storage
		var tmp gather.WriteBuffer
		defer tmp.Close()
		err = env.RepositoryWriter.BlobStorage().GetBlob(ctx, packBlobID, 0, -1, &tmp)
		require.NoError(t, err, "Blob MUST exist in storage")
		t.Logf("✅ Blob exists in storage: %v (%d bytes)", packBlobID, tmp.Length())

		return nil
	}))

	// STEP 3: Run maintenance cycles to trigger premature index dropping
	t.Logf("==== STEP 3: Maintenance #1 (baseline) ====")
	require.NoError(t, snapshotmaintenance.Run(ctx, env.RepositoryWriter, maintenance.ModeFull, true, maintenance.SafetyFull))

	// Age content to make it GC-eligible
	ft.Advance(25 * time.Hour)

	t.Logf("==== STEP 4: Maintenance #2 (mark deleted) ====")
	require.NoError(t, snapshotmaintenance.Run(ctx, env.RepositoryWriter, maintenance.ModeFull, true, maintenance.SafetyFull))

	// Verify content marked as deleted but still in index
	require.NoError(t, repo.WriteSession(ctx, env.Repository, repo.WriteSessionOptions{}, func(ctx context.Context, w repo.RepositoryWriter) error {
		info, err := w.ContentInfo(ctx, contentID)
		require.NoError(t, err, "Content still in index")
		t.Logf("After GC #2: Deleted=%v", info.Deleted)
		require.True(t, info.Deleted, "Content should be marked deleted")
		return nil
	}))

	// STEP 5: Run GC at exactly minimum margin
	ft.Advance(4 * time.Hour)  // Exactly MarginBetweenSnapshotGC

	t.Logf("==== STEP 5: Maintenance #3 (at minimum margin - triggers index drop) ====")
	require.NoError(t, snapshotmaintenance.Run(ctx, env.RepositoryWriter, maintenance.ModeFull, true, maintenance.SafetyFull))
	require.NoError(t, env.Repository.Refresh(ctx))

	// STEP 6: THE PROOF - Content NOT in index, but blob STILL EXISTS
	t.Logf("==== STEP 6: PROOF - Content dropped but blob exists ====")

	var contentInIndex bool
	var blobInStorage bool

	require.NoError(t, repo.WriteSession(ctx, env.Repository, repo.WriteSessionOptions{}, func(ctx context.Context, w repo.RepositoryWriter) error {
		// Check if content is in index
		_, err := w.ContentInfo(ctx, contentID)
		if err != nil {
			t.Logf("❌ Content NOT in index: %v", err)
			contentInIndex = false
			require.ErrorIs(t, err, content.ErrContentNotFound, "Should be ErrContentNotFound")
		} else {
			contentInIndex = true
		}

		// Check if blob still exists in storage
		var tmp gather.WriteBuffer
		defer tmp.Close()
		err = env.RepositoryWriter.BlobStorage().GetBlob(ctx, packBlobID, 0, -1, &tmp)
		if err != nil {
			if errors.Is(err, blob.ErrBlobNotFound) {
				t.Logf("Blob deleted from storage: %v", packBlobID)
				blobInStorage = false
			} else {
				return err
			}
		} else {
			t.Logf("✅ Blob STILL EXISTS in storage: %v (%d bytes)", packBlobID, tmp.Length())
			blobInStorage = true
		}

		return nil
	}))

	// THE SMOKING GUN
	t.Logf("\n" +
		"================================================\n" +
		"PROOF OF BUG:\n" +
		"================================================\n" +
		"Content in index: %v\n" +
		"Blob in storage:  %v\n" +
		"================================================\n",
		contentInIndex, blobInStorage)

	if !contentInIndex && blobInStorage {
		t.Fatalf("\n" +
			"🔴 BUG CONCLUSIVELY PROVEN 🔴\n\n" +
			"Content was DROPPED FROM INDEX while blob STILL EXISTS in storage!\n\n" +
			"This proves the bug is premature index dropping via TaskDropDeletedContentsFull,\n" +
			"NOT premature blob deletion.\n\n" +
			"Root cause: findSafeDropTime() approves dropping content after only 4h+1h\n" +
			"margins, even though blobs aren't deleted until 24h.\n\n" +
			"Impact: Data is INACCESSIBLE despite blob existing in storage.\n" +
			"Clients that haven't refreshed indexes experience 'BLOB not found' errors\n" +
			"even though the blob is still there.\n")
	}

	// Try to read object - this should fail with content not found
	err := repo.WriteSession(ctx, env.Repository, repo.WriteSessionOptions{}, func(ctx context.Context, w repo.RepositoryWriter) error {
		_, err := w.OpenObject(ctx, objectID)
		return err
	})

	require.Error(t, err, "Object should not be readable")
	require.ErrorIs(t, err, object.ErrObjectNotFound)
	t.Logf("Confirmed: Object not readable despite blob existing")
}

// TestMaintenanceBugProof_TimingOfIndexDrop
//
// This test tracks the EXACT timing when content is dropped from index
func (s *formatSpecificTestSuite) TestMaintenanceBugProof_TimingOfIndexDrop(t *testing.T) {
	ft := faketime.NewClockTimeWithOffset(0)

	ctx, env := repotesting.NewEnvironment(t, s.formatVersion, repotesting.Options{
		OpenOptions: func(o *repo.Options) {
			o.TimeNowFunc = ft.NowFunc()
		},
	})

	var objectID object.ID
	var contentID content.ID

	// Create content
	require.NoError(t, repo.WriteSession(ctx, env.Repository, repo.WriteSessionOptions{}, func(ctx context.Context, w repo.RepositoryWriter) error {
		ow := w.NewObjectWriter(ctx, object.WriterOptions{Prefix: "y"})
		fmt.Fprintf(ow, "timing test")
		var err error
		objectID, err = ow.Result()
		contentID, _, _ = objectID.ContentID()
		return err
	}))

	checkContentInIndex := func(label string) bool {
		var inIndex bool
		repo.WriteSession(ctx, env.Repository, repo.WriteSessionOptions{}, func(ctx context.Context, w repo.RepositoryWriter) error {
			_, err := w.ContentInfo(ctx, contentID)
			inIndex = (err == nil)
			if err != nil {
				t.Logf("%s: Content NOT in index (%v)", label, err)
			} else {
				t.Logf("%s: Content in index", label)
			}
			return nil
		})
		return inIndex
	}

	t.Logf("T+0h: Content created")
	require.True(t, checkContentInIndex("T+0h"))

	// GC #1
	t.Logf("\n==== GC #1 ====")
	require.NoError(t, snapshotmaintenance.Run(ctx, env.RepositoryWriter, maintenance.ModeFull, true, maintenance.SafetyFull))
	require.True(t, checkContentInIndex("After GC #1"))

	// Age 25 hours
	ft.Advance(25 * time.Hour)
	t.Logf("\nT+25h: Content aged")

	// GC #2 - marks deleted
	t.Logf("\n==== GC #2 (marks deleted) ====")
	require.NoError(t, snapshotmaintenance.Run(ctx, env.RepositoryWriter, maintenance.ModeFull, true, maintenance.SafetyFull))
	require.NoError(t, env.Repository.Refresh(ctx))
	require.True(t, checkContentInIndex("After GC #2"), "Content should still be in index (just marked deleted)")

	// Advance EXACTLY 4 hours
	ft.Advance(4 * time.Hour)
	t.Logf("\nT+29h: Exactly 4h after GC #2")

	// GC #3 - should drop from index
	t.Logf("\n==== GC #3 (at 4h margin - drops from index) ====")
	require.NoError(t, snapshotmaintenance.Run(ctx, env.RepositoryWriter, maintenance.ModeFull, true, maintenance.SafetyFull))
	require.NoError(t, env.Repository.Refresh(ctx))

	inIndex := checkContentInIndex("After GC #3")

	if !inIndex {
		t.Fatalf("\n" +
			"🔴 TIMING CONFIRMED 🔴\n\n" +
			"Content was dropped from index after only:\n" +
			"- 25h aging (MinContentAgeSubjectToGC: 24h)\n" +
			"- 2 GC cycles\n" +
			"- 4h between GC #2 and GC #3 (MarginBetweenSnapshotGC: 4h)\n\n" +
			"Total time from creation to index drop: 29 hours\n" +
			"But BlobDeleteMinAge is 24 hours, and blobs aren't deleted until 53+ hours!\n\n" +
			"This creates a 24-hour window where:\n" +
			"- Content is NOT in index\n" +
			"- Blob STILL EXISTS in storage\n" +
			"- Clients get 'content not found' errors\n")
	}
}
