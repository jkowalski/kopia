// Package maintenance_test contains reproducers for https://github.com/kopia/kopia/issues/4769
// (repository becomes broken with "BLOB not found" after maintenance deletes a referenced
// q-pack blob, e.g. "qca076cc8094ebdd6b8230d91d020ccef-sd2b548213777457a136").
//
// Both tests below use SafetyFull (maintenance safety is NEVER disabled), a fake clock shared
// by all repository handles and by the blob storage, and two independent repository handles
// (a "client" performing a snapshot and a "maintenance owner" running full maintenance),
// simulating two different machines using the same bucket.
//
// Root cause hypothesis #1 (TestPackGCDeletesPacksOfSessionCommittedDuringMaintenance):
//
// DeleteUnreferencedPacks decides that a pack blob is "referenced" using the index view that
// was frozen at the very beginning of the maintenance run:
//
//   - maintenance.RunExclusive calls rep.DisableIndexRefresh() and rep.Refresh()
//     (repo/maintenance/maintenance_run.go:159 and :222) before any task runs, so
//     the entire maintenance run uses the index as of maintenance start time (M0).
//
// but it decides that a pack blob is "protected by an active session" using a LIVE listing
// of session-marker blobs taken when the pack-gc task finally runs (M0+H):
//
//   - DeleteUnreferencedPacks calls rep.ContentManager().ListActiveSessions(ctx)
//     (repo/maintenance/pack_gc.go:79) at task execution time.
//
// The write protocol (repo/content/content_manager.go:475-485) writes index blobs FIRST and
// only then deletes the session marker, explicitly relying on the invariant: "if we managed
// to commit the session marker blobs, the index is now fully committed and will be visible
// to others, including blob GC". Maintenance violates that invariant: it reads the sessions
// AFTER the client committed (marker gone) but reads the index from BEFORE the client
// committed (pack not referenced). Any pack blob that is older than PackDeleteMinAge (24h)
// and whose index commit lands inside the maintenance window (M0, M0+H) is deleted even
// though it is referenced by a fully-committed index, and even though its session is much
// younger than SessionExpirationAge (96h).
//
// Required timing:
//   - a client writes a pack blob at T0 and does not complete any index flush for >24h
//     (PackDeleteMinAge) - e.g. the machine is suspended mid-upload, the process is
//     SIGSTOPped/cgroup-frozen, the VM is live-migrated/paused, or storage is so slow
//     that no checkpoint completes (the 45-minute checkpoint timer does not run while
//     the process is suspended);
//   - a FULL maintenance run starts at M0 >= T0+24h, while the client's flush has not
//     landed yet;
//   - the client wakes up and successfully completes its flush while the earlier, slow
//     maintenance phases are still running (snapshot GC + content rewrite; at the scale
//     reported in #4769 - 7000 users/server, snapshot listing taking tens of minutes -
//     this window is easily 30min-hours).
//
// This needs no clock skew, no safety overrides, and only a ~25h client stall, which makes
// it the most likely cause of #4769 (large S3 deployments with long full-maintenance runs
// and a fleet of clients large enough that some client is always resuming from a stall).
//
// Root cause hypothesis #2 (TestPackGCDeletesPacksOfLongRunningSession):
//
// Session markers are written ONCE when a session starts and never refreshed - see the TODO
// at repo/content/sessions.go:138 ("write this periodically when sessions span the duration
// of an upload"). SessionInfo.CheckpointTime therefore always equals the session START time.
// A session that is alive but older than SessionExpirationAge (96h) loses pack-gc protection
// entirely: full maintenance deletes both its pack blobs (older than 24h) and its session
// marker while the client is merely suspended. When the client resumes, its index flush
// succeeds (nothing re-verifies that previously-written packs still exist; the epoch
// manager's ErrVerySlowIndexWrite check only fires if >=2 epoch advances happened, which
// requires sustained index-write traffic), the snapshot completes "successfully", and the
// committed index references deleted blobs.
//
// Required timing: client suspended >96h mid-snapshot (laptop lid closed for 4+ days) while
// another machine (server/NAS/cron) runs scheduled full maintenance, and fewer than 2 epoch
// advances during the stall (always true for small/idle repositories, where the epoch
// advance thresholds - 20 index blobs - are not reached). Less likely than #1 for the
// server fleet in #4769, but a probable cause of the same symptom reported by home users.
//
// In both cases the user-visible failure is identical to the issue report: the next
// "kopia snapshot list" fails with
//
//	unable to load manifest contents: error getting cached content from blob
//	"q...-s...": failed to get blob with ID q...-s...: BLOB not found
package maintenance_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/faketime"
	"github.com/kopia/kopia/internal/repotesting"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/content"
	"github.com/kopia/kopia/repo/format"
	"github.com/kopia/kopia/repo/maintenance"
	"github.com/kopia/kopia/snapshot"
)

// issue4769Harness bundles the two repository handles ("client" machine and
// "maintenance owner" machine) that share one underlying storage and one fake clock.
type issue4769Harness struct {
	env *repotesting.Environment
	ta  *faketime.ClockTimeWithOffset

	// client is a second, independent repository handle simulating another machine.
	client repo.DirectRepositoryWriter

	// source of the snapshot written by the client.
	source snapshot.SourceInfo

	// packBlobID is the q-pack blob (with session ID suffix) holding the client's
	// snapshot manifest content. This is the blob that maintenance must not delete.
	packBlobID blob.ID
}

func setupIssue4769Harness(t *testing.T) (context.Context, *issue4769Harness) {
	t.Helper()

	h := &issue4769Harness{
		// fake clock shared by all repository handles AND the map storage backing them,
		// so blob Last-Modified timestamps move together with repository time.
		ta: faketime.NewClockTimeWithOffset(0),
	}

	ctx, env := repotesting.NewEnvironment(t, format.FormatVersion3, repotesting.Options{
		OpenOptions: func(o *repo.Options) {
			o.TimeNowFunc = h.ta.NowFunc()
		},
	})

	h.env = env

	// make env.RepositoryWriter the maintenance owner, like the scheduled maintenance
	// runner in the issue report.
	setRepositoryOwner(t, ctx, env.RepositoryWriter)

	// sanity: this format version uses the epoch manager, like the repository in #4769.
	verifyEpochManagerIsEnabled(t, ctx, env.Repository)

	// open an independent "client" handle against the same storage.
	clientRep, err := repo.Open(ctx, env.ConfigFile(), env.Password, &repo.Options{
		TimeNowFunc: h.ta.NowFunc(),
	})
	require.NoError(t, err)

	t.Cleanup(func() { clientRep.Close(ctx) })

	_, cw, err := clientRep.(repo.DirectRepository).NewDirectWriter(ctx, repo.WriteSessionOptions{Purpose: "client-snapshot"})
	require.NoError(t, err)

	t.Cleanup(func() { cw.Close(ctx) })

	h.client = cw
	h.source = snapshot.SourceInfo{Host: "client-host", UserName: "client-user", Path: "/data"}

	return ctx, h
}

// clientWritesPacksThenStallsBeforeIndexFlush brings the client into the state that exists
// in every snapshot between writing a full pack blob and completing the next index flush:
// pack blobs (with a session ID suffix) and the session marker are durably on storage but
// no index entry references them yet. DisableIndexFlush is the same production mechanism
// the manifest manager uses (repo/manifest/committed_manifest_manager.go:261); here it
// models the client being suspended after the pack write and before the index write.
func (h *issue4769Harness) clientWritesPacksThenStallsBeforeIndexFlush(t *testing.T, ctx context.Context) {
	t.Helper()

	qBlobsBefore := listBlobIDs(t, ctx, h.env.RootStorage(), content.PackBlobIDPrefixSpecial)

	h.client.ContentManager().DisableIndexFlush(ctx)

	_, err := snapshot.SaveSnapshot(ctx, h.client, &snapshot.Manifest{
		Source:      h.source,
		Description: "snapshot whose metadata pack will be deleted by maintenance",
	})
	require.NoError(t, err)

	// writes the manifest ('m') content into a 'q' pack blob and the session marker,
	// but - like a client that stalls mid-flush - no index blob.
	require.NoError(t, h.client.Flush(ctx))

	qBlobsAfter := listBlobIDs(t, ctx, h.env.RootStorage(), content.PackBlobIDPrefixSpecial)
	newQBlobs := setDiff(qBlobsAfter, qBlobsBefore)
	require.Len(t, newQBlobs, 1, "expected the client to write exactly one new q pack blob")

	h.packBlobID = newQBlobs[0]
	require.NotEmpty(t, content.SessionIDFromBlobID(h.packBlobID),
		"expected the new pack blob to carry a session ID suffix like the blob in issue #4769")

	// the client's write session is still open: its marker must be on storage.
	require.Len(t, listBlobIDs(t, ctx, h.env.RootStorage(), content.BlobIDPrefixSession), 1,
		"expected exactly one active session marker")
}

// clientResumesAndCommits completes the client's snapshot: the index flush succeeds and the
// session marker is deleted, making the snapshot fully committed from the client's (and the
// write protocol's) point of view.
func (h *issue4769Harness) clientResumesAndCommits(t *testing.T, ctx context.Context) {
	t.Helper()

	h.client.ContentManager().EnableIndexFlush(ctx)
	require.NoError(t, h.client.Flush(ctx),
		"the client's snapshot commit succeeded - kopia reported a successful backup")
}

// runFullMaintenance runs a complete FULL maintenance cycle with SafetyFull, exactly like
// "kopia maintenance run --full" does (snapshotmaintenance.Run wraps the same calls).
func (h *issue4769Harness) runFullMaintenance(t *testing.T, ctx context.Context) {
	t.Helper()

	err := maintenance.RunExclusive(ctx, h.env.RepositoryWriter, maintenance.ModeFull, true,
		func(ctx context.Context, runParams maintenance.RunParameters) error {
			return maintenance.Run(ctx, runParams, maintenance.SafetyFull)
		})
	require.NoError(t, err)
}

// verifySnapshotsStillReadable asserts the CORRECT post-conditions: the committed snapshot
// can be listed by a fresh repository handle and its metadata pack blob still exists.
//
// While the bug exists, this FAILS with the exact user-visible error from issue #4769:
//
//	unable to find manifest entries: unable to load manifest contents:
//	error getting cached content from blob "q...-s...":
//	failed to get blob with ID q...-s...: BLOB not found
func (h *issue4769Harness) verifySnapshotsStillReadable(t *testing.T, ctx context.Context) {
	t.Helper()

	freshRep, err := repo.Open(ctx, h.env.ConfigFile(), h.env.Password, &repo.Options{
		TimeNowFunc: h.ta.NowFunc(),
	})
	require.NoError(t, err)

	defer freshRep.Close(ctx)

	snaps, err := snapshot.ListSnapshots(ctx, freshRep, h.source)
	require.NoError(t, err,
		"repository is broken: the committed index references pack blob %v which was deleted by maintenance (issue #4769)", h.packBlobID)
	require.Len(t, snaps, 1)

	verifyBlobExists(t, h.env.RootStorage(), h.packBlobID)
}

// TestPackGCDeletesPacksOfSessionCommittedDuringMaintenance reproduces hypothesis #1:
// full maintenance (SafetyFull) deletes a pack blob belonging to a session that committed
// successfully WHILE maintenance was running, because pack-gc combines the index view
// frozen at maintenance start with a session listing taken hours later. The session is
// only ~25h old - well within SessionExpirationAge (96h) - so no documented safety
// boundary is crossed by the client.
func TestPackGCDeletesPacksOfSessionCommittedDuringMaintenance(t *testing.T) {
	ctx, h := setupIssue4769Harness(t)

	// T0: client writes its q pack + session marker, then stalls (suspend/freeze/slow VM)
	// before the index flush.
	h.clientWritesPacksThenStallsBeforeIndexFlush(t, ctx)

	// an ordinary scheduled FULL maintenance runs 12h later. On this first cycle the
	// content-rewrite task runs and blob deletion is deferred (MinRewriteToOrphanDeletionDelay),
	// like on any warmed-up production repository with daily full maintenance.
	h.ta.Advance(12 * time.Hour)
	h.runFullMaintenance(t, ctx)

	// the client stays stalled until slightly past PackDeleteMinAge (24h).
	h.ta.Advance(maintenance.SafetyFull.PackDeleteMinAge - 11*time.Hour)

	// M0: the next scheduled FULL maintenance starts on the owner machine; this cycle is
	// due for blob deletion. RunExclusive freezes the index view (DisableIndexRefresh +
	// Refresh) before running any task.
	err := maintenance.RunExclusive(ctx, h.env.RepositoryWriter, maintenance.ModeFull, true,
		func(ctx context.Context, runParams maintenance.RunParameters) error {
			// This callback is exactly where snapshotmaintenance.Run executes snapshot GC,
			// which at the scale of issue #4769 (7000 users/server) takes tens of minutes
			// to hours. Simulate that the GC phase takes 30 minutes...
			h.ta.Advance(30 * time.Minute)

			// ...during which the stalled client resumes and successfully commits its
			// snapshot: index blobs are written FIRST, then the session marker is deleted
			// (repo/content/content_manager.go:475-485). Per the documented protocol the
			// pack is now "fully committed and visible to others, including blob GC".
			h.clientResumesAndCommits(t, ctx)

			// session age is ~25.5h, far below SessionExpirationAge (96h): by design this
			// session's blobs must not be collected.
			require.Less(t, 26*time.Hour, maintenance.SafetyFull.SessionExpirationAge)

			// the remaining maintenance tasks now run, including full-delete-blobs, which
			// uses the frozen pre-commit index but a live post-commit session listing.
			return maintenance.Run(ctx, runParams, maintenance.SafetyFull)
		})
	require.NoError(t, err, "maintenance itself reports success")

	// correct behavior: the committed snapshot must remain readable. While the bug
	// exists this fails with the exact "BLOB not found" symptom from issue #4769.
	h.verifySnapshotsStillReadable(t, ctx)
}

// TestPackGCDeletesPacksOfLongRunningSession reproduces hypothesis #2: a client suspended
// for longer than SessionExpirationAge (96h) mid-snapshot loses its packs to a perfectly
// ordinary full maintenance run, because session markers are never refreshed
// (repo/content/sessions.go:138 TODO) and nothing re-validates pack existence when the
// client resumes and commits.
func TestPackGCDeletesPacksOfLongRunningSession(t *testing.T) {
	ctx, h := setupIssue4769Harness(t)

	// T0: client writes its q pack + session marker, then the machine is suspended.
	h.clientWritesPacksThenStallsBeforeIndexFlush(t, ctx)

	// the machine stays asleep for >96h (e.g. laptop closed over a long weekend + holidays)
	// while ordinary scheduled FULL maintenance runs happen elsewhere. The session marker
	// still exists, but its CheckpointTime (== session start, never refreshed) becomes
	// older than SessionExpirationAge, so the pack-gc cycle deletes both the pack blob and
	// the marker of the still-live session.
	h.ta.Advance(maintenance.SafetyFull.SessionExpirationAge + 2*time.Hour)
	h.runFullMaintenance(t, ctx) // first cycle: content rewrite, blob deletion deferred

	h.ta.Advance(2 * time.Hour)
	h.runFullMaintenance(t, ctx) // second cycle: blob deletion is due and runs

	// the client resumes and commits; the index flush succeeds (epochs did not advance
	// twice during the stall, so the epoch manager's ErrVerySlowIndexWrite safety does not
	// trigger) and kopia reports a successful snapshot referencing the deleted blob.
	h.clientResumesAndCommits(t, ctx)

	// correct behavior: the committed snapshot must remain readable. While the bug
	// exists this fails with the exact "BLOB not found" symptom from issue #4769.
	h.verifySnapshotsStillReadable(t, ctx)
}

func listBlobIDs(t *testing.T, ctx context.Context, st blob.Storage, prefix blob.ID) []blob.ID {
	t.Helper()

	var ids []blob.ID

	require.NoError(t, st.ListBlobs(ctx, prefix, func(bm blob.Metadata) error {
		ids = append(ids, bm.BlobID)
		return nil
	}))

	return ids
}

func setDiff(a, b []blob.ID) []blob.ID {
	old := map[blob.ID]bool{}
	for _, id := range b {
		old[id] = true
	}

	var result []blob.ID

	for _, id := range a {
		if !old[id] {
			result = append(result, id)
		}
	}

	return result
}
