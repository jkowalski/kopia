// This file contains a reproducer for https://github.com/kopia/kopia/issues/4769
// ("BLOB not found" for a q pack blob referenced by the live index).
//
// Theory being demonstrated: TRANSIENT INDEX VIEW REGRESSION AFTER CONTENT REWRITE.
//
// The protocol facts this builds on (all verified in code):
//
//  1. Superseded index entries are physically immortal. When maintenance's
//     content rewrite (RewriteContents, runs in every full maintenance cycle;
//     constantly at scale because manifest auto-compaction keeps producing
//     short 'q' packs) moves content C from old pack P_old into P_new, the old
//     index entry {C -> P_old} is never physically dropped - epoch compactions
//     preserve all entries (ManagerV1.CompactEpoch applies no watermark) and
//     the deletion watermark only hides DELETED entries at read time
//     (committed_content_index.go shouldIgnore). The old entry is merely
//     SHADOWED by the newer {C -> P_new} under newest-wins merge semantics
//     (index/merged.go contentInfoGreaterThanStruct).
//
//  2. Pack GC then deletes P_old - CORRECTLY - one maintenance cycle later
//     (MinRewriteToOrphanDeletionDelay + PackDeleteMinAge honored, SafetyFull).
//     From this moment on, the repository is only readable as long as the
//     NEWER index entry remains visible. Correctness of a past deletion
//     depends forever on the completeness of every future index-blob LIST.
//
//  3. The committed index view is rebuilt from storage LIST calls and the two
//     failure modes are handled with opposite severity: a failed GET of an
//     index blob fails the refresh loudly (old view retained), but a LIST that
//     silently omits an index blob is fully trusted - committedContentIndex.use()
//     REPLACES the active index set with the smaller one, no error, no
//     cross-check (committed_content_index.go:207, epoch refreshAttemptLocked).
//
// Consequence: ONE incomplete ListBlobs result covering the index-blob ('x')
// namespace - a lagging replica, a dropped page under throttling, any
// "S3-compatible" backend hiccup, lasting mere seconds - makes every reader
// that builds its view from it resolve C through the ANCIENT entry to the
// long-deleted P_old:
//
//	unable to load manifest contents: error getting cached content from blob
//	"q...-s...": failed to get blob with ID q...-s...: BLOB not found
//
// which is the exact failure from issue #4769, against a repository that is
// fully healthy on storage. The user inspects the bucket, finds that kopia
// itself deleted the blob (true - pack GC deleted it legitimately days
// earlier), and concludes maintenance deleted a referenced blob prematurely.
//
// Note what is NOT required: no concurrent writers, no clock skew, no client
// pauses or failures of any kind, no crash - a single kopia-server-style
// process doing routine snapshots and maintenance, plus exactly one deficient
// list call on the read path. If the same deficient view is ever used by a
// maintenance run, the same mechanism escalates to permanent loss (pack GC
// then deletes P_new as "unreferenced"), and similarly one deficient list
// during single-epoch compaction is made permanent by
// CleanupSupersededIndexes deleting xn blobs without verifying the compacted
// set covers them - those are follow-up reproducers.
//
// Required timing (all defaults, SafetyFull):
//   - T0:      snapshot committed; its manifest content lives in q pack P_old.
//   - T0+25h:  full maintenance #1 rewrites P_old's contents (older than
//     RewriteMinAge=2h, pack below the 80% short-pack threshold) into P_new and
//     commits index blob xn_B; blob deletion is deferred
//     (MinRewriteToOrphanDeletionDelay=1h).
//   - T0+50h:  full maintenance #2 deletes P_old (>24h PackDeleteMinAge,
//     unreferenced under newest-wins). Repository remains fully consistent.
//   - any later moment: a reader whose index-blob LIST omits xn_B resolves the
//     manifest content to P_old => "BLOB not found" on `kopia snapshot list`.
//     The window is unbounded: it never closes for the lifetime of the
//     repository; it only takes one bad list, years of correct operation
//     notwithstanding.
//
// The test asserts the CORRECT user-visible behavior (snapshot listing works),
// so it FAILS while the vulnerability exists, printing the exact error chain
// from the issue report.
package maintenance_test

import (
	"context"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/faketime"
	"github.com/kopia/kopia/internal/repotesting"
	"github.com/kopia/kopia/internal/testutil"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/content"
	"github.com/kopia/kopia/repo/format"
	"github.com/kopia/kopia/repo/maintenance"
	"github.com/kopia/kopia/snapshot"
)

// staleListStorage wraps blob.Storage and silently omits a configured set of
// blob IDs from ListBlobs results, while serving all reads/writes normally.
// This models the minimal possible backend listing anomaly: one LIST result
// that is missing one recently(-ish) written object - e.g. a lagging list
// replica or a dropped page - without any error being reported. Everything
// else about the storage is perfectly healthy and all blobs are readable.
type staleListStorage struct {
	blob.Storage

	mu sync.Mutex
	// +checklocks:mu
	omit map[blob.ID]bool
}

func (s *staleListStorage) hideFromLists(ids []blob.ID) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.omit = map[blob.ID]bool{}
	for _, id := range ids {
		s.omit[id] = true
	}
}

func (s *staleListStorage) ListBlobs(ctx context.Context, prefix blob.ID, cb func(blob.Metadata) error) error {
	//nolint:wrapcheck
	return s.Storage.ListBlobs(ctx, prefix, func(bm blob.Metadata) error {
		s.mu.Lock()
		omitted := s.omit[bm.BlobID]
		s.mu.Unlock()

		if omitted {
			return nil
		}

		return cb(bm)
	})
}

func TestStaleIndexBlobListResurrectsDeletedPacks(t *testing.T) {
	ta := faketime.NewClockTimeWithOffset(0)

	ctx, env := repotesting.NewEnvironment(t, format.FormatVersion3, repotesting.Options{
		OpenOptions: func(o *repo.Options) {
			o.TimeNowFunc = ta.NowFunc()
		},
	})

	setRepositoryOwner(t, ctx, env.RepositoryWriter)

	runFullMaintenance := func() {
		err := maintenance.RunExclusive(ctx, env.RepositoryWriter, maintenance.ModeFull, true,
			func(ctx context.Context, runParams maintenance.RunParameters) error {
				return maintenance.Run(ctx, runParams, maintenance.SafetyFull)
			})
		require.NoError(t, err)
	}

	// T0: an ordinary snapshot, fully and successfully committed - no stalls,
	// no failures, no concurrency. Its manifest content lands in q pack P_old.
	source := snapshot.SourceInfo{Host: "client-host", UserName: "client-user", Path: "/data"}

	qBlobsBefore := listBlobIDs(t, ctx, env.RootStorage(), content.PackBlobIDPrefixSpecial)

	_, err := snapshot.SaveSnapshot(ctx, env.RepositoryWriter, &snapshot.Manifest{
		Source:      source,
		Description: "snapshot whose metadata will be rewritten and whose original pack will be correctly deleted",
	})
	require.NoError(t, err)
	require.NoError(t, env.RepositoryWriter.Flush(ctx))

	newQBlobs := setDiff(listBlobIDs(t, ctx, env.RootStorage(), content.PackBlobIDPrefixSpecial), qBlobsBefore)
	require.Len(t, newQBlobs, 1)

	pOld := newQBlobs[0]
	require.NotEmpty(t, content.SessionIDFromBlobID(pOld),
		"expected the metadata pack to carry a session ID suffix like the blob in issue #4769")

	// T0+25h: scheduled full maintenance #1. The snapshot's q pack is a short
	// pack with contents older than RewriteMinAge, so RewriteContents moves its
	// contents into a new consolidated pack and commits new index blob(s) xn_B.
	// Blob deletion is correctly deferred to a later cycle.
	ta.Advance(25 * time.Hour)

	xBlobsBefore := listBlobIDs(t, ctx, env.RootStorage(), "x")

	runFullMaintenance()

	rewriteIndexBlobs := setDiff(listBlobIDs(t, ctx, env.RootStorage(), "x"), xBlobsBefore)
	require.NotEmpty(t, rewriteIndexBlobs, "expected maintenance #1 to commit index blobs for the content rewrite")
	verifyBlobExists(t, env.RootStorage(), pOld)

	// T0+50h: scheduled full maintenance #2 deletes P_old. This deletion is
	// CORRECT: every content of P_old has a newer index entry pointing at the
	// rewritten pack, MinRewriteToOrphanDeletionDelay and PackDeleteMinAge are
	// honored. This is the deletion the reporter of #4769 later finds in S3
	// access logs.
	ta.Advance(25 * time.Hour)
	runFullMaintenance()

	verifyBlobNotFound(t, env.RootStorage(), pOld)

	for _, b := range rewriteIndexBlobs {
		verifyBlobExists(t, env.RootStorage(), b)
	}

	// sanity: with a complete view of the index the repository is perfectly
	// healthy - a fresh reader lists the snapshot just fine.
	healthyRep := env.MustOpenAnother(t, func(o *repo.Options) {
		o.TimeNowFunc = ta.NowFunc()
	})

	snaps, err := snapshot.ListSnapshots(ctx, healthyRep, source)
	require.NoError(t, err, "repository must be healthy under a complete index view")
	require.Len(t, snaps, 1)

	// THE EVENT: one reader builds its index view from a LIST that silently
	// omits the rewrite's index blob(s) - a few seconds of backend listing
	// inconsistency, days after all writes involved. The old index entries
	// {content -> P_old} are still physically present in older index blobs and
	// now win the newest-wins merge again, resolving reads to the
	// legitimately-deleted pack.
	stale := &staleListStorage{Storage: env.RootStorage()}
	stale.hideFromLists(rewriteIndexBlobs)

	staleConfigFile := filepath.Join(testutil.TempDirectory(t), "stale-reader.config")
	require.NoError(t, repo.Connect(ctx, staleConfigFile, repotesting.NewReconnectableStorage(t, stale), env.Password, &repo.ConnectOptions{}))

	staleRep, err := repo.Open(ctx, staleConfigFile, env.Password, &repo.Options{
		TimeNowFunc: ta.NowFunc(),
	})
	require.NoError(t, err)

	defer staleRep.Close(ctx)

	// correct behavior: a momentarily-incomplete blob listing must not make a
	// healthy repository report missing blobs. While the vulnerability exists,
	// this fails with the exact issue #4769 error chain:
	//
	//   unable to find manifest entries: unable to load manifest contents:
	//   error getting cached content from blob "q...-s...": failed to get blob
	//   with ID q...-s...: BLOB not found
	//
	// where q...-s... is the pack that maintenance correctly deleted at T0+50h.
	_, err = snapshot.ListSnapshots(ctx, staleRep, source)
	require.NoError(t, err,
		"issue #4769 reproduced: one stale/short index-blob LIST resurrected superseded index entries pointing at pack %v, which pack GC correctly deleted after the content rewrite", pOld)
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
