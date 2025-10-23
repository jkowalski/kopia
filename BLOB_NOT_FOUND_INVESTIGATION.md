# Investigation: blob.ErrBlobNotFound in Issue #4769

## Objective
Reproduce the scenario where Kopia returns `blob.ErrBlobNotFound` error, as reported in issue #4769 where users experience "BLOB not found" errors after random months of operation.

## Tests Created

### 1. blob_deletion_bug_test.go
Tests focusing on blob deletion scenarios:
- `TestBlobDeletion_RewriteAndDelete`: Content rewrite followed by blob deletion
- `TestBlobDeletion_LongMaintenanceWithBlobDeletion`: Long-running maintenance with blob deletion
- `TestBlobDeletion_VerifyBlobVsContentError`: Distinguish blob deletion from content dropping

### 2. blob_not_found_actual_test.go
Test specifically targeting actual `blob.ErrBlobNotFound`:
- `TestActualBlobNotFound_AfterRewrite`: Simulates stale client scenario
  - Creates fragmented content (10 objects in separate packs)
  - Ages content significantly (48 hours)
  - One client refreshes, another stays "stale"
  - Waits through maintenance cycles for blob deletion
  - Attempts read with stale client after blob is deleted

## Key Findings

### Finding 1: Blob Deletion Through GC Path Works
✅ **Confirmed**: Blobs are successfully deleted after content goes through GC:
1. Content created but not referenced by snapshots
2. Content marked as "unused" and subject to GC
3. After aging (48h), content marked as deleted
4. After 2 maintenance cycles (~6h), blob is deleted from storage

**Evidence**:
```
blob_not_found_actual_test.go:115: Content marked as deleted (in q5bd2731e...), will be dropped soon
blob_not_found_actual_test.go:162: 🔴 Original blob q5bd2731e... DELETED at cycle 2 (T+6h)
```

### Finding 2: Recovery Mechanism Prevents Error
❌ **Unexpected**: Even after blob deletion, reads succeed!

**Evidence**:
```
blob_not_found_actual_test.go:165: ==== Attempting read with stale client ====
blob_not_found_actual_test.go:187: ✅ Read SUCCEEDED - no error (this is unexpected!)
blob_not_found_actual_test.go:192: After read attempt, readErr = <nil>
```

**Hypothesis**: Kopia has recovery mechanisms that prevent blob.ErrBlobNotFound from surfacing:
1. WriteSession might auto-refresh indexes before operations
2. System might have fallback/recovery when blob not found
3. Index synchronization happens through blob storage even without explicit Refresh()

### Finding 3: Content Rewrite Not Triggered
⚠️  **Challenge**: Cannot trigger content rewrite for active content with SafetyFull
- Created fragmented content (10 objects in separate packs)
- Aged content well past RewriteMinAge (48h vs 2h threshold)
- Content not referenced by snapshots → goes to GC path instead of rewrite path

**From logs**:
```
[snapshotgc] GC found 10 unused contents (53.2 KB)
```

## Error Type Distinction

### Content Dropped from Index (Previously Reproduced)
```
❌ content.ErrContentNotFound
   → wrapped as object.ErrObjectNotFound
```
- Content dropped from index via `TaskDropDeletedContentsFull`
- Blob might still exist in storage
- This was reproduced in earlier tests (maintenance_timing_bug_aggressive_test.go)

### Actual Blob Deleted from Storage (Seeking to Reproduce)
```
❌ blob.ErrBlobNotFound
   → should wrap up to object.ErrObjectNotFound
```
- Actual blob file deleted from storage
- Client has stale index pointing to deleted blob
- **This is what issue #4769 reports, but we haven't reproduced it yet**

## Two Paths to Blob Deletion

### GC Path (Tested Successfully)
1. Content not referenced by snapshots
2. Content marked as deleted
3. Content dropped from index
4. Pack blob deleted when all content dropped
5. **Result**: Blob deleted, but reads still succeed (recovery mechanism)

### Rewrite Path (Unable to Trigger with SafetyFull)
1. Active content (referenced by snapshots)
2. Content rewritten to new pack (consolidation)
3. Old pack becomes orphaned
4. Old pack blob deleted after MinRewriteToOrphanDeletionDelay
5. **Desired**: Stale client with old index tries to read → blob.ErrBlobNotFound
6. **Status**: Cannot trigger rewrite for test content

## Challenges

### Challenge 1: Auto-Refresh Behavior
Simply not calling `Repository.Refresh()` doesn't keep indexes stale enough:
- WriteSession might auto-refresh
- Index reads happen from blob storage
- All clients see same underlying index blobs

### Challenge 2: Triggering Content Rewrite
With SafetyFull parameters:
- Content must be referenced by snapshots to avoid GC path
- Content must be old enough (RewriteMinAge: 2h)
- Packs must be fragmented or meet rewrite criteria
- **Current tests create unreferenced content → goes to GC instead**

### Challenge 3: Recovery Mechanisms
Kopia appears to have robust recovery:
- Missing blobs don't immediately cause blob.ErrBlobNotFound
- System might retry, refresh, or use fallbacks
- This is good for reliability but makes bug reproduction difficult

## Potential Next Steps

### Option 1: Create Actual Snapshots
Modify tests to:
1. Create snapshot manifest objects that reference content
2. Keep snapshots alive so content goes through rewrite (not GC)
3. Force content rewrite using maintenance with proper parameters
4. Test with stale client after rewrite

### Option 2: Use Direct Rewrite API
```go
maintenance.RewriteContents(ctx, w, &maintenance.RewriteContentsOptions{
    ShortPacks: true,
}, maintenance.SafetyFull)
```
- Directly force content rewrite
- Control timing more precisely
- **Challenge**: User requires SafetyFull, but content_rewrite_test.go uses SafetyNone

### Option 3: Long-Running Operation Simulation
The real bug might occur when:
1. Long-running backup operation starts
2. Operation caches index entries in memory
3. Maintenance runs during operation and deletes blobs
4. Operation tries to access cached blob references → blob.ErrBlobNotFound
5. This requires testing concurrent operations

### Option 4: Investigate Recovery Code Path
Find and understand the recovery mechanism:
1. Search for where blob.ErrBlobNotFound is caught and handled
2. Understand under what conditions it's NOT recovered
3. Create test scenario that bypasses recovery

## Code Locations

### Key Test Files
- `repo/maintenance/blob_not_found_actual_test.go`: Main investigation test
- `repo/maintenance/blob_deletion_bug_test.go`: Supporting tests
- `repo/maintenance/content_rewrite_test.go`: Reference for triggering rewrite

### Related Source Files
- `repo/maintenance/maintenance_safety.go`: SafetyFull parameters
- `repo/maintenance/maintenance_run.go`: Maintenance orchestration
- `repo/object/object_reader.go`: Error wrapping for object reads
- `repo/content/content_reader.go`: Content access (likely has recovery logic)

## Questions for Further Investigation

1. **Where is blob.ErrBlobNotFound caught and recovered?**
   - Need to grep for error handling around blob.ErrBlobNotFound
   - Understand the recovery logic

2. **How do real production scenarios differ from tests?**
   - Real systems have actual snapshots
   - Operations might be truly long-running (hours/days)
   - Multiple clients with varying refresh patterns

3. **Is the issue specific to certain storage backends?**
   - Issue #4769 mentions "after random months"
   - Might be related to specific blob storage implementations
   - Cloud storage might have different consistency models

4. **What triggers the rewrite path in production?**
   - Need to understand actual production maintenance parameters
   - What makes content eligible for rewrite vs GC?

## Conclusion

**Current Status**:
- ✅ Successfully demonstrated blob deletion through GC path
- ✅ Verified SafetyFull timing mechanisms work as designed
- ❌ **Cannot reproduce blob.ErrBlobNotFound** - recovery prevents it
- ❌ Cannot trigger content rewrite for test scenarios with SafetyFull

**Root Cause Hypothesis**:
The issue might not be a bug in safety margins, but rather a race condition or edge case in:
1. Long-running operations with cached indexes
2. Recovery mechanism failures under specific conditions
3. Concurrent maintenance and access patterns
4. Specific blob storage backend behaviors

**Recommendation**:
Need to either:
1. Create tests with actual snapshots to trigger rewrite path
2. Investigate and understand the recovery mechanism
3. Test concurrent long-running operations during maintenance
4. Check if issue is specific to certain blob storage backends
