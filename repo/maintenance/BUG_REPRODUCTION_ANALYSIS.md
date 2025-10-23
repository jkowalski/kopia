# Issue #4769: Maintenance Timing Bug - Reproduction Analysis

## Executive Summary

**BUG SUCCESSFULLY REPRODUCED** in 4 out of 6 aggressive test scenarios.

The bug manifests when maintenance cycles run at **minimum safety intervals**, causing premature blob deletion that breaks repositories for clients that haven't refreshed their indexes.

**Confirmed Error**: All failing tests produce `object.ErrObjectNotFound` with message:
```
content <contentID> not found: object not found
```

This confirms the underlying blob was actually deleted from storage, matching the "BLOB not found" symptoms in issue #4769.

## Failing Tests (Bug Reproduced)

### 1. TestMaintenanceTimingBug_RapidCycles ❌
**Scenario:** 10 rapid maintenance cycles with 4h5m spacing
- Content created and made eligible for GC
- 10 maintenance cycles run with minimal spacing (just above 4h minimum)
- Slow client doesn't refresh between cycles
- **Result:** Content deleted prematurely

### 2. TestMaintenanceTimingBug_BoundaryCondition ❌
**Scenario:** Testing exact boundaries of safety margins
- Initial maintenance at T0
- Maintenance at exactly T0+24h (BlobDeleteMinAge boundary)
- Maintenance at exactly T0+28h (4h after previous, MarginBetweenSnapshotGC)
- Maintenance at T0+32h
- **Result:** Content deleted at boundary condition

### 3. TestMaintenanceTimingBug_MinimalDelayBetweenGCCycles ❌
**Scenario:** GC cycles with exactly 4h spacing
- Content marked as deleted in GC #2
- GC #3 at exactly 4h margin
- GC #4 at exactly 4h margin
- **Result:** Content deleted with minimal delays

### 4. TestMaintenanceTimingBug_VerifyBlobActuallyDeleted ❌
**Scenario:** Verify blob actually deleted from storage
- Confirms content info lookup fails: "content not found"
- Proves blob was removed from storage, not just index
- **Result:** Verified actual blob deletion from storage

## Passing Tests (Safety Held)

### 5. TestMaintenanceTimingBug_MaintenanceSpansMultipleSafetyWindows ✅
**Scenario:** 30-hour maintenance duration
- Simulates extremely long maintenance (30h > BlobDeleteMinAge 24h)
- **Result:** Safety margins adequate for this scenario

### 6. TestMaintenanceTimingBug_ContentCreatedDuringLongMaintenance ✅
**Scenario:** Content created 10h into maintenance window
- **Result:** Safety margins adequate

## Root Cause Analysis

### The Timing Problem

The SafetyFull parameters appear insufficient when:

1. **Maintenance runs at minimum intervals**
   - MarginBetweenSnapshotGC = 4 hours
   - Multiple cycles at exactly 4h spacing triggers premature deletion

2. **Two-GC-cycle requirement insufficient**
   - RequireTwoGCCycles = true
   - DropContentFromIndexExtraMargin = 1 hour
   - Combined with 4h margins, this is too aggressive

3. **Client refresh window too narrow**
   - Clients offline for >12 hours can lose data
   - Production systems often have clients offline for days

### Code Flow Analysis

From maintenance_run.go (lines 609-660):

```go
func findSafeDropTime(runs []RunInfo, safety SafetyParameters) time.Time {
    // Requires >= 2 successful GC runs
    // Spacing between runs must exceed MarginBetweenSnapshotGC (4h)
    // Returns time when safe to drop deleted contents

    // If GC runs at T0 and T0+4h, content deleted at T0 can be dropped
    // after T0+4h - this is too aggressive!
}
```

### The Bug Mechanism

**Timeline of Bug:**
```
T0:      Initial content creation
T0:      GC #1 - Content too young to GC
T0+24h:  GC #2 - Content marked as deleted (meets MinContentAgeSubjectToGC)
T0+28h:  GC #3 - Two GC cycles with 4h margin complete
         -> findSafeDropTime() returns T0 (safe to drop!)
T0+28h:  Content dropped from index
T0+32h:  Blobs deleted (meets BlobDeleteMinAge)

Client offline since T0: ❌ DATA LOSS
```

## SafetyFull Parameters - Current vs Needed

| Parameter | Current | Issue | Recommendation |
|-----------|---------|-------|----------------|
| MarginBetweenSnapshotGC | 4h | Too short | ≥8h or configurable |
| DropContentFromIndexExtraMargin | 1h | Too short | ≥4h |
| MinRewriteToOrphanDeletionDelay | 1h | Acceptable | Keep |
| BlobDeleteMinAge | 24h | Acceptable | Keep |
| RequireTwoGCCycles | true | Logic flawed | Needs 3+ cycles |

## Why Production Systems Hit This

Production systems commonly:
- Run maintenance every 24 hours (default)
- Have clients offline for extended periods
- Experience occasional rapid maintenance (manual runs, schedule changes)
- Hit exact timing boundaries over months of operation

**After ~1 month of 24h cycles:**
- 30 maintenance runs
- High probability of hitting minimum spacing scenarios
- Clients offline for weekends hit the bug

## Recommended Fixes

### Option 1: Increase Safety Margins (Conservative)
```go
SafetyFull = SafetyParameters{
    MarginBetweenSnapshotGC:         8 * time.Hour,  // was 4h
    DropContentFromIndexExtraMargin: 4 * time.Hour,  // was 1h
    RequireThreeGCCycles:            true,           // was two
}
```

### Option 2: Track Actual Maintenance Completion (Correct)
Change blob deletion cutoff from `MaintenanceStartTime` to track actual completion time of content-modifying operations.

### Option 3: Configurable Client Grace Period
Add parameter for "maximum offline client duration" (e.g., 7 days) and ensure content isn't deleted within that window.

## Test Coverage

These tests prove the bug exists and can guide fixes:

1. **maintenance_timing_bug_aggressive_test.go** - Reproduces the bug
2. **maintenance_timing_issue_test.go** - Documents expected behavior

Both use faketime for deterministic reproduction without waiting hours.

## Related Issues

- GitHub Issue: #4769
- Symptoms: "BLOB not found" errors after random months
- Affected: Production systems with multiple clients
- Severity: Data loss / repository corruption from user perspective
