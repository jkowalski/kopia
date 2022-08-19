package format

import (
	"context"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/gather"
	"github.com/kopia/kopia/repo/blob"
)

// BackupBlobIDPrefix is the prefix for all identifiers of the BLOBs that
// keep a backup copy of the FormatBlobID BLOB for the purposes of rollback
// during upgrade.
const BackupBlobIDPrefix = "kopia.repository.backup."

// BackupBlobID gets the upgrade backu pblob-id fro mthe lock.
func BackupBlobID(l UpgradeLockIntent) blob.ID {
	return blob.ID(BackupBlobIDPrefix + l.OwnerID)
}

// SetUpgradeLockIntent sets the upgrade lock intent on the repository format
// blob for other clients to notice. If a lock intent was already placed then
// it updates the existing lock using the output of the UpgradeLock.Update().
//
// This method also backs up the original format version on the upgrade lock
// intent and sets the latest format-version o nthe repository blob. This
// should cause the unsupporting clients (non-upgrade capable) to fail
// connecting to the repository.
func (m *Manager) SetUpgradeLockIntent(ctx context.Context, l UpgradeLockIntent) (*UpgradeLockIntent, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if err := m.maybeRefreshLocked(); err != nil {
		return nil, err
	}

	if err := l.Validate(); err != nil {
		return nil, errors.Wrap(err, "invalid upgrade lock intent")
	}

	if m.repoConfig.UpgradeLock == nil {
		// when we are putting a new lock then ensure that we can upgrade
		// to that version
		if m.repoConfig.ContentFormat.Version >= MaxFormatVersion {
			return nil, errors.Errorf("repository is using version %d, and version %d is the maximum",
				m.repoConfig.ContentFormat.Version, MaxFormatVersion)
		}

		// backup the current repository config from local cache to the
		// repository when we place the lock for the first time
		if err := m.j.WriteKopiaRepositoryBlobWithID(ctx, m.blobs, m.blobCfgBlob, BackupBlobID(l)); err != nil {
			return nil, errors.Wrap(err, "failed to backup the repo format blob")
		}

		// set a new lock or revoke an existing lock
		m.repoConfig.UpgradeLock = &l
		// mark the upgrade to the new format version, this will ensure that older
		// clients won't be able to parse the new version
		m.repoConfig.ContentFormat.Version = MaxFormatVersion
	} else if newL, err := m.repoConfig.UpgradeLock.Update(&l); err == nil {
		m.repoConfig.UpgradeLock = newL
	} else {
		return nil, errors.Wrap(err, "failed to update the existing lock")
	}

	if err := m.updateRepoConfigLocked(ctx); err != nil {
		return nil, errors.Wrap(err, "error updating repo config")
	}

	return m.repoConfig.UpgradeLock.Clone(), nil
}

// CommitUpgrade removes the upgrade lock from the from the repository format
// blob. This in-effect commits the new repository format t othe repository and
// resumes all access to the repository.
func (m *Manager) CommitUpgrade(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if err := m.maybeRefreshLocked(); err != nil {
		return err
	}

	if m.repoConfig.UpgradeLock == nil {
		return errors.New("no upgrade in progress")
	}

	// restore the old format version
	m.repoConfig.UpgradeLock = nil

	return m.updateRepoConfigLocked(ctx)
}

// RollbackUpgrade removes the upgrade lock while also restoring the
// format-blob's original version. This method does not restore the original
// repository data format and neither does it validate against any repository
// changes. Rolling back the repository format is currently not supported and
// hence using this API could render the repository corrupted and unreadable by
// clients.
func (m *Manager) RollbackUpgrade(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if err := m.maybeRefreshLocked(); err != nil {
		return err
	}

	if m.repoConfig.UpgradeLock == nil {
		return errors.New("no upgrade in progress")
	}

	// restore the oldest backup and delete the rest
	var oldestBackup *blob.Metadata

	if err := m.blobs.ListBlobs(ctx, BackupBlobIDPrefix, func(bm blob.Metadata) error {
		var delID blob.ID
		if oldestBackup == nil || bm.Timestamp.Before(oldestBackup.Timestamp) {
			if oldestBackup != nil {
				// delete the current candidate because we have found an even older one
				delID = oldestBackup.BlobID
			}
			oldestBackup = &bm
		} else {
			delID = bm.BlobID
		}

		if delID != "" {
			// delete the backup that we are not going to need for rollback
			if err := m.blobs.DeleteBlob(ctx, delID); err != nil {
				return errors.Wrapf(err, "failed to delete the format blob backup %q", delID)
			}
		}

		return nil
	}); err != nil {
		return errors.Wrap(err, "failed to list backup blobs")
	}

	// restore only when we find a backup, otherwise simply cleanup the local cache
	if oldestBackup != nil {
		var d gather.WriteBuffer
		if err := m.blobs.GetBlob(ctx, oldestBackup.BlobID, 0, -1, &d); err != nil {
			return errors.Wrapf(err, "failed to read from backup %q", oldestBackup.BlobID)
		}

		if err := m.blobs.PutBlob(ctx, KopiaRepositoryBlobID, d.Bytes(), blob.PutOptions{}); err != nil {
			return errors.Wrapf(err, "failed to restore format blob from backup %q", oldestBackup.BlobID)
		}

		// delete the backup after we have restored the format-blob
		if err := m.blobs.DeleteBlob(ctx, oldestBackup.BlobID); err != nil {
			return errors.Wrapf(err, "failed to delete the format blob backup %q", oldestBackup.BlobID)
		}
	}

	m.cache.Remove(ctx, []blob.ID{KopiaRepositoryBlobID})

	return nil
}

// GetUpgradeLockIntent gets the current upgrade lock intent.
func (m *Manager) GetUpgradeLockIntent(ctx context.Context) (*UpgradeLockIntent, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if err := m.maybeRefreshLocked(); err != nil {
		return nil, err
	}

	return m.repoConfig.UpgradeLock, nil
}