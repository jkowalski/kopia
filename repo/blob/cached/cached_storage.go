// Package cached implements a wrapper around blob.Storage that adds configurable
// caching and filtering capabilities via pluggable blob action functions.
package cached

import (
	"context"
	"strings"
	"sync"
	"time"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/clock"
	"github.com/kopia/kopia/internal/gather"
	"github.com/kopia/kopia/repo/blob"
)

// BlobAction defines what action to take for a blob.
type BlobAction int

const (
	// BlobActionIgnore silently ignores the blob (not stored anywhere).
	BlobActionIgnore BlobAction = iota
	// BlobActionCache caches the blob in memory and writes to base storage.
	BlobActionCache
	// BlobActionPassThrough writes to base storage only (no caching).
	BlobActionPassThrough
)

// BlobActionFunc is a function that determines what action to take for a given blob ID.
type BlobActionFunc func(blobID blob.ID) BlobAction

// cachedStorage wraps another blob storage and provides configurable caching
// and filtering based on the provided BlobActionFunc.
type cachedStorage struct {
	base       blob.Storage
	actionFunc BlobActionFunc
	blob.DefaultProviderImplementation

	mu    sync.RWMutex
	cache map[blob.ID]cachedItem
}

type cachedItem struct {
	data      []byte
	metadata  blob.Metadata
	timestamp time.Time
}

// GetCapacity passes through to the base storage.
func (s *cachedStorage) GetCapacity(ctx context.Context) (blob.Capacity, error) {
	//nolint:wrapcheck
	return s.base.GetCapacity(ctx)
}

// IsReadOnly passes through to the base storage.
func (s *cachedStorage) IsReadOnly() bool {
	return s.base.IsReadOnly()
}

// GetBlob checks the cache first, then falls back to the base storage.
func (s *cachedStorage) GetBlob(ctx context.Context, id blob.ID, offset, length int64, output blob.OutputBuffer) error {
	// Check cache first
	s.mu.RLock()
	item, found := s.cache[id]
	s.mu.RUnlock()

	if found {
		return s.serveBlobFromCache(item.data, offset, length, output)
	}

	// Fall back to base storage
	//nolint:wrapcheck
	return s.base.GetBlob(ctx, id, offset, length, output)
}

// serveBlobFromCache serves blob data from the cache with proper range handling.
func (s *cachedStorage) serveBlobFromCache(data []byte, offset, length int64, output blob.OutputBuffer) error {
	output.Reset()

	if offset < 0 || offset >= int64(len(data)) {
		return blob.ErrInvalidRange
	}

	if length < 0 {
		// Return all data from offset
		if _, err := output.Write(data[offset:]); err != nil {
			return errors.Wrap(err, "error writing data to output")
		}

		return nil
	}

	if offset+length > int64(len(data)) {
		return blob.ErrInvalidRange
	}

	if _, err := output.Write(data[offset : offset+length]); err != nil {
		return errors.Wrap(err, "error writing data to output")
	}

	return nil
}

// GetMetadata checks the cache first, then falls back to the base storage.
func (s *cachedStorage) GetMetadata(ctx context.Context, id blob.ID) (blob.Metadata, error) {
	// Check cache first
	s.mu.RLock()
	item, found := s.cache[id]
	s.mu.RUnlock()

	if found {
		return item.metadata, nil
	}

	// Fall back to base storage
	//nolint:wrapcheck
	return s.base.GetMetadata(ctx, id)
}

// PutBlob handles blob writes based on the configured action function.
func (s *cachedStorage) PutBlob(ctx context.Context, id blob.ID, data blob.Bytes, opts blob.PutOptions) error {
	action := s.actionFunc(id)

	switch action {
	case BlobActionIgnore:
		// Silently ignore the blob
		return nil

	case BlobActionCache:
		// Convert data to byte slice for caching
		var tmp gather.WriteBuffer
		defer tmp.Close()

		if _, err := data.WriteTo(&tmp); err != nil {
			return errors.Wrap(err, "error converting blob data")
		}

		dataBytes := tmp.ToByteSlice()

		// Cache the data in memory
		s.mu.Lock()
		s.cache[id] = cachedItem{
			data: dataBytes,
			metadata: blob.Metadata{
				BlobID:    id,
				Length:    int64(len(dataBytes)),
				Timestamp: clock.Now(),
			},
			timestamp: clock.Now(),
		}
		s.mu.Unlock()

		// Also write to the underlying storage
		//nolint:wrapcheck
		return s.base.PutBlob(ctx, id, data, opts)

	case BlobActionPassThrough:
		// Pass through to base storage only (no caching)
		//nolint:wrapcheck
		return s.base.PutBlob(ctx, id, data, opts)

	default:
		// Default to pass-through for unknown actions
		//nolint:wrapcheck
		return s.base.PutBlob(ctx, id, data, opts)
	}
}

// DeleteBlob removes from cache and deletes from base storage.
func (s *cachedStorage) DeleteBlob(ctx context.Context, id blob.ID) error {
	// Remove from cache
	s.mu.Lock()
	delete(s.cache, id)
	s.mu.Unlock()

	// Delete from base storage
	//nolint:wrapcheck
	return s.base.DeleteBlob(ctx, id)
}

// ListBlobs lists from both cache and base storage, merging results.
func (s *cachedStorage) ListBlobs(ctx context.Context, prefix blob.ID, callback func(blob.Metadata) error) error {
	// Track which blobs we've seen to avoid duplicates
	seen := make(map[blob.ID]bool)

	// First, list cached blobs that match the prefix
	s.mu.RLock()

	var cachedItems []cachedItem

	prefixStr := string(prefix)
	for id, item := range s.cache {
		if strings.HasPrefix(string(id), prefixStr) {
			cachedItems = append(cachedItems, item)
			seen[id] = true
		}
	}
	s.mu.RUnlock()

	// Call callback for cached items
	for _, item := range cachedItems {
		if err := callback(item.metadata); err != nil {
			return err
		}
	}

	// Then list from base storage, skipping items we've already seen
	return errors.Wrap(s.base.ListBlobs(ctx, prefix, func(metadata blob.Metadata) error {
		if seen[metadata.BlobID] {
			return nil // Skip, already processed from cache
		}

		return callback(metadata)
	}), "error listing blobs from base storage")
}

// Close closes the base storage and clears the cache.
func (s *cachedStorage) Close(ctx context.Context) error {
	s.mu.Lock()
	s.cache = make(map[blob.ID]cachedItem)
	s.mu.Unlock()

	//nolint:wrapcheck
	return s.base.Close(ctx)
}

// ConnectionInfo returns the base storage's connection info.
func (s *cachedStorage) ConnectionInfo() blob.ConnectionInfo {
	return s.base.ConnectionInfo()
}

// DisplayName returns a modified display name indicating caching.
func (s *cachedStorage) DisplayName() string {
	return "Cached: " + s.base.DisplayName()
}

// FlushCaches flushes the base storage caches and optionally clears our memory cache.
func (s *cachedStorage) FlushCaches(ctx context.Context) error {
	//nolint:wrapcheck
	return s.base.FlushCaches(ctx)
}

// NewWrapper returns a cached Storage wrapper that adds configurable caching
// and filtering capabilities to the underlying storage based on the provided action function.
func NewWrapper(wrapped blob.Storage, actionFunc BlobActionFunc) blob.Storage {
	return &cachedStorage{
		base:       wrapped,
		actionFunc: actionFunc,
		cache:      make(map[blob.ID]cachedItem),
	}
}

// Convenience functions for common blob action patterns

// PrefixBasedActionFunc returns a BlobActionFunc that performs actions based on blob ID prefixes.
func PrefixBasedActionFunc(ignorePrefixes, cachePrefixes []string) BlobActionFunc {
	return func(blobID blob.ID) BlobAction {
		str := string(blobID)

		// Check ignore prefixes first
		for _, prefix := range ignorePrefixes {
			if strings.HasPrefix(str, prefix) {
				return BlobActionIgnore
			}
		}

		// Check cache prefixes
		for _, prefix := range cachePrefixes {
			if strings.HasPrefix(str, prefix) {
				return BlobActionCache
			}
		}

		// Default to pass-through
		return BlobActionPassThrough
	}
}

// IgnorePrefixesActionFunc returns a BlobActionFunc that ignores blobs with specified prefixes
// and caches all others.
func IgnorePrefixesActionFunc(ignorePrefixes []string) BlobActionFunc {
	return func(blobID blob.ID) BlobAction {
		str := string(blobID)

		for _, prefix := range ignorePrefixes {
			if strings.HasPrefix(str, prefix) {
				return BlobActionIgnore
			}
		}

		return BlobActionCache
	}
}

// CacheAllActionFunc returns a BlobActionFunc that caches all blobs.
func CacheAllActionFunc() BlobActionFunc {
	return func(_ blob.ID) BlobAction {
		return BlobActionCache
	}
}

// PassThroughAllActionFunc returns a BlobActionFunc that passes through all blobs without caching.
func PassThroughAllActionFunc() BlobActionFunc {
	return func(_ blob.ID) BlobAction {
		return BlobActionPassThrough
	}
}
