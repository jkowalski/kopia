package cache

import (
	"context"
	"sync"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/gather"
	"github.com/kopia/kopia/repo/blob"
)

// PackedContentCache implements ContentCache interface using packed storage.
type PackedContentCache struct {
	storage *PackedStorage
	mu      sync.RWMutex
}

// NewPackedContentCache creates a new packed content cache.
func NewPackedContentCache(ctx context.Context, cacheDir string) (ContentCache, error) {
	storage, err := NewPackedStorage(ctx, cacheDir)
	if err != nil {
		return nil, errors.Wrap(err, "error creating packed storage")
	}

	return &PackedContentCache{
		storage: storage,
	}, nil
}

func (c *PackedContentCache) GetContent(ctx context.Context, contentID string, blobID blob.ID, offset, length int64, output *gather.WriteBuffer) error {
	// For packed cache, we use contentID as the blob ID since we store contents directly
	return c.storage.GetBlob(ctx, blob.ID(contentID), offset, length, output)
}

func (c *PackedContentCache) PrefetchBlob(ctx context.Context, blobID blob.ID) error {
	// No prefetching needed for packed cache since we store contents directly
	return nil
}

func (c *PackedContentCache) Close(ctx context.Context) {
	if err := c.storage.Close(ctx); err != nil {
		// We can't return an error from Close() due to interface definition
		// Log it or handle it as appropriate for your application
	}
}

func (c *PackedContentCache) CacheStorage() Storage {
	return c.storage
}

func (c *PackedContentCache) GetBlob(ctx context.Context, id blob.ID, offset, length int64, output blob.OutputBuffer) error {
	return c.storage.GetBlob(ctx, id, offset, length, output)
}
