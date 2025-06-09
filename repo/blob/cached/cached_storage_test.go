package cached

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/kopia/kopia/internal/blobtesting"
	"github.com/kopia/kopia/internal/gather"
	"github.com/kopia/kopia/repo/blob"
)

func TestCachedStorage_IgnoresPrefixes(t *testing.T) {
	baseStorage := blobtesting.NewMapStorage(blobtesting.DataMap{}, nil, nil)

	// Create action func that ignores blobs starting with 'p' and 'q'
	actionFunc := IgnorePrefixesActionFunc([]string{"p", "q"})
	cached := NewWrapper(baseStorage, actionFunc)

	ctx := context.Background()

	// Test writes to blobs starting with 'p' and 'q' are ignored
	testData := gather.FromSlice([]byte("test data"))

	err := cached.PutBlob(ctx, "ptest", testData, blob.PutOptions{})
	require.NoError(t, err, "PutBlob with 'p' prefix should not error")

	err = cached.PutBlob(ctx, "qtest", testData, blob.PutOptions{})
	require.NoError(t, err, "PutBlob with 'q' prefix should not error")

	// Verify they were not written to base storage
	var output gather.WriteBuffer
	defer output.Close()

	err = baseStorage.GetBlob(ctx, "ptest", 0, -1, &output)
	assert.Equal(t, blob.ErrBlobNotFound, err, "Blob with 'p' prefix should not be in base storage")

	err = baseStorage.GetBlob(ctx, "qtest", 0, -1, &output)
	assert.Equal(t, blob.ErrBlobNotFound, err, "Blob with 'q' prefix should not be in base storage")

	// Verify they are not cached either (should return not found)
	err = cached.GetBlob(ctx, "ptest", 0, -1, &output)
	assert.Equal(t, blob.ErrBlobNotFound, err, "Blob with 'p' prefix should not be cached")

	err = cached.GetBlob(ctx, "qtest", 0, -1, &output)
	assert.Equal(t, blob.ErrBlobNotFound, err, "Blob with 'q' prefix should not be cached")
}

func TestCachedStorage_CachesOtherBlobs(t *testing.T) {
	baseStorage := blobtesting.NewMapStorage(blobtesting.DataMap{}, nil, nil)

	// Create action func that caches all blobs
	actionFunc := CacheAllActionFunc()
	cached := NewWrapper(baseStorage, actionFunc)

	ctx := context.Background()
	testData := gather.FromSlice([]byte("test data"))

	// Write blob that should be cached
	err := cached.PutBlob(ctx, "test-blob", testData, blob.PutOptions{})
	require.NoError(t, err)

	// Verify it was written to base storage
	var output gather.WriteBuffer
	defer output.Close()

	err = baseStorage.GetBlob(ctx, "test-blob", 0, -1, &output)
	require.NoError(t, err, "Blob should be in base storage")
	assert.Equal(t, []byte("test data"), output.ToByteSlice())

	// Clear the base storage to test caching
	err = baseStorage.DeleteBlob(ctx, "test-blob")
	require.NoError(t, err)

	// Should still be able to read from cache
	output.Reset()
	err = cached.GetBlob(ctx, "test-blob", 0, -1, &output)
	require.NoError(t, err, "Blob should be served from cache")
	assert.Equal(t, []byte("test data"), output.ToByteSlice())
}

func TestCachedStorage_RangeReads(t *testing.T) {
	baseStorage := blobtesting.NewMapStorage(blobtesting.DataMap{}, nil, nil)

	// Create action func that caches all blobs
	actionFunc := CacheAllActionFunc()
	cached := NewWrapper(baseStorage, actionFunc)

	ctx := context.Background()
	testData := gather.FromSlice([]byte("0123456789"))

	// Write and cache the blob
	err := cached.PutBlob(ctx, "range-test", testData, blob.PutOptions{})
	require.NoError(t, err)

	// Delete from base to ensure we're reading from cache
	err = baseStorage.DeleteBlob(ctx, "range-test")
	require.NoError(t, err)

	var output gather.WriteBuffer
	defer output.Close()

	// Test range read from cache
	err = cached.GetBlob(ctx, "range-test", 2, 5, &output)
	require.NoError(t, err)
	assert.Equal(t, []byte("23456"), output.ToByteSlice())

	// Test read from offset
	output.Reset()
	err = cached.GetBlob(ctx, "range-test", 5, -1, &output)
	require.NoError(t, err)
	assert.Equal(t, []byte("56789"), output.ToByteSlice())
}

func TestCachedStorage_GetMetadata(t *testing.T) {
	baseStorage := blobtesting.NewMapStorage(blobtesting.DataMap{}, nil, nil)

	// Create action func that caches all blobs
	actionFunc := CacheAllActionFunc()
	cached := NewWrapper(baseStorage, actionFunc)

	ctx := context.Background()
	testData := gather.FromSlice([]byte("test metadata"))

	// Write blob
	err := cached.PutBlob(ctx, "meta-test", testData, blob.PutOptions{})
	require.NoError(t, err)

	// Get metadata from cache
	meta, err := cached.GetMetadata(ctx, "meta-test")
	require.NoError(t, err)
	assert.Equal(t, blob.ID("meta-test"), meta.BlobID)
	assert.Equal(t, int64(13), meta.Length)
}

func TestCachedStorage_ListBlobs(t *testing.T) {
	baseStorage := blobtesting.NewMapStorage(blobtesting.DataMap{}, nil, nil)

	// Create action func that caches all blobs
	actionFunc := CacheAllActionFunc()
	cached := NewWrapper(baseStorage, actionFunc)

	ctx := context.Background()

	// Add some blobs to base storage directly
	testData := gather.FromSlice([]byte("base data"))
	err := baseStorage.PutBlob(ctx, "base-blob", testData, blob.PutOptions{})
	require.NoError(t, err)

	// Add some blobs through cached storage
	testData2 := gather.FromSlice([]byte("cached data"))
	err = cached.PutBlob(ctx, "cached-blob", testData2, blob.PutOptions{})
	require.NoError(t, err)

	// List all blobs
	var foundBlobs []blob.Metadata
	err = cached.ListBlobs(ctx, "", func(meta blob.Metadata) error {
		foundBlobs = append(foundBlobs, meta)
		return nil
	})
	require.NoError(t, err)

	// Should see both blobs
	assert.Len(t, foundBlobs, 2)

	blobIDs := make(map[blob.ID]bool)
	for _, meta := range foundBlobs {
		blobIDs[meta.BlobID] = true
	}

	assert.True(t, blobIDs["base-blob"])
	assert.True(t, blobIDs["cached-blob"])
}

func TestCachedStorage_DeleteBlob(t *testing.T) {
	baseStorage := blobtesting.NewMapStorage(blobtesting.DataMap{}, nil, nil)

	// Create action func that caches all blobs
	actionFunc := CacheAllActionFunc()
	cached := NewWrapper(baseStorage, actionFunc)

	ctx := context.Background()
	testData := gather.FromSlice([]byte("delete test"))

	// Write blob
	err := cached.PutBlob(ctx, "delete-test", testData, blob.PutOptions{})
	require.NoError(t, err)

	// Verify it's cached
	var output gather.WriteBuffer
	defer output.Close()

	err = cached.GetBlob(ctx, "delete-test", 0, -1, &output)
	require.NoError(t, err)

	// Delete blob
	err = cached.DeleteBlob(ctx, "delete-test")
	require.NoError(t, err)

	// Should not be in cache anymore
	output.Reset()
	err = cached.GetBlob(ctx, "delete-test", 0, -1, &output)
	assert.Equal(t, blob.ErrBlobNotFound, err)

	// Should not be in base storage either
	err = baseStorage.GetBlob(ctx, "delete-test", 0, -1, &output)
	assert.Equal(t, blob.ErrBlobNotFound, err)
}

func TestCachedStorage_PassThroughAction(t *testing.T) {
	baseStorage := blobtesting.NewMapStorage(blobtesting.DataMap{}, nil, nil)

	// Create action func that passes through all blobs (no caching)
	actionFunc := PassThroughAllActionFunc()
	cached := NewWrapper(baseStorage, actionFunc)

	ctx := context.Background()
	testData := gather.FromSlice([]byte("pass through test"))

	// Write blob
	err := cached.PutBlob(ctx, "passthrough-test", testData, blob.PutOptions{})
	require.NoError(t, err)

	// Verify it was written to base storage
	var output gather.WriteBuffer
	defer output.Close()

	err = baseStorage.GetBlob(ctx, "passthrough-test", 0, -1, &output)
	require.NoError(t, err, "Blob should be in base storage")
	assert.Equal(t, []byte("pass through test"), output.ToByteSlice())

	// Delete from base storage to test that it's NOT cached
	err = baseStorage.DeleteBlob(ctx, "passthrough-test")
	require.NoError(t, err)

	// Should not be able to read from cache (should return not found)
	output.Reset()
	err = cached.GetBlob(ctx, "passthrough-test", 0, -1, &output)
	assert.Equal(t, blob.ErrBlobNotFound, err, "Blob should not be cached")
}

func TestCachedStorage_PrefixBasedAction(t *testing.T) {
	baseStorage := blobtesting.NewMapStorage(blobtesting.DataMap{}, nil, nil)

	// Create action func with specific prefix rules
	actionFunc := PrefixBasedActionFunc(
		[]string{"ignore"}, // ignore blobs starting with "ignore"
		[]string{"cache"},  // cache blobs starting with "cache"
	)
	cached := NewWrapper(baseStorage, actionFunc)

	ctx := context.Background()
	testData := gather.FromSlice([]byte("test data"))

	// Test ignored prefix
	err := cached.PutBlob(ctx, "ignore-this", testData, blob.PutOptions{})
	require.NoError(t, err)

	var output gather.WriteBuffer
	defer output.Close()

	err = cached.GetBlob(ctx, "ignore-this", 0, -1, &output)
	assert.Equal(t, blob.ErrBlobNotFound, err, "Ignored blob should not be found")

	// Test cached prefix
	err = cached.PutBlob(ctx, "cache-this", testData, blob.PutOptions{})
	require.NoError(t, err)

	// Should be cached
	err = cached.GetBlob(ctx, "cache-this", 0, -1, &output)
	require.NoError(t, err, "Cached blob should be found")
	assert.Equal(t, []byte("test data"), output.ToByteSlice())

	// Test pass-through prefix (doesn't match ignore or cache)
	err = cached.PutBlob(ctx, "other-blob", testData, blob.PutOptions{})
	require.NoError(t, err)

	// Should be written to base storage
	output.Reset()
	err = baseStorage.GetBlob(ctx, "other-blob", 0, -1, &output)
	require.NoError(t, err, "Pass-through blob should be in base storage")

	// Delete from base to test it's not cached
	err = baseStorage.DeleteBlob(ctx, "other-blob")
	require.NoError(t, err)

	output.Reset()
	err = cached.GetBlob(ctx, "other-blob", 0, -1, &output)
	assert.Equal(t, blob.ErrBlobNotFound, err, "Pass-through blob should not be cached")
}

func TestCachedStorage_CustomActionFunc(t *testing.T) {
	baseStorage := blobtesting.NewMapStorage(blobtesting.DataMap{}, nil, nil)

	// Create custom action func that caches blobs with even lengths and ignores odd lengths
	actionFunc := func(blobID blob.ID) BlobAction {
		if len(string(blobID))%2 == 0 {
			return BlobActionCache
		}
		return BlobActionIgnore
	}

	cached := NewWrapper(baseStorage, actionFunc)

	ctx := context.Background()
	testData := gather.FromSlice([]byte("test"))

	// Test even length blob ID (should be cached)
	err := cached.PutBlob(ctx, "even", testData, blob.PutOptions{}) // 4 chars = even
	require.NoError(t, err)

	var output gather.WriteBuffer
	defer output.Close()

	err = cached.GetBlob(ctx, "even", 0, -1, &output)
	require.NoError(t, err, "Even length blob should be cached")
	assert.Equal(t, []byte("test"), output.ToByteSlice())

	// Test odd length blob ID (should be ignored)
	err = cached.PutBlob(ctx, "odd", testData, blob.PutOptions{}) // 3 chars = odd
	require.NoError(t, err)

	output.Reset()
	err = cached.GetBlob(ctx, "odd", 0, -1, &output)
	assert.Equal(t, blob.ErrBlobNotFound, err, "Odd length blob should be ignored")
}
