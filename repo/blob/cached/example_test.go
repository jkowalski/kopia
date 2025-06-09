package cached_test

import (
	"context"
	"fmt"
	"log"
	"strings"

	"github.com/kopia/kopia/internal/blobtesting"
	"github.com/kopia/kopia/internal/gather"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/blob/cached"
)

func ExampleNewWrapper() {
	// Create a base storage (in this example, an in-memory map storage)
	baseStorage := blobtesting.NewMapStorage(blobtesting.DataMap{}, nil, nil)

	// Create an action function that ignores blobs starting with 'p' and 'q'
	// and caches all others
	actionFunc := cached.IgnorePrefixesActionFunc([]string{"p", "q"})

	// Wrap it with the cached storage provider
	cachedStorage := cached.NewWrapper(baseStorage, actionFunc)

	ctx := context.Background()

	// Example 1: Writes to blobs starting with 'p' and 'q' are silently ignored
	data1 := gather.FromSlice([]byte("This will be ignored"))
	err := cachedStorage.PutBlob(ctx, "p_ignored_blob", data1, blob.PutOptions{})
	if err != nil {
		log.Fatal(err)
	}

	err = cachedStorage.PutBlob(ctx, "q_ignored_blob", data1, blob.PutOptions{})
	if err != nil {
		log.Fatal(err)
	}

	// Example 2: Other blobs are cached in memory and written to base storage
	data2 := gather.FromSlice([]byte("This will be cached"))
	err = cachedStorage.PutBlob(ctx, "cached_blob", data2, blob.PutOptions{})
	if err != nil {
		log.Fatal(err)
	}

	// Remove the blob from base storage to demonstrate caching
	err = baseStorage.DeleteBlob(ctx, "cached_blob")
	if err != nil {
		log.Fatal(err)
	}

	// Reading the blob still works because it's cached in memory
	var output gather.WriteBuffer
	defer output.Close()

	err = cachedStorage.GetBlob(ctx, "cached_blob", 0, -1, &output)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("Retrieved from cache: %s\n", string(output.ToByteSlice()))

	// Example 3: Attempting to read ignored blobs returns not found
	output.Reset()
	err = cachedStorage.GetBlob(ctx, "p_ignored_blob", 0, -1, &output)
	if err == blob.ErrBlobNotFound {
		fmt.Println("Blob starting with 'p' was ignored as expected")
	}

	// Output:
	// Retrieved from cache: This will be cached
	// Blob starting with 'p' was ignored as expected
}

func ExamplePrefixBasedActionFunc() {
	baseStorage := blobtesting.NewMapStorage(blobtesting.DataMap{}, nil, nil)

	// Create an action function with specific prefix rules
	actionFunc := cached.PrefixBasedActionFunc(
		[]string{"temp", "debug"}, // ignore blobs starting with "temp" or "debug"
		[]string{"cache", "fast"}, // cache blobs starting with "cache" or "fast"
		// all other blobs will be passed through (written to storage but not cached)
	)

	cachedStorage := cached.NewWrapper(baseStorage, actionFunc)

	ctx := context.Background()
	testData := gather.FromSlice([]byte("example data"))

	// This will be ignored
	cachedStorage.PutBlob(ctx, "temp_file", testData, blob.PutOptions{})

	// This will be cached in memory and written to storage
	cachedStorage.PutBlob(ctx, "cache_important", testData, blob.PutOptions{})

	// This will be passed through to storage only (not cached)
	cachedStorage.PutBlob(ctx, "regular_blob", testData, blob.PutOptions{})

	fmt.Println("Different blob handling based on prefixes")
	// Output: Different blob handling based on prefixes
}

func ExampleBlobActionFunc() {
	baseStorage := blobtesting.NewMapStorage(blobtesting.DataMap{}, nil, nil)

	// Create a custom action function
	actionFunc := func(blobID blob.ID) cached.BlobAction {
		str := string(blobID)

		// Ignore test blobs
		if strings.Contains(str, "test") {
			return cached.BlobActionIgnore
		}

		// Cache important blobs
		if strings.Contains(str, "important") {
			return cached.BlobActionCache
		}

		// Pass through everything else
		return cached.BlobActionPassThrough
	}

	cachedStorage := cached.NewWrapper(baseStorage, actionFunc)

	ctx := context.Background()
	data := gather.FromSlice([]byte("data"))

	// These demonstrate the custom logic
	cachedStorage.PutBlob(ctx, "test_blob", data, blob.PutOptions{})      // ignored
	cachedStorage.PutBlob(ctx, "important_blob", data, blob.PutOptions{}) // cached
	cachedStorage.PutBlob(ctx, "regular_blob", data, blob.PutOptions{})   // passed through

	fmt.Println("Custom blob handling logic")
	// Output: Custom blob handling logic
}
