package cached_test

import (
	"fmt"
	"strings"

	"github.com/kopia/kopia/internal/blobtesting"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/blob/cached"
)

// ExampleMigrationFromOriginal shows how to recreate the original behavior
// (ignore blobs starting with 'p' and 'q', cache everything else)
// using the new generic API.
func Example_recreateOriginalBehavior() {
	baseStorage := blobtesting.NewMapStorage(blobtesting.DataMap{}, nil, nil)

	// Original behavior: ignore 'p' and 'q' prefixes, cache everything else
	// Method 1: Using the convenience function
	actionFunc1 := cached.IgnorePrefixesActionFunc([]string{"p", "q"})
	cachedStorage1 := cached.NewWrapper(baseStorage, actionFunc1)

	// Method 2: Using a custom function that mimics IgnorePrefixesActionFunc
	actionFunc2 := func(blobID blob.ID) cached.BlobAction {
		str := string(blobID)
		for _, prefix := range []string{"p", "q"} {
			if strings.HasPrefix(str, prefix) {
				return cached.BlobActionIgnore
			}
		}

		return cached.BlobActionCache
	}
	cachedStorage2 := cached.NewWrapper(baseStorage, actionFunc2)

	// Method 3: Custom function (most explicit)
	actionFunc3 := func(blobID blob.ID) cached.BlobAction {
		str := string(blobID)
		if strings.HasPrefix(str, "p") || strings.HasPrefix(str, "q") {
			return cached.BlobActionIgnore
		}

		return cached.BlobActionCache
	}
	cachedStorage3 := cached.NewWrapper(baseStorage, actionFunc3)

	// All three approaches provide the same behavior
	fmt.Printf("Created storage 1: %s\n", cachedStorage1.DisplayName())
	fmt.Printf("Created storage 2: %s\n", cachedStorage2.DisplayName())
	fmt.Printf("Created storage 3: %s\n", cachedStorage3.DisplayName())

	// Output:
	// Created storage 1: Cached: Map
	// Created storage 2: Cached: Map
	// Created storage 3: Cached: Map
}

// OriginalBehaviorActionFunc demonstrates how to recreate the original hardcoded behavior
// using the new generic API. This ignores blobs starting with 'p' or 'q' and caches all others.
func OriginalBehaviorActionFunc() cached.BlobActionFunc {
	return func(blobID blob.ID) cached.BlobAction {
		str := string(blobID)

		if strings.HasPrefix(str, "p") || strings.HasPrefix(str, "q") {
			return cached.BlobActionIgnore
		}

		return cached.BlobActionCache
	}
}

// CustomBehaviorActionFunc demonstrates creating a custom action function
// that handles different blob types differently.
func CustomBehaviorActionFunc() cached.BlobActionFunc {
	return func(blobID blob.ID) cached.BlobAction {
		str := string(blobID)

		// Ignore temporary files
		if strings.Contains(str, "temp") {
			return cached.BlobActionIgnore
		}

		return cached.BlobActionCache
	}
}
