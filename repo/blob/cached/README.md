# Cached Blob Storage Provider

The `cached` package implements a wrapper around any blob storage that adds configurable memory caching and filtering capabilities via pluggable blob action functions.

## Features

- **Configurable Actions**: Use pluggable functions to decide what to do with each blob
- **Three Actions**: Ignore (silently drop), Cache (store in memory + base storage), or PassThrough (base storage only)
- **Memory Caching**: Fast in-memory reads for cached blobs
- **Thread Safe**: Concurrent access supported
- **Convenience Functions**: Common patterns provided out-of-the-box

## Blob Actions

- **`BlobActionIgnore`**: Silently ignores the blob (not stored anywhere)
- **`BlobActionCache`**: Caches the blob in memory AND writes to base storage  
- **`BlobActionPassThrough`**: Writes to base storage only (no caching)

## Usage

### Basic Usage with Custom Action Function

```go
import (
    "github.com/kopia/kopia/repo/blob/cached"
    "github.com/kopia/kopia/repo/blob/filesystem" // or any other storage
)

// Create your base storage
baseStorage, err := filesystem.New(ctx, &filesystem.Options{
    Path: "/path/to/storage",
}, false)
if err != nil {
    return err
}

// Define your blob action logic
actionFunc := func(blobID blob.ID) cached.BlobAction {
    str := string(blobID)
    
    if strings.HasPrefix(str, "temp") {
        return cached.BlobActionIgnore
    }
    
    if strings.HasPrefix(str, "important") {
        return cached.BlobActionCache
    }
    
    return cached.BlobActionPassThrough
}

// Wrap it with caching capabilities
cachedStorage := cached.NewWrapper(baseStorage, actionFunc)

// Use cachedStorage as you would any blob.Storage
```

### Using Convenience Functions

```go
// Ignore blobs with specific prefixes, cache everything else
actionFunc := cached.IgnorePrefixesActionFunc([]string{"temp", "debug"})
cachedStorage := cached.NewWrapper(baseStorage, actionFunc)

// Complex prefix-based rules
actionFunc := cached.PrefixBasedActionFunc(
    []string{"temp", "debug"}, // ignore these prefixes
    []string{"cache", "fast"},  // cache these prefixes
    // everything else passes through
)
cachedStorage := cached.NewWrapper(baseStorage, actionFunc)

// Cache everything
actionFunc := cached.CacheAllActionFunc()
cachedStorage := cached.NewWrapper(baseStorage, actionFunc)

// Pass through everything (no caching)
actionFunc := cached.PassThroughAllActionFunc()
cachedStorage := cached.NewWrapper(baseStorage, actionFunc)
```

## Behavior

### Write Operations

The action function determines what happens to each blob:
- **Ignore**: No error returned, but blob is not stored anywhere
- **Cache**: Blob is cached in memory AND written to base storage
- **PassThrough**: Blob is written to base storage only (not cached)

### Read Operations

- First checks the memory cache
- If found in cache, serves from memory (very fast)
- If not found in cache, falls back to the underlying storage
- Range reads are supported from the cache

### List Operations

- Returns blobs from both the cache and the underlying storage
- Automatically deduplicates entries that appear in both locations

### Delete Operations

- Removes the blob from both the cache and the underlying storage

## Available Convenience Functions

- **`IgnorePrefixesActionFunc(ignorePrefixes)`**: Ignores specified prefixes, caches everything else
- **`PrefixBasedActionFunc(ignorePrefixes, cachePrefixes)`**: Complex prefix-based rules
- **`CacheAllActionFunc()`**: Caches all blobs
- **`PassThroughAllActionFunc()`**: Passes through all blobs without caching

## Thread Safety

The cached storage provider is thread-safe and can be used concurrently from multiple goroutines.

## Memory Usage

The cache stores the full blob data in memory, so be mindful of memory usage when working with large blobs or many cached blobs. 