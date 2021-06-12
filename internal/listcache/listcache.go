// Package listcache defines a blob.Storage wrapper that caches results of list calls
// for short duration of time.
package listcache

import (
	"context"
	"encoding/json"
	"strings"
	"time"

	"github.com/kopia/kopia/internal/clock"
	"github.com/kopia/kopia/internal/gather"
	"github.com/kopia/kopia/internal/hmac"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/logging"
)

var log = logging.GetContextLoggerFunc("listcache")

type listCacheStorage struct {
	blob.Storage
	cacheStorage  blob.Storage
	cacheDuration time.Duration
	cacheTimeFunc func() time.Time
	hmacSecret    []byte
	prefixes      []blob.ID
}

type cachedList struct {
	ExpireAfter time.Time       `json:"expireAfter"`
	Blobs       []blob.Metadata `json:"blobs"`
}

func (s *listCacheStorage) saveListToCache(ctx context.Context, prefix blob.ID, cl *cachedList) {
	data, err := json.Marshal(cl)
	if err != nil {
		log(ctx).Debugf("unable to marshal list cache entry: %v", err)
		return
	}

	b := hmac.Append(data, s.hmacSecret)

	if err := s.cacheStorage.PutBlob(ctx, prefix, gather.FromSlice(b)); err != nil {
		log(ctx).Debugf("unable to persist list cache entry: %v", err)
	}
}

func (s *listCacheStorage) readBlobsFromCache(ctx context.Context, prefix blob.ID) *cachedList {
	cl := &cachedList{}

	data, err := s.cacheStorage.GetBlob(ctx, prefix, 0, -1)
	if err != nil {
		log(ctx).Debugf("error getting %v from cache: %v", prefix, err)
		return nil
	}

	data, err = hmac.VerifyAndStrip(data, s.hmacSecret)
	if err != nil {
		log(ctx).Debugf("warning: invalid list cache HMAC for %v, ignoring", prefix)
		return nil
	}

	if err := json.Unmarshal(data, &cl); err != nil {
		log(ctx).Debugf("warning: cant't unmarshal cached list results for %v, ignoring", prefix)
		return nil
	}

	if s.cacheTimeFunc().Before(cl.ExpireAfter) {
		return cl
	}

	// list cache expired
	return nil
}

// ListBlobs implements blob.Storage and caches previous list results for a given prefix.
func (s *listCacheStorage) ListBlobs(ctx context.Context, prefix blob.ID, cb func(blob.Metadata) error) error {
	if !s.isCachedPrefix(prefix) {
		// nolint:wrapcheck
		return s.Storage.ListBlobs(ctx, prefix, cb)
	}

	cached := s.readBlobsFromCache(ctx, prefix)
	if cached == nil {
		all, err := blob.ListAllBlobs(ctx, s.Storage, prefix)
		if err != nil {
			// nolint:wrapcheck
			return err
		}

		cached = &cachedList{
			ExpireAfter: s.cacheTimeFunc().Add(s.cacheDuration),
			Blobs:       all,
		}

		log(ctx).Debugf("saving %v to cache under %v", cached, prefix)
		s.saveListToCache(ctx, prefix, cached)
	}

	for _, v := range cached.Blobs {
		if err := cb(v); err != nil {
			return err
		}
	}

	return nil
}

// PutBlob implements blob.Storage and writes markers into local cache for all successful writes.
func (s *listCacheStorage) PutBlob(ctx context.Context, blobID blob.ID, data blob.Bytes) error {
	err := s.Storage.PutBlob(ctx, blobID, data)
	s.invalidateAfterUpdate(ctx, blobID)

	// nolint:wrapcheck
	return err
}

// DeleteBlob implements blob.Storage and writes markers into local cache for all successful deletes.
func (s *listCacheStorage) DeleteBlob(ctx context.Context, blobID blob.ID) error {
	err := s.Storage.DeleteBlob(ctx, blobID)
	s.invalidateAfterUpdate(ctx, blobID)

	// nolint:wrapcheck
	return err
}

func (s *listCacheStorage) isCachedPrefix(prefix blob.ID) bool {
	for _, p := range s.prefixes {
		if prefix == p {
			return true
		}
	}

	return false
}

func (s *listCacheStorage) invalidateAfterUpdate(ctx context.Context, blobID blob.ID) {
	for _, p := range s.prefixes {
		if strings.HasPrefix(string(blobID), string(p)) {
			if err := s.cacheStorage.DeleteBlob(ctx, p); err != nil {
				log(ctx).Debugf("unable to delete cached list: %v", err)
			}
		}
	}
}

// NewWrapper returns new wrapper that ensures list consistency with local writes for the given set of blob prefixes.
// It leverages the provided local cache storage to maintain markers keeping track of recently created and deleted blobs.
func NewWrapper(st, cacheStorage blob.Storage, prefixes []blob.ID, hmacSecret []byte, duration time.Duration) blob.Storage {
	if cacheStorage == nil {
		return st
	}

	return &listCacheStorage{
		Storage:       st,
		cacheStorage:  cacheStorage,
		prefixes:      prefixes,
		cacheTimeFunc: clock.Now,
		hmacSecret:    hmacSecret,
		cacheDuration: duration,
	}
}

var _ blob.Storage = (*listCacheStorage)(nil)
