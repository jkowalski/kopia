package cache

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/repo/blob"
)

const (
	packFilePrefix = "pack-"
	indexFileName  = "index.v2"
	packSize       = 20 * 1024 * 1024 // 20MB per pack file
)

// PackedStorage implements Storage interface using pack files and index v2.
type PackedStorage struct {
	directory string
	mu        sync.RWMutex

	currentPack   *os.File
	currentOffset int64
	currentPackID blob.ID
	index         map[blob.ID]packEntry
	touchTimes    map[blob.ID]time.Time
}

type packEntry struct {
	packID   blob.ID
	offset   int64
	length   int64
	modified time.Time
}

// indexEntry represents an entry in the index v2 format.
type indexEntry struct {
	blobID    blob.ID
	packID    blob.ID
	offset    int64
	length    int64
	timestamp time.Time
}

// NewPackedStorage creates a new packed storage in the given directory.
func NewPackedStorage(ctx context.Context, directory string) (*PackedStorage, error) {
	if err := os.MkdirAll(directory, 0o700); err != nil {
		return nil, errors.Wrap(err, "error creating directory")
	}

	ps := &PackedStorage{
		directory:  directory,
		index:      make(map[blob.ID]packEntry),
		touchTimes: make(map[blob.ID]time.Time),
	}

	if err := ps.loadExistingPacks(ctx); err != nil {
		return nil, errors.Wrap(err, "error loading existing packs")
	}

	return ps, nil
}

func (ps *PackedStorage) loadExistingPacks(ctx context.Context) error {
	entries, err := os.ReadDir(ps.directory)
	if err != nil {
		return errors.Wrap(err, "error reading directory")
	}

	for _, e := range entries {
		if e.IsDir() {
			continue
		}

		if e.Name() == indexFileName {
			if err := ps.loadIndex(ctx); err != nil {
				return errors.Wrap(err, "error loading index")
			}
			continue
		}

		if len(e.Name()) < len(packFilePrefix) || e.Name()[:len(packFilePrefix)] != packFilePrefix {
			continue
		}

		packID := blob.ID(e.Name()[len(packFilePrefix):])
		info, err := e.Info()
		if err != nil {
			continue
		}

		ps.index[packID] = packEntry{
			packID:   packID,
			offset:   0,
			length:   info.Size(),
			modified: info.ModTime(),
		}
	}

	return nil
}

func (ps *PackedStorage) loadIndex(ctx context.Context) error {
	f, err := os.Open(filepath.Join(ps.directory, indexFileName))
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return errors.Wrap(err, "error opening index file")
	}
	defer f.Close()

	var entries []indexEntry
	dec := json.NewDecoder(f)
	if err := dec.Decode(&entries); err != nil {
		return errors.Wrap(err, "error decoding index")
	}

	for _, e := range entries {
		ps.index[e.blobID] = packEntry{
			packID:   e.packID,
			offset:   e.offset,
			length:   e.length,
			modified: e.timestamp,
		}
	}

	return nil
}

func (ps *PackedStorage) saveIndex(ctx context.Context) error {
	var entries []indexEntry

	for blobID, entry := range ps.index {
		entries = append(entries, indexEntry{
			blobID:    blobID,
			packID:    entry.packID,
			offset:    entry.offset,
			length:    entry.length,
			timestamp: entry.modified,
		})
	}

	sort.Slice(entries, func(i, j int) bool {
		return string(entries[i].blobID) < string(entries[j].blobID)
	})

	f, err := os.Create(filepath.Join(ps.directory, indexFileName+".tmp"))
	if err != nil {
		return errors.Wrap(err, "error creating temporary index file")
	}
	defer f.Close()

	enc := json.NewEncoder(f)
	if err := enc.Encode(entries); err != nil {
		return errors.Wrap(err, "error encoding index")
	}

	if err := f.Close(); err != nil {
		return errors.Wrap(err, "error closing temporary index file")
	}

	if err := os.Rename(filepath.Join(ps.directory, indexFileName+".tmp"), filepath.Join(ps.directory, indexFileName)); err != nil {
		return errors.Wrap(err, "error renaming temporary index file")
	}

	return nil
}

func (ps *PackedStorage) GetBlob(ctx context.Context, id blob.ID, offset, length int64, output blob.OutputBuffer) error {
	ps.mu.RLock()
	entry, ok := ps.index[id]
	ps.mu.RUnlock()

	if !ok {
		return blob.ErrBlobNotFound
	}

	f, err := os.Open(ps.packPath(entry.packID))
	if err != nil {
		return errors.Wrap(err, "error opening pack file")
	}
	defer f.Close()

	if length < 0 {
		length = entry.length - offset
	}

	if offset < 0 || offset+length > entry.length {
		return errors.Errorf("invalid offset/length")
	}

	_, err = f.Seek(entry.offset+offset, io.SeekStart)
	if err != nil {
		return errors.Wrap(err, "error seeking")
	}

	_, err = io.CopyN(output, f, length)
	return errors.Wrap(err, "error reading data")
}

func (ps *PackedStorage) PutBlob(ctx context.Context, id blob.ID, data blob.Bytes, opts blob.PutOptions) error {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	if ps.currentPack == nil || ps.currentOffset+int64(data.Length()) > packSize {
		if err := ps.rotatePackFile(); err != nil {
			return errors.Wrap(err, "error rotating pack file")
		}
	}

	n, err := data.WriteTo(ps.currentPack)
	if err != nil {
		return errors.Wrap(err, "error writing data")
	}

	now := time.Now()
	ps.index[id] = packEntry{
		packID:   ps.currentPackID,
		offset:   ps.currentOffset,
		length:   n,
		modified: now,
	}
	ps.touchTimes[id] = now
	ps.currentOffset += n

	return nil
}

func (ps *PackedStorage) DeleteBlob(ctx context.Context, id blob.ID) error {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	delete(ps.index, id)
	delete(ps.touchTimes, id)
	return nil
}

func (ps *PackedStorage) ListBlobs(ctx context.Context, prefix blob.ID, cb func(blob.Metadata) error) error {
	ps.mu.RLock()
	defer ps.mu.RUnlock()

	for id, entry := range ps.index {
		if prefix != "" && len(string(prefix)) > len(string(id)) || !strings.HasPrefix(string(id), string(prefix)) {
			continue
		}

		if err := cb(blob.Metadata{
			BlobID:    id,
			Length:    entry.length,
			Timestamp: entry.modified,
		}); err != nil {
			return errors.Wrap(err, "error in callback")
		}
	}

	return nil
}

func (ps *PackedStorage) TouchBlob(ctx context.Context, id blob.ID, threshold time.Duration) (time.Time, error) {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	lastTime, ok := ps.touchTimes[id]
	if !ok {
		return time.Time{}, blob.ErrBlobNotFound
	}

	if time.Since(lastTime) < threshold {
		return lastTime, nil
	}

	now := time.Now()
	ps.touchTimes[id] = now
	return now, nil
}

func (ps *PackedStorage) Close(ctx context.Context) error {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	if ps.currentPack != nil {
		if err := ps.currentPack.Close(); err != nil {
			return errors.Wrap(err, "error closing current pack")
		}
		ps.currentPack = nil
	}

	return ps.saveIndex(ctx)
}

func (ps *PackedStorage) rotatePackFile() error {
	if ps.currentPack != nil {
		if err := ps.currentPack.Close(); err != nil {
			return errors.Wrap(err, "error closing current pack")
		}
	}

	newPackID := blob.ID(fmt.Sprintf("%v%v", time.Now().UnixNano(), ps.currentOffset))
	f, err := os.OpenFile(ps.packPath(newPackID), os.O_CREATE|os.O_WRONLY|os.O_EXCL, 0o600)
	if err != nil {
		return errors.Wrap(err, "error creating new pack")
	}

	ps.currentPack = f
	ps.currentPackID = newPackID
	ps.currentOffset = 0

	return nil
}

func (ps *PackedStorage) packPath(id blob.ID) string {
	return filepath.Join(ps.directory, packFilePrefix+string(id))
}

func (ps *PackedStorage) ConnectionInfo() blob.ConnectionInfo {
	return blob.ConnectionInfo{
		Type:   "PACKED-CACHE",
		Config: map[string]string{"directory": ps.directory},
	}
}

func (ps *PackedStorage) DisplayName() string {
	return fmt.Sprintf("Packed Cache: %v", ps.directory)
}

func (ps *PackedStorage) GetCapacity(ctx context.Context) (blob.Capacity, error) {
	return blob.Capacity{}, nil
}

func (ps *PackedStorage) IsReadOnly() bool {
	return false
}

func (ps *PackedStorage) FlushCaches(ctx context.Context) error {
	return nil
}

func (ps *PackedStorage) GetMetadata(ctx context.Context, id blob.ID) (blob.Metadata, error) {
	ps.mu.RLock()
	entry, ok := ps.index[id]
	ps.mu.RUnlock()

	if !ok {
		return blob.Metadata{}, blob.ErrBlobNotFound
	}

	return blob.Metadata{
		BlobID:    id,
		Length:    entry.length,
		Timestamp: entry.modified,
	}, nil
}

func (ps *PackedStorage) ExtendBlobRetention(ctx context.Context, id blob.ID, opts blob.ExtendOptions) error {
	return blob.ErrUnsupportedObjectLock
}
