package client

import (
	"time"

	irodsclient_fs "github.com/cyverse/go-irodsclient/fs"
	gocache "github.com/patrickmn/go-cache"
)

// MetadataCache manages filesystem metadata caches
type MetadataCache struct {
	cacheTimeout   time.Duration
	cleanupTimeout time.Duration
	entryCache     *gocache.Cache
	dirCache       *gocache.Cache
}

// NewMetadataCache creates a new MetadataCache
func NewMetadataCache(cacheTimeout time.Duration, cleanup time.Duration) *MetadataCache {
	entryCache := gocache.New(cacheTimeout, cleanup)
	dirCache := gocache.New(cacheTimeout, cleanup)

	return &MetadataCache{
		cacheTimeout:   cacheTimeout,
		cleanupTimeout: cleanup,
		entryCache:     entryCache,
		dirCache:       dirCache,
	}
}

// AddDirCache adds a dir cache
func (cache *MetadataCache) AddDirCache(path string, entries []string) {
	cache.dirCache.Set(path, entries, 0)
}

// RemoveDirCache removes a dir cache
func (cache *MetadataCache) RemoveDirCache(path string) {
	cache.dirCache.Delete(path)
}

// GetDirCache retrives a dir cache
func (cache *MetadataCache) GetDirCache(path string) []string {
	data, exist := cache.dirCache.Get(path)
	if exist {
		if entries, ok := data.([]string); ok {
			return entries
		}
	}
	return nil
}

// ClearDirCache clears all dir caches
func (cache *MetadataCache) ClearDirCache() {
	cache.dirCache.Flush()
}

// AddEntryCache adds an entry cache
func (cache *MetadataCache) AddEntryCache(entry *irodsclient_fs.Entry) {
	entryCopy := *entry
	cache.entryCache.Set(entry.Path, &entryCopy, 0)
}

// RemoveEntryCache removes an entry cache
func (cache *MetadataCache) RemoveEntryCache(path string) {
	cache.entryCache.Delete(path)
}

// GetEntryCache retrieves an entry cache
func (cache *MetadataCache) GetEntryCache(path string) *irodsclient_fs.Entry {
	entry, _ := cache.entryCache.Get(path)
	if fsentry, ok := entry.(*irodsclient_fs.Entry); ok {
		entryCopy := *fsentry
		return &entryCopy
	}
	return nil
}

// ClearEntryCache clears all entry caches
func (cache *MetadataCache) ClearEntryCache() {
	cache.entryCache.Flush()
}
