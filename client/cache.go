package client

import (
	"time"

	irodsclient_fs "github.com/cyverse/go-irodsclient/fs"
	"github.com/cyverse/go-irodsclient/irods/types"
	gocache "github.com/patrickmn/go-cache"
)

// MetadataCache manages filesystem metadata caches
type MetadataCache struct {
	cacheTimeout     time.Duration
	cleanupTimeout   time.Duration
	entryCache       *gocache.Cache
	dirCache         *gocache.Cache
	aclCache         *gocache.Cache
	dirEntryAclCache *gocache.Cache
}

// NewMetadataCache creates a new MetadataCache
func NewMetadataCache(cacheTimeout time.Duration, cleanup time.Duration) *MetadataCache {
	entryCache := gocache.New(cacheTimeout, cleanup)
	dirCache := gocache.New(cacheTimeout, cleanup)
	aclCache := gocache.New(cacheTimeout, cleanup)
	dirEntryAclCache := gocache.New(cacheTimeout, cleanup)

	return &MetadataCache{
		cacheTimeout:     cacheTimeout,
		cleanupTimeout:   cleanup,
		entryCache:       entryCache,
		dirCache:         dirCache,
		aclCache:         aclCache,
		dirEntryAclCache: dirEntryAclCache,
	}
}

// AddDirCache adds a dir cache
func (cache *MetadataCache) AddDirCache(path string, entries []*irodsclient_fs.Entry) {
	cache.dirCache.Set(path, entries, 0)
}

// RemoveDirCache removes a dir cache
func (cache *MetadataCache) RemoveDirCache(path string) {
	cache.dirCache.Delete(path)
}

// GetDirCache retrives a dir cache
func (cache *MetadataCache) GetDirCache(path string) []*irodsclient_fs.Entry {
	data, exist := cache.dirCache.Get(path)
	if exist {
		if entries, ok := data.([]*irodsclient_fs.Entry); ok {
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
	cache.entryCache.Set(entry.Path, entry, 0)
}

// RemoveEntryCache removes an entry cache
func (cache *MetadataCache) RemoveEntryCache(path string) {
	cache.entryCache.Delete(path)
}

// GetEntryCache retrieves an entry cache
func (cache *MetadataCache) GetEntryCache(path string) *irodsclient_fs.Entry {
	entry, _ := cache.entryCache.Get(path)
	if fsentry, ok := entry.(*irodsclient_fs.Entry); ok {
		return fsentry
	}
	return nil
}

// ClearEntryCache clears all entry caches
func (cache *MetadataCache) ClearEntryCache() {
	cache.entryCache.Flush()
}

// AddACLsCache adds a ACLs cache
func (cache *MetadataCache) AddACLsCache(path string, accesses []*types.IRODSAccess) {
	cache.aclCache.Set(path, accesses, 0)
}

// AddACLsCache adds multiple ACLs caches
func (cache *MetadataCache) AddACLsCacheMulti(accesses []*types.IRODSAccess) {
	m := map[string][]*types.IRODSAccess{}

	for _, access := range accesses {
		if existingAccesses, ok := m[access.Path]; ok {
			// has it, add
			existingAccesses = append(existingAccesses, access)
			m[access.Path] = existingAccesses
		} else {
			// create it
			m[access.Path] = []*types.IRODSAccess{access}
		}
	}

	for path, access := range m {
		cache.aclCache.Set(path, access, 0)
	}
}

// RemoveACLsCache removes a ACLs cache
func (cache *MetadataCache) RemoveACLsCache(path string) {
	cache.aclCache.Delete(path)
}

// GetACLsCache retrives a ACLs cache
func (cache *MetadataCache) GetACLsCache(path string) []*types.IRODSAccess {
	data, exist := cache.aclCache.Get(path)
	if exist {
		if entries, ok := data.([]*types.IRODSAccess); ok {
			return entries
		}
	}
	return nil
}

// ClearACLsCache clears all ACLs caches
func (cache *MetadataCache) ClearACLsCache() {
	cache.aclCache.Flush()
}

// AddDirEntryACLsCache adds a ACLs cache
func (cache *MetadataCache) AddDirEntryACLsCache(path string, accesses []*types.IRODSAccess) {
	cache.dirEntryAclCache.Set(path, accesses, 0)
}

// RemoveDirEntryACLsCache removes a ACLs cache
func (cache *MetadataCache) RemoveDirEntryACLsCache(path string) {
	cache.dirEntryAclCache.Delete(path)
}

// GetDirEntryACLsCache retrives a ACLs cache
func (cache *MetadataCache) GetDirEntryACLsCache(path string) []*types.IRODSAccess {
	data, exist := cache.dirEntryAclCache.Get(path)
	if exist {
		if entries, ok := data.([]*types.IRODSAccess); ok {
			return entries
		}
	}
	return nil
}

// ClearDirEntryACLsCache clears all ACLs caches
func (cache *MetadataCache) ClearDirEntryACLsCache() {
	cache.dirEntryAclCache.Flush()
}
