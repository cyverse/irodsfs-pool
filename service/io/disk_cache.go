package io

import (
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"sync"
	"time"

	"github.com/cyverse/irodsfs-pool/utils"
	lrucache "github.com/hashicorp/golang-lru"
	log "github.com/sirupsen/logrus"
)

type DiskCacheEntry struct {
	key          string
	group        string
	size         int
	creationTime time.Time
	filePath     string
}

func NewDiskCacheEntry(cache *DiskCache, key string, group string, data []byte) (*DiskCacheEntry, error) {
	logger := log.WithFields(log.Fields{
		"package":  "io",
		"struct":   "DiskCache",
		"function": "Release",
	})

	// write to disk
	hash := utils.MakeHash(key)
	filePath := utils.JoinPath(cache.GetRootPath(), hash)

	logger.Infof("Writing data cache to %s", filePath)
	err := ioutil.WriteFile(filePath, data, 0666)
	if err != nil {
		logger.Error(err)
		return nil, err
	}

	return &DiskCacheEntry{
		key:          key,
		group:        group,
		size:         len(data),
		creationTime: time.Now(),
		filePath:     filePath,
	}, nil
}

func (entry *DiskCacheEntry) GetKey() string {
	return entry.key
}

func (entry *DiskCacheEntry) GetGroup() string {
	return entry.group
}

func (entry *DiskCacheEntry) GetSize() int {
	return entry.size
}

func (entry *DiskCacheEntry) GetCreationTime() time.Time {
	return entry.creationTime
}

func (entry *DiskCacheEntry) GetData() ([]byte, error) {
	data, err := ioutil.ReadFile(entry.filePath)
	if err != nil {
		return nil, err
	}

	return data, nil
}

func (entry *DiskCacheEntry) deleteDataFile() error {
	err := os.Remove(entry.filePath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}
	return nil
}

// DiskCache
type DiskCache struct {
	sizeCap        int64
	entryNumberCap int
	rootPath       string
	cache          *lrucache.Cache
	groups         map[string]map[string]bool // key = group name, value = cache keys for a group
	mutex          sync.Mutex
}

func NewDiskCache(sizeCap int64, rootPath string) (*DiskCache, error) {
	err := os.MkdirAll(rootPath, 0777)
	if err != nil {
		return nil, err
	}

	var maxCacheEntryNum int = int(sizeCap / int64(BlockSize))

	diskCache := &DiskCache{
		sizeCap:        sizeCap,
		entryNumberCap: maxCacheEntryNum,
		rootPath:       rootPath,
		cache:          nil,
		groups:         map[string]map[string]bool{},
	}

	lruCache, err := lrucache.NewWithEvict(maxCacheEntryNum, diskCache.onEvicted)
	if err != nil {
		return nil, err
	}

	diskCache.cache = lruCache
	return diskCache, nil
}

func (cache *DiskCache) Release() {
	logger := log.WithFields(log.Fields{
		"package":  "io",
		"struct":   "DiskCache",
		"function": "Release",
	})

	cache.mutex.Lock()
	defer cache.mutex.Unlock()

	// clear
	logger.Info("Deleting all data cache entries")
	cache.groups = map[string]map[string]bool{}
	cache.cache.Purge()

	logger.Infof("Deleting cache files and directory %s", cache.rootPath)
	os.RemoveAll(cache.rootPath)
}

func (cache *DiskCache) GetSizeCap() int64 {
	return cache.sizeCap
}

func (cache *DiskCache) GetRootPath() string {
	return cache.rootPath
}

func (cache *DiskCache) GetTotalEntries() int {
	cache.mutex.Lock()
	defer cache.mutex.Unlock()

	return cache.cache.Len()
}

func (cache *DiskCache) GetTotalEntrySize() int64 {
	cache.mutex.Lock()
	defer cache.mutex.Unlock()

	return int64(cache.cache.Len()) * int64(BlockSize)
}

func (cache *DiskCache) GetAvailableSize() int64 {
	cache.mutex.Lock()
	defer cache.mutex.Unlock()

	availableEntries := cache.entryNumberCap - cache.cache.Len()
	return int64(availableEntries) * int64(BlockSize)
}

func (cache *DiskCache) DeleteAllEntries() {
	cache.mutex.Lock()
	defer cache.mutex.Unlock()

	// clear
	cache.groups = map[string]map[string]bool{}

	cache.cache.Purge()
}

func (cache *DiskCache) DeleteAllEntriesForGroup(group string) {
	cache.mutex.Lock()
	defer cache.mutex.Unlock()

	if cacheGroup, ok := cache.groups[group]; ok {
		for key := range cacheGroup {
			cache.cache.Remove(key)
		}
	}
}

func (cache *DiskCache) GetEntryKeys() []string {
	cache.mutex.Lock()
	defer cache.mutex.Unlock()

	keys := []string{}
	for _, key := range cache.cache.Keys() {
		if strkey, ok := key.(string); ok {
			keys = append(keys, strkey)
		}
	}
	return keys
}

func (cache *DiskCache) GetEntryKeysForGroup(group string) []string {
	cache.mutex.Lock()
	defer cache.mutex.Unlock()

	keys := []string{}
	if cacheGroup, ok := cache.groups[group]; ok {
		for key := range cacheGroup {
			if cache.cache.Contains(key) {
				keys = append(keys, key)
			}
		}
	}
	return keys
}

func (cache *DiskCache) CreateEntry(key string, group string, data []byte) (CacheEntry, error) {
	logger := log.WithFields(log.Fields{
		"package":  "io",
		"struct":   "DiskCache",
		"function": "CreateEntry",
	})

	if BlockSize < len(data) {
		return nil, fmt.Errorf("requested data %d is larger than block size %d", len(data), BlockSize)
	}

	entry, err := NewDiskCacheEntry(cache, key, group, data)
	if err != nil {
		return nil, err
	}

	cache.mutex.Lock()
	defer cache.mutex.Unlock()

	logger.Infof("putting a new cache with a key %s, group %s", key, group)
	cache.cache.Add(key, entry)

	if cacheGroup, ok := cache.groups[group]; ok {
		cacheGroup[key] = true
	} else {
		cacheGroup = map[string]bool{}
		cacheGroup[key] = true
		cache.groups[group] = cacheGroup
	}

	return entry, nil
}

func (cache *DiskCache) HasEntry(key string) bool {
	cache.mutex.Lock()
	defer cache.mutex.Unlock()

	return cache.cache.Contains(key)
}

func (cache *DiskCache) GetEntry(key string) CacheEntry {
	logger := log.WithFields(log.Fields{
		"package":  "io",
		"struct":   "DiskCache",
		"function": "GetEntry",
	})

	cache.mutex.Lock()
	defer cache.mutex.Unlock()

	if entry, ok := cache.cache.Get(key); ok {
		if cacheEntry, ok := entry.(*DiskCacheEntry); ok {
			logger.Infof("getting a cache with a key %s", key)
			return cacheEntry
		}
	}

	return nil
}

func (cache *DiskCache) DeleteEntry(key string) {
	cache.mutex.Lock()
	defer cache.mutex.Unlock()

	cache.cache.Remove(key)
}

func (cache *DiskCache) onEvicted(key interface{}, entry interface{}) {
	if cacheEntry, ok := entry.(*DiskCacheEntry); ok {
		cacheEntry.deleteDataFile()

		if cacheGroup, ok := cache.groups[cacheEntry.group]; ok {
			delete(cacheGroup, cacheEntry.key)

			if len(cacheGroup) == 0 {
				delete(cache.groups, cacheEntry.group)
			}
		}
	}
}
