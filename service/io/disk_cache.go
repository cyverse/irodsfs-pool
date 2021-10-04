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
	Key          string
	Group        string
	Size         int
	CreationTime time.Time
	FilePath     string
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
		Key:          key,
		Group:        group,
		Size:         len(data),
		CreationTime: time.Now(),
		FilePath:     filePath,
	}, nil
}

func (entry *DiskCacheEntry) GetKey() string {
	return entry.Key
}

func (entry *DiskCacheEntry) GetGroup() string {
	return entry.Group
}

func (entry *DiskCacheEntry) GetSize() int {
	return entry.Size
}

func (entry *DiskCacheEntry) GetCreationTime() time.Time {
	return entry.CreationTime
}

func (entry *DiskCacheEntry) GetData() ([]byte, error) {
	data, err := ioutil.ReadFile(entry.FilePath)
	if err != nil {
		return nil, err
	}

	return data, nil
}

func (entry *DiskCacheEntry) deleteDataFile() error {
	err := os.Remove(entry.FilePath)
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
	SizeCap  int64
	EntryCap int
	RootPath string
	Cache    *lrucache.Cache
	Groups   map[string]map[string]bool // key = group name, value = cache keys for a group
	Mutex    sync.Mutex
}

func NewDiskCache(sizeCap int64, rootPath string) (*DiskCache, error) {
	err := os.MkdirAll(rootPath, 0777)
	if err != nil {
		return nil, err
	}

	var maxCacheEntryNum int = int(sizeCap / int64(BlockSize))

	diskCache := &DiskCache{
		SizeCap:  sizeCap,
		EntryCap: maxCacheEntryNum,
		RootPath: rootPath,
		Cache:    nil,
		Groups:   map[string]map[string]bool{},
	}

	lruCache, err := lrucache.NewWithEvict(maxCacheEntryNum, diskCache.onEvicted)
	if err != nil {
		return nil, err
	}

	diskCache.Cache = lruCache
	return diskCache, nil
}

func (cache *DiskCache) Release() {
	logger := log.WithFields(log.Fields{
		"package":  "io",
		"struct":   "DiskCache",
		"function": "Release",
	})

	cache.Mutex.Lock()
	defer cache.Mutex.Unlock()

	// clear
	logger.Info("Deleting all data cache entries")
	cache.Groups = map[string]map[string]bool{}
	cache.Cache.Purge()

	logger.Infof("Deleting cache files and directory %s", cache.RootPath)
	os.RemoveAll(cache.RootPath)
}

func (cache *DiskCache) GetSizeCap() int64 {
	return cache.SizeCap
}

func (cache *DiskCache) GetRootPath() string {
	return cache.RootPath
}

func (cache *DiskCache) GetTotalEntries() int {
	cache.Mutex.Lock()
	defer cache.Mutex.Unlock()

	return cache.Cache.Len()
}

func (cache *DiskCache) GetTotalEntrySize() int64 {
	cache.Mutex.Lock()
	defer cache.Mutex.Unlock()

	return int64(cache.Cache.Len()) * int64(BlockSize)
}

func (cache *DiskCache) GetAvailableSize() int64 {
	cache.Mutex.Lock()
	defer cache.Mutex.Unlock()

	availableEntries := cache.EntryCap - cache.Cache.Len()
	return int64(availableEntries) * int64(BlockSize)
}

func (cache *DiskCache) DeleteAllEntries() {
	cache.Mutex.Lock()
	defer cache.Mutex.Unlock()

	// clear
	cache.Groups = map[string]map[string]bool{}

	cache.Cache.Purge()
}

func (cache *DiskCache) DeleteAllEntriesForGroup(group string) {
	cache.Mutex.Lock()
	defer cache.Mutex.Unlock()

	if cacheGroup, ok := cache.Groups[group]; ok {
		for key, _ := range cacheGroup {
			cache.Cache.Remove(key)
		}
	}
}

func (cache *DiskCache) GetEntryKeys() []string {
	cache.Mutex.Lock()
	defer cache.Mutex.Unlock()

	keys := []string{}
	for _, key := range cache.Cache.Keys() {
		if strkey, ok := key.(string); ok {
			keys = append(keys, strkey)
		}
	}
	return keys
}

func (cache *DiskCache) GetEntryKeysForGroup(group string) []string {
	cache.Mutex.Lock()
	defer cache.Mutex.Unlock()

	keys := []string{}
	if cacheGroup, ok := cache.Groups[group]; ok {
		for key, _ := range cacheGroup {
			if cache.Cache.Contains(key) {
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

	cache.Mutex.Lock()
	defer cache.Mutex.Unlock()

	logger.Infof("putting a new cache with a key %s, group %s", key, group)
	cache.Cache.Add(key, entry)

	if cacheGroup, ok := cache.Groups[group]; ok {
		cacheGroup[key] = true
	} else {
		cacheGroup = map[string]bool{}
		cacheGroup[key] = true
		cache.Groups[group] = cacheGroup
	}

	return entry, nil
}

func (cache *DiskCache) HasEntry(key string) bool {
	cache.Mutex.Lock()
	defer cache.Mutex.Unlock()

	return cache.Cache.Contains(key)
}

func (cache *DiskCache) GetEntry(key string) CacheEntry {
	logger := log.WithFields(log.Fields{
		"package":  "io",
		"struct":   "DiskCache",
		"function": "GetEntry",
	})

	cache.Mutex.Lock()
	defer cache.Mutex.Unlock()

	if entry, ok := cache.Cache.Get(key); ok {
		if cacheEntry, ok := entry.(*DiskCacheEntry); ok {
			logger.Infof("getting a cache with a key %s", key)
			return cacheEntry
		}
	}

	return nil
}

func (cache *DiskCache) DeleteEntry(key string) {
	cache.Mutex.Lock()
	defer cache.Mutex.Unlock()

	cache.Cache.Remove(key)
}

func (cache *DiskCache) onEvicted(key interface{}, entry interface{}) {
	if cacheEntry, ok := entry.(*DiskCacheEntry); ok {
		cacheEntry.deleteDataFile()

		if cacheGroup, ok := cache.Groups[cacheEntry.Group]; ok {
			delete(cacheGroup, cacheEntry.Key)

			if len(cacheGroup) == 0 {
				delete(cache.Groups, cacheEntry.Group)
			}
		}
	}
}
