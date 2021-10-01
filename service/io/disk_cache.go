package io

import (
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"time"

	"github.com/cyverse/irodsfs-pool/utils"
	lrucache "github.com/hashicorp/golang-lru"
	log "github.com/sirupsen/logrus"
)

type DiskCacheEntry struct {
	Key          string
	Size         int
	CreationTime time.Time
	FilePath     string
}

func NewDiskCacheEntry(cache *DiskCache, key string, data []byte) (*DiskCacheEntry, error) {
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
		Size:         len(data),
		CreationTime: time.Now(),
		FilePath:     filePath,
	}, nil
}

func (entry *DiskCacheEntry) GetKey() string {
	return entry.Key
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

	logger.Info("Deleting all data cache entries")
	cache.DeleteAllEntries()

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
	return cache.Cache.Len()
}

func (cache *DiskCache) GetTotalEntrySize() int64 {
	return int64(cache.Cache.Len()) * int64(BlockSize)
}

func (cache *DiskCache) GetAvailableSize() int64 {
	availableEntries := cache.EntryCap - cache.Cache.Len()
	return int64(availableEntries) * int64(BlockSize)
}

func (cache *DiskCache) DeleteAllEntries() {
	cache.Cache.Purge()
}

func (cache *DiskCache) GetEntryKeys() []string {
	keys := []string{}
	for _, key := range cache.Cache.Keys() {
		if strkey, ok := key.(string); ok {
			keys = append(keys, strkey)
		}
	}
	return keys
}

func (cache *DiskCache) CreateEntry(key string, data []byte) (CacheEntry, error) {
	logger := log.WithFields(log.Fields{
		"package":  "io",
		"struct":   "DiskCache",
		"function": "CreateEntry",
	})

	if BlockSize < len(data) {
		return nil, fmt.Errorf("requested data %d is larger than block size %d", len(data), BlockSize)
	}

	entry, err := NewDiskCacheEntry(cache, key, data)
	if err != nil {
		return nil, err
	}

	logger.Infof("putting a new cache with a key %s", key)
	cache.Cache.Add(key, entry)

	return entry, nil
}

func (cache *DiskCache) HasEntry(key string) bool {
	return cache.Cache.Contains(key)
}

func (cache *DiskCache) GetEntry(key string) CacheEntry {
	logger := log.WithFields(log.Fields{
		"package":  "io",
		"struct":   "DiskCache",
		"function": "GetEntry",
	})

	if entry, ok := cache.Cache.Get(key); ok {
		if cacheEntry, ok := entry.(*DiskCacheEntry); ok {
			logger.Infof("getting a cache with a key %s", key)
			return cacheEntry
		}
	}

	return nil
}

func (cache *DiskCache) DeleteEntry(key string) {
	cache.Cache.Remove(key)
}

func (cache *DiskCache) onEvicted(key interface{}, entry interface{}) {
	if cacheEntry, ok := entry.(*DiskCacheEntry); ok {
		cacheEntry.deleteDataFile()
	}
}
