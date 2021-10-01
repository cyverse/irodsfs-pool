package io

import (
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"sync"
	"time"

	"github.com/cyverse/irodsfs-pool/utils"
	heap "github.com/emirpasic/gods/trees/binaryheap"
	gocache "github.com/patrickmn/go-cache"
	log "github.com/sirupsen/logrus"
)

type DiskCacheEntry struct {
	Key            string
	Size           int
	CreationTime   time.Time
	LastAccessTime time.Time
	FilePath       string
}

func NewDiskCacheEntry(cache *DiskCache, key string, data []byte) (*DiskCacheEntry, error) {
	logger := log.WithFields(log.Fields{
		"package":  "io",
		"struct":   "DiskCache",
		"function": "Release",
	})

	// write to disk
	filePath := utils.JoinPath(cache.GetRootPath(), key)

	logger.Infof("Writing data cache to %s", filePath)
	err := ioutil.WriteFile(filePath, data, 0666)
	if err != nil {
		logger.Error(err)
		return nil, err
	}

	return &DiskCacheEntry{
		Key:            key,
		Size:           len(data),
		CreationTime:   time.Now(),
		LastAccessTime: time.Now(),
		FilePath:       filePath,
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

func (entry *DiskCacheEntry) GetLastAccessTime() time.Time {
	return entry.LastAccessTime
}

func (entry *DiskCacheEntry) GetData() ([]byte, error) {
	data, err := ioutil.ReadFile(entry.FilePath)
	if err != nil {
		return nil, err
	}

	entry.LastAccessTime = time.Now()

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
	RootPath string

	Cache       *gocache.Cache
	CurrentSize int64
	Mutex       sync.Mutex
}

func NewDiskCache(sizeCap int64, rootPath string, cacheTimeout time.Duration, cleanup time.Duration) (*DiskCache, error) {
	cache := gocache.New(cacheTimeout, cleanup)

	err := os.MkdirAll(rootPath, 0777)
	if err != nil {
		return nil, err
	}

	diskCache := &DiskCache{
		SizeCap:  sizeCap,
		RootPath: rootPath,

		Cache:       cache,
		CurrentSize: 0,
	}

	cache.OnEvicted(diskCache.onEvicted)

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

	cache.Mutex.Lock()
	defer cache.Mutex.Unlock()

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

	return cache.Cache.ItemCount()
}

func (cache *DiskCache) GetTotalEntrySize() int64 {
	cache.Mutex.Lock()
	defer cache.Mutex.Unlock()

	return cache.CurrentSize
}

func (cache *DiskCache) GetAvailableSize() int64 {
	cache.Mutex.Lock()
	defer cache.Mutex.Unlock()

	return cache.SizeCap - cache.CurrentSize
}

func (cache *DiskCache) DeleteAllEntries() {
	cache.Mutex.Lock()
	defer cache.Mutex.Unlock()

	cache.Cache.Flush()

	cache.CurrentSize = 0
}

func (cache *DiskCache) GetEntryKeys() []string {
	cache.Mutex.Lock()
	defer cache.Mutex.Unlock()

	keys := []string{}
	for key, _ := range cache.Cache.Items() {
		keys = append(keys, key)
	}
	return keys
}

func (cache *DiskCache) CreateEntry(key string, data []byte) (CacheEntry, error) {
	logger := log.WithFields(log.Fields{
		"package":  "io",
		"struct":   "DiskCache",
		"function": "CreateEntry",
	})

	cache.Mutex.Lock()
	defer cache.Mutex.Unlock()

	if cache.SizeCap < int64(len(data)) {
		return nil, fmt.Errorf("requested data %d is larger than size cap %d", len(data), cache.SizeCap)
	}

	avail := cache.SizeCap - cache.CurrentSize

	if avail < int64(len(data)) {
		// no space to write -- flush expired
		cache.Cache.DeleteExpired()
		avail = cache.SizeCap - cache.CurrentSize
	}

	if avail < int64(len(data)) {
		// kick oldest
		cacheEntryComparator := func(a interface{}, b interface{}) int {
			aAsserted, ok := a.(*DiskCacheEntry)
			if !ok {
				return 0
			}

			bAsserted, ok := b.(*DiskCacheEntry)
			if !ok {
				return 0
			}

			switch {
			case aAsserted.LastAccessTime.After(bAsserted.LastAccessTime):
				return -1
			case aAsserted.LastAccessTime.Before(bAsserted.LastAccessTime):
				return 1
			default:
				return 0
			}
		}

		deleteHeap := heap.NewWith(cacheEntryComparator)

		for _, cacheItem := range cache.Cache.Items() {
			if cacheEntry, ok := cacheItem.Object.(*DiskCacheEntry); ok {
				deleteHeap.Push(cacheEntry)
			}
		}

		for avail < int64(len(data)) && deleteHeap.Size() > 0 {
			if deleteEntry, ok := deleteHeap.Pop(); ok {
				if cacheEntry, ok := deleteEntry.(*DiskCacheEntry); ok {
					cache.Cache.Delete(cacheEntry.Key)

					cache.CurrentSize -= int64(cacheEntry.Size)
					if cache.CurrentSize < 0 {
						cache.CurrentSize = 0
					}
				}
			} else {
				return nil, fmt.Errorf("failed to pop entry from heap")
			}
		}

		if avail < int64(len(data)) {
			return nil, fmt.Errorf("failed to make space for new cache data in size %d", len(data))
		}
	}

	hash := utils.MakeHash(key)
	logger.Infof("creating a new cache key %s", hash)
	entry, err := NewDiskCacheEntry(cache, hash, data)
	if err != nil {
		return nil, err
	}

	logger.Infof("putting a new cache key %s", entry.Key)
	cache.Cache.Set(entry.Key, entry, 0)

	cache.CurrentSize += int64(len(data))
	return entry, nil
}

func (cache *DiskCache) HasEntry(key string) bool {
	cache.Mutex.Lock()
	defer cache.Mutex.Unlock()

	hash := utils.MakeHash(key)
	if entry, ok := cache.Cache.Get(hash); ok {
		_, ok = entry.(*DiskCacheEntry)
		return ok
	}
	return false
}

func (cache *DiskCache) GetEntry(key string) CacheEntry {
	logger := log.WithFields(log.Fields{
		"package":  "io",
		"struct":   "DiskCache",
		"function": "GetEntry",
	})

	cache.Mutex.Lock()
	defer cache.Mutex.Unlock()

	hash := utils.MakeHash(key)
	if entry, ok := cache.Cache.Get(hash); ok {
		if cacheEntry, ok := entry.(*DiskCacheEntry); ok {
			logger.Infof("getting a cache key %s", cacheEntry.Key)
			cacheEntry.LastAccessTime = time.Now()
			return cacheEntry
		}
	}

	return nil
}

func (cache *DiskCache) DeleteEntry(key string) {
	cache.Mutex.Lock()
	defer cache.Mutex.Unlock()

	hash := utils.MakeHash(key)

	if entry, ok := cache.Cache.Get(hash); ok {
		if cacheEntry, ok := entry.(*DiskCacheEntry); ok {
			cache.Cache.Delete(hash)

			cache.CurrentSize -= int64(cacheEntry.Size)
			if cache.CurrentSize < 0 {
				cache.CurrentSize = 0
			}
		}
	}
}

func (cache *DiskCache) onEvicted(key string, entry interface{}) {
	if cacheEntry, ok := entry.(*DiskCacheEntry); ok {
		cacheEntry.deleteDataFile()
	}
}
