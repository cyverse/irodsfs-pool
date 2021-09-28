package io

import (
	"time"
)

// CacheEntry is a cache entry (e.g., a file chunk)
type CacheEntry interface {
	GetKey() string
	GetSize() int
	GetCreationTime() time.Time
	GetLastAccessTime() time.Time

	GetData() ([]byte, error)
}

// Cache is a cache management object
type Cache interface {
	GetSizeCap() int64

	GetTotalEntries() int
	GetTotalEntrySize() int64
	GetAvailableSize() int64

	DeleteAllEntries()
	GetEntryKeys() []string

	CreateEntry(key string, data []byte) (CacheEntry, error)
	HasEntry(key string) bool
	GetEntry(key string) CacheEntry
	DeleteEntry(key string)
}
