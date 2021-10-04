package io

import (
	"time"
)

// CacheEntry is a cache entry (e.g., a file chunk)
type CacheEntry interface {
	GetKey() string
	GetGroup() string
	GetSize() int
	GetCreationTime() time.Time

	GetData() ([]byte, error)
}

// Cache is a cache management object
type Cache interface {
	Release()

	GetSizeCap() int64

	GetTotalEntries() int
	GetTotalEntrySize() int64
	GetAvailableSize() int64

	DeleteAllEntries()
	DeleteAllEntriesForGroup(group string)

	GetEntryKeys() []string
	GetEntryKeysForGroup(group string) []string

	CreateEntry(key string, group string, data []byte) (CacheEntry, error)
	HasEntry(key string) bool
	GetEntry(key string) CacheEntry
	DeleteEntry(key string)
}
