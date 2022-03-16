package io

import (
	"sync"

	irodsfs "github.com/cyverse/go-irodsclient/fs"
	log "github.com/sirupsen/logrus"
)

// SyncReader helps sync read
type SyncReader struct {
	path           string
	fileHandle     *irodsfs.FileHandle
	fileHandleLock *sync.Mutex
}

// NewSyncReader create a new SyncReader
func NewSyncReader(path string, fileHandle *irodsfs.FileHandle, fileHandleLock *sync.Mutex) *SyncReader {
	syncReader := &SyncReader{
		path:           path,
		fileHandle:     fileHandle,
		fileHandleLock: fileHandleLock,
	}

	return syncReader
}

// Release releases all resources
func (reader *SyncReader) Release() {
}

// ReadAt reads data
func (reader *SyncReader) ReadAt(offset int64, length int) ([]byte, error) {
	logger := log.WithFields(log.Fields{
		"package":  "io",
		"struct":   "SyncReader",
		"function": "ReadAt",
	})

	if length <= 0 || offset < 0 {
		return []byte{}, nil
	}

	logger.Infof("Sync Reading - %s, offset %d, length %d", reader.path, offset, length)

	reader.fileHandleLock.Lock()

	data, err := reader.fileHandle.ReadAt(offset, length)
	if err != nil {
		reader.fileHandleLock.Unlock()
		logger.WithError(err).Errorf("failed to read data - %s, offset %d, length %d", reader.path, offset, length)
		return nil, err
	}

	reader.fileHandleLock.Unlock()
	return data, nil
}

func (reader *SyncReader) GetPendingError() error {
	return nil
}
