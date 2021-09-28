package io

import (
	"sync"

	irodsfs "github.com/cyverse/go-irodsclient/fs"
	log "github.com/sirupsen/logrus"
)

// SyncReader helps sync read
type SyncReader struct {
	Path            string
	IRODSFileHandle *irodsfs.FileHandle
	FileHandleLock  *sync.Mutex
}

// NewSyncReader create a new SyncReader
func NewSyncReader(path string, fileHandle *irodsfs.FileHandle, fileHandleLock *sync.Mutex) *SyncReader {
	syncReader := &SyncReader{
		Path:            path,
		IRODSFileHandle: fileHandle,
		FileHandleLock:  fileHandleLock,
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

	logger.Infof("Sync Reading - %s, offset %d, length %d", reader.Path, offset, length)

	reader.FileHandleLock.Lock()

	data, err := reader.IRODSFileHandle.ReadAt(offset, length)
	if err != nil {
		reader.FileHandleLock.Unlock()
		logger.WithError(err).Errorf("failed to read data - %s, offset %d, length %d", reader.Path, offset, length)
		return nil, err
	}

	reader.FileHandleLock.Unlock()
	return data, nil
}

func (reader *SyncReader) GetPendingError() error {
	return nil
}