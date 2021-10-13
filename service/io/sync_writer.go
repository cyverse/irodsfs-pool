package io

import (
	"sync"

	irodsfs "github.com/cyverse/go-irodsclient/fs"
	log "github.com/sirupsen/logrus"
)

// SyncWriter helps sync write
type SyncWriter struct {
	Path            string
	IRODSFileHandle *irodsfs.FileHandle
	FileHandleLock  *sync.Mutex
}

// NewSyncWriter create a new SyncWriter
func NewSyncWriter(path string, fileHandle *irodsfs.FileHandle, fileHandleLock *sync.Mutex) *SyncWriter {
	syncWriter := &SyncWriter{
		Path:            path,
		IRODSFileHandle: fileHandle,
		FileHandleLock:  fileHandleLock,
	}

	return syncWriter
}

// Release releases all resources
func (writer *SyncWriter) Release() {
	logger := log.WithFields(log.Fields{
		"package":  "io",
		"struct":   "SyncWriter",
		"function": "Release",
	})

	defer func() {
		if r := recover(); r != nil {
			logger.Panic(r)
		}
	}()

	writer.Flush()
}

// WriteAt writes data
func (writer *SyncWriter) WriteAt(offset int64, data []byte) error {
	logger := log.WithFields(log.Fields{
		"package":  "io",
		"struct":   "SyncWriter",
		"function": "WriteAt",
	})

	defer func() {
		if r := recover(); r != nil {
			logger.Panic(r)
		}
	}()

	if len(data) == 0 || offset < 0 {
		return nil
	}

	logger.Infof("Sync Writing - %s, offset %d, length %d", writer.Path, offset, len(data))

	writer.FileHandleLock.Lock()

	err := writer.IRODSFileHandle.WriteAt(offset, data)
	if err != nil {
		writer.FileHandleLock.Unlock()
		logger.WithError(err).Errorf("failed to write data - %s, offset %d, length %d", writer.Path, offset, len(data))
		return err
	}

	writer.FileHandleLock.Unlock()
	return nil
}

func (writer *SyncWriter) Flush() error {
	return nil
}

func (writer *SyncWriter) GetPendingError() error {
	return nil
}
