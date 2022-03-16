package io

import (
	"runtime/debug"
	"sync"

	irodsfs "github.com/cyverse/go-irodsclient/fs"
	log "github.com/sirupsen/logrus"
)

// SyncWriter helps sync write
type SyncWriter struct {
	path            string
	fileHandle      *irodsfs.FileHandle
	fileHandleMutex *sync.Mutex
}

// NewSyncWriter create a new SyncWriter
func NewSyncWriter(path string, fileHandle *irodsfs.FileHandle, fileHandleLock *sync.Mutex) *SyncWriter {
	syncWriter := &SyncWriter{
		path:            path,
		fileHandle:      fileHandle,
		fileHandleMutex: fileHandleLock,
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
			logger.Errorf("stacktrace from panic: %s", string(debug.Stack()))
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
			logger.Errorf("stacktrace from panic: %s", string(debug.Stack()))
			logger.Panic(r)
		}
	}()

	if len(data) == 0 || offset < 0 {
		return nil
	}

	logger.Infof("Sync Writing - %s, offset %d, length %d", writer.path, offset, len(data))

	writer.fileHandleMutex.Lock()

	err := writer.fileHandle.WriteAt(offset, data)
	if err != nil {
		writer.fileHandleMutex.Unlock()
		logger.WithError(err).Errorf("failed to write data - %s, offset %d, length %d", writer.path, offset, len(data))
		return err
	}

	writer.fileHandleMutex.Unlock()
	return nil
}

func (writer *SyncWriter) Flush() error {
	return nil
}

func (writer *SyncWriter) GetPendingError() error {
	return nil
}
