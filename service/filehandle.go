package service

import (
	"fmt"

	irodsclient_types "github.com/cyverse/go-irodsclient/irods/types"
	irodsfs_common_io "github.com/cyverse/irodsfs-common/io"
	irodsfs_common_irods "github.com/cyverse/irodsfs-common/irods"
	irodsfs_common_utils "github.com/cyverse/irodsfs-common/utils"
	log "github.com/sirupsen/logrus"
)

const (
	iRODSIOBlockSize int = 4 * 1024 * 1024 // 4MB
	iRODSReadSize    int = 128 * 1024      // 128KB
)

// PoolFileHandle is a file handle managed by iRODSFS-Pool
type PoolFileHandle struct {
	poolServer              *PoolServer
	poolSessionID           string
	irodsFsClientInstanceID string

	writer            irodsfs_common_io.Writer
	reader            irodsfs_common_io.Reader
	irodsFsFileHandle irodsfs_common_irods.IRODSFSFileHandle
}

// NewPoolFileHandle creates a new pool file handle
func NewPoolFileHandle(poolServer *PoolServer, poolSessionID string, irodsFsClientInstanceID string, irodsFsFileHandle irodsfs_common_irods.IRODSFSFileHandle) *PoolFileHandle {
	var writer irodsfs_common_io.Writer
	var reader irodsfs_common_io.Reader

	openMode := irodsFsFileHandle.GetOpenMode()
	if openMode.IsReadOnly() {
		// writer
		writer = irodsfs_common_io.NewNilWriter(irodsFsFileHandle)

		// reader
		if len(poolServer.config.TempRootPath) > 0 {
			syncReader := irodsfs_common_io.NewSyncReader(irodsFsFileHandle, nil)
			if poolServer.cacheStore != nil {
				reader = irodsfs_common_io.NewAsyncBlockReaderWithCache(syncReader, iRODSIOBlockSize, iRODSReadSize, irodsFsFileHandle.GetEntry().CheckSum, poolServer.cacheStore, poolServer.config.TempRootPath)
			} else {
				reader = irodsfs_common_io.NewAsyncBlockReader(syncReader, iRODSIOBlockSize, iRODSReadSize, poolServer.config.TempRootPath)
			}
		} else {
			reader = irodsfs_common_io.NewSyncReader(irodsFsFileHandle, nil)
		}
	} else if openMode.IsWriteOnly() {
		// writer
		if len(poolServer.config.TempRootPath) > 0 {
			syncWriter := irodsfs_common_io.NewSyncWriter(irodsFsFileHandle, nil)
			writer = irodsfs_common_io.NewAsyncWriter(syncWriter, iRODSIOBlockSize, poolServer.config.TempRootPath)
		} else {
			syncWriter := irodsfs_common_io.NewSyncWriter(irodsFsFileHandle, nil)
			writer = irodsfs_common_io.NewSyncBufferedWriter(syncWriter, iRODSIOBlockSize)
		}

		// reader
		reader = irodsfs_common_io.NewNilReader(irodsFsFileHandle)
	} else {
		writer = irodsfs_common_io.NewSyncWriter(irodsFsFileHandle, nil)
		reader = irodsfs_common_io.NewSyncReader(irodsFsFileHandle, nil)
	}

	return &PoolFileHandle{
		poolServer:              poolServer,
		poolSessionID:           poolSessionID,
		irodsFsClientInstanceID: irodsFsClientInstanceID,

		writer:            writer,
		reader:            reader,
		irodsFsFileHandle: irodsFsFileHandle,
	}
}

func (handle *PoolFileHandle) Release() error {
	logger := log.WithFields(log.Fields{
		"package":  "service",
		"struct":   "PoolFileHandle",
		"function": "Release",
	})

	defer irodsfs_common_utils.StackTraceFromPanic(logger)

	errs := []error{}

	if handle.writer != nil {
		err := handle.writer.Flush()
		if err != nil {
			errs = append(errs, err)
		}

		handle.writer.Release()
		handle.writer = nil
	}

	if handle.reader != nil {
		err := handle.reader.GetPendingError()
		if err != nil {
			errs = append(errs, err)
		}

		handle.reader.Release()
		handle.reader = nil
	}

	if handle.irodsFsFileHandle != nil {
		err := handle.irodsFsFileHandle.Close()
		if err != nil {
			errs = append(errs, err)
		}

		handle.irodsFsFileHandle = nil
	}

	if len(errs) > 0 {
		return errs[0]
	}
	return nil
}

func (handle *PoolFileHandle) GetID() string {
	return handle.irodsFsFileHandle.GetID()
}

func (handle *PoolFileHandle) Flush() error {
	logger := log.WithFields(log.Fields{
		"package":  "service",
		"struct":   "PoolFileHandle",
		"function": "Flush",
	})

	defer irodsfs_common_utils.StackTraceFromPanic(logger)

	if handle.writer != nil {
		return handle.writer.Flush()
	}
	return nil
}

func (handle *PoolFileHandle) GetOpenMode() irodsclient_types.FileOpenMode {
	return handle.irodsFsFileHandle.GetOpenMode()
}

func (handle *PoolFileHandle) GetEntryPath() string {
	return handle.irodsFsFileHandle.GetEntry().Path
}

func (handle *PoolFileHandle) GetOffset() int64 {
	logger := log.WithFields(log.Fields{
		"package":  "service",
		"struct":   "PoolFileHandle",
		"function": "GetOffset",
	})

	defer irodsfs_common_utils.StackTraceFromPanic(logger)

	if handle.writer != nil {
		handle.writer.Flush()
	}

	return handle.irodsFsFileHandle.GetOffset()
}

func (handle *PoolFileHandle) ReadAt(buffer []byte, offset int64) (int, error) {
	logger := log.WithFields(log.Fields{
		"package":  "service",
		"struct":   "PoolFileHandle",
		"function": "ReadAt",
	})

	defer irodsfs_common_utils.StackTraceFromPanic(logger)

	if handle.reader != nil {
		return handle.reader.ReadAt(buffer, offset)
	}
	return 0, fmt.Errorf("reader is not initialized")
}

func (handle *PoolFileHandle) WriteAt(data []byte, offset int64) (int, error) {
	logger := log.WithFields(log.Fields{
		"package":  "service",
		"struct":   "PoolFileHandle",
		"function": "WriteAt",
	})

	defer irodsfs_common_utils.StackTraceFromPanic(logger)

	if handle.writer != nil {
		return handle.writer.WriteAt(data, offset)
	}
	return 0, fmt.Errorf("writer is not initialized")
}

func (handle *PoolFileHandle) Truncate(size int64) error {
	logger := log.WithFields(log.Fields{
		"package":  "service",
		"struct":   "PoolFileHandle",
		"function": "Truncate",
	})

	defer irodsfs_common_utils.StackTraceFromPanic(logger)

	if handle.writer != nil {
		handle.writer.Flush()
	}

	return handle.irodsFsFileHandle.Truncate(size)
}
