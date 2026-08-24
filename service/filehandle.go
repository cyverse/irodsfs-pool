package service

import (
	irodsclient_types "github.com/cyverse/go-irodsclient/irods/types"
	irodsfs_common_irods "github.com/cyverse/irodsfs-common/irods"
	log "github.com/sirupsen/logrus"
)

// PoolFileHandle is a file handle managed by iRODSFS-Pool
type PoolFileHandle struct {
	poolSessionID string

	irodsFsFileHandle irodsfs_common_irods.IRODSFSFileHandle
}

// NewPoolFileHandle creates a new pool file handle
func NewPoolFileHandle(poolSessionID string, irodsFsFileHandle irodsfs_common_irods.IRODSFSFileHandle) (*PoolFileHandle, error) {
	return &PoolFileHandle{
		poolSessionID:     poolSessionID,
		irodsFsFileHandle: irodsFsFileHandle,
	}, nil
}

func (handle *PoolFileHandle) Release() error {
	if handle.irodsFsFileHandle != nil {
		err := handle.irodsFsFileHandle.Close()
		handle.irodsFsFileHandle = nil
		return err
	}
	return nil
}

func (handle *PoolFileHandle) GetID() string {
	return handle.irodsFsFileHandle.GetID()
}

func (handle *PoolFileHandle) GetOpenMode() irodsclient_types.FileOpenMode {
	return handle.irodsFsFileHandle.GetOpenMode()
}

func (handle *PoolFileHandle) GetEntryPath() string {
	return handle.irodsFsFileHandle.GetEntry().Path
}

func (handle *PoolFileHandle) ReadAt(buffer []byte, offset int64) (int, error) {
	return handle.irodsFsFileHandle.ReadAt(buffer, offset)
}

func (handle *PoolFileHandle) GetAvailable(offset int64) int64 {
	return handle.irodsFsFileHandle.GetAvailable(offset)
}

func (handle *PoolFileHandle) WriteAt(data []byte, offset int64) (int, error) {
	return handle.irodsFsFileHandle.WriteAt(data, offset)
}

func (handle *PoolFileHandle) Truncate(size int64) error {
	return handle.irodsFsFileHandle.Truncate(size)
}

func (handle *PoolFileHandle) Flush() error {
	return handle.irodsFsFileHandle.Flush()
}

// stagingSyncer is the optional interface implemented by IRODSFSClientBuffered.
// Calling Sync() uploads all locally-staged data to iRODS without releasing
// the underlying client, so metrics can be read afterwards.
type stagingSyncer interface {
	Sync() error
}

// flushSessionStaging synchronously uploads any pending staged data for the
// session to iRODS.  It is called before CollectSessionMetrics so that
// BytesSent reflects the actual iRODS upload rather than only the local write.
func flushSessionStaging(session *PoolSession, logger *log.Entry) {
	if session.fsClient == nil {
		return
	}
	syncer, ok := session.fsClient.(stagingSyncer)
	if !ok {
		return
	}
	if err := syncer.Sync(); err != nil {
		logger.Warnf("staging flush before metrics collection failed for session %q: %v", session.id, err)
	}
}
