package service

import (
	"context"

	"github.com/cockroachdb/errors"
	irodsclient_types "github.com/cyverse/go-irodsclient/irods/types"
	irodsfs_common_irods "github.com/cyverse/irodsfs-common/irods"
	log "github.com/sirupsen/logrus"
)

// errNoFileLockManager is returned when a handle was created without a lock
// manager, so it has nowhere to record locks
var errNoFileLockManager = errors.New("file lock manager is unavailable")

// PoolFileHandle is a file handle managed by iRODSFS-Pool
type PoolFileHandle struct {
	poolSessionID string

	irodsFsFileHandle irodsfs_common_irods.IRODSFSFileHandle

	// fileLockManager is shared by every session of the server, so that two
	// mounts locking the same file are told about each other
	fileLockManager *irodsfs_common_irods.FileLockManager
}

// NewPoolFileHandle creates a new pool file handle
func NewPoolFileHandle(poolSessionID string, irodsFsFileHandle irodsfs_common_irods.IRODSFSFileHandle, fileLockManager *irodsfs_common_irods.FileLockManager) (*PoolFileHandle, error) {
	return &PoolFileHandle{
		poolSessionID:     poolSessionID,
		irodsFsFileHandle: irodsFsFileHandle,
		fileLockManager:   fileLockManager,
	}, nil
}

func (handle *PoolFileHandle) Release() error {
	if handle.irodsFsFileHandle != nil {
		// locks go away with the handle that took them, as they do for flock()
		// and OFD locks
		if handle.fileLockManager != nil {
			handle.fileLockManager.Release(handle.irodsFsFileHandle.GetID())
		}

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

// Getlk returns a lock that conflicts with the given lock, or nil if the lock
// can be acquired
func (handle *PoolFileHandle) Getlk(lock *irodsfs_common_irods.FileLock) (*irodsfs_common_irods.FileLock, error) {
	if handle.fileLockManager == nil {
		return nil, errNoFileLockManager
	}

	return handle.fileLockManager.Test(handle.GetEntryPath(), handle.ownedLock(lock)), nil
}

// Setlk acquires or releases a lock without waiting. It returns an error
// wrapping irods.ErrFileLockConflict if another owner holds a conflicting lock.
func (handle *PoolFileHandle) Setlk(lock *irodsfs_common_irods.FileLock) error {
	if handle.fileLockManager == nil {
		return errNoFileLockManager
	}

	return handle.fileLockManager.Lock(handle.GetEntryPath(), handle.ownedLock(lock))
}

// Setlkw acquires a lock, waiting until it becomes available or the context is
// canceled. The context is the one of the gRPC call, so a client that gives up
// or disconnects gives up the wait.
func (handle *PoolFileHandle) Setlkw(ctx context.Context, lock *irodsfs_common_irods.FileLock) error {
	if handle.fileLockManager == nil {
		return errNoFileLockManager
	}

	return handle.fileLockManager.LockWait(ctx, handle.GetEntryPath(), handle.ownedLock(lock))
}

// ownedLock returns a copy of the lock owned by this handle. The lock owner a
// client reports is only unique within that client, so the session scopes it.
func (handle *PoolFileHandle) ownedLock(lock *irodsfs_common_irods.FileLock) *irodsfs_common_irods.FileLock {
	owned := *lock
	owned.Owner.Scope = handle.poolSessionID
	owned.Owner.Handle = handle.GetID()
	return &owned
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
		logger.WithError(err).Warnf("staging flush before metrics collection failed for session %q", session.id)
	}
}
