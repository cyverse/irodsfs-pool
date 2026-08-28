package service

import (
	"fmt"
	"time"

	"github.com/cockroachdb/errors"
	irodsclient_fs "github.com/cyverse/go-irodsclient/fs"
	irodsfs_common_irods "github.com/cyverse/irodsfs-common/irods"
)

// RecoveryResult holds the outcome of a RecoverSession call.
type RecoveryResult struct {
	SessionID   string    `json:"session_id"`
	StartedAt   time.Time `json:"started_at"`
	CompletedAt time.Time `json:"completed_at"`
	Success     bool      `json:"success"`
	Error       string    `json:"error,omitempty"`
}

// RecoverSession restores a failed/interrupted session from the DB, syncs its
// staged data to iRODS, and then cleanly releases the session.
//
// The session must have status interrupted or release_failed and must have an
// encrypted account stored (written by snapshotFailedSession). If the sync and
// release both succeed the DB record is deleted; on failure it is updated with
// the new status.
func (manager *PoolSessionManager) RecoverSession(sessionID string) (*RecoveryResult, error) {
	result := &RecoveryResult{
		SessionID: sessionID,
		StartedAt: time.Now(),
	}

	// Read stored session info.
	info, err := manager.getStoredSession(sessionID)
	if err != nil {
		return nil, errors.Wrap(err, "failed to read session from store")
	}
	if info == nil {
		return nil, errors.Errorf("session %q not found in recovery store", sessionID)
	}
	switch info.Status {
	case FailedSessionStatusActive, FailedSessionStatusRecovering:
		return nil, errors.Errorf("session %q cannot be recovered while in status %q", sessionID, info.Status)
	}
	if info.EncryptedAccount == nil {
		return nil, errors.Errorf("session %q has no encrypted credentials; manual recovery required", sessionID)
	}

	// Mark as recovering so concurrent calls see the right state.
	info.Status = FailedSessionStatusRecovering
	if saveErr := manager.saveFailedSession(*info); saveErr != nil {
		manager.logger.WithError(saveErr).Warnf("Failed to mark session %q as recovering in store", sessionID)
	}

	recoverErr := manager.doRecoverSession(sessionID, info, result)

	result.CompletedAt = time.Now()
	if recoverErr != nil {
		result.Error = recoverErr.Error()
		// Revert to interrupted so the operator can retry.
		info.Status = FailedSessionStatusInterrupted
		if saveErr := manager.saveFailedSession(*info); saveErr != nil {
			manager.logger.WithError(saveErr).Warnf("Failed to revert recovery status for session %q", sessionID)
		}
	} else {
		result.Success = true
	}
	return result, nil
}

func (manager *PoolSessionManager) doRecoverSession(sessionID string, info *FailedSessionInfo, result *RecoveryResult) error {
	// Decrypt credentials.
	irodsAccount, err := manager.DecryptSessionAccount(info)
	if err != nil {
		return errors.Wrap(err, "failed to decrypt iRODS account")
	}

	// Build session logger.
	sessionLogger, sessionLogFile, err := newSessionLogger(manager.config.logRootPath, sessionID)
	if err != nil {
		return errors.Wrapf(err, "failed to create session logger for recovery of %q", sessionID)
	}

	irodsClientLogger, err := newIrodsClientLogger(sessionLogFile)
	if err != nil {
		sessionLogFile.Close()
		return errors.Wrapf(err, "failed to create iRODS client logger for recovery of %q", sessionID)
	}

	// Connect to iRODS.
	fsConfig := irodsclient_fs.NewFileSystemConfig("irodsfs-pool-recovery")
	fsConfig.LogEntry = irodsClientLogger
	fsConfig.IOConnection.MaxNumber = manager.config.maxIOConnectionPerSession
	fsConfig.Cache.MetadataTimeoutSettings = manager.config.metadataCacheTimeoutSettings
	fsConfig.Cache.StartNewTransaction = manager.config.startNewTransaction
	fsConfig.Cache.Backend.Type = irodsclient_fs.CacheBackendTypeRistretto
	fsConfig.Cache.Backend.Ristretto.MaxEntries = manager.config.maxMetadataCacheEntriesPerSession
	fsConfig.Cache.Backend.Ristretto.MaxCost = manager.config.maxMetadataCacheSizePerSession
	fsConfig.Cache.Backend.Ristretto.BufferItems = manager.config.maxMetadataCacheBufferItemsPerSession
	fsConfig.Cache.Backend.Ristretto.DefaultTTL = manager.config.metadataCacheTTL

	fs, err := irodsclient_fs.NewFileSystem(irodsAccount, fsConfig)
	if err != nil {
		sessionLogFile.Close()
		return errors.Wrap(err, "failed to connect to iRODS for recovery")
	}

	// Build buffered client with persistence so any staged files are reloaded.
	sessionStagingPath := manager.config.stagingRootPath
	if sessionStagingPath != "" {
		sessionStagingPath = fmt.Sprintf("%s/%s", sessionStagingPath, sessionID)
	}

	buffConfig := &irodsfs_common_irods.IRODSFSClientBufferedConfig{
		BlockSize:          int(manager.config.dataBlockSize),
		StagingRootPath:    sessionStagingPath,
		MaxStagingDataSize: manager.config.maxStagingDataSize,
		MaxCacheFileSize:   manager.config.maxCacheFileSize,
		SyncInterval:       manager.config.stagingDataGracePeriod / 2,
		GracePeriod:        manager.config.stagingDataGracePeriod,
		UsePersistence:     true,
	}

	fsClient, err := irodsfs_common_irods.NewIRODSFSClientBuffered(fs, manager.cacheManager, buffConfig)
	if err != nil {
		fs.Release()
		sessionLogFile.Close()
		return errors.Wrap(err, "failed to create buffered FS client for recovery")
	}

	session := &PoolSession{
		id:              sessionID,
		accountKey:      info.AccountKey,
		irodsAccount:    irodsAccount,
		fs:              fs,
		fsClient:        fsClient,
		connections:     map[string]connInfo{},
		lastAccessTime:  time.Now(),
		poolFileHandles: map[string]*PoolFileHandle{},
		logger:          sessionLogger,
		sessionLogFile:  sessionLogFile,
	}

	// Sync staged files to iRODS.
	sessionLogger.Infof("Starting recovery sync for session %q", sessionID)
	if syncErr := fsClient.Sync(); syncErr != nil {
		sessionLogger.WithError(syncErr).Errorf("Recovery sync failed for session %q", sessionID)
		// Still attempt a clean release so resources are freed.
		releaseErr := session.release()
		manager.handleSessionReleaseResult(session, errors.CombineErrors(syncErr, releaseErr))
		return errors.Wrap(syncErr, "sync failed during recovery")
	}
	sessionLogger.Infof("Recovery sync completed for session %q", sessionID)

	// Release cleanly; handleSessionReleaseResult removes the DB record on success.
	if releaseErr := session.release(); releaseErr != nil {
		manager.handleSessionReleaseResult(session, releaseErr)
		return errors.Wrap(releaseErr, "release failed after recovery sync")
	}
	manager.handleSessionReleaseResult(session, nil)
	return nil
}
