package service

import (
	"fmt"
	"os"
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
	Adopted     bool      `json:"adopted,omitempty"`
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
		return nil, errors.Newf("session %q not found in recovery store", sessionID)
	}
	switch info.Status {
	case FailedSessionStatusActive, FailedSessionStatusRecovering:
		return nil, errors.Newf("session %q cannot be recovered while in status %q", sessionID, info.Status)
	}
	if info.EncryptedAccount == nil {
		return nil, errors.Newf("session %q has no encrypted credentials; manual recovery required", sessionID)
	}

	// A session that is live, or on its way out, already holds the staging area
	// this recovery would open, and uploads it on its own.
	if manager.sessionInUse(sessionID) {
		return nil, errors.Newf("session %q is in use by a client, which uploads its staged data itself", sessionID)
	}

	// Mark as recovering so concurrent calls see the right state.
	info.Status = FailedSessionStatusRecovering
	if saveErr := manager.saveFailedSession(*info); saveErr != nil {
		manager.logger.WithError(saveErr).Warnf("Failed to mark session %q as recovering in store", sessionID)
	}

	adopted, recoverErr := manager.doRecoverSession(sessionID, info)

	result.CompletedAt = time.Now()
	switch {
	case recoverErr != nil:
		result.Error = recoverErr.Error()
		// Revert to interrupted so the operator can retry.
		info.Status = FailedSessionStatusInterrupted
		if saveErr := manager.saveFailedSession(*info); saveErr != nil {
			manager.logger.WithError(saveErr).Warnf("Failed to revert recovery status for session %q", sessionID)
		}
	case adopted:
		// A client took the session over mid-recovery, so it is a running
		// session again and what is left in its staging area is uploaded by its
		// own release.
		result.Success = true
		result.Adopted = true
		info.Status = FailedSessionStatusActive
		if saveErr := manager.saveFailedSession(*info); saveErr != nil {
			manager.logger.WithError(saveErr).Warnf("Failed to mark recovered session %q as active", sessionID)
		}
	default:
		result.Success = true
	}
	return result, nil
}

// doRecoverSession rebuilds a session from the recovery store, uploads the data
// left in its staging area and then releases it. The session is parked in the
// releasing map for as long as that runs, so a login for the same account takes
// it over - with its staging area, and the lock it holds on that staging area,
// intact - rather than colliding with it over the same staging directory. It
// reports whether that take-over happened, in which case nothing was released.
func (manager *PoolSessionManager) doRecoverSession(sessionID string, info *FailedSessionInfo) (bool, error) {
	// A recovery holds a session and its staging area just like a release does,
	// so a shutdown has to wait for it rather than close the store underneath it.
	manager.releaseWg.Add(1)
	defer manager.releaseWg.Done()

	// Decrypt credentials.
	irodsAccount, err := manager.DecryptSessionAccount(info)
	if err != nil {
		return false, errors.Wrap(err, "failed to decrypt iRODS account")
	}

	// Build session logger.
	sessionLogger, sessionLogFile, err := newSessionLogger(manager.config.logRootPath, sessionID)
	if err != nil {
		return false, errors.Wrapf(err, "failed to create session logger for recovery of %q", sessionID)
	}

	irodsClientLogger, err := newIrodsClientLogger(sessionLogFile)
	if err != nil {
		sessionLogFile.Close()
		return false, errors.Wrapf(err, "failed to create iRODS client logger for recovery of %q", sessionID)
	}

	session := &PoolSession{
		id:              sessionID,
		accountKey:      info.AccountKey,
		irodsAccount:    irodsAccount,
		connections:     map[string]connInfo{},
		lastAccessTime:  time.Now(),
		poolFileHandles: map[string]*PoolFileHandle{},
		logger:          sessionLogger,
		sessionLogFile:  sessionLogFile,
	}

	// Claim the session before its staging area is opened, so a login that
	// arrives while the recovery is still connecting waits for it here instead
	// of opening the same staging area beside it.
	epoch := manager.registerRecoveringSession(session)

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
		manager.abandonRecoveringSession(session)
		sessionLogFile.Close()
		return false, errors.Wrap(err, "failed to connect to iRODS for recovery")
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
		PackedDirectories:  manager.config.packedDirectories,
	}

	fsClient, err := irodsfs_common_irods.NewIRODSFSClientBuffered(fs, manager.cacheManager, buffConfig)
	if err != nil {
		fs.Release()
		manager.abandonRecoveringSession(session)
		sessionLogFile.Close()
		return false, errors.Wrap(err, "failed to create buffered FS client for recovery")
	}

	session.mutex.Lock()
	session.fs = fs
	session.fsClient = fsClient
	session.mutex.Unlock()

	// The session can serve a client again, so a login arriving from here on
	// takes it over instead of waiting for the upload below to finish.
	manager.offerRecoveringSessionForAdoption(session)

	// Sync staged files to iRODS.
	sessionLogger.Infof("Starting recovery sync for session %q", sessionID)
	syncErr := manager.uploadRecoveredStaging(session, epoch)
	if syncErr != nil {
		sessionLogger.WithError(syncErr).Errorf("Recovery sync failed for session %q", sessionID)
	} else {
		sessionLogger.Infof("Recovery sync completed for session %q", sessionID)
	}

	// Release cleanly; handleSessionReleaseResult removes the DB record on success.
	tornDown, releaseErr := manager.teardownReleasingSession(session, epoch, syncErr)
	if !tornDown {
		sessionLogger.Infof("Kept recovered session %q, a new login took it over during its recovery", sessionID)
		return true, nil
	}

	switch {
	case syncErr != nil:
		return false, errors.Wrap(syncErr, "sync failed during recovery")
	case releaseErr != nil:
		return false, errors.Wrap(releaseErr, "release failed after recovery sync")
	}
	return false, nil
}

// uploadRecoveredStaging empties the staging area a recovered session was built
// on. It drains path by path first, which leaves a client that takes the
// session over free to write, and only holds writers off for the flush that
// finishes the upload - which a session nobody came back for runs alone.
func (manager *PoolSessionManager) uploadRecoveredStaging(session *PoolSession, epoch uint64) error {
	session.releaseWork.Lock()
	defer session.releaseWork.Unlock()

	drainSessionStaging(session, session.logger)

	if manager.sessionTakenOver(session, epoch) {
		return nil
	}

	fsClient := session.getIRODSFSClient()
	if fsClient == nil {
		return nil
	}
	return fsClient.Sync()
}

// DiscardResult holds the outcome of a DiscardSessionStaging call.
type DiscardResult struct {
	SessionID   string `json:"session_id"`
	StagingPath string `json:"staging_path,omitempty"`
	Removed     bool   `json:"removed"`
	Success     bool   `json:"success"`
	Error       string `json:"error,omitempty"`
}

// DiscardSessionStaging removes the local staging directory for a
// failed/interrupted session and deletes its DB record. Use this when the data
// has already been pushed to iRODS by other means and only the local leftovers
// need to be cleaned up — no iRODS connection is required.
func (manager *PoolSessionManager) DiscardSessionStaging(sessionID string) (*DiscardResult, error) {
	result := &DiscardResult{SessionID: sessionID}

	info, err := manager.getStoredSession(sessionID)
	if err != nil {
		return nil, errors.Wrap(err, "failed to read session from store")
	}
	if info == nil {
		return nil, errors.Newf("session %q not found in recovery store", sessionID)
	}
	switch info.Status {
	case FailedSessionStatusActive, FailedSessionStatusRecovering:
		return nil, errors.Newf("session %q cannot be discarded while in status %q", sessionID, info.Status)
	}

	// Remove local staging directory if it exists.
	stagingPath := manager.config.stagingRootPath
	if stagingPath != "" {
		stagingPath = fmt.Sprintf("%s/%s", stagingPath, sessionID)
		result.StagingPath = stagingPath

		if _, statErr := os.Stat(stagingPath); statErr == nil {
			if removeErr := os.RemoveAll(stagingPath); removeErr != nil {
				result.Error = removeErr.Error()
				return result, nil
			}
			result.Removed = true
			manager.logger.Infof("Discarded local staging directory %q for session %q", stagingPath, sessionID)
		}
	}

	// Remove DB record.
	if removeErr := manager.RemoveFailedSession(sessionID); removeErr != nil {
		result.Error = removeErr.Error()
		return result, nil
	}

	result.Success = true
	return result, nil
}
