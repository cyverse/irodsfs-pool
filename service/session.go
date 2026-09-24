package service

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	irodsclient_fs "github.com/cyverse/go-irodsclient/fs"
	irodsclient_types "github.com/cyverse/go-irodsclient/irods/types"
	irodsfs_common_irods "github.com/cyverse/irodsfs-common/irods"
	irodsfs_common_cache "github.com/cyverse/irodsfs-common/irods/cache"
	irodsfs_common_util "github.com/cyverse/irodsfs-common/util"
	"github.com/cyverse/irodsfs-pool/commons"
	"github.com/cyverse/irodsfs-pool/service/api"
	"github.com/dgraph-io/badger/v3"
	log "github.com/sirupsen/logrus"

	"github.com/cockroachdb/errors"
	"gopkg.in/natefinch/lumberjack.v2"
)

const (
	sessionLogMaxSizeMB  = 10
	sessionLogMaxAgeDays = 30
)

var errSessionUnavailable = errors.New("session is not available")

// PoolSessionManager manages PoolSession
type PoolSessionManager struct {
	config       *PoolServerConfig
	cacheManager *irodsfs_common_cache.MemoryCacheManager
	sessions     map[string]*PoolSession // key: account key (hash)
	connMap      map[string]string       // key: connection id -> session id
	logger       *log.Entry

	failedSessionDBPath string
	failedSessionDB     *badger.DB
	failedSessionMutex  sync.Mutex
	recoveryCipher      *recoveryAccountCipher

	onBeforeSessionRelease func(session *PoolSession)

	// fileLockManager holds the file locks of every session, so that two mounts
	// locking the same file conflict with each other
	fileLockManager *irodsfs_common_irods.FileLockManager

	// pendingReleases holds a grace-period timer for sessions whose last
	// connection was removed but have not yet been released.  Access is
	// protected by mutex.
	pendingReleases map[string]*time.Timer

	// releasingSessions holds sessions whose asynchronous release is still
	// running.  They leave sessions as soon as the release starts, so a new
	// login never reuses one, but their staging upload can take minutes and
	// their resources are still held, so monitoring has to keep reporting them
	// until the release actually finishes.  Access is protected by mutex.
	releasingSessions map[string]*PoolSession

	mutex         sync.RWMutex
	releaseWg     sync.WaitGroup // tracks in-progress async session releases
	terminateChan chan bool
}

func NewPoolSessionManager(config *PoolServerConfig) (*PoolSessionManager, error) {
	if config == nil {
		return nil, errors.New("config is required")
	}

	var myLogger *log.Entry
	if config != nil && config.logger != nil {
		myLogger = config.logger
	} else {
		// create new logger object
		myLogger = log.StandardLogger().WithFields(log.Fields{})
	}

	cacheConfig := &irodsfs_common_cache.MemoryCacheConfig{
		NumCounters: config.maxDataMemCacheSize / config.dataBlockSize * 10,
		MaxCost:     config.maxDataMemCacheSize,
		BufferItems: config.maxDataMemCacheBufferItems,
		TTL:         config.dataMemCacheTTL,
	}

	cacheManager, err := irodsfs_common_cache.NewMemoryCacheManager(cacheConfig)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create memory cache manager")
	}

	manager := &PoolSessionManager{
		config:       config,
		cacheManager: cacheManager,
		sessions:     map[string]*PoolSession{},
		connMap:      map[string]string{},
		logger:       myLogger,

		pendingReleases:   map[string]*time.Timer{},
		releasingSessions: map[string]*PoolSession{},
		fileLockManager:   irodsfs_common_irods.NewFileLockManager(),

		mutex:         sync.RWMutex{},
		terminateChan: make(chan bool),
	}
	recoveryCipher, err := newRecoveryAccountCipher(config.recoveryEncryptionKey)
	if err != nil {
		cacheManager.Release()
		return nil, err
	}
	manager.recoveryCipher = recoveryCipher

	manager.failedSessionDBPath = filepath.Join(config.dataRootPath, failedSessionDBDirectoryName)
	if err := manager.loadFailedSessionStore(); err != nil {
		cacheManager.Release()
		return nil, errors.Wrap(err, "failed to load failed session store")
	}

	checkInterval := manager.config.sessionTimeoutCheckInterval

	go func() {
		ticker := time.NewTicker(checkInterval)
		defer ticker.Stop()

		for {
			select {
			case <-manager.terminateChan:
				return
			case <-ticker.C:
				manager.checkpointActiveSessions()
				manager.releaseStaleSessions()
			}
		}
	}()

	return manager, nil
}

func (manager *PoolSessionManager) Release() {
	defer irodsfs_common_util.StackTraceFromPanic(manager.logger)

	manager.logger.Info("Releasing the pool session manager")
	defer manager.logger.Info("Released the pool session manager")

	manager.terminateChan <- true

	manager.mutex.Lock()

	// Stop all pending grace-period timers before releasing sessions.
	for sessionID, t := range manager.pendingReleases {
		t.Stop()
		manager.logger.Infof("Cancelled pending grace-period release for session %q (manager releasing)", sessionID)
	}
	manager.pendingReleases = map[string]*time.Timer{}

	wg := sync.WaitGroup{}
	alreadyReleasing := make(map[string]bool, len(manager.sessions))
	for _, session := range manager.sessions {
		// The manager is going away, so no session is up for adoption. Marking
		// them here, while the manager lock is held, keeps a release from
		// starting beside the one below.
		session.mutex.RLock()
		releasing := session.releasing
		session.mutex.RUnlock()
		alreadyReleasing[session.id] = releasing
		if !releasing {
			markSessionReleasing(session, false)
		} else {
			session.mutex.Lock()
			session.tearingDown = true
			session.mutex.Unlock()
		}

		wg.Add(1)
		go func(sess *PoolSession) {
			defer wg.Done()

			if !alreadyReleasing[sess.id] {
				manager.releaseSessionResources(sess)
				close(sess.releaseDone)
			} else {
				<-sess.releaseDone
			}
			manager.finishAsyncRelease(sess.id)
		}(session)
	}

	if manager.cacheManager != nil {
		manager.cacheManager.Release()
		manager.cacheManager = nil
	}

	manager.sessions = map[string]*PoolSession{}
	manager.connMap = map[string]string{}
	manager.mutex.Unlock()
	wg.Wait()

	// Also wait for any sessions that were released asynchronously (e.g. via RemoveConnection).
	manager.releaseWg.Wait()

	if err := manager.closeFailedSessionStore(); err != nil {
		manager.logger.WithError(err).Error("Failed to close failed session store")
	}
}

func (manager *PoolSessionManager) NewSession(account *api.Account, appName string) (*PoolSession, error) {
	defer irodsfs_common_util.StackTraceFromPanic(manager.logger)

	irodsAccount := convertAccountFromAPIToIRODS(account)
	accountKey := makeAccountKey(irodsAccount)

	for {
		manager.mutex.Lock()

		// Check if session already exists for this account
		if session, ok := manager.sessions[accountKey]; ok {
			session.mutex.RLock()
			isReleasing := session.releasing
			releaseDone := session.releaseDone
			session.mutex.RUnlock()

			if isReleasing {
				// Session is being released, wait for it to complete
				manager.mutex.Unlock()
				manager.logger.Infof("Waiting for session %q release to complete before creating new session for username %q", accountKey, irodsAccount.ClientUser)
				<-releaseDone
				continue
			}

			// Cancel any pending grace-period release so a new connection
			// arriving shortly after the last one left doesn't force a
			// teardown-and-recreate cycle.
			if t, ok := manager.pendingReleases[accountKey]; ok {
				t.Stop()
				delete(manager.pendingReleases, accountKey)
				manager.logger.Infof("Cancelled pending grace-period release for session %q (new login)", accountKey)
			}

			session.UpdateLastAccessTime()
			manager.mutex.Unlock()
			manager.checkpointSession(session)

			manager.logger.Infof("Reusing existing session %q for username %q", accountKey, irodsAccount.ClientUser)
			return session, nil
		}

		// A session that is being released is still flushing its staged data
		// to iRODS, which an interrupted run can stretch into minutes. Take it
		// over rather than wait: it works throughout that flush, and a session
		// created beside it would collide with it over the same staging area.
		if session, ok := manager.releasingSessions[accountKey]; ok {
			if manager.adoptReleasingSessionUnlocked(session) {
				session.UpdateLastAccessTime()
				manager.mutex.Unlock()
				manager.checkpointSession(session)

				manager.logger.Infof("Took over the release of session %q for username %q", accountKey, irodsAccount.ClientUser)
				return session, nil
			}

			// The teardown has begun, so the session cannot come back. Wait for
			// it to finish, which also frees its staging area for the session
			// that replaces it.
			session.mutex.RLock()
			releaseDone := session.releaseDone
			session.mutex.RUnlock()
			manager.mutex.Unlock()

			manager.logger.Infof("Waiting for session %q release to complete before creating new session for username %q", accountKey, irodsAccount.ClientUser)
			if releaseDone != nil {
				<-releaseDone
			}
			continue
		}

		manager.mutex.Unlock()
		break
	}

	sessionID := accountKey

	// Create new session
	manager.logger.Infof("Creating a new pool session for username %q", irodsAccount.ClientUser)

	sessionLogger, sessionLogFile, err := newSessionLogger(manager.config.logRootPath, sessionID, manager.config.logMaxBackups)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to create log file for session %q", sessionID)
	}
	sessionLogger.Infof("Creating a new pool session for username %q", irodsAccount.ClientUser)

	irodsClientLogger, err := newIrodsClientLogger(sessionLogFile)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to create log file for go-irodsclient %q", sessionID)
	}

	fsConfig := irodsclient_fs.NewFileSystemConfig(appName)
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
		return nil, errors.Wrap(err, "failed to create iRODS filesystem")
	}

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
		sessionLogFile.Close()
		return nil, errors.Wrap(err, "failed to create buffered client")
	}

	session := &PoolSession{
		id:           sessionID,
		accountKey:   accountKey,
		irodsAccount: irodsAccount,

		fs:       fs,
		fsClient: fsClient,

		connections:     map[string]connInfo{},
		lastAccessTime:  time.Now(),
		poolFileHandles: map[string]*PoolFileHandle{},

		logger:         sessionLogger,
		sessionLogFile: sessionLogFile,
	}
	if err := manager.trackActiveSession(session); err != nil {
		releaseErr := session.release()
		return nil, errors.CombineErrors(errors.Wrap(err, "failed to persist session lifecycle record"), releaseErr)
	}

	manager.mutex.Lock()
	manager.sessions[session.id] = session
	manager.mutex.Unlock()

	manager.logger.Infof("Created a new pool session %q for username %q", session.id, irodsAccount.ClientUser)
	session.logger.Infof("Created a new pool session for username %q", irodsAccount.ClientUser)
	return session, nil
}

func (manager *PoolSessionManager) ReleaseSession(sessionID string) {
	defer irodsfs_common_util.StackTraceFromPanic(manager.logger)

	manager.mutex.Lock()
	session, ok := manager.sessions[sessionID]
	if !ok {
		manager.mutex.Unlock()
		return
	}

	remaining := session.getConnectionCount()
	manager.logger.Infof("Session %q has %d connections remaining", sessionID, remaining)

	if remaining > 0 {
		session.UpdateLastAccessTime()
		manager.mutex.Unlock()
		return
	}

	// No connections, mark as releasing. This path tears the session down
	// right away, so there is no flush for a new login to take it over from.
	markSessionReleasing(session, false)
	manager.mutex.Unlock()

	manager.logger.Infof("Releasing pool session %q (no more connections)", sessionID)
	if manager.onBeforeSessionRelease != nil {
		manager.onBeforeSessionRelease(session)
	}
	manager.releaseSessionResources(session)

	// After release completes, remove from maps and signal waiters
	manager.mutex.Lock()
	delete(manager.sessions, sessionID)
	manager.mutex.Unlock()

	close(session.releaseDone)
}

func (manager *PoolSessionManager) ReleaseAllSessions() {
	defer irodsfs_common_util.StackTraceFromPanic(manager.logger)

	manager.mutex.Lock()
	sessions := make([]*PoolSession, 0, len(manager.sessions))
	alreadyReleasing := make(map[string]bool, len(manager.sessions))
	for _, session := range manager.sessions {
		sessions = append(sessions, session)
		manager.releasingSessions[session.id] = session

		session.mutex.RLock()
		releasing := session.releasing
		session.mutex.RUnlock()
		alreadyReleasing[session.id] = releasing

		// Every session goes away, so none of them is up for adoption. Marking
		// them here, under the manager lock, also means a session in the
		// releasing map always has a channel for a waiter to watch.
		if !releasing {
			markSessionReleasing(session, false)
		} else {
			session.mutex.Lock()
			session.tearingDown = true
			session.mutex.Unlock()
		}
	}
	manager.sessions = map[string]*PoolSession{}
	manager.connMap = map[string]string{}
	manager.mutex.Unlock()

	wg := sync.WaitGroup{}
	for _, session := range sessions {
		wg.Add(1)
		go func(sess *PoolSession) {
			defer wg.Done()

			manager.logger.Infof("Force releasing pool session %q", sess.id)
			if manager.onBeforeSessionRelease != nil {
				manager.onBeforeSessionRelease(sess)
			}

			if !alreadyReleasing[sess.id] {
				manager.releaseSessionResources(sess)
				close(sess.releaseDone)
			} else {
				<-sess.releaseDone
			}
			manager.finishAsyncRelease(sess.id)
		}(session)
	}
	wg.Wait()
}

func (manager *PoolSessionManager) AddConnection(connID string, clientID string, sessionID string, appName string, description string) {
	defer irodsfs_common_util.StackTraceFromPanic(manager.logger)

	manager.mutex.Lock()
	defer manager.mutex.Unlock()

	// If this connID was previously mapped to another session, remove it
	if oldSessionID, ok := manager.connMap[connID]; ok {
		if oldSessionID != sessionID {
			if oldSession, ok := manager.sessions[oldSessionID]; ok {
				oldSession.removeConnection(connID)
				manager.checkpointSession(oldSession)
				manager.logger.Infof("Moved connection %q from session %q to session %q", connID, oldSessionID, sessionID)
			}
		}
	}

	manager.connMap[connID] = sessionID

	if session, ok := manager.sessions[sessionID]; ok {
		// Cancel any pending grace-period release now that a new connection
		// is being established for this session.
		if t, ok := manager.pendingReleases[sessionID]; ok {
			t.Stop()
			delete(manager.pendingReleases, sessionID)
			manager.logger.Infof("Cancelled pending grace-period release for session %q (connection %q added)", sessionID, connID)
		}
		session.addConnection(connID, clientID, appName, description)
		manager.checkpointSession(session)
		manager.logger.Infof("Added connection %q (client=%q, app=%q) to session %q (connections=%d)", connID, clientID, appName, sessionID, session.getConnectionCount())
	}
}

// RemoveConnection drops a connection that went away without a logout, a
// transport drop for one. The client may be back, and its file handles are kept
// for it until the session is released.
func (manager *PoolSessionManager) RemoveConnection(connID string) {
	manager.removeConnection(connID, false)
}

// LogoutConnection drops a connection whose client logged out. The client said
// it was done, so what it left open is collected right away rather than held
// until the session is released, which lets its staged data start going out
// while the session is still in its grace period.
func (manager *PoolSessionManager) LogoutConnection(connID string) {
	manager.removeConnection(connID, true)
}

func (manager *PoolSessionManager) removeConnection(connID string, loggedOut bool) {
	defer irodsfs_common_util.StackTraceFromPanic(manager.logger)

	manager.mutex.Lock()

	sessionID, ok := manager.connMap[connID]
	if !ok {
		manager.mutex.Unlock()
		return
	}

	delete(manager.connMap, connID)

	session, ok := manager.sessions[sessionID]
	if !ok {
		manager.mutex.Unlock()
		return
	}

	remaining := session.removeConnection(connID)
	session.UpdateLastAccessTime()
	manager.checkpointSession(session)
	manager.logger.Infof("Removed connection %q from session %q (remaining connections=%d)", connID, sessionID, remaining)

	if loggedOut {
		collectFileHandlesOfDepartedClients(session)
	}

	if remaining > 0 {
		manager.mutex.Unlock()
		return
	}

	// No connections remaining.  If a grace period is configured, defer the
	// actual release so a quickly-reconnecting client reuses the session
	// without paying the teardown/setup cost.  Otherwise release immediately.
	if manager.config.sessionCloseGracePeriod > 0 {
		// Discard any stale timer that somehow survived (shouldn't normally happen).
		if t, ok := manager.pendingReleases[sessionID]; ok {
			t.Stop()
			delete(manager.pendingReleases, sessionID)
		}
		t := time.AfterFunc(manager.config.sessionCloseGracePeriod, func() {
			manager.startSessionRelease(sessionID)
		})
		manager.pendingReleases[sessionID] = t
		manager.mutex.Unlock()
		manager.logger.Infof("Session %q has no connections; will release after grace period %q", sessionID, manager.config.sessionCloseGracePeriod)
		return
	}

	// No grace period — release right away (still asynchronous so the Logout
	// RPC returns before the iRODS upload completes).
	epoch := markSessionReleasing(session, true)
	manager.beginAsyncReleaseUnlocked(session)
	manager.mutex.Unlock()

	manager.releaseSessionAsync(session, epoch, "no more connections")
}

// startSessionRelease is called by the grace-period timer.  It re-checks that
// no new connection arrived during the grace period before proceeding.
// All access to sessions and pendingReleases is protected by manager.mutex.
func (manager *PoolSessionManager) startSessionRelease(sessionID string) {
	manager.mutex.Lock()

	session, ok := manager.sessions[sessionID]
	if !ok {
		// Already released by another path (e.g. forceReleaseSession).
		delete(manager.pendingReleases, sessionID)
		manager.mutex.Unlock()
		return
	}

	if session.getConnectionCount() > 0 {
		// A new connection arrived during the grace period; keep the session.
		delete(manager.pendingReleases, sessionID)
		manager.mutex.Unlock()
		return
	}

	delete(manager.pendingReleases, sessionID)

	epoch := markSessionReleasing(session, true)
	manager.beginAsyncReleaseUnlocked(session)
	manager.mutex.Unlock()

	manager.releaseSessionAsync(session, epoch, "no more connections after the grace period")
}

func (manager *PoolSessionManager) releaseStaleSessions() {
	manager.mutex.RLock()

	sessionTimeout := manager.config.sessionTimeout
	staleIDs := []string{}

	for _, session := range manager.sessions {
		if time.Since(session.GetLastAccessTime()) > sessionTimeout {
			staleIDs = append(staleIDs, session.id)
		}
	}

	manager.mutex.RUnlock()

	for _, sessionID := range staleIDs {
		manager.forceReleaseSession(sessionID)
	}
}

func (manager *PoolSessionManager) checkpointActiveSessions() {
	for _, session := range manager.GetAllSessions() {
		manager.checkpointSession(session)
	}
}

func (manager *PoolSessionManager) forceReleaseSession(sessionID string) {
	defer irodsfs_common_util.StackTraceFromPanic(manager.logger)

	manager.mutex.Lock()
	session, ok := manager.sessions[sessionID]
	if !ok {
		manager.mutex.Unlock()
		return
	}

	// Skip if already being released
	session.mutex.RLock()
	if session.releasing {
		session.mutex.RUnlock()
		manager.mutex.Unlock()
		return
	}
	session.mutex.RUnlock()

	// Mark as releasing and remove from map so Release() won't double-release.
	epoch := markSessionReleasing(session, true)
	manager.beginAsyncReleaseUnlocked(session)
	manager.mutex.Unlock()

	manager.releaseSessionAsync(session, epoch, "the session went stale")
}

// beginAsyncRelease moves a session out of the live map and into the releasing
// one, so it can no longer be reused while staying visible to monitoring. The
// caller must hold manager.mutex.
func (manager *PoolSessionManager) beginAsyncReleaseUnlocked(session *PoolSession) {
	delete(manager.sessions, session.id)
	manager.releasingSessions[session.id] = session
}

// markSessionReleasing puts a session into the releasing state and gives
// waiters a channel to watch. adoptable says whether a flush still runs ahead
// of the teardown, which is the part a new login may take the session over
// from. The caller must hold manager.mutex.
func markSessionReleasing(session *PoolSession, adoptable bool) uint64 {
	session.mutex.Lock()
	defer session.mutex.Unlock()

	session.releasing = true
	session.releaseDone = make(chan struct{})
	session.tearingDown = !adoptable
	session.releaseEpoch++
	return session.releaseEpoch
}

// releaseSessionAsync flushes the session's staged data to iRODS and then tears
// the session down. The flush is the long part of a release, and the session
// serves requests normally while it runs, so a login that arrives during it
// takes the session over and the teardown is dropped.
func (manager *PoolSessionManager) releaseSessionAsync(session *PoolSession, epoch uint64, reason string) {
	manager.logger.Infof("Releasing pool session %q asynchronously (%s)", session.id, reason)

	manager.releaseWg.Add(1)
	go func() {
		defer manager.releaseWg.Done()

		manager.drainSessionForRelease(session, epoch)

		if !manager.beginSessionTeardown(session, epoch) {
			manager.logger.Infof("Kept pool session %q, a new login took it over while it was being released", session.id)
			return
		}

		if manager.onBeforeSessionRelease != nil {
			manager.onBeforeSessionRelease(session)
		}
		manager.releaseSessionResources(session)
		manager.finishAsyncRelease(session.id)
		close(session.releaseDone)
	}()
}

// drainSessionForRelease finishes everything that moves data while the session
// is still whole, so that the teardown after it only has to close things. A
// login that arrives during the drain still takes the session over, and then
// none of this work is repeated: the drain leaves nothing for the teardown to
// redo.
//
// The order matters in both directions. The session's own work runs against its
// file handles - an in-flight ReadAt looks one up, an async CacheFile reads
// through the session - so it has to finish first, and the handles it closes on
// its way out are closed properly. Only what is left after that is collected by
// force: handles of a client that is gone and will never close them. They have
// to go before the flush, because staging refuses to sync at all while a write
// handle is open, which would leave every upload to the teardown, where a
// returning client can no longer be served.
func (manager *PoolSessionManager) drainSessionForRelease(session *PoolSession, epoch uint64) {
	session.releaseWork.Lock()
	defer session.releaseWork.Unlock()

	// Everything below belongs to the client that has left. Once a login has
	// taken the session over, the file handles, the staged data and the session
	// itself are the new client's, and a release that reached here late leaves
	// them alone. The epoch is re-read before each step, because the take-over
	// can land during any of them.
	if manager.sessionTakenOver(session, epoch) {
		return
	}

	session.backgroundWg.Wait()

	if manager.sessionTakenOver(session, epoch) {
		return
	}

	if err := session.releaseFileHandlesOfDepartedClients(); err != nil {
		session.logger.WithError(err).Warn("Failed to release some file handles before the session release")
	}

	if manager.sessionTakenOver(session, epoch) {
		return
	}

	// Upload path by path first. This moves the bulk of the staged data while
	// leaving a client that takes the session over free to write, which a full
	// flush would not: it holds every write handle off for as long as it runs.
	drainSessionStaging(session, session.logger)

	if manager.sessionTakenOver(session, epoch) {
		// The session is back in service, and what the drain could not move is
		// left to the background sync, which does not hold its client off.
		return
	}

	// Nobody came back, so empty the staging area for good. The drain before it
	// keeps this short, which matters because a login arriving now waits for it
	// to finish before its first write. It also runs before the metrics are
	// captured, so BytesSent reflects the actual iRODS upload rather than just
	// the local-disk write.
	flushSessionStaging(session, session.logger)
}

// sessionTakenOver reports whether a login has taken the session over since the
// release identified by epoch began.
func (manager *PoolSessionManager) sessionTakenOver(session *PoolSession, epoch uint64) bool {
	session.mutex.RLock()
	defer session.mutex.RUnlock()

	return session.releaseEpoch != epoch
}

// beginSessionTeardown closes the window in which a release can still be taken
// over and reports whether the teardown owns the session. When a login got
// there first it owns the session instead, and has already put it back in
// service.
func (manager *PoolSessionManager) beginSessionTeardown(session *PoolSession, epoch uint64) bool {
	manager.mutex.Lock()
	defer manager.mutex.Unlock()

	session.mutex.Lock()
	defer session.mutex.Unlock()

	if session.releaseEpoch != epoch {
		return false
	}

	session.tearingDown = true
	return true
}

// adoptReleasingSessionUnlocked hands a session that is being released back to
// a new login. It succeeds only while the release is still flushing staged
// data, because nothing has been closed until then: the session returns to
// service with its staging area, and the lock it holds on that staging area,
// intact. Waiters are woken so they reuse the session instead of waiting for a
// release that is no longer coming. The caller must hold manager.mutex.
func (manager *PoolSessionManager) adoptReleasingSessionUnlocked(session *PoolSession) bool {
	session.mutex.Lock()
	defer session.mutex.Unlock()

	if !session.releasing || session.tearingDown {
		return false
	}

	session.releasing = false
	session.releaseEpoch++
	if session.releaseDone != nil {
		close(session.releaseDone)
		session.releaseDone = nil
	}

	delete(manager.releasingSessions, session.id)
	manager.sessions[session.id] = session
	return true
}

// finishAsyncRelease drops a session once its release has completed.
func (manager *PoolSessionManager) finishAsyncRelease(sessionID string) {
	manager.mutex.Lock()
	delete(manager.releasingSessions, sessionID)
	manager.mutex.Unlock()
}

// allSessionsUnlocked returns the live sessions followed by the ones still
// releasing. The caller must hold manager.mutex.
func (manager *PoolSessionManager) allSessionsUnlocked() []*PoolSession {
	sessions := make([]*PoolSession, 0, len(manager.sessions)+len(manager.releasingSessions))
	for _, session := range manager.sessions {
		sessions = append(sessions, session)
	}
	for _, session := range manager.releasingSessions {
		sessions = append(sessions, session)
	}
	return sessions
}

func (manager *PoolSessionManager) GetSession(sessionID string) (*PoolSession, error) {
	manager.mutex.RLock()
	defer manager.mutex.RUnlock()

	if session, ok := manager.sessions[sessionID]; ok {
		return session, nil
	}
	// A session that is still uploading is reported rather than hidden. The
	// actions that take a session already refuse a releasing one, so they fail
	// with "unavailable" instead of the misleading "not found".
	if session, ok := manager.releasingSessions[sessionID]; ok {
		return session, nil
	}

	return nil, commons.NewSessionNotFoundError(sessionID)
}

func (manager *PoolSessionManager) InvalidateSessionMetadataCache(sessionID string) error {
	session, err := manager.GetSession(sessionID)
	if err != nil {
		return err
	}

	return session.invalidateMetadataCache()
}

func (manager *PoolSessionManager) SyncSessionStaging(sessionID string) error {
	session, err := manager.GetSession(sessionID)
	if err != nil {
		return err
	}

	return session.syncStaging()
}

func (manager *PoolSessionManager) GetCacheManager() *irodsfs_common_cache.MemoryCacheManager {
	return manager.cacheManager
}

func (manager *PoolSessionManager) GetAllSessions() []*PoolSession {
	manager.mutex.RLock()
	defer manager.mutex.RUnlock()

	return manager.allSessionsUnlocked()
}

func (manager *PoolSessionManager) GetTotalSessions() int {
	manager.mutex.RLock()
	defer manager.mutex.RUnlock()

	// A releasing session still holds its connections and its staging data, so
	// counting only the live ones understates what the server is doing.
	return len(manager.sessions) + len(manager.releasingSessions)
}

func (manager *PoolSessionManager) GetTotalIRODSFSClientInstances() int {
	manager.mutex.RLock()
	defer manager.mutex.RUnlock()

	return len(manager.sessions) + len(manager.releasingSessions)
}

func (manager *PoolSessionManager) GetTotalIRODSFSClientConnections() int {
	manager.mutex.RLock()
	defer manager.mutex.RUnlock()

	total := 0
	for _, session := range manager.allSessionsUnlocked() {
		if session.fsClient != nil {
			total += session.fsClient.GetOpenConnections()
		}
	}
	return total
}

func (manager *PoolSessionManager) releaseSessionResources(session *PoolSession) {
	releaseErr := session.release()
	manager.handleSessionReleaseResult(session, releaseErr)
}

// connInfo holds per-connection metadata supplied at Login time.
type connInfo struct {
	clientID    string // the id the client sent, empty for a client that sends none
	appName     string
	description string
}

type sessionFileSystem interface {
	ClearCache()
	Release()
}

// PoolSession represents a shared session for the same account
type PoolSession struct {
	id           string
	accountKey   string
	irodsAccount *irodsclient_types.IRODSAccount

	fs       sessionFileSystem
	fsClient irodsfs_common_irods.IRODSFSClient

	connections     map[string]connInfo // connID -> client info
	lastAccessTime  time.Time
	poolFileHandles map[string]*PoolFileHandle

	backgroundWg sync.WaitGroup

	releasing   bool
	releaseDone chan struct{}

	// tearingDown marks the point in a release where the session's resources
	// start closing. Until then a release is only flushing staged data and the
	// session still works, so a new login takes it over instead of waiting for
	// a flush that an interrupted run can stretch into minutes.
	tearingDown bool
	// releaseWork serializes the drains of successive releases of one session.
	// A release that was taken over can still be inside its drain when the next
	// one starts, and the two must not run against the session at the same
	// time, or the older one would still be working through a session the newer
	// one is already closing.
	releaseWork sync.Mutex
	// releaseEpoch identifies the release a goroutine is running. Taking a
	// session over, and releasing it again afterwards, each start a new epoch,
	// so the goroutine left over from an earlier release recognises that it no
	// longer owns the session and stops instead of tearing down a session that
	// is back in use.
	releaseEpoch uint64

	logger         *log.Entry
	sessionLogFile io.WriteCloser

	mutex sync.RWMutex
}

func (session *PoolSession) invalidateMetadataCache() error {
	session.mutex.Lock()
	defer session.mutex.Unlock()

	if session.releasing || session.fs == nil {
		return errors.Wrapf(errSessionUnavailable, "session %q", session.id)
	}

	// go-irodsclient groups the filesystem's entry, directory, ACL, and AVU
	// caches behind ClearCache. The pool's shared data block cache is separate
	// and is not affected by this call.
	session.fs.ClearCache()
	if session.logger != nil {
		session.logger.Info("Invalidated the session metadata cache")
	}
	return nil
}

func (session *PoolSession) syncStaging() error {
	session.mutex.Lock()
	defer session.mutex.Unlock()

	if session.releasing || session.fsClient == nil {
		return errors.Wrapf(errSessionUnavailable, "session %q", session.id)
	}

	session.lastAccessTime = time.Now()
	if err := session.fsClient.Sync(); err != nil {
		return errors.Wrap(err, "failed to sync session staging data")
	}

	if session.logger != nil {
		session.logger.Info("Synced the session staging data")
	}
	return nil
}

func (session *PoolSession) release() error {
	defer func() {
		if session.sessionLogFile != nil {
			session.sessionLogFile.Close()
			session.sessionLogFile = nil
		}
	}()
	defer irodsfs_common_util.StackTraceFromPanic(session.logger)

	session.logger.Info("Releasing the pool session")

	session.backgroundWg.Wait()

	// Both are no-ops for a session that came through the drain, and cover the
	// paths that release a session without one.
	releaseErr := session.releaseFileHandles()

	session.mutex.Lock()
	defer session.mutex.Unlock()

	if session.fsClient != nil {
		releaseErr = errors.CombineErrors(releaseErr, session.fsClient.Release())
		session.fsClient = nil
	}

	if session.fs != nil {
		session.fs.Release()
		session.fs = nil
	}

	if releaseErr != nil {
		session.logger.WithError(releaseErr).Error("Released the pool session with errors")
	} else {
		session.logger.Info("Released the pool session")
	}

	return releaseErr
}

func (session *PoolSession) addConnection(connID string, clientID string, appName string, description string) {
	session.mutex.Lock()
	defer session.mutex.Unlock()

	session.connections[connID] = connInfo{clientID: clientID, appName: appName, description: description}
}

func (session *PoolSession) removeConnection(connID string) int {
	session.mutex.Lock()
	defer session.mutex.Unlock()

	delete(session.connections, connID)
	return len(session.connections)
}

func (session *PoolSession) getConnectionCount() int {
	session.mutex.RLock()
	defer session.mutex.RUnlock()

	return len(session.connections)
}

func (session *PoolSession) GetID() string {
	return session.id
}

func (session *PoolSession) GetIRODSAccount() *irodsclient_types.IRODSAccount {
	return session.irodsAccount
}

func (session *PoolSession) GetIRODSFSClient() irodsfs_common_irods.IRODSFSClient {
	return session.fsClient
}

// getIRODSFSClient reads the client under the session lock, for the release
// paths that run beside a teardown clearing it. It returns nil once the session
// has been torn down, and a nil interface value satisfies no type assertion, so
// callers can assert on it directly.
func (session *PoolSession) getIRODSFSClient() irodsfs_common_irods.IRODSFSClient {
	session.mutex.RLock()
	defer session.mutex.RUnlock()

	return session.fsClient
}

func (session *PoolSession) UpdateLastAccessTime() {
	session.mutex.Lock()
	defer session.mutex.Unlock()

	session.lastAccessTime = time.Now()
}

func (session *PoolSession) GetLastAccessTime() time.Time {
	session.mutex.RLock()
	defer session.mutex.RUnlock()

	return session.lastAccessTime
}

// releaseFileHandles closes every file handle the session holds and forgets
// them. It belongs to the teardown, where the session itself is going away.
func (session *PoolSession) releaseFileHandles() error {
	return session.releaseFileHandlesMatching(func(*PoolFileHandle) bool { return true })
}

// collectFileHandlesOfDepartedClients closes what clients that are no longer
// connected left open, in the background so that a logout does not wait for the
// files to close. It is tracked on the session, so a release that starts in the
// meantime waits for it instead of racing it.
func collectFileHandlesOfDepartedClients(session *PoolSession) {
	session.backgroundWg.Add(1)
	go func() {
		defer session.backgroundWg.Done()

		if err := session.releaseFileHandlesOfDepartedClients(); err != nil {
			session.logger.WithError(err).Warn("Failed to release the file handles left by a client that logged out")
		}
	}()
}

// releaseFileHandlesOfDepartedClients closes the handles whose client is no
// longer connected. A handle belongs to the client that opened it, and that id
// survives the client's reconnects, so a client that comes back keeps its
// handles while the ones left behind by a client that is gone are collected.
// They have to go before the staged data can be flushed, because staging
// refuses to sync at all while a write handle is open.
//
// A handle from a client that reported no id cannot be attributed to anyone, so
// it is collected only when no client is connected at all.
func (session *PoolSession) releaseFileHandlesOfDepartedClients() error {
	session.mutex.RLock()
	connected := make(map[string]bool, len(session.connections))
	for _, connection := range session.connections {
		connected[connection.clientID] = true
	}
	anyConnection := len(session.connections) > 0
	session.mutex.RUnlock()

	return session.releaseFileHandlesMatching(func(handle *PoolFileHandle) bool {
		if handle.clientID == "" {
			return !anyConnection
		}
		return !connected[handle.clientID]
	})
}

func (session *PoolSession) releaseFileHandlesMatching(departed func(*PoolFileHandle) bool) error {
	session.mutex.Lock()
	handles := make([]*PoolFileHandle, 0, len(session.poolFileHandles))
	for id, handle := range session.poolFileHandles {
		if !departed(handle) {
			continue
		}
		handles = append(handles, handle)
		delete(session.poolFileHandles, id)
	}
	session.mutex.Unlock()

	if len(handles) == 0 {
		return nil
	}

	handleWg := sync.WaitGroup{}
	handleErrChan := make(chan error, len(handles))
	for _, handle := range handles {
		handleWg.Add(1)
		go func(h *PoolFileHandle) {
			defer handleWg.Done()
			if err := h.Release(); err != nil {
				handleErrChan <- err
			}
		}(handle)
	}
	handleWg.Wait()
	close(handleErrChan)

	var releaseErr error
	for err := range handleErrChan {
		releaseErr = errors.CombineErrors(releaseErr, err)
	}
	return releaseErr
}

func (session *PoolSession) AddPoolFileHandle(poolFileHandle *PoolFileHandle) {
	session.mutex.Lock()
	defer session.mutex.Unlock()

	session.poolFileHandles[poolFileHandle.GetID()] = poolFileHandle
}

func (session *PoolSession) RemovePoolFileHandle(poolFileHandleID string) {
	session.mutex.Lock()
	defer session.mutex.Unlock()

	delete(session.poolFileHandles, poolFileHandleID)
}

// GetPoolFileHandle returns the handle for callerID to act on. A handle belongs
// to the client that opened it, and a session is shared by every mount of one
// iRODS account, so another client asking for it is told the same thing as if it
// did not exist rather than handed someone else's file.
func (session *PoolSession) GetPoolFileHandle(callerID string, poolFileHandleID string) (*PoolFileHandle, error) {
	session.mutex.RLock()
	defer session.mutex.RUnlock()

	if handle, ok := session.poolFileHandles[poolFileHandleID]; ok && handle.ownedBy(callerID) {
		return handle, nil
	}

	return nil, commons.NewFileHandleNotFoundError(poolFileHandleID)
}

// GetFileLockManager returns the file lock manager shared by every session
func (manager *PoolSessionManager) GetFileLockManager() *irodsfs_common_irods.FileLockManager {
	return manager.fileLockManager
}

// makeAccountKey creates a unique key for an iRODS account
func makeAccountKey(account *irodsclient_types.IRODSAccount) string {
	h := sha256.New()
	h.Write([]byte(account.Host))
	h.Write([]byte(fmt.Sprintf("%d", account.Port)))
	h.Write([]byte(account.ClientUser))
	h.Write([]byte(account.ClientZone))
	h.Write([]byte(account.ProxyUser))
	h.Write([]byte(account.ProxyZone))
	h.Write([]byte(account.Ticket))
	h.Write([]byte(account.DefaultResource))
	return hex.EncodeToString(h.Sum(nil))
}

func newSessionLogger(logRootPath string, sessionID string, maxBackups int) (*log.Entry, io.WriteCloser, error) {
	if len(logRootPath) == 0 {
		return nil, nil, errors.New("log root path is required")
	}

	sessionLogRootPath := filepath.Join(logRootPath, "session_logs")
	if err := os.MkdirAll(sessionLogRootPath, 0775); err != nil {
		return nil, nil, errors.Wrapf(err, "failed to create session log directory %q", sessionLogRootPath)
	}

	logFilePath := filepath.Join(sessionLogRootPath, fmt.Sprintf("%s.log", sessionID))
	logWriter := &lumberjack.Logger{
		Filename:   logFilePath,
		MaxSize:    sessionLogMaxSizeMB,
		MaxBackups: maxBackups,
		MaxAge:     sessionLogMaxAgeDays,
		Compress:   false,
	}

	myFormatter := &irodsfs_common_util.StacktraceTextFormatter{
		TextFormatter: log.TextFormatter{
			TimestampFormat: "2006-01-02 15:04:05.000000",
			FullTimestamp:   true,
		},
	}

	sessionLogger := log.New()
	sessionLogger.SetOutput(logWriter)
	sessionLogger.SetFormatter(myFormatter)
	sessionLogger.SetLevel(log.GetLevel())
	sessionLogger.SetReportCaller(true)

	return sessionLogger.WithField("session_id", sessionID), logWriter, nil
}

func newIrodsClientLogger(logWriter io.WriteCloser) (*log.Entry, error) {
	myFormatter := &irodsfs_common_util.StacktraceTextFormatter{
		TextFormatter: log.TextFormatter{
			TimestampFormat: "2006-01-02 15:04:05.000000",
			FullTimestamp:   true,
		},
	}

	irodsClientLogger := log.New()
	irodsClientLogger.SetOutput(logWriter)
	irodsClientLogger.SetFormatter(myFormatter)
	irodsClientLogger.SetLevel(log.ErrorLevel)
	irodsClientLogger.SetReportCaller(true)

	return irodsClientLogger.WithFields(log.Fields{}), nil
}
