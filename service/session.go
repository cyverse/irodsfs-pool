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
	log "github.com/sirupsen/logrus"

	"github.com/cockroachdb/errors"
	"gopkg.in/natefinch/lumberjack.v2"
)

const (
	sessionLogMaxSizeMB  = 10
	sessionLogMaxBackups = 10
	sessionLogMaxAgeDays = 30
)

// PoolSessionManager manages PoolSession
type PoolSessionManager struct {
	config       *PoolServerConfig
	cacheManager *irodsfs_common_cache.MemoryCacheManager
	sessions     map[string]*PoolSession // key: account key (hash)
	connMap      map[string]string       // key: connection id -> session id
	logger       *log.Entry

	onBeforeSessionRelease func(session *PoolSession)

	// pendingReleases holds a grace-period timer for sessions whose last
	// connection was removed but have not yet been released.  Access is
	// protected by mutex.
	pendingReleases map[string]*time.Timer

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

		pendingReleases: map[string]*time.Timer{},

		mutex:         sync.RWMutex{},
		terminateChan: make(chan bool),
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
	for _, session := range manager.sessions {
		wg.Add(1)
		go func(sess *PoolSession) {
			defer wg.Done()

			sess.mutex.Lock()
			alreadyReleasing := sess.releasing
			if !alreadyReleasing {
				sess.releasing = true
				sess.releaseDone = make(chan struct{})
			}
			sess.mutex.Unlock()

			if !alreadyReleasing {
				sess.release()
				close(sess.releaseDone)
			} else {
				<-sess.releaseDone
			}
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

			manager.logger.Infof("Reusing existing session %q for username %q", accountKey, irodsAccount.ClientUser)
			return session, nil
		}

		manager.mutex.Unlock()
		break
	}

	sessionID := accountKey

	// Create new session
	manager.logger.Infof("Creating a new pool session for username %q", account.ClientUser)

	sessionLogger, sessionLogFile, err := newSessionLogger(manager.config.logRootPath, sessionID)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to create log file for session %q", sessionID)
	}
	sessionLogger.Infof("Creating a new pool session for username %q", account.ClientUser)

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

	// No connections, mark as releasing
	session.mutex.Lock()
	session.releasing = true
	session.releaseDone = make(chan struct{})
	session.mutex.Unlock()
	manager.mutex.Unlock()

	manager.logger.Infof("Releasing pool session %q (no more connections)", sessionID)
	if manager.onBeforeSessionRelease != nil {
		manager.onBeforeSessionRelease(session)
	}
	session.release()

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
	for _, session := range manager.sessions {
		sessions = append(sessions, session)
	}
	manager.sessions = map[string]*PoolSession{}
	manager.connMap = map[string]string{}
	manager.mutex.Unlock()

	for _, session := range sessions {
		manager.logger.Infof("Force releasing pool session %q", session.id)
		if manager.onBeforeSessionRelease != nil {
			manager.onBeforeSessionRelease(session)
		}

		session.mutex.Lock()
		alreadyReleasing := session.releasing
		if !alreadyReleasing {
			session.releasing = true
			session.releaseDone = make(chan struct{})
		}
		session.mutex.Unlock()

		if !alreadyReleasing {
			session.release()
			close(session.releaseDone)
		} else {
			<-session.releaseDone
		}
	}
}

func (manager *PoolSessionManager) AddConnection(connID string, sessionID string, appName string, description string) {
	defer irodsfs_common_util.StackTraceFromPanic(manager.logger)

	manager.mutex.Lock()
	defer manager.mutex.Unlock()

	// If this connID was previously mapped to another session, remove it
	if oldSessionID, ok := manager.connMap[connID]; ok {
		if oldSessionID != sessionID {
			if oldSession, ok := manager.sessions[oldSessionID]; ok {
				oldSession.removeConnection(connID)
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
		session.addConnection(connID, appName, description)
		manager.logger.Infof("Added connection %q (app=%q) to session %q (connections=%d)", connID, appName, sessionID, session.getConnectionCount())
	}
}

func (manager *PoolSessionManager) RemoveConnection(connID string) {
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
	manager.logger.Infof("Removed connection %q from session %q (remaining connections=%d)", connID, sessionID, remaining)

	if remaining > 0 {
		session.UpdateLastAccessTime()
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
		session.UpdateLastAccessTime()
		manager.mutex.Unlock()
		manager.logger.Infof("Session %q has no connections; will release after grace period %q", sessionID, manager.config.sessionCloseGracePeriod)
		return
	}

	// No grace period — release right away (still asynchronous so the Logout
	// RPC returns before the iRODS upload completes).
	session.mutex.Lock()
	session.releasing = true
	session.releaseDone = make(chan struct{})
	session.mutex.Unlock()

	delete(manager.sessions, sessionID)
	manager.mutex.Unlock()

	manager.logger.Infof("Releasing pool session %q asynchronously (no more connections)", sessionID)

	manager.releaseWg.Add(1)
	go func() {
		defer manager.releaseWg.Done()
		// Flush staging before capturing metrics so BytesSent reflects the
		// actual iRODS upload, not just the local-disk write.
		flushSessionStaging(session, session.logger)
		if manager.onBeforeSessionRelease != nil {
			manager.onBeforeSessionRelease(session)
		}
		session.release()
		close(session.releaseDone)
	}()
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

	session.mutex.Lock()
	session.releasing = true
	session.releaseDone = make(chan struct{})
	session.mutex.Unlock()

	delete(manager.sessions, sessionID)
	manager.mutex.Unlock()

	manager.logger.Infof("Releasing pool session %q asynchronously after grace period (no more connections)", sessionID)

	manager.releaseWg.Add(1)
	go func() {
		defer manager.releaseWg.Done()
		flushSessionStaging(session, session.logger)
		if manager.onBeforeSessionRelease != nil {
			manager.onBeforeSessionRelease(session)
		}
		session.release()
		close(session.releaseDone)
	}()
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
	session.mutex.Lock()
	session.releasing = true
	session.releaseDone = make(chan struct{})
	session.mutex.Unlock()

	delete(manager.sessions, sessionID)
	manager.mutex.Unlock()

	manager.logger.Infof("Force releasing stale pool session %q asynchronously", sessionID)

	manager.releaseWg.Add(1)
	go func() {
		defer manager.releaseWg.Done()
		flushSessionStaging(session, session.logger)
		if manager.onBeforeSessionRelease != nil {
			manager.onBeforeSessionRelease(session)
		}
		session.release()
		close(session.releaseDone)
	}()
}

func (manager *PoolSessionManager) GetSession(sessionID string) (*PoolSession, error) {
	manager.mutex.RLock()
	defer manager.mutex.RUnlock()

	if session, ok := manager.sessions[sessionID]; ok {
		return session, nil
	}

	return nil, commons.NewSessionNotFoundError(sessionID)
}

func (manager *PoolSessionManager) GetCacheManager() *irodsfs_common_cache.MemoryCacheManager {
	return manager.cacheManager
}

func (manager *PoolSessionManager) GetAllSessions() []*PoolSession {
	manager.mutex.RLock()
	defer manager.mutex.RUnlock()

	sessions := make([]*PoolSession, 0, len(manager.sessions))
	for _, session := range manager.sessions {
		sessions = append(sessions, session)
	}
	return sessions
}

func (manager *PoolSessionManager) GetTotalSessions() int {
	manager.mutex.RLock()
	defer manager.mutex.RUnlock()

	return len(manager.sessions)
}

func (manager *PoolSessionManager) GetTotalIRODSFSClientInstances() int {
	manager.mutex.RLock()
	defer manager.mutex.RUnlock()

	return len(manager.sessions)
}

func (manager *PoolSessionManager) GetTotalIRODSFSClientConnections() int {
	manager.mutex.RLock()
	defer manager.mutex.RUnlock()

	total := 0
	for _, session := range manager.sessions {
		if session.fsClient != nil {
			total += session.fsClient.GetOpenConnections()
		}
	}
	return total
}

// connInfo holds per-connection metadata supplied at Login time.
type connInfo struct {
	appName     string
	description string
}

// PoolSession represents a shared session for the same account
type PoolSession struct {
	id           string
	accountKey   string
	irodsAccount *irodsclient_types.IRODSAccount

	fs       *irodsclient_fs.FileSystem
	fsClient irodsfs_common_irods.IRODSFSClient

	connections     map[string]connInfo // connID -> client info
	lastAccessTime  time.Time
	poolFileHandles map[string]*PoolFileHandle

	backgroundWg sync.WaitGroup

	releasing   bool
	releaseDone chan struct{}

	logger         *log.Entry
	sessionLogFile io.WriteCloser

	mutex sync.RWMutex
}

func (session *PoolSession) release() {
	defer func() {
		if session.sessionLogFile != nil {
			session.sessionLogFile.Close()
			session.sessionLogFile = nil
		}
	}()
	defer irodsfs_common_util.StackTraceFromPanic(session.logger)

	session.logger.Info("Releasing the pool session")

	session.backgroundWg.Wait()

	session.mutex.Lock()
	defer session.mutex.Unlock()

	for _, handle := range session.poolFileHandles {
		handle.Release()
	}
	session.poolFileHandles = map[string]*PoolFileHandle{}

	if session.fsClient != nil {
		session.fsClient.Release()
		session.fsClient = nil
	}

	if session.fs != nil {
		session.fs.Release()
		session.fs = nil
	}

	session.logger.Info("Released the pool session")
}

func (session *PoolSession) addConnection(connID string, appName string, description string) {
	session.mutex.Lock()
	defer session.mutex.Unlock()

	session.connections[connID] = connInfo{appName: appName, description: description}
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

func (session *PoolSession) GetPoolFileHandle(poolFileHandleID string) (*PoolFileHandle, error) {
	session.mutex.RLock()
	defer session.mutex.RUnlock()

	if handle, ok := session.poolFileHandles[poolFileHandleID]; ok {
		return handle, nil
	}

	return nil, commons.NewFileHandleNotFoundError(poolFileHandleID)
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

func newSessionLogger(logRootPath string, sessionID string) (*log.Entry, io.WriteCloser, error) {
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
		MaxBackups: sessionLogMaxBackups,
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
