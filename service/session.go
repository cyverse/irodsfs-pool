package service

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sync"
	"time"

	irodsclient_fs "github.com/cyverse/go-irodsclient/fs"
	irodsclient_types "github.com/cyverse/go-irodsclient/irods/types"
	irodsfs_common_irods "github.com/cyverse/irodsfs-common/irods"
	irodsfs_common_cache "github.com/cyverse/irodsfs-common/irods/cache"
	irodsfs_common_util "github.com/cyverse/irodsfs-common/util"
	"github.com/cyverse/irodsfs-pool/commons"
	"github.com/cyverse/irodsfs-pool/service/api"
	"github.com/rs/xid"
	log "github.com/sirupsen/logrus"

	"github.com/cockroachdb/errors"
)

// PoolSessionManager manages PoolSession
type PoolSessionManager struct {
	config       *PoolServerConfig
	cacheManager *irodsfs_common_cache.MemoryCacheManager
	sessions     map[string]*PoolSession // key: session id
	keyMap       map[string]string       // key: account hash -> session id
	connMap      map[string]string       // key: connection id -> session id
	logger       *log.Entry

	onBeforeSessionRelease func(session *PoolSession)

	mutex         sync.RWMutex
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
		keyMap:       map[string]string{},
		connMap:      map[string]string{},
		logger:       myLogger,

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
	defer manager.mutex.Unlock()

	wg := sync.WaitGroup{}
	for _, session := range manager.sessions {
		wg.Add(1)
		go func(sess *PoolSession) {
			defer wg.Done()
			sess.release()
		}(session)
	}

	if manager.cacheManager != nil {
		manager.cacheManager.Release()
		manager.cacheManager = nil
	}

	manager.sessions = map[string]*PoolSession{}
	manager.keyMap = map[string]string{}
	manager.connMap = map[string]string{}
	wg.Wait()
}

func (manager *PoolSessionManager) NewSession(account *api.Account, appName string) (*PoolSession, error) {
	defer irodsfs_common_util.StackTraceFromPanic(manager.logger)

	irodsAccount := convertAccountFromAPIToIRODS(account)
	accountKey := makeAccountKey(irodsAccount)

	manager.mutex.Lock()

	// Check if session already exists for this account
	if sessionID, ok := manager.keyMap[accountKey]; ok {
		if session, ok := manager.sessions[sessionID]; ok {
			session.UpdateLastAccessTime()
			manager.mutex.Unlock()

			manager.logger.Infof("Reusing existing session %q for username %q", sessionID, irodsAccount.ClientUser)
			return session, nil
		}
	}

	manager.mutex.Unlock()

	// Create new session
	manager.logger.Infof("Creating a new pool session for username %q", account.ClientUser)

	fsConfig := irodsclient_fs.NewFileSystemConfig(appName)
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
		return nil, errors.Wrap(err, "failed to create iRODS filesystem")
	}

	buffConfig := &irodsfs_common_irods.IRODSFSClientBufferedConfig{
		BlockSize:       int(manager.config.dataBlockSize),
		StagingRootPath: manager.config.stagingRootPath,
		SyncInterval:    manager.config.stagingDataGracePeriod / 2,
		GracePeriod:     manager.config.stagingDataGracePeriod,
		UsePersistence:  true,
	}

	fsClient, err := irodsfs_common_irods.NewIRODSFSClientBuffered(fs, manager.cacheManager, buffConfig)
	if err != nil {
		fs.Release()
		return nil, errors.Wrap(err, "failed to create buffered client")
	}

	sessionID := xid.New().String()

	session := &PoolSession{
		id:           sessionID,
		accountKey:   accountKey,
		irodsAccount: irodsAccount,

		fs:       fs,
		fsClient: fsClient,

		connections:     map[string]struct{}{},
		lastAccessTime:  time.Now(),
		poolFileHandles: map[string]*PoolFileHandle{},

		logger: manager.logger.WithFields(log.Fields{
			"session_id": sessionID,
		}),
	}

	manager.mutex.Lock()
	manager.sessions[session.id] = session
	manager.keyMap[accountKey] = session.id
	manager.mutex.Unlock()

	manager.logger.Infof("Created a new pool session %q for username %q", session.id, irodsAccount.ClientUser)
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

	// No connections, remove session
	delete(manager.sessions, sessionID)
	delete(manager.keyMap, session.accountKey)
	manager.mutex.Unlock()

	manager.logger.Infof("Releasing pool session %q (no more connections)", sessionID)
	if manager.onBeforeSessionRelease != nil {
		manager.onBeforeSessionRelease(session)
	}
	session.release()
}

func (manager *PoolSessionManager) ReleaseAllSessions() {
	defer irodsfs_common_util.StackTraceFromPanic(manager.logger)

	manager.mutex.Lock()
	sessions := make([]*PoolSession, 0, len(manager.sessions))
	for _, session := range manager.sessions {
		sessions = append(sessions, session)
	}
	manager.sessions = map[string]*PoolSession{}
	manager.keyMap = map[string]string{}
	manager.connMap = map[string]string{}
	manager.mutex.Unlock()

	for _, session := range sessions {
		manager.logger.Infof("Force releasing pool session %q", session.id)
		if manager.onBeforeSessionRelease != nil {
			manager.onBeforeSessionRelease(session)
		}
		session.release()
	}
}

func (manager *PoolSessionManager) AddConnection(connID string, sessionID string) {
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
		session.addConnection(connID)
		manager.logger.Infof("Added connection %q to session %q (connections=%d)", connID, sessionID, session.getConnectionCount())
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

	// No connections remaining, release session
	delete(manager.sessions, sessionID)
	delete(manager.keyMap, session.accountKey)
	manager.mutex.Unlock()

	manager.logger.Infof("Releasing pool session %q (no more connections)", sessionID)
	if manager.onBeforeSessionRelease != nil {
		manager.onBeforeSessionRelease(session)
	}
	session.release()
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

	delete(manager.sessions, sessionID)
	delete(manager.keyMap, session.accountKey)
	manager.mutex.Unlock()

	manager.logger.Infof("Force releasing stale pool session %q", sessionID)
	if manager.onBeforeSessionRelease != nil {
		manager.onBeforeSessionRelease(session)
	}
	session.release()
}

func (manager *PoolSessionManager) GetSession(sessionID string) (*PoolSession, error) {
	manager.mutex.RLock()
	defer manager.mutex.RUnlock()

	if session, ok := manager.sessions[sessionID]; ok {
		return session, nil
	}

	return nil, errors.Errorf("pool session not found: %s: %w", sessionID, commons.NewSessionNotFoundError(sessionID))
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

// PoolSession represents a shared session for the same account
type PoolSession struct {
	id           string
	accountKey   string
	irodsAccount *irodsclient_types.IRODSAccount

	fs       *irodsclient_fs.FileSystem
	fsClient irodsfs_common_irods.IRODSFSClient

	connections     map[string]struct{} // set of connection IDs
	lastAccessTime  time.Time
	poolFileHandles map[string]*PoolFileHandle

	logger *log.Entry

	mutex sync.RWMutex
}

func (session *PoolSession) release() {
	defer irodsfs_common_util.StackTraceFromPanic(session.logger)

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
}

func (session *PoolSession) addConnection(connID string) {
	session.mutex.Lock()
	defer session.mutex.Unlock()

	session.connections[connID] = struct{}{}
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

	return nil, errors.Errorf("pool file handle not found: %s: %w", poolFileHandleID, commons.NewFileHandleNotFoundError(poolFileHandleID))
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
