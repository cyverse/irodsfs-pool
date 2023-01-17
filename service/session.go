package service

import (
	"fmt"
	"sync"
	"time"

	irodsclient_fs "github.com/cyverse/go-irodsclient/fs"
	irodsfs_common_irods "github.com/cyverse/irodsfs-common/irods"
	irodsfs_common_utils "github.com/cyverse/irodsfs-common/utils"
	"github.com/cyverse/irodsfs-pool/service/api"
	"github.com/rs/xid"
	log "github.com/sirupsen/logrus"
)

// PoolSessionManager manages PoolSession
type PoolSessionManager struct {
	config                       *PoolServerConfig
	sessions                     map[string]*PoolSession // key: pool session id
	irodsFsClientInstanceManager *IRODSFSClientInstanceManager

	mutex sync.RWMutex // mutex to access PoolSessionManager
}

func NewPoolSessionManager(config *PoolServerConfig) *PoolSessionManager {
	return &PoolSessionManager{
		config:                       config,
		sessions:                     map[string]*PoolSession{},
		irodsFsClientInstanceManager: NewIRODSFSClientInstanceManager(config),

		mutex: sync.RWMutex{},
	}
}

func (manager *PoolSessionManager) Release() {
	logger := log.WithFields(log.Fields{
		"package":  "service",
		"struct":   "PoolSessionManager",
		"function": "Release",
	})

	defer irodsfs_common_utils.StackTraceFromPanic(logger)

	manager.mutex.Lock()
	defer manager.mutex.Unlock()

	for _, session := range manager.sessions {
		manager.irodsFsClientInstanceManager.RemovePoolSession(session.GetIRODSFSClientInstanceID(), session.GetID())

		session.release()
	}

	manager.sessions = map[string]*PoolSession{}

	manager.irodsFsClientInstanceManager.Release()
}

func (manager *PoolSessionManager) NewSession(account *api.Account, clientID string, appName string) (*PoolSession, error) {
	logger := log.WithFields(log.Fields{
		"package":  "service",
		"struct":   "PoolSessionManager",
		"function": "NewSession",
	})

	defer irodsfs_common_utils.StackTraceFromPanic(logger)

	manager.mutex.Lock()
	defer manager.mutex.Unlock()

	session := newPoolSession(manager, clientID)

	// add the session to instance
	instanceID, err := manager.irodsFsClientInstanceManager.AddPoolSession(account, session, appName)
	if err != nil {
		return nil, err
	}

	// let session know instance ID
	session.SetIRODSFSClientInstanceID(instanceID)

	manager.sessions[session.GetID()] = session
	return session, nil
}

func (manager *PoolSessionManager) ReleaseSession(sessionID string) {
	logger := log.WithFields(log.Fields{
		"package":  "service",
		"struct":   "PoolSessionManager",
		"function": "ReleaseSession",
	})

	defer irodsfs_common_utils.StackTraceFromPanic(logger)

	manager.mutex.Lock()
	defer manager.mutex.Unlock()

	if session, ok := manager.sessions[sessionID]; ok {
		manager.irodsFsClientInstanceManager.RemovePoolSession(session.GetIRODSFSClientInstanceID(), sessionID)

		session.release()
		delete(manager.sessions, sessionID)
	}
}

func (manager *PoolSessionManager) GetSession(sessionID string) (*PoolSession, error) {
	logger := log.WithFields(log.Fields{
		"package":  "service",
		"struct":   "PoolSessionManager",
		"function": "GetSession",
	})

	defer irodsfs_common_utils.StackTraceFromPanic(logger)

	manager.mutex.RLock()
	defer manager.mutex.RUnlock()

	if session, ok := manager.sessions[sessionID]; ok {
		return session, nil
	}

	return nil, NewSessionNotFoundErrorf("failed to find the pool session for id %s", sessionID)
}

func (manager *PoolSessionManager) GetSessionAndIRODSFSClientInstance(sessionID string) (*PoolSession, *IRODSFSClientInstance, error) {
	logger := log.WithFields(log.Fields{
		"package":  "service",
		"struct":   "PoolSessionManager",
		"function": "GetSessionAndIRODSFSClientInstance",
	})

	defer irodsfs_common_utils.StackTraceFromPanic(logger)

	manager.mutex.RLock()
	defer manager.mutex.RUnlock()

	if session, ok := manager.sessions[sessionID]; ok {
		instanceID := session.GetIRODSFSClientInstanceID()
		instance, err := manager.irodsFsClientInstanceManager.GetInstance(instanceID)
		if err != nil {
			logger.WithError(err).Errorf("failed to find the irods fs client instance for instance id %s and session id %s", instanceID, sessionID)
			return nil, nil, err
		}

		return session, instance, nil
	}

	return nil, nil, NewSessionNotFoundErrorf("failed to find the pool session for id %s", sessionID)
}

func (manager *PoolSessionManager) GetSessionAndIRODSFSClient(sessionID string) (*PoolSession, irodsfs_common_irods.IRODSFSClient, error) {
	logger := log.WithFields(log.Fields{
		"package":  "service",
		"struct":   "PoolSessionManager",
		"function": "GetSessionAndIRODSFSClient",
	})

	defer irodsfs_common_utils.StackTraceFromPanic(logger)

	manager.mutex.RLock()
	defer manager.mutex.RUnlock()

	if session, ok := manager.sessions[sessionID]; ok {
		instanceID := session.GetIRODSFSClientInstanceID()
		instance, err := manager.irodsFsClientInstanceManager.GetInstance(instanceID)
		if err != nil {
			logger.WithError(err).Errorf("failed to find the irods fs client instance for instance id %s and session id %s", instanceID, sessionID)
			return nil, nil, err
		}

		fsClient := instance.GetFSClient()
		return session, fsClient, nil
	}

	return nil, nil, NewSessionNotFoundErrorf("failed to find the pool session for id %s", sessionID)
}

func (manager *PoolSessionManager) GetSessions() []*PoolSession {
	logger := log.WithFields(log.Fields{
		"package":  "service",
		"struct":   "PoolSessionManager",
		"function": "GetSessions",
	})

	defer irodsfs_common_utils.StackTraceFromPanic(logger)

	manager.mutex.RLock()
	defer manager.mutex.RUnlock()

	sessions := make([]*PoolSession, len(manager.sessions))
	idx := 0
	for _, session := range manager.sessions {
		sessions[idx] = session
		idx++
	}

	return sessions
}

func (manager *PoolSessionManager) GetTotalSessions() int {
	logger := log.WithFields(log.Fields{
		"package":  "service",
		"struct":   "PoolSessionManager",
		"function": "GetTotalSessions",
	})

	defer irodsfs_common_utils.StackTraceFromPanic(logger)

	manager.mutex.RLock()
	defer manager.mutex.RUnlock()

	return len(manager.sessions)
}

func (manager *PoolSessionManager) GetTotalIRODSFSClientInstances() int {
	logger := log.WithFields(log.Fields{
		"package":  "service",
		"struct":   "PoolSessionManager",
		"function": "GetTotalIRODSFSClientInstances",
	})

	defer irodsfs_common_utils.StackTraceFromPanic(logger)

	manager.mutex.RLock()
	defer manager.mutex.RUnlock()

	return manager.irodsFsClientInstanceManager.GetTotalInstances()
}

func (manager *PoolSessionManager) GetTotalIRODSFSClientConnections() int {
	logger := log.WithFields(log.Fields{
		"package":  "service",
		"struct":   "PoolSessionManager",
		"function": "GetTotalIRODSFSClientConnections",
	})

	defer irodsfs_common_utils.StackTraceFromPanic(logger)

	manager.mutex.RLock()
	defer manager.mutex.RUnlock()

	return manager.irodsFsClientInstanceManager.GetTotalConnections()
}

func (manager *PoolSessionManager) GetIRODSFSClientInstanceForSession(sessionID string) (*IRODSFSClientInstance, error) {
	logger := log.WithFields(log.Fields{
		"package":  "service",
		"struct":   "PoolSessionManager",
		"function": "GetIRODSFSClientInstanceForSession",
	})

	defer irodsfs_common_utils.StackTraceFromPanic(logger)

	manager.mutex.RLock()
	defer manager.mutex.RUnlock()

	if session, ok := manager.sessions[sessionID]; ok {
		instanceID := session.GetIRODSFSClientInstanceID()
		instance, err := manager.irodsFsClientInstanceManager.GetInstance(instanceID)
		if err != nil {
			logger.WithError(err).Errorf("failed to find the irods fs client instance for instance id %s and session id %s", instanceID, sessionID)
			return nil, err
		}

		return instance, nil
	}

	err := NewSessionNotFoundErrorf("failed to find the session for session id %s", sessionID)
	return nil, err
}

func (manager *PoolSessionManager) GetIRODSFSClientInstances() []*IRODSFSClientInstance {
	logger := log.WithFields(log.Fields{
		"package":  "service",
		"struct":   "PoolSessionManager",
		"function": "GetIRODSFSClientInstances",
	})

	defer irodsfs_common_utils.StackTraceFromPanic(logger)

	manager.mutex.RLock()
	defer manager.mutex.RUnlock()

	return manager.irodsFsClientInstanceManager.GetInstances()
}

// PoolSession is a struct for client login
type PoolSession struct {
	id                      string // pool session id
	poolClientID            string
	irodsFsClientInstanceID string

	lastAccessTime                   time.Time
	irodsFsClientCacheEventHandlerID string
	poolFileHandles                  map[string]*PoolFileHandle
	poolCacheEventHandlers           map[string]irodsclient_fs.FilesystemCacheEventHandler

	mutex sync.RWMutex
}

func newPoolSession(manager *PoolSessionManager, poolClientID string) *PoolSession {
	return &PoolSession{
		id:                      xid.New().String(),
		poolClientID:            poolClientID,
		irodsFsClientInstanceID: "", // to be set later

		lastAccessTime:                   time.Now(),
		irodsFsClientCacheEventHandlerID: "",
		poolFileHandles:                  map[string]*PoolFileHandle{},
		poolCacheEventHandlers:           map[string]irodsclient_fs.FilesystemCacheEventHandler{},

		mutex: sync.RWMutex{},
	}
}

func (session *PoolSession) release() {
	logger := log.WithFields(log.Fields{
		"package":  "service",
		"struct":   "PoolSession",
		"function": "release",
	})

	defer irodsfs_common_utils.StackTraceFromPanic(logger)

	session.mutex.Lock()
	defer session.mutex.Unlock()

	logger.Infof("Release the pool session for session id %s, client id %s", session.id, session.poolClientID)

	for _, fileHandle := range session.poolFileHandles {
		fileHandle.Release()
	}

	session.irodsFsClientInstanceID = ""

	// empty
	session.irodsFsClientCacheEventHandlerID = ""
	session.poolFileHandles = map[string]*PoolFileHandle{}
	session.poolCacheEventHandlers = map[string]irodsclient_fs.FilesystemCacheEventHandler{}
}

func (session *PoolSession) GetID() string {
	session.mutex.RLock()
	defer session.mutex.RUnlock()

	return session.id
}

func (session *PoolSession) GetPoolClientID() string {
	session.mutex.RLock()
	defer session.mutex.RUnlock()

	return session.poolClientID
}

func (session *PoolSession) SetIRODSFSClientInstanceID(irodsFsClientInstanceID string) {
	session.mutex.Lock()
	defer session.mutex.Unlock()

	session.irodsFsClientInstanceID = irodsFsClientInstanceID
}

func (session *PoolSession) GetIRODSFSClientInstanceID() string {
	session.mutex.RLock()
	defer session.mutex.RUnlock()

	return session.irodsFsClientInstanceID
}

func (session *PoolSession) UpdateLastAccessTime() {
	session.mutex.Lock()
	defer session.mutex.Unlock()

	session.lastAccessTime = time.Now()
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

	if poolFileHandle, ok := session.poolFileHandles[poolFileHandleID]; ok {
		return poolFileHandle, nil
	}

	return nil, NewFileHandleNotFoundErrorf("failed to find the pool file handle for handle id %s", poolFileHandleID)
}

func (session *PoolSession) handleCacheEvent(path string, eventType irodsclient_fs.FilesystemCacheEventType) {
	session.mutex.RLock()
	defer session.mutex.RUnlock()

	for _, handler := range session.poolCacheEventHandlers {
		handler(path, eventType)
	}
}

func (session *PoolSession) AddPoolCacheEventHandler(handler irodsclient_fs.FilesystemCacheEventHandler) string {
	handlerID := xid.New().String()

	session.mutex.Lock()
	defer session.mutex.Unlock()

	session.poolCacheEventHandlers[handlerID] = handler

	return handlerID
}

func (session *PoolSession) RemovePoolCacheEventHandler(handlerID string) {
	session.mutex.Lock()
	defer session.mutex.Unlock()

	delete(session.poolCacheEventHandlers, handlerID)
}

func (session *PoolSession) GetPoolCacheEventHandler(handlerID string) (irodsclient_fs.FilesystemCacheEventHandler, error) {
	session.mutex.RLock()
	defer session.mutex.RUnlock()

	if handler, ok := session.poolCacheEventHandlers[handlerID]; ok {
		return handler, nil
	}

	return nil, fmt.Errorf("failed to find the cache event handler for handler id %s", handlerID)
}

func (session *PoolSession) HandleCacheEvent(path string, eventType irodsclient_fs.FilesystemCacheEventType) {
	session.mutex.RLock()
	defer session.mutex.RUnlock()

	for _, handler := range session.poolCacheEventHandlers {
		handler(path, eventType)
	}
}
