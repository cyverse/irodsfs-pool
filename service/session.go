package service

import (
	"sync"
	"time"

	irodsfs_common_irods "github.com/cyverse/irodsfs-common/irods"
	irodsfs_common_utils "github.com/cyverse/irodsfs-common/utils"
	"github.com/cyverse/irodsfs-pool/commons"
	"github.com/cyverse/irodsfs-pool/service/api"
	"github.com/rs/xid"
	log "github.com/sirupsen/logrus"
	"golang.org/x/xerrors"
)

// PoolSessionManager manages PoolSession
type PoolSessionManager struct {
	config                       *PoolServerConfig
	sessions                     map[string]*PoolSession // key: pool session id
	irodsFsClientInstanceManager *IRODSFSClientInstanceManager

	mutex         sync.RWMutex // mutex to access PoolSessionManager
	terminateChan chan bool
}

func NewPoolSessionManager(config *PoolServerConfig) *PoolSessionManager {
	manager := &PoolSessionManager{
		config:                       config,
		sessions:                     map[string]*PoolSession{},
		irodsFsClientInstanceManager: NewIRODSFSClientInstanceManager(config),

		mutex:         sync.RWMutex{},
		terminateChan: make(chan bool),
	}

	// run a goroutine to release stale sessions
	tickerReleaseStaleConnections := time.NewTicker(1 * time.Second)
	defer tickerReleaseStaleConnections.Stop()

	go func() {
		for {
			select {
			case <-manager.terminateChan:
				// terminate
				return
			case <-tickerReleaseStaleConnections.C:
				manager.releaseStaleSessions()
			}
		}
	}()

	return manager
}

func (manager *PoolSessionManager) Release() {
	logger := log.WithFields(log.Fields{
		"package":  "service",
		"struct":   "PoolSessionManager",
		"function": "Release",
	})

	defer irodsfs_common_utils.StackTraceFromPanic(logger)

	manager.terminateChan <- true

	manager.mutex.Lock()
	defer manager.mutex.Unlock()

	wg := sync.WaitGroup{}

	for _, session := range manager.sessions {
		wg.Add(1)

		// release the session first as it needs irodsfs client instance to close open file handles
		go func(sess *PoolSession) {
			defer wg.Done()

			instanceID := sess.GetIRODSFSClientInstanceID()
			sessionID := sess.GetID()

			sess.release()
			manager.irodsFsClientInstanceManager.RemovePoolSession(instanceID, sessionID)
		}(session)
	}

	manager.sessions = map[string]*PoolSession{}

	wg.Wait()

	manager.irodsFsClientInstanceManager.Release()
}

func (manager *PoolSessionManager) NewSession(account *api.Account, clientID string, appName string) (*PoolSession, error) {
	logger := log.WithFields(log.Fields{
		"package":  "service",
		"struct":   "PoolSessionManager",
		"function": "NewSession",
	})

	defer irodsfs_common_utils.StackTraceFromPanic(logger)

	logger.Infof("Creating a new pool session for client id %q", clientID)

	session := newPoolSession(clientID)

	// add the session to instance
	instanceID, err := manager.irodsFsClientInstanceManager.AddPoolSession(account, session, appName)
	if err != nil {
		logger.Errorf("Failed to create a new pool session for session id %q, client id %q: %+v", session.GetID(), session.GetPoolClientID(), err)
		return nil, err
	}

	// let session know instance ID
	session.SetIRODSFSClientInstanceID(instanceID)

	manager.mutex.Lock()
	manager.sessions[session.GetID()] = session
	defer manager.mutex.Unlock()

	logger.Infof("Created a new pool session for session id %q, client id %q", session.GetID(), session.GetPoolClientID())
	return session, nil
}

func (manager *PoolSessionManager) ReleaseSession(sessionID string) {
	logger := log.WithFields(log.Fields{
		"package":  "service",
		"struct":   "PoolSessionManager",
		"function": "ReleaseSession",
	})

	defer irodsfs_common_utils.StackTraceFromPanic(logger)

	logger.Infof("Releasing the pool session for session id %q", sessionID)

	manager.mutex.Lock()

	if session, ok := manager.sessions[sessionID]; ok {
		delete(manager.sessions, sessionID)
		manager.mutex.Unlock()

		instanceID := session.GetIRODSFSClientInstanceID()
		sessionID := session.GetID()

		// release the session first as it needs irodsfs client instance to close open file handles
		session.release()
		manager.irodsFsClientInstanceManager.RemovePoolSession(instanceID, sessionID)
	} else {
		manager.mutex.Unlock()
	}
}

func (manager *PoolSessionManager) releaseStaleSessions() {
	logger := log.WithFields(log.Fields{
		"package":  "service",
		"struct":   "PoolSessionManager",
		"function": "releaseStaleSessions",
	})

	defer irodsfs_common_utils.StackTraceFromPanic(logger)

	staleSessionIDs := []string{}

	manager.mutex.Lock()

	for _, session := range manager.sessions {
		if session.lastAccessTime.Add(time.Duration(manager.config.SessionTimeout)).Before(time.Now()) {
			// stale
			staleSessionIDs = append(staleSessionIDs, session.GetID())
		}
	}

	manager.mutex.Unlock()

	for _, sessionID := range staleSessionIDs {
		if len(manager.terminateChan) > 0 {
			// terminate
			return
		}

		diff := time.Since(manager.sessions[sessionID].lastAccessTime)
		logger.Infof("Releasing the pool session for session id %q as it was idle for %s", sessionID, diff.String())
		manager.ReleaseSession(sessionID)
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

	return nil, xerrors.Errorf("failed to find the pool session for id %q: %w", sessionID, commons.NewSessionNotFoundError(sessionID))
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
			instanceErr := xerrors.Errorf("failed to find the irods fs client instance for instance id %q and session id %q: %w", instanceID, sessionID, err)
			logger.Errorf("%+v", instanceErr)
			return nil, nil, err
		}

		return session, instance, nil
	}

	return nil, nil, xerrors.Errorf("failed to find the pool session for id %q: %w", sessionID, commons.NewSessionNotFoundError(sessionID))
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
			instanceErr := xerrors.Errorf("failed to find the irods fs client instance for instance id %q and session id %q: %w", instanceID, sessionID, err)
			logger.Errorf("%+v", instanceErr)
			return nil, nil, err
		}

		fsClient := instance.GetFSClient()
		return session, fsClient, nil
	}

	return nil, nil, xerrors.Errorf("failed to find the pool session for id %q: %w", sessionID, commons.NewSessionNotFoundError(sessionID))
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
			instanceErr := xerrors.Errorf("failed to find the irods fs client instance for instance id %q and session id %q: %w", instanceID, sessionID, err)
			logger.Errorf("%+v", instanceErr)
			return nil, err
		}

		return instance, nil
	}

	return nil, xerrors.Errorf("failed to find the pool session for id %q: %w", sessionID, commons.NewSessionNotFoundError(sessionID))
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

	lastAccessTime  time.Time
	poolFileHandles map[string]*PoolFileHandle

	mutex sync.RWMutex
}

func newPoolSession(poolClientID string) *PoolSession {
	return &PoolSession{
		id:                      xid.New().String(),
		poolClientID:            poolClientID,
		irodsFsClientInstanceID: "", // to be set later

		lastAccessTime:  time.Now(),
		poolFileHandles: map[string]*PoolFileHandle{},

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

	logger.Infof("Release the pool session for session id %q, client id %q", session.id, session.poolClientID)

	wg := sync.WaitGroup{}
	for _, fileHandle := range session.poolFileHandles {
		wg.Add(1)
		go func(handle *PoolFileHandle) {
			defer wg.Done()
			handle.Release() // release file handles asynchronously
		}(fileHandle)
	}

	wg.Wait()

	session.irodsFsClientInstanceID = ""

	// empty
	session.poolFileHandles = map[string]*PoolFileHandle{}
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

func (session *PoolSession) GetLastAccessTime() time.Time {
	session.mutex.Lock()
	defer session.mutex.Unlock()

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

	if poolFileHandle, ok := session.poolFileHandles[poolFileHandleID]; ok {
		return poolFileHandle, nil
	}

	return nil, xerrors.Errorf("failed to find the pool file handle for handle id %q: %w", poolFileHandleID, commons.NewFileHandleNotFoundError(poolFileHandleID))
}
