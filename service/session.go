package service

import (
	"sync"
	"time"

	irodsfs_common_utils "github.com/cyverse/irodsfs-common/utils"
	"github.com/rs/xid"
	log "github.com/sirupsen/logrus"
)

// PoolSession is a struct for client login
type PoolSession struct {
	id                      string // pool session id
	poolClientID            string
	irodsFsClientInstanceID string

	lastAccessTime  time.Time
	poolFileHandles map[string]*PoolFileHandle
	mutex           sync.Mutex // mutex to access lastAccessTime, poolFileHandles
}

func NewPoolSession(poolClientID string) *PoolSession {
	return &PoolSession{
		id:                      xid.New().String(),
		poolClientID:            poolClientID,
		irodsFsClientInstanceID: "",

		lastAccessTime:  time.Now(),
		poolFileHandles: map[string]*PoolFileHandle{},
		mutex:           sync.Mutex{},
	}
}

func (session *PoolSession) Release() {
	logger := log.WithFields(log.Fields{
		"package":  "service",
		"struct":   "PoolSession",
		"function": "Release",
	})

	defer irodsfs_common_utils.StackTraceFromPanic(logger)

	session.mutex.Lock()
	defer session.mutex.Unlock()

	for _, fileHandle := range session.poolFileHandles {
		fileHandle.Release()
	}

	// empty
	session.poolFileHandles = map[string]*PoolFileHandle{}
}

func (session *PoolSession) GetID() string {
	return session.id
}

func (session *PoolSession) GetPoolClientID() string {
	return session.poolClientID
}

func (session *PoolSession) SetIRODSFSClientInstanceID(irodsFsClientInstanceID string) {
	session.irodsFsClientInstanceID = irodsFsClientInstanceID
}

func (session *PoolSession) GetIRODSFSClientInstanceID() string {
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

func (session *PoolSession) GetPoolFileHandle(poolFileHandleID string) *PoolFileHandle {
	session.mutex.Lock()
	defer session.mutex.Unlock()

	if poolFileHandle, ok := session.poolFileHandles[poolFileHandleID]; ok {
		return poolFileHandle
	}
	return nil
}
