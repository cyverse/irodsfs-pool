package service

import (
	"runtime/debug"
	"sync"
	"time"

	"github.com/rs/xid"
	log "github.com/sirupsen/logrus"
)

// Session is a struct for client login
type Session struct {
	id                string // session id
	clientID          string
	irodsConnectionID string

	lastAccessTime time.Time
	fileHandles    map[string]*FileHandle
	mutex          sync.Mutex // mutex to access lastAccessTime, fileHandles
}

func NewSession(clientID string, connectionID string) *Session {
	return &Session{
		id:                xid.New().String(),
		clientID:          clientID,
		irodsConnectionID: connectionID,

		lastAccessTime: time.Now(),
		fileHandles:    map[string]*FileHandle{},
		mutex:          sync.Mutex{},
	}
}

func (session *Session) Release() {
	logger := log.WithFields(log.Fields{
		"package":  "service",
		"struct":   "Session",
		"function": "Release",
	})

	defer func() {
		if r := recover(); r != nil {
			logger.Errorf("stacktrace from panic: %s", string(debug.Stack()))
			logger.Panic(r)
		}
	}()

	session.mutex.Lock()
	defer session.mutex.Unlock()

	for _, fileHandle := range session.fileHandles {
		fileHandle.Release()
	}

	// empty
	session.fileHandles = map[string]*FileHandle{}
}

func (session *Session) GetID() string {
	return session.id
}

func (session *Session) GetClientID() string {
	return session.clientID
}

func (session *Session) GetConnectionID() string {
	return session.irodsConnectionID
}

func (session *Session) UpdateLastAccessTime() {
	session.mutex.Lock()
	defer session.mutex.Unlock()

	session.lastAccessTime = time.Now()
}

func (session *Session) AddFileHandle(fileHandle *FileHandle) {
	session.mutex.Lock()
	defer session.mutex.Unlock()

	session.fileHandles[fileHandle.GetID()] = fileHandle
}

func (session *Session) RemoveFileHandle(fileHandleID string) {
	session.mutex.Lock()
	defer session.mutex.Unlock()

	delete(session.fileHandles, fileHandleID)
}

func (session *Session) GetFileHandle(fileHandleID string) *FileHandle {
	session.mutex.Lock()
	defer session.mutex.Unlock()

	if handle, ok := session.fileHandles[fileHandleID]; ok {
		return handle
	}
	return nil
}
