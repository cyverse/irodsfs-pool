package service

import (
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"runtime/debug"
	"sync"

	irodsclient_fs "github.com/cyverse/go-irodsclient/fs"
	irodsclient_types "github.com/cyverse/go-irodsclient/irods/types"
	"github.com/cyverse/irodsfs-pool/service/api"
	log "github.com/sirupsen/logrus"
)

// IRODSConnection is a struct for iRODS Connection
type IRODSConnection struct {
	id      string // connection id
	account *irodsclient_types.IRODSAccount
	irodsFS *irodsclient_fs.FileSystem

	sessions map[string]bool // sessions referencing this, key: session id
	mutex    sync.Mutex      // mutex to access sessions
}

func getConnectionID(account *api.Account) string {
	hash := sha1.New()
	hash.Write([]byte(account.Host))
	hash.Write([]byte(fmt.Sprintf("%d", account.Port)))
	hash.Write([]byte(account.ClientUser))
	hash.Write([]byte(account.ClientZone))
	hash.Write([]byte(account.ProxyUser))
	hash.Write([]byte(account.ProxyZone))
	hash.Write([]byte(account.ServerDn))
	hash.Write([]byte(account.Password))
	hash.Write([]byte(account.Ticket))
	return hex.EncodeToString(hash.Sum(nil))
}

func NewIRODSConnection(connectionID string, account *api.Account, applicationName string) (*IRODSConnection, error) {
	logger := log.WithFields(log.Fields{
		"package":  "service",
		"function": "NewIRODSConnection",
	})

	defer func() {
		if r := recover(); r != nil {
			logger.Errorf("stacktrace from panic: %s", string(debug.Stack()))
			logger.Panic(r)
		}
	}()

	irodsAccount := &irodsclient_types.IRODSAccount{
		AuthenticationScheme:    irodsclient_types.AuthScheme(account.AuthenticationScheme),
		ClientServerNegotiation: account.ClientServerNegotiation,
		CSNegotiationPolicy:     irodsclient_types.CSNegotiationRequire(account.CsNegotiationPolicy),
		Host:                    account.Host,
		Port:                    int(account.Port),
		ClientUser:              account.ClientUser,
		ClientZone:              account.ClientZone,
		ProxyUser:               account.ProxyUser,
		ProxyZone:               account.ProxyZone,
		ServerDN:                account.ServerDn,
		Password:                account.Password,
		Ticket:                  account.Ticket,
		PamTTL:                  int(account.PamTtl),
	}

	irodsFS, err := irodsclient_fs.NewFileSystemWithDefault(irodsAccount, applicationName)
	if err != nil {
		return nil, err
	}

	return &IRODSConnection{
		id:      connectionID,
		account: irodsAccount,
		irodsFS: irodsFS,

		sessions: map[string]bool{},
		mutex:    sync.Mutex{},
	}, nil
}

func (connection *IRODSConnection) Release() {
	logger := log.WithFields(log.Fields{
		"package":  "service",
		"struct":   "IRODSConnection",
		"function": "Release",
	})

	defer func() {
		if r := recover(); r != nil {
			logger.Errorf("stacktrace from panic: %s", string(debug.Stack()))
			logger.Panic(r)
		}
	}()

	connection.mutex.Lock()
	defer connection.mutex.Unlock()

	// close file system for connection
	if connection.irodsFS != nil {
		connection.irodsFS.Release()
		connection.irodsFS = nil
	}

	// empty
	connection.sessions = map[string]bool{}
}

func (connection *IRODSConnection) GetID() string {
	return connection.id
}

func (connection *IRODSConnection) GetIRODSFS() *irodsclient_fs.FileSystem {
	connection.mutex.Lock()
	defer connection.mutex.Unlock()

	return connection.irodsFS
}

func (connection *IRODSConnection) AddSession(sessionID string) {
	connection.mutex.Lock()
	defer connection.mutex.Unlock()

	connection.sessions[sessionID] = true
}

func (connection *IRODSConnection) RemoveSession(sessionID string) {
	connection.mutex.Lock()
	defer connection.mutex.Unlock()

	delete(connection.sessions, sessionID)
}

func (connection *IRODSConnection) GetSessions() int {
	connection.mutex.Lock()
	defer connection.mutex.Unlock()

	return len(connection.sessions)
}

func (connection *IRODSConnection) ReleaseIfNoSession() bool {
	logger := log.WithFields(log.Fields{
		"package":  "service",
		"struct":   "IRODSConnection",
		"function": "ReleaseIfNoSession",
	})

	defer func() {
		if r := recover(); r != nil {
			logger.Errorf("stacktrace from panic: %s", string(debug.Stack()))
			logger.Panic(r)
		}
	}()

	connection.mutex.Lock()
	defer connection.mutex.Unlock()

	if len(connection.sessions) == 0 {
		// close file system for connection
		if connection.irodsFS != nil {
			connection.irodsFS.Release()
			connection.irodsFS = nil
		}

		return true
	}

	return false
}
