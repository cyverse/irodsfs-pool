package service

import (
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"sync"
	"time"

	irodsclient_fs "github.com/cyverse/go-irodsclient/fs"
	irodsclient_types "github.com/cyverse/go-irodsclient/irods/types"
	irodsfs_common_irods "github.com/cyverse/irodsfs-common/irods"
	irodsfs_common_utils "github.com/cyverse/irodsfs-common/utils"
	"github.com/cyverse/irodsfs-pool/commons"
	"github.com/cyverse/irodsfs-pool/service/api"
	log "github.com/sirupsen/logrus"
)

// IRODSFSClientInstance is a struct for iRODS FS Client Instance
type IRODSFSClientInstance struct {
	id            string // iRODS fs client instance id
	irodsAccount  *irodsclient_types.IRODSAccount
	irodsFsClient irodsfs_common_irods.IRODSFSClient

	poolSessions map[string]bool // key: session id, value: just true
	mutex        sync.Mutex      // mutex to access pool sessions
}

// MakeIRODSFSClientInstanceID creates an iRODSFSClientInstance ID
func MakeIRODSFSClientInstanceID(account *api.Account) string {
	hash := sha1.New()
	hash.Write([]byte(account.Host))
	hash.Write([]byte(fmt.Sprintf("%d", account.Port)))
	hash.Write([]byte(account.ClientUser))
	hash.Write([]byte(account.ClientZone))
	hash.Write([]byte(account.ProxyUser))
	hash.Write([]byte(account.ProxyZone))
	hash.Write([]byte(account.Password))
	hash.Write([]byte(account.Ticket))
	hash.Write([]byte(account.DefaultResource))
	return hex.EncodeToString(hash.Sum(nil))
}

// NewIRODSFSClientInstance creates a new IRODSFSClientInstance
func NewIRODSFSClientInstance(irodsFsClientInstanceID string, account *api.Account, applicationName string, cacheTimeoutSettings []commons.MetadataCacheTimeoutSetting) (*IRODSFSClientInstance, error) {
	logger := log.WithFields(log.Fields{
		"package":  "service",
		"function": "NewIRODSFSClientInstance",
	})

	defer irodsfs_common_utils.StackTraceFromPanic(logger)

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
		Password:                account.Password,
		Ticket:                  account.Ticket,
		DefaultResource:         account.DefaultResource,
		PamTTL:                  int(account.PamTtl),
	}

	irodsConfig := irodsclient_fs.NewFileSystemConfigWithDefault(applicationName)

	for _, cacheTimeoutSetting := range cacheTimeoutSettings {
		cacheTimeoutSettingConv := irodsclient_fs.MetadataCacheTimeoutSetting{
			Path:    cacheTimeoutSetting.Path,
			Timeout: time.Duration(cacheTimeoutSetting.Timeout),
			Inherit: cacheTimeoutSetting.Inherit,
		}

		irodsConfig.CacheTimeoutSettings = append(irodsConfig.CacheTimeoutSettings, cacheTimeoutSettingConv)
	}

	irodsFsClient, err := irodsfs_common_irods.NewIRODSFSClientDirect(irodsAccount, irodsConfig)
	if err != nil {
		return nil, err
	}

	return &IRODSFSClientInstance{
		id:            irodsFsClientInstanceID,
		irodsAccount:  irodsAccount,
		irodsFsClient: irodsFsClient,

		poolSessions: map[string]bool{},
		mutex:        sync.Mutex{},
	}, nil
}

func (instance *IRODSFSClientInstance) Release() {
	logger := log.WithFields(log.Fields{
		"package":  "service",
		"struct":   "IRODSFSClientInstance",
		"function": "Release",
	})

	defer irodsfs_common_utils.StackTraceFromPanic(logger)

	instance.mutex.Lock()
	defer instance.mutex.Unlock()

	// close file system for connection
	if instance.irodsFsClient != nil {
		instance.irodsFsClient.Release()
		instance.irodsFsClient = nil
	}

	// empty
	instance.poolSessions = map[string]bool{}
}

func (instance *IRODSFSClientInstance) GetID() string {
	return instance.id
}

func (instance *IRODSFSClientInstance) GetFSClient() irodsfs_common_irods.IRODSFSClient {
	instance.mutex.Lock()
	defer instance.mutex.Unlock()

	return instance.irodsFsClient
}

func (instance *IRODSFSClientInstance) AddPoolSession(poolSession *PoolSession) {
	instance.mutex.Lock()
	defer instance.mutex.Unlock()

	instance.poolSessions[poolSession.GetID()] = true
	poolSession.SetIRODSFSClientInstanceID(instance.id)
}

func (instance *IRODSFSClientInstance) RemovePoolSession(poolSessionID string) {
	instance.mutex.Lock()
	defer instance.mutex.Unlock()

	delete(instance.poolSessions, poolSessionID)
}

func (instance *IRODSFSClientInstance) GetPoolSessions() int {
	instance.mutex.Lock()
	defer instance.mutex.Unlock()

	return len(instance.poolSessions)
}

func (instance *IRODSFSClientInstance) ReleaseIfNoPoolSession() bool {
	logger := log.WithFields(log.Fields{
		"package":  "service",
		"struct":   "IRODSFSClientInstance",
		"function": "ReleaseIfNoSession",
	})

	defer irodsfs_common_utils.StackTraceFromPanic(logger)

	instance.mutex.Lock()
	defer instance.mutex.Unlock()

	if len(instance.poolSessions) == 0 {
		// close file system for connection
		if instance.irodsFsClient != nil {
			instance.irodsFsClient.Release()
			instance.irodsFsClient = nil
		}

		return true
	}

	return false
}
