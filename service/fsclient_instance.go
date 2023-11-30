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
	"golang.org/x/xerrors"
)

// IRODSFSClientInstanceManager manages IRODSFSClientInstance
type IRODSFSClientInstanceManager struct {
	config    *PoolServerConfig
	instances map[string]*IRODSFSClientInstance // key: iRODS FS Client instance id

	mutex sync.RWMutex
}

func NewIRODSFSClientInstanceManager(config *PoolServerConfig) *IRODSFSClientInstanceManager {
	return &IRODSFSClientInstanceManager{
		config:    config,
		instances: map[string]*IRODSFSClientInstance{},

		mutex: sync.RWMutex{},
	}
}

func (manager *IRODSFSClientInstanceManager) Release() {
	logger := log.WithFields(log.Fields{
		"package":  "service",
		"struct":   "IRODSFSClientInstanceManager",
		"function": "Release",
	})

	defer irodsfs_common_utils.StackTraceFromPanic(logger)

	manager.mutex.Lock()
	defer manager.mutex.Unlock()

	for _, instance := range manager.instances {
		instance.release()
	}

	manager.instances = map[string]*IRODSFSClientInstance{}
}

func (manager *IRODSFSClientInstanceManager) AddPoolSession(account *api.Account, session *PoolSession, appName string) (string, error) {
	logger := log.WithFields(log.Fields{
		"package":  "service",
		"struct":   "IRODSFSClientInstanceManager",
		"function": "AddPoolSession",
	})

	defer irodsfs_common_utils.StackTraceFromPanic(logger)

	manager.mutex.Lock()
	defer manager.mutex.Unlock()

	instanceID := manager.makeInstanceID(account)

	if instance, ok := manager.instances[instanceID]; ok {
		// if we have the instance already
		logger.Infof("Reusing the existing irods fs client instance %q for session %q", instanceID, session.GetID())

		instance.addPoolSession(session)
	} else {
		logger.Infof("Creating a new irods fs client instance %q for session %q", instanceID, session.GetID())

		// new irods fs client instance
		instance, err := newIRODSFSClientInstance(instanceID, account, appName, manager.config.CacheTimeoutSettings)
		if err != nil {
			instanceErr := xerrors.Errorf("failed to create a new irods fs client instance: %w", err)
			logger.Errorf("%+v", instanceErr)
			return "", err
		}

		instance.addPoolSession(session)
		manager.instances[instanceID] = instance
	}

	return instanceID, nil
}

func (manager *IRODSFSClientInstanceManager) RemovePoolSession(instanceID string, sessionID string) {
	logger := log.WithFields(log.Fields{
		"package":  "service",
		"struct":   "IRODSFSClientInstanceManager",
		"function": "RemovePoolSession",
	})

	defer irodsfs_common_utils.StackTraceFromPanic(logger)

	manager.mutex.Lock()
	defer manager.mutex.Unlock()

	if instance, ok := manager.instances[instanceID]; ok {
		instance.removePoolSession(sessionID)

		// if there's no sessions using the instance, release it
		if instance.getPoolSessions() == 0 {
			instance.release()
		}

		delete(manager.instances, instanceID)
	}
}

func (manager *IRODSFSClientInstanceManager) GetInstance(instanceID string) (*IRODSFSClientInstance, error) {
	logger := log.WithFields(log.Fields{
		"package":  "service",
		"struct":   "IRODSFSClientInstanceManager",
		"function": "GetInstance",
	})

	defer irodsfs_common_utils.StackTraceFromPanic(logger)

	manager.mutex.RLock()
	defer manager.mutex.RUnlock()

	if instance, ok := manager.instances[instanceID]; ok {
		return instance, nil
	}

	return nil, xerrors.Errorf("failed to find the irods fs client instance for instance id %q: %w", instanceID, commons.NewIRODSFSClientInstanceNotFoundError(instanceID))
}

func (manager *IRODSFSClientInstanceManager) GetInstances() []*IRODSFSClientInstance {
	logger := log.WithFields(log.Fields{
		"package":  "service",
		"struct":   "IRODSFSClientInstanceManager",
		"function": "GetInstances",
	})

	defer irodsfs_common_utils.StackTraceFromPanic(logger)

	manager.mutex.RLock()
	defer manager.mutex.RUnlock()

	instances := make([]*IRODSFSClientInstance, len(manager.instances))
	idx := 0
	for _, instance := range manager.instances {
		instances[idx] = instance
		idx++
	}

	return instances
}

func (manager *IRODSFSClientInstanceManager) GetTotalInstances() int {
	logger := log.WithFields(log.Fields{
		"package":  "service",
		"struct":   "IRODSFSClientInstanceManager",
		"function": "GetTotalInstances",
	})

	defer irodsfs_common_utils.StackTraceFromPanic(logger)

	manager.mutex.RLock()
	defer manager.mutex.RUnlock()

	return len(manager.instances)
}

func (manager *IRODSFSClientInstanceManager) GetTotalConnections() int {
	logger := log.WithFields(log.Fields{
		"package":  "service",
		"struct":   "IRODSFSClientInstanceManager",
		"function": "GetTotalConnections",
	})

	defer irodsfs_common_utils.StackTraceFromPanic(logger)

	manager.mutex.RLock()
	defer manager.mutex.RUnlock()

	connections := 0
	for _, instance := range manager.instances {
		connections += instance.GetFSClient().GetConnections()
	}

	return connections
}

// makeInstanceID creates an ID of iRODSFSClientInstance
func (manager *IRODSFSClientInstanceManager) makeInstanceID(account *api.Account) string {
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

// IRODSFSClientInstance is a struct for iRODS FS Client Instance
type IRODSFSClientInstance struct {
	id            string // iRODS fs client instance id
	irodsAccount  *irodsclient_types.IRODSAccount
	irodsFsClient irodsfs_common_irods.IRODSFSClient

	cacheEventHandlerID string

	poolSessions map[string]*PoolSession // key: session id
	mutex        sync.RWMutex            // mutex to access pool sessions
}

// newIRODSFSClientInstance creates a new IRODSFSClientInstance
func newIRODSFSClientInstance(irodsFsClientInstanceID string, account *api.Account, applicationName string, cacheTimeoutSettings []commons.MetadataCacheTimeoutSetting) (*IRODSFSClientInstance, error) {
	logger := log.WithFields(log.Fields{
		"package":  "service",
		"function": "newIRODSFSClientInstance",
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

	instance := &IRODSFSClientInstance{
		id:            irodsFsClientInstanceID,
		irodsAccount:  irodsAccount,
		irodsFsClient: irodsFsClient,

		poolSessions: map[string]*PoolSession{},
		mutex:        sync.RWMutex{},
	}

	cacheEventHandlerID, err := irodsFsClient.AddCacheEventHandler(instance.handleCacheEvent)
	if err != nil {
		irodsFsClient.Release()
		return nil, err
	}

	instance.cacheEventHandlerID = cacheEventHandlerID

	return instance, nil
}

func (instance *IRODSFSClientInstance) release() {
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
		if len(instance.cacheEventHandlerID) > 0 {
			instance.irodsFsClient.RemoveCacheEventHandler(instance.cacheEventHandlerID)
			instance.cacheEventHandlerID = ""
		}

		instance.irodsFsClient.Release()
		instance.irodsFsClient = nil
	}

	// empty
	instance.poolSessions = map[string]*PoolSession{}
}

func (instance *IRODSFSClientInstance) handleCacheEvent(path string, eventType irodsclient_fs.FilesystemCacheEventType) {
	instance.mutex.RLock()
	defer instance.mutex.RUnlock()

	for _, session := range instance.poolSessions {
		session.handleCacheEvent(path, eventType)
	}
}

func (instance *IRODSFSClientInstance) GetID() string {
	instance.mutex.RLock()
	defer instance.mutex.RUnlock()

	return instance.id
}

func (instance *IRODSFSClientInstance) GetFSClient() irodsfs_common_irods.IRODSFSClient {
	instance.mutex.RLock()
	defer instance.mutex.RUnlock()

	return instance.irodsFsClient
}

func (instance *IRODSFSClientInstance) addPoolSession(poolSession *PoolSession) {
	instance.mutex.Lock()
	defer instance.mutex.Unlock()

	instance.poolSessions[poolSession.GetID()] = poolSession
}

func (instance *IRODSFSClientInstance) removePoolSession(sessionID string) {
	instance.mutex.Lock()
	defer instance.mutex.Unlock()

	delete(instance.poolSessions, sessionID)
}

func (instance *IRODSFSClientInstance) getPoolSessions() int {
	instance.mutex.RLock()
	defer instance.mutex.RUnlock()

	return len(instance.poolSessions)
}
