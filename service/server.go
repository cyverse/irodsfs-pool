package service

import (
	"context"
	"fmt"
	"io"
	"sync"

	irodsclient_types "github.com/cyverse/go-irodsclient/irods/types"
	irodsfs_common_cache "github.com/cyverse/irodsfs-common/io/cache"
	irodsfs_common "github.com/cyverse/irodsfs-common/irods"
	irodsfs_common_utils "github.com/cyverse/irodsfs-common/utils"
	"github.com/cyverse/irodsfs-pool/commons"
	"github.com/cyverse/irodsfs-pool/service/api"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	cacheEntrySizeMax int = 4 * 1024 * 1024 // 4MB
)

// PoolServerConfig is a configuration for Server
type PoolServerConfig struct {
	CacheSizeMax         int64
	CacheRootPath        string
	CacheTimeoutSettings []commons.MetadataCacheTimeoutSetting
	TempRootPath         string
}

// PoolServer is a struct for PoolServer
type PoolServer struct {
	api.UnimplementedPoolAPIServer

	config *PoolServerConfig

	cacheStore irodsfs_common_cache.CacheStore

	poolSessions           map[string]*PoolSession           // key: pool session id
	irodsFsClientInstances map[string]*IRODSFSClientInstance // key: iRODS FS Client instance id
	mutex                  sync.RWMutex                      // mutex to access Sessions
}

func NewPoolServer(config *PoolServerConfig) (*PoolServer, error) {
	var err error
	var diskCacheStore irodsfs_common_cache.CacheStore
	if config.CacheSizeMax > 0 {
		diskCacheStore, err = irodsfs_common_cache.NewDiskCacheStore(config.CacheSizeMax, cacheEntrySizeMax, config.CacheRootPath)
		if err != nil {
			return nil, err
		}
	}

	return &PoolServer{
		config: config,

		cacheStore: diskCacheStore,

		poolSessions:           map[string]*PoolSession{},
		irodsFsClientInstances: map[string]*IRODSFSClientInstance{},
		mutex:                  sync.RWMutex{},
	}, nil
}

func (server *PoolServer) Release() {
	logger := log.WithFields(log.Fields{
		"package":  "service",
		"struct":   "PoolServer",
		"function": "Release",
	})

	defer irodsfs_common_utils.StackTraceFromPanic(logger)

	server.mutex.Lock()

	for _, session := range server.poolSessions {
		logger.Infof("Logout pool session for pool client id %s", session.GetPoolClientID())

		session.Release()
	}
	server.poolSessions = map[string]*PoolSession{}

	for _, irodsFsClientInstance := range server.irodsFsClientInstances {
		logger.Infof("Release irods fs client instance for id %s", irodsFsClientInstance.GetID())

		// close file system for connection
		irodsFsClientInstance.Release()
	}
	server.irodsFsClientInstances = map[string]*IRODSFSClientInstance{}

	server.mutex.Unlock()

	if server.cacheStore != nil {
		server.cacheStore.Release()
		server.cacheStore = nil
	}
}

func (server *PoolServer) errorToStatus(err error) error {
	if err == nil {
		return nil
	}

	if irodsclient_types.IsFileNotFoundError(err) {
		return status.Error(codes.NotFound, err.Error())
	} else if irodsclient_types.IsCollectionNotEmptyError(err) {
		// there's no matching error type for not empty
		return status.Error(codes.AlreadyExists, err.Error())
	}

	return status.Error(codes.Internal, err.Error())
}

func (server *PoolServer) Login(context context.Context, request *api.LoginRequest) (*api.LoginResponse, error) {
	logger := log.WithFields(log.Fields{
		"package":  "service",
		"struct":   "PoolServer",
		"function": "Login",
	})

	defer irodsfs_common_utils.StackTraceFromPanic(logger)

	logger.Infof("Login request from client id %s, host %s, user %s", request.ClientId, request.Account.Host, request.Account.ClientUser)
	defer logger.Infof("Login response to client id %s, host %s, user %s", request.ClientId, request.Account.Host, request.Account.ClientUser)

	server.mutex.Lock()
	defer server.mutex.Unlock()

	irodsFsClientInstanceID := MakeIRODSFSClientInstanceID(request.Account)

	// create a poolSession
	poolSession := NewPoolSession(request.ClientId)
	poolSessionID := poolSession.GetID()

	// find irods fs client instance for the same account if exists
	if irodsFsClientInstance, ok := server.irodsFsClientInstances[irodsFsClientInstanceID]; ok {
		logger.Infof("Reusing existing irods fs client instance: %s", irodsFsClientInstanceID)

		irodsFsClientInstance.AddPoolSession(poolSession)
	} else {
		logger.Infof("Creating a new irods fs client instance: %s", irodsFsClientInstanceID)

		// new irods fs client instance
		newIrodsFsClientInstance, err := NewIRODSFSClientInstance(irodsFsClientInstanceID, request.Account, request.ApplicationName, server.config.CacheTimeoutSettings)
		if err != nil {
			logger.WithError(err).Error("failed to create a new irods fs client instance")
			return nil, server.errorToStatus(err)
		}

		newIrodsFsClientInstance.AddPoolSession(poolSession)
		server.irodsFsClientInstances[irodsFsClientInstanceID] = newIrodsFsClientInstance
	}

	server.poolSessions[poolSessionID] = poolSession

	response := &api.LoginResponse{
		SessionId: poolSessionID,
	}

	return response, nil
}

func (server *PoolServer) Logout(context context.Context, request *api.LogoutRequest) (*api.Empty, error) {
	logger := log.WithFields(log.Fields{
		"package":  "service",
		"struct":   "PoolServer",
		"function": "Logout",
	})

	defer irodsfs_common_utils.StackTraceFromPanic(logger)

	logger.Infof("Logout request from client, pool session id %s", request.SessionId)
	defer logger.Infof("Logout response to client, pool session id %s", request.SessionId)

	server.mutex.Lock()
	defer server.mutex.Unlock()

	if poolSession, ok := server.poolSessions[request.SessionId]; ok {
		logger.Infof("Logout pool session for pool client id %s", poolSession.GetPoolClientID())

		irodsFsClientInstanceID := poolSession.GetIRODSFSClientInstanceID()
		poolSession.Release()
		delete(server.poolSessions, request.SessionId)

		if irodsFsClientInstance, ok := server.irodsFsClientInstances[irodsFsClientInstanceID]; ok {
			irodsFsClientInstance.RemovePoolSession(request.SessionId)
			if irodsFsClientInstance.ReleaseIfNoPoolSession() {
				delete(server.irodsFsClientInstances, irodsFsClientInstanceID)
			}
		}

		return &api.Empty{}, nil
	}

	// session might be already closed due to timeout
	return &api.Empty{}, nil
}

func (server *PoolServer) LogoutAll() {
	logger := log.WithFields(log.Fields{
		"package":  "service",
		"struct":   "PoolServer",
		"function": "LogoutAll",
	})

	defer irodsfs_common_utils.StackTraceFromPanic(logger)

	logger.Info("Logout All")
	defer logger.Info("Logged-out All")

	server.mutex.Lock()
	defer server.mutex.Unlock()

	for _, session := range server.poolSessions {
		logger.Infof("Logout pool session for pool client id %s", session.GetPoolClientID())

		session.Release()
	}
	server.poolSessions = map[string]*PoolSession{}

	for _, irodsFsClientInstance := range server.irodsFsClientInstances {
		logger.Infof("Release irods fs client instances for instance id %s", irodsFsClientInstance.GetID())

		// close fs client instance
		irodsFsClientInstance.Release()
	}
	server.irodsFsClientInstances = map[string]*IRODSFSClientInstance{}
}

func (server *PoolServer) GetPoolSessions() int {
	server.mutex.Lock()
	defer server.mutex.Unlock()

	return len(server.poolSessions)
}

func (server *PoolServer) GetIRODSFSClientInstanceCount() int {
	server.mutex.Lock()
	defer server.mutex.Unlock()

	return len(server.irodsFsClientInstances)
}

func (server *PoolServer) GetIRODSConnections() int {
	logger := log.WithFields(log.Fields{
		"package":  "service",
		"struct":   "PoolServer",
		"function": "GetIRODSConnections",
	})

	defer irodsfs_common_utils.StackTraceFromPanic(logger)

	server.mutex.Lock()
	defer server.mutex.Unlock()

	connections := 0
	for _, irodsFsClientInstance := range server.irodsFsClientInstances {
		connections += irodsFsClientInstance.irodsFsClient.GetConnections()
	}

	return connections
}

func (server *PoolServer) getPoolSessionAndFsClientInstance(poolSessionID string) (*PoolSession, *IRODSFSClientInstance, error) {
	logger := log.WithFields(log.Fields{
		"package":  "service",
		"struct":   "PoolServer",
		"function": "getPoolSessionAndFsClientInstance",
	})

	defer irodsfs_common_utils.StackTraceFromPanic(logger)

	server.mutex.RLock()
	defer server.mutex.RUnlock()

	poolSession, ok := server.poolSessions[poolSessionID]
	if !ok {
		err := fmt.Errorf("cannot find pool session for id %s", poolSessionID)
		logger.Error(err)
		return nil, nil, err
	}

	irodsFsClientInstanceID := poolSession.GetIRODSFSClientInstanceID()

	irodsFsClientInstance, ok := server.irodsFsClientInstances[irodsFsClientInstanceID]
	if !ok {
		err := fmt.Errorf("cannot find irods fs client instance for id %s", irodsFsClientInstanceID)
		logger.Error(err)
		return nil, nil, err
	}

	return poolSession, irodsFsClientInstance, nil
}

func (server *PoolServer) getPoolFileHandle(poolSessionID string, poolFileHandleID string) (*PoolFileHandle, error) {
	logger := log.WithFields(log.Fields{
		"package":  "service",
		"struct":   "PoolServer",
		"function": "getPoolFileHandle",
	})

	defer irodsfs_common_utils.StackTraceFromPanic(logger)

	server.mutex.RLock()
	defer server.mutex.RUnlock()

	poolSession, ok := server.poolSessions[poolSessionID]
	if !ok {
		err := fmt.Errorf("cannot find pool session for id %s", poolSessionID)
		logger.Error(err)
		return nil, err
	}

	poolSession.UpdateLastAccessTime()

	poolFileHandle := poolSession.GetPoolFileHandle(poolFileHandleID)
	if poolFileHandle == nil {
		err := fmt.Errorf("failed to find pool file handle %s", poolFileHandleID)
		logger.Error(err)
		return nil, err
	}

	return poolFileHandle, nil
}

func (server *PoolServer) List(context context.Context, request *api.ListRequest) (*api.ListResponse, error) {
	logger := log.WithFields(log.Fields{
		"package":  "service",
		"struct":   "PoolServer",
		"function": "List",
	})

	defer irodsfs_common_utils.StackTraceFromPanic(logger)

	logger.Infof("List request from pool session id %s, path %s", request.SessionId, request.Path)
	defer logger.Infof("List response to pool session id %s, path %s", request.SessionId, request.Path)

	poolSession, irodsFsClientInstance, err := server.getPoolSessionAndFsClientInstance(request.SessionId)
	if err != nil {
		logger.Error(err)
		return nil, server.errorToStatus(err)
	}

	poolSession.UpdateLastAccessTime()

	fsClient := irodsFsClientInstance.GetFSClient()
	if fsClient == nil {
		err = fmt.Errorf("failed to get FS Client from irods fs client instance")
		logger.Error(err)
		return nil, server.errorToStatus(err)
	}

	entries, err := fsClient.List(request.Path)
	if err != nil {
		logger.Error(err)
		return nil, server.errorToStatus(err)
	}

	responseEntries := []*api.Entry{}
	for _, entry := range entries {
		responseEntry := &api.Entry{
			Id:         entry.ID,
			Type:       string(entry.Type),
			Name:       entry.Name,
			Path:       entry.Path,
			Owner:      entry.Owner,
			Size:       entry.Size,
			DataType:   entry.DataType,
			CreateTime: irodsfs_common_utils.MakeTimeToString(entry.CreateTime),
			ModifyTime: irodsfs_common_utils.MakeTimeToString(entry.ModifyTime),
			Checksum:   entry.CheckSum,
		}
		responseEntries = append(responseEntries, responseEntry)
	}

	response := &api.ListResponse{
		Entries: responseEntries,
	}

	return response, nil
}

func (server *PoolServer) Stat(context context.Context, request *api.StatRequest) (*api.StatResponse, error) {
	logger := log.WithFields(log.Fields{
		"package":  "service",
		"struct":   "PoolServer",
		"function": "Stat",
	})

	defer irodsfs_common_utils.StackTraceFromPanic(logger)

	logger.Infof("Stat request from pool session id %s, path %s", request.SessionId, request.Path)
	defer logger.Infof("Stat response to pool session id %s, path %s", request.SessionId, request.Path)

	poolSession, irodsFsClientInstance, err := server.getPoolSessionAndFsClientInstance(request.SessionId)
	if err != nil {
		logger.Error(err)
		return nil, server.errorToStatus(err)
	}

	poolSession.UpdateLastAccessTime()

	fsClient := irodsFsClientInstance.GetFSClient()
	if fsClient == nil {
		err = fmt.Errorf("failed to get FS Client from irods fs client instance")
		logger.Error(err)
		return nil, server.errorToStatus(err)
	}

	entry, err := fsClient.Stat(request.Path)
	if err != nil {
		logger.Error(err)
		return nil, server.errorToStatus(err)
	}

	responseEntry := &api.Entry{
		Id:         entry.ID,
		Type:       string(entry.Type),
		Name:       entry.Name,
		Path:       entry.Path,
		Owner:      entry.Owner,
		Size:       entry.Size,
		DataType:   entry.DataType,
		CreateTime: irodsfs_common_utils.MakeTimeToString(entry.CreateTime),
		ModifyTime: irodsfs_common_utils.MakeTimeToString(entry.ModifyTime),
		Checksum:   entry.CheckSum,
	}

	response := &api.StatResponse{
		Entry: responseEntry,
	}

	return response, nil
}

func (server *PoolServer) ExistsDir(context context.Context, request *api.ExistsDirRequest) (*api.ExistsDirResponse, error) {
	logger := log.WithFields(log.Fields{
		"package":  "service",
		"struct":   "PoolServer",
		"function": "ExistsDir",
	})

	defer irodsfs_common_utils.StackTraceFromPanic(logger)

	logger.Infof("ExistsDir request from pool session id %s, path %s", request.SessionId, request.Path)
	defer logger.Infof("ExistsDir response to pool session id %s, path %s", request.SessionId, request.Path)

	poolSession, irodsFsClientInstance, err := server.getPoolSessionAndFsClientInstance(request.SessionId)
	if err != nil {
		logger.Error(err)
		return nil, server.errorToStatus(err)
	}

	poolSession.UpdateLastAccessTime()

	fsClient := irodsFsClientInstance.GetFSClient()
	if fsClient == nil {
		err = fmt.Errorf("failed to get FS Client from irods fs client instance")
		logger.Error(err)
		return nil, server.errorToStatus(err)
	}

	exist := fsClient.ExistsDir(request.Path)
	return &api.ExistsDirResponse{
		Exist: exist,
	}, nil
}

func (server *PoolServer) ExistsFile(context context.Context, request *api.ExistsFileRequest) (*api.ExistsFileResponse, error) {
	logger := log.WithFields(log.Fields{
		"package":  "service",
		"struct":   "PoolServer",
		"function": "ExistsFile",
	})

	defer irodsfs_common_utils.StackTraceFromPanic(logger)

	logger.Infof("ExistsFile request from pool session id %s, path %s", request.SessionId, request.Path)
	defer logger.Infof("ExistsFile response to pool session id %s, path %s", request.SessionId, request.Path)

	poolSession, irodsFsClientInstance, err := server.getPoolSessionAndFsClientInstance(request.SessionId)
	if err != nil {
		logger.Error(err)
		return nil, server.errorToStatus(err)
	}

	poolSession.UpdateLastAccessTime()

	fsClient := irodsFsClientInstance.GetFSClient()
	if fsClient == nil {
		err = fmt.Errorf("failed to get FS Client from irods fs client instance")
		logger.Error(err)
		return nil, server.errorToStatus(err)
	}

	exist := fsClient.ExistsFile(request.Path)
	return &api.ExistsFileResponse{
		Exist: exist,
	}, nil
}

func (server *PoolServer) ListUserGroups(context context.Context, request *api.ListUserGroupsRequest) (*api.ListUserGroupsResponse, error) {
	logger := log.WithFields(log.Fields{
		"package":  "service",
		"struct":   "PoolServer",
		"function": "ListUserGroups",
	})

	defer irodsfs_common_utils.StackTraceFromPanic(logger)

	logger.Infof("ListUserGroups request from pool session id %s, user name %s", request.SessionId, request.UserName)
	defer logger.Infof("ListUserGroups response to pool session id %s, user name %s", request.SessionId, request.UserName)

	poolSession, irodsFsClientInstance, err := server.getPoolSessionAndFsClientInstance(request.SessionId)
	if err != nil {
		logger.Error(err)
		return nil, server.errorToStatus(err)
	}

	poolSession.UpdateLastAccessTime()

	fsClient := irodsFsClientInstance.GetFSClient()
	if fsClient == nil {
		err = fmt.Errorf("failed to get FS Client from irods fs client instance")
		logger.Error(err)
		return nil, server.errorToStatus(err)
	}

	groups, err := fsClient.ListUserGroups(request.UserName)
	if err != nil {
		logger.Error(err)
		return nil, server.errorToStatus(err)
	}

	responseGroups := []*api.User{}
	for _, group := range groups {
		responseGroup := &api.User{
			Name: group.Name,
			Zone: group.Zone,
			Type: string(group.Type),
		}
		responseGroups = append(responseGroups, responseGroup)
	}

	response := &api.ListUserGroupsResponse{
		Users: responseGroups,
	}

	return response, nil
}

func (server *PoolServer) ListDirACLs(context context.Context, request *api.ListDirACLsRequest) (*api.ListDirACLsResponse, error) {
	logger := log.WithFields(log.Fields{
		"package":  "service",
		"struct":   "PoolServer",
		"function": "ListDirACLs",
	})

	defer irodsfs_common_utils.StackTraceFromPanic(logger)

	logger.Infof("ListDirACLs request from pool session id %s, path %s", request.SessionId, request.Path)
	defer logger.Infof("ListDirACLs response to pool session id %s, path %s", request.SessionId, request.Path)

	poolSession, irodsFsClientInstance, err := server.getPoolSessionAndFsClientInstance(request.SessionId)
	if err != nil {
		logger.Error(err)
		return nil, server.errorToStatus(err)
	}

	poolSession.UpdateLastAccessTime()

	fsClient := irodsFsClientInstance.GetFSClient()
	if fsClient == nil {
		err = fmt.Errorf("failed to get FS Client from irods fs client instance")
		logger.Error(err)
		return nil, server.errorToStatus(err)
	}

	accesses, err := fsClient.ListDirACLs(request.Path)
	if err != nil {
		logger.Error(err)
		return nil, server.errorToStatus(err)
	}

	responseAccesses := []*api.Access{}
	for _, access := range accesses {
		responseAccess := &api.Access{
			Path:        access.Path,
			UserName:    access.UserName,
			UserZone:    access.UserZone,
			UserType:    string(access.UserType),
			AccessLevel: string(access.AccessLevel),
		}
		responseAccesses = append(responseAccesses, responseAccess)
	}

	response := &api.ListDirACLsResponse{
		Accesses: responseAccesses,
	}

	return response, nil
}

func (server *PoolServer) ListFileACLs(context context.Context, request *api.ListFileACLsRequest) (*api.ListFileACLsResponse, error) {
	logger := log.WithFields(log.Fields{
		"package":  "service",
		"struct":   "PoolServer",
		"function": "ListFileACLs",
	})

	defer irodsfs_common_utils.StackTraceFromPanic(logger)

	logger.Infof("ListFileACLs request from pool session id %s, path %s", request.SessionId, request.Path)
	defer logger.Infof("ListFileACLs response to pool session id %s, path %s", request.SessionId, request.Path)

	poolSession, irodsFsClientInstance, err := server.getPoolSessionAndFsClientInstance(request.SessionId)
	if err != nil {
		logger.Error(err)
		return nil, server.errorToStatus(err)
	}

	poolSession.UpdateLastAccessTime()

	fsClient := irodsFsClientInstance.GetFSClient()
	if fsClient == nil {
		err = fmt.Errorf("failed to get FS Client from irods fs client instance")
		logger.Error(err)
		return nil, server.errorToStatus(err)
	}

	accesses, err := fsClient.ListFileACLs(request.Path)
	if err != nil {
		logger.Error(err)
		return nil, server.errorToStatus(err)
	}

	responseAccesses := []*api.Access{}
	for _, access := range accesses {
		responseAccess := &api.Access{
			Path:        access.Path,
			UserName:    access.UserName,
			UserZone:    access.UserZone,
			UserType:    string(access.UserType),
			AccessLevel: string(access.AccessLevel),
		}
		responseAccesses = append(responseAccesses, responseAccess)
	}

	response := &api.ListFileACLsResponse{
		Accesses: responseAccesses,
	}

	return response, nil
}

func (server *PoolServer) ListACLsForEntries(context context.Context, request *api.ListACLsForEntriesRequest) (*api.ListACLsForEntriesResponse, error) {
	logger := log.WithFields(log.Fields{
		"package":  "service",
		"struct":   "PoolServer",
		"function": "ListACLsForEntries",
	})

	defer irodsfs_common_utils.StackTraceFromPanic(logger)

	logger.Infof("ListACLsForEntries request from pool session id %s, path %s", request.SessionId, request.Path)
	defer logger.Infof("ListACLsForEntries response to pool session id %s, path %s", request.SessionId, request.Path)

	poolSession, irodsFsClientInstance, err := server.getPoolSessionAndFsClientInstance(request.SessionId)
	if err != nil {
		logger.Error(err)
		return nil, server.errorToStatus(err)
	}

	poolSession.UpdateLastAccessTime()

	fsClient := irodsFsClientInstance.GetFSClient()
	if fsClient == nil {
		err = fmt.Errorf("failed to get FS Client from irods fs client instance")
		logger.Error(err)
		return nil, server.errorToStatus(err)
	}

	accesses, err := fsClient.ListACLsForEntries(request.Path)
	if err != nil {
		logger.Error(err)
		return nil, server.errorToStatus(err)
	}

	responseAccesses := []*api.Access{}
	for _, access := range accesses {
		responseAccess := &api.Access{
			Path:        access.Path,
			UserName:    access.UserName,
			UserZone:    access.UserZone,
			UserType:    string(access.UserType),
			AccessLevel: string(access.AccessLevel),
		}
		responseAccesses = append(responseAccesses, responseAccess)
	}

	response := &api.ListACLsForEntriesResponse{
		Accesses: responseAccesses,
	}

	return response, nil
}

func (server *PoolServer) RemoveFile(context context.Context, request *api.RemoveFileRequest) (*api.Empty, error) {
	logger := log.WithFields(log.Fields{
		"package":  "service",
		"struct":   "PoolServer",
		"function": "RemoveFile",
	})

	defer irodsfs_common_utils.StackTraceFromPanic(logger)

	logger.Infof("RemoveFile request from pool session id %s, path %s", request.SessionId, request.Path)
	defer logger.Infof("RemoveFile response to pool session id %s, path %s", request.SessionId, request.Path)

	poolSession, irodsFsClientInstance, err := server.getPoolSessionAndFsClientInstance(request.SessionId)
	if err != nil {
		logger.Error(err)
		return nil, server.errorToStatus(err)
	}

	poolSession.UpdateLastAccessTime()

	fsClient := irodsFsClientInstance.GetFSClient()
	if fsClient == nil {
		err = fmt.Errorf("failed to get FS Client from irods fs client instance")
		logger.Error(err)
		return nil, server.errorToStatus(err)
	}

	err = fsClient.RemoveFile(request.Path, request.Force)
	if err != nil {
		logger.Error(err)
		return nil, server.errorToStatus(err)
	}

	// clear cache for the path if exists
	server.cacheStore.DeleteAllEntriesForGroup(request.Path)

	return &api.Empty{}, nil
}

func (server *PoolServer) RemoveDir(context context.Context, request *api.RemoveDirRequest) (*api.Empty, error) {
	logger := log.WithFields(log.Fields{
		"package":  "service",
		"struct":   "PoolServer",
		"function": "RemoveDir",
	})

	defer irodsfs_common_utils.StackTraceFromPanic(logger)

	logger.Infof("RemoveDir request from pool session id %s, path %s", request.SessionId, request.Path)
	defer logger.Infof("RemoveDir response to pool session id %s, path %s", request.SessionId, request.Path)

	poolSession, irodsFsClientInstance, err := server.getPoolSessionAndFsClientInstance(request.SessionId)
	if err != nil {
		logger.Error(err)
		return nil, fmt.Errorf("failed to get FS Client from connection")
	}

	poolSession.UpdateLastAccessTime()

	fsClient := irodsFsClientInstance.GetFSClient()
	if fsClient == nil {
		err = fmt.Errorf("failed to get FS Client from irods fs client instance")
		logger.Error(err)
		return nil, server.errorToStatus(err)
	}

	err = fsClient.RemoveDir(request.Path, request.Recurse, request.Force)
	if err != nil {
		logger.Error(err)
		return nil, server.errorToStatus(err)
	}

	return &api.Empty{}, nil
}

func (server *PoolServer) MakeDir(context context.Context, request *api.MakeDirRequest) (*api.Empty, error) {
	logger := log.WithFields(log.Fields{
		"package":  "service",
		"struct":   "PoolServer",
		"function": "MakeDir",
	})

	defer irodsfs_common_utils.StackTraceFromPanic(logger)

	logger.Infof("MakeDir request from pool session id %s, path %s", request.SessionId, request.Path)
	defer logger.Infof("MakeDir response to pool session id %s, path %s", request.SessionId, request.Path)

	poolSession, irodsFsClientInstance, err := server.getPoolSessionAndFsClientInstance(request.SessionId)
	if err != nil {
		logger.Error(err)
		return nil, server.errorToStatus(err)
	}

	poolSession.UpdateLastAccessTime()

	fsClient := irodsFsClientInstance.GetFSClient()
	if fsClient == nil {
		err = fmt.Errorf("failed to get FS Client from irods fs client instance")
		logger.Error(err)
		return nil, server.errorToStatus(err)
	}

	err = fsClient.MakeDir(request.Path, request.Recurse)
	if err != nil {
		logger.Error(err)
		return nil, server.errorToStatus(err)
	}

	return &api.Empty{}, nil
}

func (server *PoolServer) RenameDirToDir(context context.Context, request *api.RenameDirToDirRequest) (*api.Empty, error) {
	logger := log.WithFields(log.Fields{
		"package":  "service",
		"struct":   "PoolServer",
		"function": "RenameDirToDir",
	})

	defer irodsfs_common_utils.StackTraceFromPanic(logger)

	logger.Infof("RenameDirToDir request from pool session id %s, source path %s -> destination path %s", request.SessionId, request.SourcePath, request.DestinationPath)
	defer logger.Infof("RenameDirToDir response to pool session id %s, source path %s -> destination path %s", request.SessionId, request.SourcePath, request.DestinationPath)

	poolSession, irodsFsClientInstance, err := server.getPoolSessionAndFsClientInstance(request.SessionId)
	if err != nil {
		logger.Error(err)
		return nil, server.errorToStatus(err)
	}

	poolSession.UpdateLastAccessTime()

	fsClient := irodsFsClientInstance.GetFSClient()
	if fsClient == nil {
		err = fmt.Errorf("failed to get FS Client from irods fs client instance")
		logger.Error(err)
		return nil, server.errorToStatus(err)
	}

	err = fsClient.RenameDirToDir(request.SourcePath, request.DestinationPath)
	if err != nil {
		logger.Error(err)
		return nil, server.errorToStatus(err)
	}

	return &api.Empty{}, nil
}

func (server *PoolServer) RenameFileToFile(context context.Context, request *api.RenameFileToFileRequest) (*api.Empty, error) {
	logger := log.WithFields(log.Fields{
		"package":  "service",
		"struct":   "PoolServer",
		"function": "RenameFileToFile",
	})

	defer irodsfs_common_utils.StackTraceFromPanic(logger)

	logger.Infof("RenameFileToFile request from pool session id %s, source path %s -> destination path %s", request.SessionId, request.SourcePath, request.DestinationPath)
	defer logger.Infof("RenameFileToFile response to pool session id %s, source path %s -> destination path %s", request.SessionId, request.SourcePath, request.DestinationPath)

	poolSession, irodsFsClientInstance, err := server.getPoolSessionAndFsClientInstance(request.SessionId)
	if err != nil {
		logger.Error(err)
		return nil, server.errorToStatus(err)
	}

	poolSession.UpdateLastAccessTime()

	fsClient := irodsFsClientInstance.GetFSClient()
	if fsClient == nil {
		err = fmt.Errorf("failed to get FS Client from irods fs client instance")
		logger.Error(err)
		return nil, server.errorToStatus(err)
	}

	err = fsClient.RenameFileToFile(request.SourcePath, request.DestinationPath)
	if err != nil {
		logger.Error(err)
		return nil, server.errorToStatus(err)
	}

	// clear cache for the path if exists
	server.cacheStore.DeleteAllEntriesForGroup(request.SourcePath)

	return &api.Empty{}, nil
}

func (server *PoolServer) CreateFile(context context.Context, request *api.CreateFileRequest) (*api.CreateFileResponse, error) {
	logger := log.WithFields(log.Fields{
		"package":  "service",
		"struct":   "PoolServer",
		"function": "CreateFile",
	})

	defer irodsfs_common_utils.StackTraceFromPanic(logger)

	logger.Infof("CreateFile request from pool session id %s, path %s", request.SessionId, request.Path)
	defer logger.Infof("CreateFile response to pool session id %s, path %s", request.SessionId, request.Path)

	poolSession, irodsFsClientInstance, err := server.getPoolSessionAndFsClientInstance(request.SessionId)
	if err != nil {
		logger.Error(err)
		return nil, server.errorToStatus(err)
	}

	poolSession.UpdateLastAccessTime()

	fsClient := irodsFsClientInstance.GetFSClient()
	if fsClient == nil {
		err = fmt.Errorf("failed to get FS Client from irods fs client instance")
		logger.Error(err)
		return nil, server.errorToStatus(err)
	}

	// clear cache for the path if exists
	server.cacheStore.DeleteAllEntriesForGroup(request.Path)

	irodsFsFileHandle, err := fsClient.CreateFile(request.Path, request.Resource, request.Mode)
	if err != nil {
		logger.Error(err)
		return nil, server.errorToStatus(err)
	}

	fileOpenMode := irodsclient_types.FileOpenMode(request.Mode)
	if fileOpenMode.IsWrite() {
		// clear cache for the path if exists
		server.cacheStore.DeleteAllEntriesForGroup(request.Path)
	}

	poolFileHandle, err := NewPoolFileHandle(server, request.SessionId, irodsFsClientInstance.GetID(), irodsFsFileHandle, nil)
	if err != nil {
		logger.Error(err)
		return nil, server.errorToStatus(err)
	}

	poolSession.AddPoolFileHandle(poolFileHandle)

	fsEntry := irodsFsFileHandle.GetEntry()

	responseEntry := &api.Entry{
		Id:         fsEntry.ID,
		Type:       string(fsEntry.Type),
		Name:       fsEntry.Name,
		Path:       fsEntry.Path,
		Owner:      fsEntry.Owner,
		Size:       fsEntry.Size,
		DataType:   fsEntry.DataType,
		CreateTime: irodsfs_common_utils.MakeTimeToString(fsEntry.CreateTime),
		ModifyTime: irodsfs_common_utils.MakeTimeToString(fsEntry.ModifyTime),
		Checksum:   fsEntry.CheckSum,
	}

	response := &api.CreateFileResponse{
		FileHandleId: irodsFsFileHandle.GetID(),
		Entry:        responseEntry,
	}

	return response, nil
}

func (server *PoolServer) OpenFile(context context.Context, request *api.OpenFileRequest) (*api.OpenFileResponse, error) {
	logger := log.WithFields(log.Fields{
		"package":  "service",
		"struct":   "PoolServer",
		"function": "OpenFile",
	})

	defer irodsfs_common_utils.StackTraceFromPanic(logger)

	logger.Infof("OpenFile request from pool session id %s, path %s, mode(%s)", request.SessionId, request.Path, request.Mode)
	defer logger.Infof("OpenFile response to pool session id %s, path %s, mode(%s)", request.SessionId, request.Path, request.Mode)

	poolSession, irodsFsClientInstance, err := server.getPoolSessionAndFsClientInstance(request.SessionId)
	if err != nil {
		logger.Error(err)
		return nil, server.errorToStatus(err)
	}

	poolSession.UpdateLastAccessTime()

	fsClient := irodsFsClientInstance.GetFSClient()
	if fsClient == nil {
		err = fmt.Errorf("failed to get FS Client from irods fs client instance")
		logger.Error(err)
		return nil, server.errorToStatus(err)
	}

	fileOpenMode := irodsclient_types.FileOpenMode(request.Mode)

	irodsFsFlieHandle, err := fsClient.OpenFile(request.Path, request.Resource, request.Mode)
	if err != nil {
		logger.Error(err)
		return nil, server.errorToStatus(err)
	}

	if fileOpenMode.IsWrite() {
		// clear cache for the path if exists
		server.cacheStore.DeleteAllEntriesForGroup(request.Path)
	}

	poolFileHandle, err := NewPoolFileHandle(server, request.SessionId, irodsFsClientInstance.GetID(), irodsFsFlieHandle, nil)
	if err != nil {
		logger.Error(err)
		return nil, server.errorToStatus(err)
	}

	poolSession.AddPoolFileHandle(poolFileHandle)

	// read-only mode requires multiple file handles for prefetching
	go func() {
		if fileOpenMode.IsReadOnly() {
			if len(server.config.TempRootPath) > 0 {
				entry, err := fsClient.Stat(request.Path)
				if err != nil {
					logger.Error(err)
					return
				}

				// the file must be large enough
				if entry.Size > int64(iRODSIOBlockSize*3) {
					prefetchingIrodsFsFlieHandle, err := fsClient.OpenFile(request.Path, request.Resource, request.Mode)
					if err != nil {
						logger.Error(err)
						return
					}

					irodsFsFlieHandlesForPrefetching := []irodsfs_common.IRODSFSFileHandle{prefetchingIrodsFsFlieHandle}
					poolFileHandle.AddFileHandlesForPrefetching(irodsFsFlieHandlesForPrefetching)
				}
			}
		}
	}()

	fsEntry := irodsFsFlieHandle.GetEntry()

	responseEntry := &api.Entry{
		Id:         fsEntry.ID,
		Type:       string(fsEntry.Type),
		Name:       fsEntry.Name,
		Path:       fsEntry.Path,
		Owner:      fsEntry.Owner,
		Size:       fsEntry.Size,
		DataType:   fsEntry.DataType,
		CreateTime: irodsfs_common_utils.MakeTimeToString(fsEntry.CreateTime),
		ModifyTime: irodsfs_common_utils.MakeTimeToString(fsEntry.ModifyTime),
		Checksum:   fsEntry.CheckSum,
	}

	response := &api.OpenFileResponse{
		FileHandleId: irodsFsFlieHandle.GetID(),
		Entry:        responseEntry,
	}

	return response, nil
}

func (server *PoolServer) TruncateFile(context context.Context, request *api.TruncateFileRequest) (*api.Empty, error) {
	logger := log.WithFields(log.Fields{
		"package":  "service",
		"struct":   "PoolServer",
		"function": "TruncateFile",
	})

	defer irodsfs_common_utils.StackTraceFromPanic(logger)

	logger.Infof("TruncateFile request from pool session id %s, path %s", request.SessionId, request.Path)
	defer logger.Infof("TruncateFile response to pool session id %s, path %s", request.SessionId, request.Path)

	poolSession, irodsFsClientInstance, err := server.getPoolSessionAndFsClientInstance(request.SessionId)
	if err != nil {
		logger.Error(err)
		return nil, server.errorToStatus(err)
	}

	poolSession.UpdateLastAccessTime()

	fsClient := irodsFsClientInstance.GetFSClient()
	if fsClient == nil {
		err = fmt.Errorf("failed to get FS Client from irods fs client instance")
		logger.Error(err)
		return nil, server.errorToStatus(err)
	}

	// clear cache for the path if exists
	server.cacheStore.DeleteAllEntriesForGroup(request.Path)

	err = fsClient.TruncateFile(request.Path, request.Size)
	if err != nil {
		logger.Error(err)
		return nil, server.errorToStatus(err)
	}

	return &api.Empty{}, nil
}

func (server *PoolServer) GetOffset(context context.Context, request *api.GetOffsetRequest) (*api.GetOffsetResponse, error) {
	logger := log.WithFields(log.Fields{
		"package":  "service",
		"struct":   "PoolServer",
		"function": "GetOffset",
	})

	defer irodsfs_common_utils.StackTraceFromPanic(logger)

	logger.Debugf("GetOffset request from pool session id %s, pool file handle id %s", request.SessionId, request.FileHandleId)
	defer logger.Debugf("GetOffset response to pool session id %s, pool file handle id %s", request.SessionId, request.FileHandleId)

	poolFileHandle, err := server.getPoolFileHandle(request.SessionId, request.FileHandleId)
	if err != nil {
		logger.Error(err)
		return nil, server.errorToStatus(err)
	}

	response := &api.GetOffsetResponse{
		Offset: poolFileHandle.GetOffset(),
	}

	return response, nil
}

func (server *PoolServer) ReadAt(context context.Context, request *api.ReadAtRequest) (*api.ReadAtResponse, error) {
	logger := log.WithFields(log.Fields{
		"package":  "service",
		"struct":   "PoolServer",
		"function": "ReadAt",
	})

	defer irodsfs_common_utils.StackTraceFromPanic(logger)

	logger.Debugf("ReadAt request from pool session id %s, pool file handle id %s, offset %d, length %d", request.SessionId, request.FileHandleId, request.Offset, request.Length)
	defer logger.Debugf("ReadAt response to pool session id %s, pool file handle id %s, offset %d, length %d", request.SessionId, request.FileHandleId, request.Offset, request.Length)

	poolFileHandle, err := server.getPoolFileHandle(request.SessionId, request.FileHandleId)
	if err != nil {
		logger.Error(err)
		return nil, server.errorToStatus(err)
	}

	buffer := make([]byte, request.Length)

	readLen, err := poolFileHandle.ReadAt(buffer, request.Offset)
	if err != nil && err != io.EOF {
		logger.Error(err)
		return nil, server.errorToStatus(err)
	}

	available := poolFileHandle.GetAvailable(request.Offset + int64(readLen))

	response := &api.ReadAtResponse{
		Data:      buffer[:readLen],
		Available: available,
	}

	return response, nil
}

func (server *PoolServer) WriteAt(context context.Context, request *api.WriteAtRequest) (*api.Empty, error) {
	logger := log.WithFields(log.Fields{
		"package":  "service",
		"struct":   "PoolServer",
		"function": "WriteAt",
	})

	defer irodsfs_common_utils.StackTraceFromPanic(logger)

	logger.Debugf("WriteAt request from pool session id %s, pool file handle id %s, offset %d, length %d", request.SessionId, request.FileHandleId, request.Offset, len(request.Data))
	defer logger.Debugf("WriteAt response to pool session id %s, pool file handle id %s, offset %d, length %d", request.SessionId, request.FileHandleId, request.Offset, len(request.Data))

	poolFileHandle, err := server.getPoolFileHandle(request.SessionId, request.FileHandleId)
	if err != nil {
		logger.Error(err)
		return nil, server.errorToStatus(err)
	}

	_, err = poolFileHandle.WriteAt(request.Data, request.Offset)
	if err != nil {
		logger.Error(err)
		return nil, server.errorToStatus(err)
	}

	return &api.Empty{}, nil
}

func (server *PoolServer) Truncate(context context.Context, request *api.TruncateRequest) (*api.Empty, error) {
	logger := log.WithFields(log.Fields{
		"package":  "service",
		"struct":   "PoolServer",
		"function": "Truncate",
	})

	defer irodsfs_common_utils.StackTraceFromPanic(logger)

	logger.Infof("Truncate request from pool session id %s, pool file handle id %s, size %d", request.SessionId, request.FileHandleId, request.Size)
	defer logger.Infof("Truncate response to pool session id %s, pool file handle id %s, size %d", request.SessionId, request.FileHandleId, request.Size)

	poolFileHandle, err := server.getPoolFileHandle(request.SessionId, request.FileHandleId)
	if err != nil {
		logger.Error(err)
		return nil, server.errorToStatus(err)
	}

	err = poolFileHandle.Truncate(request.Size)
	if err != nil {
		logger.Error(err)
		return nil, server.errorToStatus(err)
	}

	return &api.Empty{}, nil
}

func (server *PoolServer) Flush(context context.Context, request *api.FlushRequest) (*api.Empty, error) {
	logger := log.WithFields(log.Fields{
		"package":  "service",
		"struct":   "PoolServer",
		"function": "Flush",
	})

	defer irodsfs_common_utils.StackTraceFromPanic(logger)

	logger.Infof("Flush request from pool session id %s, pool file handle id %s", request.SessionId, request.FileHandleId)
	defer logger.Infof("Flush response to pool session id %s, pool file handle id %s", request.SessionId, request.FileHandleId)

	poolFileHandle, err := server.getPoolFileHandle(request.SessionId, request.FileHandleId)
	if err != nil {
		logger.Error(err)
		return nil, server.errorToStatus(err)
	}

	err = poolFileHandle.Flush()
	if err != nil {
		logger.Error(err)
		return nil, server.errorToStatus(err)
	}

	return &api.Empty{}, nil
}

func (server *PoolServer) Close(context context.Context, request *api.CloseRequest) (*api.Empty, error) {
	logger := log.WithFields(log.Fields{
		"package":  "service",
		"struct":   "PoolServer",
		"function": "Close",
	})

	defer irodsfs_common_utils.StackTraceFromPanic(logger)

	logger.Infof("Close request from pool session id %s, pool file handle id %s", request.SessionId, request.FileHandleId)
	defer logger.Infof("Close response to pool session id %s, pool file handle id %s", request.SessionId, request.FileHandleId)

	poolSession, _, err := server.getPoolSessionAndFsClientInstance(request.SessionId)
	if err != nil {
		logger.Error(err)
		return nil, server.errorToStatus(err)
	}

	poolSession.UpdateLastAccessTime()

	poolFileHandle := poolSession.GetPoolFileHandle(request.FileHandleId)
	if poolFileHandle == nil {
		err := fmt.Errorf("failed to find pool file handle %s", request.FileHandleId)
		logger.Error(err)
		return nil, server.errorToStatus(err)
	}

	poolSession.RemovePoolFileHandle(request.FileHandleId)

	if poolFileHandle.GetOpenMode() != irodsclient_types.FileOpenModeReadOnly {
		// not read-only
		// clear cache for the path if exists
		server.cacheStore.DeleteAllEntriesForGroup(poolFileHandle.GetEntryPath())
	}

	err = poolFileHandle.Release()
	if err != nil {
		logger.Error(err)
		return nil, server.errorToStatus(err)
	}

	return &api.Empty{}, nil
}
