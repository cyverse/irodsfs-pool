package service

import (
	"container/list"
	"context"
	"fmt"
	"io"
	"sync"

	irodsclient_fs "github.com/cyverse/go-irodsclient/fs"
	irodsclient_types "github.com/cyverse/go-irodsclient/irods/types"
	irodsfs_common_cache "github.com/cyverse/irodsfs-common/io/cache"
	irodsfs_common "github.com/cyverse/irodsfs-common/irods"
	irodsfs_common_utils "github.com/cyverse/irodsfs-common/utils"
	"github.com/cyverse/irodsfs-pool/commons"
	"github.com/cyverse/irodsfs-pool/service/api"
	log "github.com/sirupsen/logrus"
	"golang.org/x/xerrors"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	cacheEntrySizeMax  int = 4 * 1024 * 1024 // 4MB
	cacheEventQueueMax int = 100000
)

// PoolServerConfig is a configuration for Server
type PoolServerConfig struct {
	CacheSizeMax         int64
	CacheRootPath        string
	CacheTimeoutSettings []commons.MetadataCacheTimeoutSetting
}

type PoolServerCacheEventSubscription struct {
	sessionID string
	handlerID string
	eventList list.List // type of api.CacheEvent
	mutex     sync.Mutex
}

// PoolServer is a struct for PoolServer
type PoolServer struct {
	api.UnimplementedPoolAPIServer

	config *PoolServerConfig

	cacheStore              irodsfs_common_cache.CacheStore
	sessionManager          *PoolSessionManager
	cacheEventSubscriptions map[string]*PoolServerCacheEventSubscription
	mutex                   sync.RWMutex
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

		cacheStore:              diskCacheStore,
		sessionManager:          NewPoolSessionManager(config),
		cacheEventSubscriptions: map[string]*PoolServerCacheEventSubscription{},
		mutex:                   sync.RWMutex{},
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

	server.sessionManager.Release()

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

	if IsSessionNotFoundError(err) {
		return status.Error(codes.Unauthenticated, err.Error())
	} else if IsIrodsFsClientInstanceNotFoundError(err) {
		return status.Error(codes.Unauthenticated, err.Error())
	} else if IsFileHandleNotFoundError(err) {
		return status.Error(codes.Internal, err.Error())
	} else if irodsclient_types.IsFileNotFoundError(err) {
		return status.Error(codes.NotFound, err.Error())
	} else if irodsclient_types.IsCollectionNotEmptyError(err) {
		// there's no matching error type for not empty
		return status.Error(codes.AlreadyExists, err.Error())
	}

	return status.Error(codes.Internal, err.Error())
}

func (server *PoolServer) GetSessionManager() *PoolSessionManager {
	server.mutex.RLock()
	defer server.mutex.RUnlock()

	return server.sessionManager
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

	promCounterForGRPCCalls.Inc()

	server.mutex.Lock()
	defer server.mutex.Unlock()

	session, err := server.sessionManager.NewSession(request.Account, request.ClientId, request.ApplicationName)
	if err != nil {
		logger.Errorf("%+v", err)
		return nil, server.errorToStatus(err)
	}

	response := &api.LoginResponse{
		SessionId: session.GetID(),
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

	promCounterForGRPCCalls.Inc()

	// collect metrics before release
	server.CollectPrometheusMetrics()

	server.mutex.Lock()
	defer server.mutex.Unlock()

	session, err := server.sessionManager.GetSession(request.SessionId)
	if err != nil {
		// session might be already closed due to timeout, so ignore error
		sessionErr := xerrors.Errorf("failed to logout because the session for id %s is not found, ignoring...: %w", request.SessionId, err)
		logger.Errorf("%+v", sessionErr)
		return &api.Empty{}, nil
	}

	subscription, ok := server.cacheEventSubscriptions[request.SessionId]
	if ok {
		delete(server.cacheEventSubscriptions, request.SessionId)

		promCounterForCacheEventSubscriptions.Dec()
	}

	if ok && subscription != nil {
		session.RemovePoolCacheEventHandler(subscription.handlerID)
	}

	server.sessionManager.ReleaseSession(session.GetID())
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

	server.sessionManager.Release()
}

func (server *PoolServer) GetPoolSessions() int {
	server.mutex.RLock()
	defer server.mutex.RUnlock()

	return server.sessionManager.GetTotalSessions()
}

func (server *PoolServer) GetIRODSFSClientInstances() int {
	server.mutex.RLock()
	defer server.mutex.RUnlock()

	return server.sessionManager.GetTotalIRODSFSClientInstances()
}

func (server *PoolServer) GetIRODSConnections() int {
	logger := log.WithFields(log.Fields{
		"package":  "service",
		"struct":   "PoolServer",
		"function": "GetIRODSConnections",
	})

	defer irodsfs_common_utils.StackTraceFromPanic(logger)

	server.mutex.RLock()
	defer server.mutex.RUnlock()

	return server.sessionManager.GetTotalIRODSFSClientConnections()
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

	promCounterForGRPCCalls.Inc()

	session, fsClient, err := server.sessionManager.GetSessionAndIRODSFSClient(request.SessionId)
	if err != nil {
		logger.Errorf("%+v", err)
		return nil, server.errorToStatus(err)
	}

	session.UpdateLastAccessTime()

	entries, err := fsClient.List(request.Path)
	if err != nil {
		logger.Errorf("%+v", err)
		return nil, server.errorToStatus(err)
	}

	responseEntries := make([]*api.Entry, len(entries))
	idx := 0
	for _, entry := range entries {
		responseEntries[idx] = &api.Entry{
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
		idx++
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

	promCounterForGRPCCalls.Inc()

	session, fsClient, err := server.sessionManager.GetSessionAndIRODSFSClient(request.SessionId)
	if err != nil {
		logger.Errorf("%+v", err)
		return nil, server.errorToStatus(err)
	}

	session.UpdateLastAccessTime()

	entry, err := fsClient.Stat(request.Path)
	if err != nil {
		logger.Errorf("%+v", err)
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

func (server *PoolServer) ListXattr(context context.Context, request *api.ListXattrRequest) (*api.ListXattrResponse, error) {
	logger := log.WithFields(log.Fields{
		"package":  "service",
		"struct":   "PoolServer",
		"function": "ListXattr",
	})

	defer irodsfs_common_utils.StackTraceFromPanic(logger)

	logger.Infof("ListXattr request from pool session id %s, path %s", request.SessionId, request.Path)
	defer logger.Infof("ListXattr response to pool session id %s, path %s", request.SessionId, request.Path)

	promCounterForGRPCCalls.Inc()

	session, fsClient, err := server.sessionManager.GetSessionAndIRODSFSClient(request.SessionId)
	if err != nil {
		logger.Errorf("%+v", err)
		return nil, server.errorToStatus(err)
	}

	session.UpdateLastAccessTime()

	irodsMetadata, err := fsClient.ListXattr(request.Path)
	if err != nil {
		logger.Errorf("%+v", err)
		return nil, server.errorToStatus(err)
	}

	responseMetadata := make([]*api.Metadata, len(irodsMetadata))
	idx := 0
	for _, irodsMeta := range irodsMetadata {
		responseMetadata[idx] = &api.Metadata{
			Id:    irodsMeta.AVUID,
			Name:  irodsMeta.Name,
			Value: irodsMeta.Value,
			Unit:  irodsMeta.Units,
		}
		idx++
	}

	response := &api.ListXattrResponse{
		Metadata: responseMetadata,
	}

	return response, nil
}

func (server *PoolServer) GetXattr(context context.Context, request *api.GetXattrRequest) (*api.GetXattrResponse, error) {
	logger := log.WithFields(log.Fields{
		"package":  "service",
		"struct":   "PoolServer",
		"function": "GetXattr",
	})

	defer irodsfs_common_utils.StackTraceFromPanic(logger)

	logger.Infof("GetXattr request from pool session id %s, path %s, name %s", request.SessionId, request.Path, request.Name)
	defer logger.Infof("GetXattr response to pool session id %s, path %s, name %s", request.SessionId, request.Path, request.Name)

	promCounterForGRPCCalls.Inc()

	session, fsClient, err := server.sessionManager.GetSessionAndIRODSFSClient(request.SessionId)
	if err != nil {
		logger.Errorf("%+v", err)
		return nil, server.errorToStatus(err)
	}

	session.UpdateLastAccessTime()

	irodsMeta, err := fsClient.GetXattr(request.Path, request.Name)
	if err != nil {
		logger.Errorf("%+v", err)
		return nil, server.errorToStatus(err)
	}

	if irodsMeta == nil {
		// not exist
		errorMessage := fmt.Sprintf("failed to find xattr - %s", request.Name)
		return nil, status.Error(codes.NotFound, errorMessage)
	}

	responseMeta := &api.Metadata{
		Id:    irodsMeta.AVUID,
		Name:  irodsMeta.Name,
		Value: irodsMeta.Value,
		Unit:  irodsMeta.Units,
	}

	response := &api.GetXattrResponse{
		Metadata: responseMeta,
	}

	return response, nil
}

func (server *PoolServer) SetXattr(context context.Context, request *api.SetXattrRequest) (*api.Empty, error) {
	logger := log.WithFields(log.Fields{
		"package":  "service",
		"struct":   "PoolServer",
		"function": "SetXattr",
	})

	defer irodsfs_common_utils.StackTraceFromPanic(logger)

	logger.Infof("SetXattr request from pool session id %s, path %s, name %s", request.SessionId, request.Path, request.Name)
	defer logger.Infof("SetXattr response to pool session id %s, path %s, name %s", request.SessionId, request.Path, request.Name)

	promCounterForGRPCCalls.Inc()

	session, fsClient, err := server.sessionManager.GetSessionAndIRODSFSClient(request.SessionId)
	if err != nil {
		logger.Errorf("%+v", err)
		return nil, server.errorToStatus(err)
	}

	session.UpdateLastAccessTime()

	err = fsClient.SetXattr(request.Path, request.Name, request.Value)
	if err != nil {
		logger.Errorf("%+v", err)
		return nil, server.errorToStatus(err)
	}

	return &api.Empty{}, nil
}

func (server *PoolServer) RemoveXattr(context context.Context, request *api.RemoveXattrRequest) (*api.Empty, error) {
	logger := log.WithFields(log.Fields{
		"package":  "service",
		"struct":   "PoolServer",
		"function": "RemoveXattr",
	})

	defer irodsfs_common_utils.StackTraceFromPanic(logger)

	logger.Infof("RemoveXattr request from pool session id %s, path %s, name %s", request.SessionId, request.Path, request.Name)
	defer logger.Infof("RemoveXattr response to pool session id %s, path %s, name %s", request.SessionId, request.Path, request.Name)

	promCounterForGRPCCalls.Inc()

	session, fsClient, err := server.sessionManager.GetSessionAndIRODSFSClient(request.SessionId)
	if err != nil {
		logger.Errorf("%+v", err)
		return nil, server.errorToStatus(err)
	}

	session.UpdateLastAccessTime()

	err = fsClient.RemoveXattr(request.Path, request.Name)
	if err != nil {
		logger.Errorf("%+v", err)
		return nil, server.errorToStatus(err)
	}

	return &api.Empty{}, nil
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

	promCounterForGRPCCalls.Inc()

	session, fsClient, err := server.sessionManager.GetSessionAndIRODSFSClient(request.SessionId)
	if err != nil {
		logger.Errorf("%+v", err)
		return nil, server.errorToStatus(err)
	}

	session.UpdateLastAccessTime()

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

	promCounterForGRPCCalls.Inc()

	session, fsClient, err := server.sessionManager.GetSessionAndIRODSFSClient(request.SessionId)
	if err != nil {
		logger.Errorf("%+v", err)
		return nil, server.errorToStatus(err)
	}

	session.UpdateLastAccessTime()

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

	promCounterForGRPCCalls.Inc()

	session, fsClient, err := server.sessionManager.GetSessionAndIRODSFSClient(request.SessionId)
	if err != nil {
		logger.Errorf("%+v", err)
		return nil, server.errorToStatus(err)
	}

	session.UpdateLastAccessTime()

	groups, err := fsClient.ListUserGroups(request.UserName)
	if err != nil {
		logger.Errorf("%+v", err)
		return nil, server.errorToStatus(err)
	}

	responseGroups := make([]*api.User, len(groups))
	idx := 0
	for _, group := range groups {
		responseGroups[idx] = &api.User{
			Name: group.Name,
			Zone: group.Zone,
			Type: string(group.Type),
		}
		idx++
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

	promCounterForGRPCCalls.Inc()

	session, fsClient, err := server.sessionManager.GetSessionAndIRODSFSClient(request.SessionId)
	if err != nil {
		logger.Errorf("%+v", err)
		return nil, server.errorToStatus(err)
	}

	session.UpdateLastAccessTime()

	accesses, err := fsClient.ListDirACLs(request.Path)
	if err != nil {
		logger.Errorf("%+v", err)
		return nil, server.errorToStatus(err)
	}

	responseAccesses := make([]*api.Access, len(accesses))
	idx := 0
	for _, access := range accesses {
		responseAccesses[idx] = &api.Access{
			Path:        access.Path,
			UserName:    access.UserName,
			UserZone:    access.UserZone,
			UserType:    string(access.UserType),
			AccessLevel: string(access.AccessLevel),
		}
		idx++
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

	promCounterForGRPCCalls.Inc()

	session, fsClient, err := server.sessionManager.GetSessionAndIRODSFSClient(request.SessionId)
	if err != nil {
		logger.Errorf("%+v", err)
		return nil, server.errorToStatus(err)
	}

	session.UpdateLastAccessTime()

	accesses, err := fsClient.ListFileACLs(request.Path)
	if err != nil {
		logger.Errorf("%+v", err)
		return nil, server.errorToStatus(err)
	}

	responseAccesses := make([]*api.Access, len(accesses))
	idx := 0
	for _, access := range accesses {
		responseAccesses[idx] = &api.Access{
			Path:        access.Path,
			UserName:    access.UserName,
			UserZone:    access.UserZone,
			UserType:    string(access.UserType),
			AccessLevel: string(access.AccessLevel),
		}
		idx++
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

	promCounterForGRPCCalls.Inc()

	session, fsClient, err := server.sessionManager.GetSessionAndIRODSFSClient(request.SessionId)
	if err != nil {
		logger.Errorf("%+v", err)
		return nil, server.errorToStatus(err)
	}

	session.UpdateLastAccessTime()

	accesses, err := fsClient.ListACLsForEntries(request.Path)
	if err != nil {
		logger.Errorf("%+v", err)
		return nil, server.errorToStatus(err)
	}

	responseAccesses := make([]*api.Access, len(accesses))
	idx := 0
	for _, access := range accesses {
		responseAccesses[idx] = &api.Access{
			Path:        access.Path,
			UserName:    access.UserName,
			UserZone:    access.UserZone,
			UserType:    string(access.UserType),
			AccessLevel: string(access.AccessLevel),
		}
		idx++
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

	promCounterForGRPCCalls.Inc()

	session, fsClient, err := server.sessionManager.GetSessionAndIRODSFSClient(request.SessionId)
	if err != nil {
		logger.Errorf("%+v", err)
		return nil, server.errorToStatus(err)
	}

	session.UpdateLastAccessTime()

	err = fsClient.RemoveFile(request.Path, request.Force)
	if err != nil {
		logger.Errorf("%+v", err)
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

	promCounterForGRPCCalls.Inc()

	session, fsClient, err := server.sessionManager.GetSessionAndIRODSFSClient(request.SessionId)
	if err != nil {
		logger.Errorf("%+v", err)
		return nil, server.errorToStatus(err)
	}

	session.UpdateLastAccessTime()

	err = fsClient.RemoveDir(request.Path, request.Recurse, request.Force)
	if err != nil {
		logger.Errorf("%+v", err)
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

	promCounterForGRPCCalls.Inc()

	session, fsClient, err := server.sessionManager.GetSessionAndIRODSFSClient(request.SessionId)
	if err != nil {
		logger.Errorf("%+v", err)
		return nil, server.errorToStatus(err)
	}

	session.UpdateLastAccessTime()

	err = fsClient.MakeDir(request.Path, request.Recurse)
	if err != nil {
		logger.Errorf("%+v", err)
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

	promCounterForGRPCCalls.Inc()

	session, fsClient, err := server.sessionManager.GetSessionAndIRODSFSClient(request.SessionId)
	if err != nil {
		logger.Errorf("%+v", err)
		return nil, server.errorToStatus(err)
	}

	session.UpdateLastAccessTime()

	err = fsClient.RenameDirToDir(request.SourcePath, request.DestinationPath)
	if err != nil {
		logger.Errorf("%+v", err)
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

	promCounterForGRPCCalls.Inc()

	session, fsClient, err := server.sessionManager.GetSessionAndIRODSFSClient(request.SessionId)
	if err != nil {
		logger.Errorf("%+v", err)
		return nil, server.errorToStatus(err)
	}

	session.UpdateLastAccessTime()

	err = fsClient.RenameFileToFile(request.SourcePath, request.DestinationPath)
	if err != nil {
		logger.Errorf("%+v", err)
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

	logger.Infof("CreateFile request from pool session id %s, path %s, mode(%s)", request.SessionId, request.Path, request.Mode)
	defer logger.Infof("CreateFile response to pool session id %s, path %s, mode(%s)", request.SessionId, request.Path, request.Mode)

	promCounterForGRPCCalls.Inc()

	session, fsClient, err := server.sessionManager.GetSessionAndIRODSFSClient(request.SessionId)
	if err != nil {
		logger.Errorf("%+v", err)
		return nil, server.errorToStatus(err)
	}

	session.UpdateLastAccessTime()

	// clear cache for the path if exists
	server.cacheStore.DeleteAllEntriesForGroup(request.Path)

	irodsFsFileHandle, err := fsClient.CreateFile(request.Path, request.Resource, request.Mode)
	if err != nil {
		logger.Errorf("%+v", err)
		return nil, server.errorToStatus(err)
	}

	fileOpenMode := irodsclient_types.FileOpenMode(request.Mode)
	if fileOpenMode.IsWrite() {
		// clear cache for the path if exists
		server.cacheStore.DeleteAllEntriesForGroup(request.Path)
	}

	poolFileHandle, err := NewPoolFileHandle(server, request.SessionId, irodsFsFileHandle, nil)
	if err != nil {
		logger.Errorf("%+v", err)
		return nil, server.errorToStatus(err)
	}

	session.AddPoolFileHandle(poolFileHandle)

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

	logger.Infof("CreateFile> pool session id %s, path %s, mode(%s), handle id %s", request.SessionId, request.Path, request.Mode, irodsFsFileHandle.GetID())

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

	promCounterForGRPCCalls.Inc()

	session, fsClient, err := server.sessionManager.GetSessionAndIRODSFSClient(request.SessionId)
	if err != nil {
		logger.Errorf("%+v", err)
		return nil, server.errorToStatus(err)
	}

	session.UpdateLastAccessTime()

	fileOpenMode := irodsclient_types.FileOpenMode(request.Mode)

	irodsFsFileHandle, err := fsClient.OpenFile(request.Path, request.Resource, request.Mode)
	if err != nil {
		logger.Errorf("%+v", err)
		return nil, server.errorToStatus(err)
	}

	if fileOpenMode.IsWrite() {
		// clear cache for the path if exists
		server.cacheStore.DeleteAllEntriesForGroup(request.Path)
	}

	poolFileHandle, err := NewPoolFileHandle(server, request.SessionId, irodsFsFileHandle, nil)
	if err != nil {
		logger.Errorf("%+v", err)
		return nil, server.errorToStatus(err)
	}

	session.AddPoolFileHandle(poolFileHandle)

	// read-only mode requires multiple file handles for prefetching
	if fileOpenMode.IsReadOnly() {
		// the file must be large enough
		if irodsFsFileHandle.GetEntry().Size > int64(iRODSIOBlockSize*3) {
			prefetchNum := 2
			poolFileHandle.AddExpectedFileHandlesForPrefetching(prefetchNum)
			go func() {
				for i := 0; i < prefetchNum; i++ {
					prefetchingIrodsFsFileHandle, err := fsClient.OpenFile(request.Path, request.Resource, request.Mode)
					if err != nil {
						logger.Errorf("%+v", err)
						return
					}

					irodsFsFileHandlesForPrefetching := []irodsfs_common.IRODSFSFileHandle{prefetchingIrodsFsFileHandle}
					poolFileHandle.AddFileHandlesForPrefetching(irodsFsFileHandlesForPrefetching)
				}
			}()
		}
	}

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

	response := &api.OpenFileResponse{
		FileHandleId: irodsFsFileHandle.GetID(),
		Entry:        responseEntry,
	}

	logger.Infof("OpenFile> pool session id %s, path %s, mode(%s), handle id %s", request.SessionId, request.Path, request.Mode, irodsFsFileHandle.GetID())

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

	promCounterForGRPCCalls.Inc()

	session, fsClient, err := server.sessionManager.GetSessionAndIRODSFSClient(request.SessionId)
	if err != nil {
		logger.Errorf("%+v", err)
		return nil, server.errorToStatus(err)
	}

	session.UpdateLastAccessTime()

	// clear cache for the path if exists
	server.cacheStore.DeleteAllEntriesForGroup(request.Path)

	err = fsClient.TruncateFile(request.Path, request.Size)
	if err != nil {
		logger.Errorf("%+v", err)
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

	promCounterForGRPCCalls.Inc()

	session, err := server.sessionManager.GetSession(request.SessionId)
	if err != nil {
		logger.Errorf("%+v", err)
		return nil, server.errorToStatus(err)
	}

	session.UpdateLastAccessTime()

	handle, err := session.GetPoolFileHandle(request.FileHandleId)
	if err != nil {
		logger.Errorf("%+v", err)
		return nil, server.errorToStatus(err)
	}

	response := &api.GetOffsetResponse{
		Offset: handle.GetOffset(),
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

	promCounterForGRPCCalls.Inc()

	session, err := server.sessionManager.GetSession(request.SessionId)
	if err != nil {
		logger.Errorf("%+v", err)
		return nil, server.errorToStatus(err)
	}

	session.UpdateLastAccessTime()

	handle, err := session.GetPoolFileHandle(request.FileHandleId)
	if err != nil {
		logger.Errorf("%+v", err)
		return nil, server.errorToStatus(err)
	}

	buffer := make([]byte, request.Length)

	readLen, err := handle.ReadAt(buffer, request.Offset)
	if err != nil && err != io.EOF {
		logger.Errorf("%+v", err)
		return nil, server.errorToStatus(err)
	}

	available := handle.GetAvailable(request.Offset + int64(readLen))

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

	promCounterForGRPCCalls.Inc()

	session, err := server.sessionManager.GetSession(request.SessionId)
	if err != nil {
		logger.Errorf("%+v", err)
		return nil, server.errorToStatus(err)
	}

	session.UpdateLastAccessTime()

	handle, err := session.GetPoolFileHandle(request.FileHandleId)
	if err != nil {
		logger.Errorf("%+v", err)
		return nil, server.errorToStatus(err)
	}

	_, err = handle.WriteAt(request.Data, request.Offset)
	if err != nil {
		logger.Errorf("%+v", err)
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

	promCounterForGRPCCalls.Inc()

	session, err := server.sessionManager.GetSession(request.SessionId)
	if err != nil {
		logger.Errorf("%+v", err)
		return nil, server.errorToStatus(err)
	}

	session.UpdateLastAccessTime()

	handle, err := session.GetPoolFileHandle(request.FileHandleId)
	if err != nil {
		logger.Errorf("%+v", err)
		return nil, server.errorToStatus(err)
	}

	err = handle.Truncate(request.Size)
	if err != nil {
		logger.Errorf("%+v", err)
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

	promCounterForGRPCCalls.Inc()

	session, err := server.sessionManager.GetSession(request.SessionId)
	if err != nil {
		logger.Errorf("%+v", err)
		return nil, server.errorToStatus(err)
	}

	session.UpdateLastAccessTime()

	handle, err := session.GetPoolFileHandle(request.FileHandleId)
	if err != nil {
		logger.Errorf("%+v", err)
		return nil, server.errorToStatus(err)
	}

	err = handle.Flush()
	if err != nil {
		logger.Errorf("%+v", err)
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

	promCounterForGRPCCalls.Inc()

	session, err := server.sessionManager.GetSession(request.SessionId)
	if err != nil {
		logger.Errorf("%+v", err)
		return nil, server.errorToStatus(err)
	}

	session.UpdateLastAccessTime()

	handle, err := session.GetPoolFileHandle(request.FileHandleId)
	if err != nil {
		logger.Errorf("%+v", err)
		return nil, server.errorToStatus(err)
	}

	session.RemovePoolFileHandle(request.FileHandleId)

	if handle.GetOpenMode() != irodsclient_types.FileOpenModeReadOnly {
		// not read-only
		// clear cache for the path if exists
		server.cacheStore.DeleteAllEntriesForGroup(handle.GetEntryPath())
	}

	err = handle.Release()
	if err != nil {
		logger.Errorf("%+v", err)
		return nil, server.errorToStatus(err)
	}

	return &api.Empty{}, nil
}

func (server *PoolServer) cacheEventHandler(sessionID string, path string, eventType irodsclient_fs.FilesystemCacheEventType) {
	event := &api.CacheEvent{
		EventType: string(eventType),
		Path:      path,
	}

	server.mutex.RLock()
	subscription, ok := server.cacheEventSubscriptions[sessionID]
	if ok {
		server.mutex.RUnlock()

		subscription.mutex.Lock()
		defer subscription.mutex.Unlock()

		subscription.eventList.PushBack(event)

		// remove overflows to not maintain too many of them
		if subscription.eventList.Len() > cacheEventQueueMax {
			for i := 0; i < subscription.eventList.Len()-cacheEventQueueMax; i++ {
				front := subscription.eventList.Front()
				if front == nil {
					break
				}

				subscription.eventList.Remove(front)
			}
		}

		return
	}

	server.mutex.RUnlock()
}

func (server *PoolServer) SubscribeCacheEvents(context context.Context, request *api.SubscribeCacheEventsRequest) (*api.Empty, error) {
	logger := log.WithFields(log.Fields{
		"package":  "service",
		"struct":   "PoolServer",
		"function": "SubscribeCacheEvents",
	})

	defer irodsfs_common_utils.StackTraceFromPanic(logger)

	logger.Infof("SubscribeCacheEvents request from pool session id %s", request.SessionId)
	defer logger.Infof("SubscribeCacheEvents response to pool session id %s", request.SessionId)

	promCounterForGRPCCalls.Inc()

	session, err := server.sessionManager.GetSession(request.SessionId)
	if err != nil {
		logger.Errorf("%+v", err)
		return nil, server.errorToStatus(err)
	}

	session.UpdateLastAccessTime()

	// subscription
	cacheEventHandler := func(path string, eventType irodsclient_fs.FilesystemCacheEventType) {
		server.cacheEventHandler(request.SessionId, path, eventType)
	}
	cacheEventHandlerID := session.AddPoolCacheEventHandler(cacheEventHandler)

	subscription := &PoolServerCacheEventSubscription{
		sessionID: request.SessionId,
		handlerID: cacheEventHandlerID,
		mutex:     sync.Mutex{},
	}

	server.mutex.Lock()

	server.cacheEventSubscriptions[request.SessionId] = subscription

	server.mutex.Unlock()

	promCounterForCacheEventSubscriptions.Inc()

	return &api.Empty{}, nil
}

func (server *PoolServer) PullCacheEvents(context context.Context, request *api.PullCacheEventsRequest) (*api.PullCacheEventsResponse, error) {
	logger := log.WithFields(log.Fields{
		"package":  "service",
		"struct":   "PoolServer",
		"function": "PullCacheEvents",
	})

	defer irodsfs_common_utils.StackTraceFromPanic(logger)

	promCounterForGRPCCalls.Inc()

	session, err := server.sessionManager.GetSession(request.SessionId)
	if err != nil {
		logger.Errorf("%+v", err)
		return nil, server.errorToStatus(err)
	}

	session.UpdateLastAccessTime()

	server.mutex.RLock()

	responseEvents := []*api.CacheEvent{}
	subscription, ok := server.cacheEventSubscriptions[request.SessionId]
	if ok {
		subscription.mutex.Lock()
		for {
			front := subscription.eventList.Front()
			if front == nil {
				break
			}

			responseEvents = append(responseEvents, front.Value.(*api.CacheEvent))
			subscription.eventList.Remove(front)
		}
		subscription.mutex.Unlock()
	}

	server.mutex.RUnlock()

	response := &api.PullCacheEventsResponse{
		Events: responseEvents,
	}

	return response, nil
}

func (server *PoolServer) UnsubscribeCacheEvents(context context.Context, request *api.UnsubscribeCacheEventsRequest) (*api.Empty, error) {
	logger := log.WithFields(log.Fields{
		"package":  "service",
		"struct":   "PoolServer",
		"function": "UnsubscribeCacheEvents",
	})

	defer irodsfs_common_utils.StackTraceFromPanic(logger)

	logger.Infof("UnsubscribeCacheEvents request from pool session id %s", request.SessionId)
	defer logger.Infof("UnsubscribeCacheEvents response to pool session id %s", request.SessionId)

	promCounterForGRPCCalls.Inc()

	session, err := server.sessionManager.GetSession(request.SessionId)
	if err != nil {
		logger.Errorf("%+v", err)
		return nil, server.errorToStatus(err)
	}

	session.UpdateLastAccessTime()

	// unsubscribe
	server.mutex.Lock()
	subscription, ok := server.cacheEventSubscriptions[request.SessionId]
	if ok {
		delete(server.cacheEventSubscriptions, request.SessionId)

		promCounterForCacheEventSubscriptions.Dec()
	}
	server.mutex.Unlock()

	if ok && subscription != nil {
		session.RemovePoolCacheEventHandler(subscription.handlerID)
	}

	return &api.Empty{}, nil
}
