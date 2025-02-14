package service

import (
	"context"
	"fmt"
	"io"

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
	cacheEntrySizeMax int = 4 * 1024 * 1024 // 4MB
)

// PoolServerConfig is a configuration for Server
type PoolServerConfig struct {
	CacheSizeMax         int64
	CacheRootPath        string
	CacheTimeoutSettings []irodsclient_fs.MetadataCacheTimeoutSetting
	OperationTimeout     int
	SessionTimeout       int
}

// PoolServer is a struct for PoolServer
type PoolServer struct {
	api.UnimplementedPoolAPIServer

	config *PoolServerConfig

	cacheStore     irodsfs_common_cache.CacheStore
	sessionManager *PoolSessionManager
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

		cacheStore:     diskCacheStore,
		sessionManager: NewPoolSessionManager(config),
	}, nil
}

func (server *PoolServer) Release() {
	logger := log.WithFields(log.Fields{
		"package":  "service",
		"struct":   "PoolServer",
		"function": "Release",
	})

	defer irodsfs_common_utils.StackTraceFromPanic(logger)

	logger.Info("Releasing the iRODS FUSE Lite Pool server")
	defer logger.Info("Released the iRODS FUSE Lite Pool server")

	server.sessionManager.Release()

	if server.cacheStore != nil {
		server.cacheStore.Release()
		server.cacheStore = nil
	}
}

func (server *PoolServer) GetSessionManager() *PoolSessionManager {
	return server.sessionManager
}

func (server *PoolServer) Login(context context.Context, request *api.LoginRequest) (*api.LoginResponse, error) {
	logger := log.WithFields(log.Fields{
		"package":  "service",
		"struct":   "PoolServer",
		"function": "Login",
	})

	defer irodsfs_common_utils.StackTraceFromPanic(logger)

	logger.Infof("Login request from client id %q, host %q, user %q", request.ClientId, request.Account.Host, request.Account.ClientUser)
	defer logger.Infof("Login response to client id %q, host %q, user %q", request.ClientId, request.Account.Host, request.Account.ClientUser)

	promCounterForGRPCCalls.Inc()
	defer promCounterForGRPCCallReturns.Inc()

	session, err := server.sessionManager.NewSession(request.Account, request.ClientId, request.ApplicationName)
	if err != nil {
		sessionErr := xerrors.Errorf("Failed to create a new session for client id %q, host %q, user %q: %w", request.ClientId, request.Account.Host, request.Account.ClientUser, err)
		logger.Errorf("%+v", sessionErr)
		return nil, commons.ErrorToStatus(err)
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

	logger.Infof("Logout request from client, pool session id %q", request.SessionId)
	defer logger.Infof("Logout response to client, pool session id %q", request.SessionId)

	promCounterForGRPCCalls.Inc()
	defer promCounterForGRPCCallReturns.Inc()

	// collect metrics before release
	server.CollectPrometheusMetrics()

	session, err := server.sessionManager.GetSession(request.SessionId)
	if err != nil {
		// session might be already closed due to timeout, so ignore error
		sessionErr := xerrors.Errorf("failed to logout because the session for id %q is not found, ignoring...: %w", request.SessionId, err)
		logger.Errorf("%+v", sessionErr)
		return &api.Empty{}, nil
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

	server.sessionManager.ReleaseAllSessions()
}

func (server *PoolServer) KeepAlive(context context.Context, request *api.KeepAliveRequest) (*api.Empty, error) {
	logger := log.WithFields(log.Fields{
		"package":  "service",
		"struct":   "PoolServer",
		"function": "KeepAlive",
	})

	defer irodsfs_common_utils.StackTraceFromPanic(logger)

	session, err := server.sessionManager.GetSession(request.SessionId)
	if err != nil {
		// session might be already closed due to timeout, so ignore error
		sessionErr := xerrors.Errorf("failed to find the session for id %q: %w", request.SessionId, err)
		logger.Errorf("%+v", sessionErr)
		return nil, commons.ErrorToStatus(err)
	}

	session.UpdateLastAccessTime()
	return &api.Empty{}, nil
}

func (server *PoolServer) GetPoolSessions() int {
	return server.sessionManager.GetTotalSessions()
}

func (server *PoolServer) GetIRODSFSClientInstances() int {
	return server.sessionManager.GetTotalIRODSFSClientInstances()
}

func (server *PoolServer) GetIRODSConnections() int {
	logger := log.WithFields(log.Fields{
		"package":  "service",
		"struct":   "PoolServer",
		"function": "GetIRODSConnections",
	})

	defer irodsfs_common_utils.StackTraceFromPanic(logger)

	return server.sessionManager.GetTotalIRODSFSClientConnections()
}

func (server *PoolServer) List(context context.Context, request *api.ListRequest) (*api.ListResponse, error) {
	logger := log.WithFields(log.Fields{
		"package":  "service",
		"struct":   "PoolServer",
		"function": "List",
	})

	defer irodsfs_common_utils.StackTraceFromPanic(logger)

	logger.Infof("List request from pool session id %q, path %q", request.SessionId, request.Path)
	defer logger.Infof("List response to pool session id %q, path %q", request.SessionId, request.Path)

	promCounterForGRPCCalls.Inc()
	defer promCounterForGRPCCallReturns.Inc()

	session, fsClient, err := server.sessionManager.GetSessionAndIRODSFSClient(request.SessionId)
	if err != nil {
		logger.Errorf("%+v", err)
		return nil, commons.ErrorToStatus(err)
	}

	session.UpdateLastAccessTime()

	entries, err := fsClient.List(request.Path)
	if err != nil {
		logger.Errorf("%+v", err)
		return nil, commons.ErrorToStatus(err)
	}

	responseEntries := make([]*api.Entry, len(entries))
	idx := 0
	for _, entry := range entries {
		responseEntries[idx] = &api.Entry{
			Id:                entry.ID,
			Type:              string(entry.Type),
			Name:              entry.Name,
			Path:              entry.Path,
			Owner:             entry.Owner,
			Size:              entry.Size,
			DataType:          entry.DataType,
			CreateTime:        irodsfs_common_utils.MakeTimeToString(entry.CreateTime),
			ModifyTime:        irodsfs_common_utils.MakeTimeToString(entry.ModifyTime),
			ChecksumAlgorithm: string(entry.CheckSumAlgorithm),
			Checksum:          entry.CheckSum,
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

	logger.Infof("Stat request from pool session id %q, path %q", request.SessionId, request.Path)
	defer logger.Infof("Stat response to pool session id %q, path %q", request.SessionId, request.Path)

	promCounterForGRPCCalls.Inc()
	defer promCounterForGRPCCallReturns.Inc()

	session, fsClient, err := server.sessionManager.GetSessionAndIRODSFSClient(request.SessionId)
	if err != nil {
		logger.Errorf("%+v", err)
		return nil, commons.ErrorToStatus(err)
	}

	session.UpdateLastAccessTime()

	entry, err := fsClient.Stat(request.Path)
	if err != nil {
		if !irodsclient_types.IsFileNotFoundError(err) {
			logger.Errorf("%+v", err)
		}

		return nil, commons.ErrorToStatus(err)
	}

	responseEntry := &api.Entry{
		Id:                entry.ID,
		Type:              string(entry.Type),
		Name:              entry.Name,
		Path:              entry.Path,
		Owner:             entry.Owner,
		Size:              entry.Size,
		DataType:          entry.DataType,
		CreateTime:        irodsfs_common_utils.MakeTimeToString(entry.CreateTime),
		ModifyTime:        irodsfs_common_utils.MakeTimeToString(entry.ModifyTime),
		ChecksumAlgorithm: string(entry.CheckSumAlgorithm),
		Checksum:          entry.CheckSum,
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

	logger.Infof("ListXattr request from pool session id %q, path %q", request.SessionId, request.Path)
	defer logger.Infof("ListXattr response to pool session id %q, path %q", request.SessionId, request.Path)

	promCounterForGRPCCalls.Inc()
	defer promCounterForGRPCCallReturns.Inc()

	session, fsClient, err := server.sessionManager.GetSessionAndIRODSFSClient(request.SessionId)
	if err != nil {
		logger.Errorf("%+v", err)
		return nil, commons.ErrorToStatus(err)
	}

	session.UpdateLastAccessTime()

	irodsMetadata, err := fsClient.ListXattr(request.Path)
	if err != nil {
		logger.Errorf("%+v", err)
		return nil, commons.ErrorToStatus(err)
	}

	responseMetadata := make([]*api.Metadata, len(irodsMetadata))
	idx := 0
	for _, irodsMeta := range irodsMetadata {
		responseMetadata[idx] = &api.Metadata{
			Id:         irodsMeta.AVUID,
			Name:       irodsMeta.Name,
			Value:      irodsMeta.Value,
			Unit:       irodsMeta.Units,
			CreateTime: irodsfs_common_utils.MakeTimeToString(irodsMeta.CreateTime),
			ModifyTime: irodsfs_common_utils.MakeTimeToString(irodsMeta.ModifyTime),
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

	logger.Infof("GetXattr request from pool session id %q, path %q, name %q", request.SessionId, request.Path, request.Name)
	defer logger.Infof("GetXattr response to pool session id %q, path %q, name %q", request.SessionId, request.Path, request.Name)

	promCounterForGRPCCalls.Inc()
	defer promCounterForGRPCCallReturns.Inc()

	session, fsClient, err := server.sessionManager.GetSessionAndIRODSFSClient(request.SessionId)
	if err != nil {
		logger.Errorf("%+v", err)
		return nil, commons.ErrorToStatus(err)
	}

	session.UpdateLastAccessTime()

	irodsMeta, err := fsClient.GetXattr(request.Path, request.Name)
	if err != nil {
		logger.Errorf("%+v", err)
		return nil, commons.ErrorToStatus(err)
	}

	if irodsMeta == nil {
		// not exist
		errorMessage := fmt.Sprintf("failed to find xattr %q", request.Name)
		return nil, status.Error(codes.NotFound, errorMessage)
	}

	responseMeta := &api.Metadata{
		Id:         irodsMeta.AVUID,
		Name:       irodsMeta.Name,
		Value:      irodsMeta.Value,
		Unit:       irodsMeta.Units,
		CreateTime: irodsfs_common_utils.MakeTimeToString(irodsMeta.CreateTime),
		ModifyTime: irodsfs_common_utils.MakeTimeToString(irodsMeta.ModifyTime),
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

	logger.Infof("SetXattr request from pool session id %q, path %q, name %q", request.SessionId, request.Path, request.Name)
	defer logger.Infof("SetXattr response to pool session id %q, path %q, name %q", request.SessionId, request.Path, request.Name)

	promCounterForGRPCCalls.Inc()
	defer promCounterForGRPCCallReturns.Inc()

	session, fsClient, err := server.sessionManager.GetSessionAndIRODSFSClient(request.SessionId)
	if err != nil {
		logger.Errorf("%+v", err)
		return nil, commons.ErrorToStatus(err)
	}

	session.UpdateLastAccessTime()

	err = fsClient.SetXattr(request.Path, request.Name, request.Value)
	if err != nil {
		logger.Errorf("%+v", err)
		return nil, commons.ErrorToStatus(err)
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

	logger.Infof("RemoveXattr request from pool session id %q, path %q, name %q", request.SessionId, request.Path, request.Name)
	defer logger.Infof("RemoveXattr response to pool session id %q, path %q, name %q", request.SessionId, request.Path, request.Name)

	promCounterForGRPCCalls.Inc()
	defer promCounterForGRPCCallReturns.Inc()

	session, fsClient, err := server.sessionManager.GetSessionAndIRODSFSClient(request.SessionId)
	if err != nil {
		logger.Errorf("%+v", err)
		return nil, commons.ErrorToStatus(err)
	}

	session.UpdateLastAccessTime()

	err = fsClient.RemoveXattr(request.Path, request.Name)
	if err != nil {
		logger.Errorf("%+v", err)
		return nil, commons.ErrorToStatus(err)
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

	logger.Infof("ExistsDir request from pool session id %q, path %q", request.SessionId, request.Path)
	defer logger.Infof("ExistsDir response to pool session id %q, path %q", request.SessionId, request.Path)

	promCounterForGRPCCalls.Inc()
	defer promCounterForGRPCCallReturns.Inc()

	session, fsClient, err := server.sessionManager.GetSessionAndIRODSFSClient(request.SessionId)
	if err != nil {
		logger.Errorf("%+v", err)
		return nil, commons.ErrorToStatus(err)
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

	logger.Infof("ExistsFile request from pool session id %q, path %q", request.SessionId, request.Path)
	defer logger.Infof("ExistsFile response to pool session id %q, path %q", request.SessionId, request.Path)

	promCounterForGRPCCalls.Inc()
	defer promCounterForGRPCCallReturns.Inc()

	session, fsClient, err := server.sessionManager.GetSessionAndIRODSFSClient(request.SessionId)
	if err != nil {
		logger.Errorf("%+v", err)
		return nil, commons.ErrorToStatus(err)
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

	logger.Infof("ListUserGroups request from pool session id %q, user name %q", request.SessionId, request.UserName)
	defer logger.Infof("ListUserGroups response to pool session id %q, user name %q", request.SessionId, request.UserName)

	promCounterForGRPCCalls.Inc()
	defer promCounterForGRPCCallReturns.Inc()

	session, fsClient, err := server.sessionManager.GetSessionAndIRODSFSClient(request.SessionId)
	if err != nil {
		logger.Errorf("%+v", err)
		return nil, commons.ErrorToStatus(err)
	}

	session.UpdateLastAccessTime()

	groups, err := fsClient.ListUserGroups(request.Zone, request.UserName)
	if err != nil {
		logger.Errorf("%+v", err)
		return nil, commons.ErrorToStatus(err)
	}

	responseGroups := make([]*api.User, len(groups))
	idx := 0
	for _, group := range groups {
		responseGroups[idx] = &api.User{
			Id:   group.ID,
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

	logger.Infof("ListDirACLs request from pool session id %q, path %q", request.SessionId, request.Path)
	defer logger.Infof("ListDirACLs response to pool session id %q, path %q", request.SessionId, request.Path)

	promCounterForGRPCCalls.Inc()
	defer promCounterForGRPCCallReturns.Inc()

	session, fsClient, err := server.sessionManager.GetSessionAndIRODSFSClient(request.SessionId)
	if err != nil {
		logger.Errorf("%+v", err)
		return nil, commons.ErrorToStatus(err)
	}

	session.UpdateLastAccessTime()

	accesses, err := fsClient.ListDirACLs(request.Path)
	if err != nil {
		logger.Errorf("%+v", err)
		return nil, commons.ErrorToStatus(err)
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

	logger.Infof("ListFileACLs request from pool session id %q, path %q", request.SessionId, request.Path)
	defer logger.Infof("ListFileACLs response to pool session id %q, path %q", request.SessionId, request.Path)

	promCounterForGRPCCalls.Inc()
	defer promCounterForGRPCCallReturns.Inc()

	session, fsClient, err := server.sessionManager.GetSessionAndIRODSFSClient(request.SessionId)
	if err != nil {
		logger.Errorf("%+v", err)
		return nil, commons.ErrorToStatus(err)
	}

	session.UpdateLastAccessTime()

	accesses, err := fsClient.ListFileACLs(request.Path)
	if err != nil {
		logger.Errorf("%+v", err)
		return nil, commons.ErrorToStatus(err)
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

	logger.Infof("ListACLsForEntries request from pool session id %q, path %q", request.SessionId, request.Path)
	defer logger.Infof("ListACLsForEntries response to pool session id %q, path %q", request.SessionId, request.Path)

	promCounterForGRPCCalls.Inc()
	defer promCounterForGRPCCallReturns.Inc()

	session, fsClient, err := server.sessionManager.GetSessionAndIRODSFSClient(request.SessionId)
	if err != nil {
		logger.Errorf("%+v", err)
		return nil, commons.ErrorToStatus(err)
	}

	session.UpdateLastAccessTime()

	accesses, err := fsClient.ListACLsForEntries(request.Path)
	if err != nil {
		logger.Errorf("%+v", err)
		return nil, commons.ErrorToStatus(err)
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

	logger.Infof("RemoveFile request from pool session id %q, path %q", request.SessionId, request.Path)
	defer logger.Infof("RemoveFile response to pool session id %q, path %q", request.SessionId, request.Path)

	promCounterForGRPCCalls.Inc()
	defer promCounterForGRPCCallReturns.Inc()

	session, fsClient, err := server.sessionManager.GetSessionAndIRODSFSClient(request.SessionId)
	if err != nil {
		logger.Errorf("%+v", err)
		return nil, commons.ErrorToStatus(err)
	}

	session.UpdateLastAccessTime()

	err = fsClient.RemoveFile(request.Path, request.Force)
	if err != nil {
		logger.Errorf("%+v", err)
		return nil, commons.ErrorToStatus(err)
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

	logger.Infof("RemoveDir request from pool session id %q, path %q", request.SessionId, request.Path)
	defer logger.Infof("RemoveDir response to pool session id %q, path %q", request.SessionId, request.Path)

	promCounterForGRPCCalls.Inc()
	defer promCounterForGRPCCallReturns.Inc()

	session, fsClient, err := server.sessionManager.GetSessionAndIRODSFSClient(request.SessionId)
	if err != nil {
		logger.Errorf("%+v", err)
		return nil, commons.ErrorToStatus(err)
	}

	session.UpdateLastAccessTime()

	err = fsClient.RemoveDir(request.Path, request.Recurse, request.Force)
	if err != nil {
		logger.Errorf("%+v", err)
		return nil, commons.ErrorToStatus(err)
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

	logger.Infof("MakeDir request from pool session id %q, path %q", request.SessionId, request.Path)
	defer logger.Infof("MakeDir response to pool session id %q, path %q", request.SessionId, request.Path)

	promCounterForGRPCCalls.Inc()
	defer promCounterForGRPCCallReturns.Inc()

	session, fsClient, err := server.sessionManager.GetSessionAndIRODSFSClient(request.SessionId)
	if err != nil {
		logger.Errorf("%+v", err)
		return nil, commons.ErrorToStatus(err)
	}

	session.UpdateLastAccessTime()

	err = fsClient.MakeDir(request.Path, request.Recurse)
	if err != nil {
		logger.Errorf("%+v", err)
		return nil, commons.ErrorToStatus(err)
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

	logger.Infof("RenameDirToDir request from pool session id %q, source path %q -> destination path %q", request.SessionId, request.SourcePath, request.DestinationPath)
	defer logger.Infof("RenameDirToDir response to pool session id %q, source path %q -> destination path %q", request.SessionId, request.SourcePath, request.DestinationPath)

	promCounterForGRPCCalls.Inc()
	defer promCounterForGRPCCallReturns.Inc()

	session, fsClient, err := server.sessionManager.GetSessionAndIRODSFSClient(request.SessionId)
	if err != nil {
		logger.Errorf("%+v", err)
		return nil, commons.ErrorToStatus(err)
	}

	session.UpdateLastAccessTime()

	err = fsClient.RenameDirToDir(request.SourcePath, request.DestinationPath)
	if err != nil {
		logger.Errorf("%+v", err)
		return nil, commons.ErrorToStatus(err)
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

	logger.Infof("RenameFileToFile request from pool session id %q, source path %q -> destination path %q", request.SessionId, request.SourcePath, request.DestinationPath)
	defer logger.Infof("RenameFileToFile response to pool session id %q, source path %q -> destination path %q", request.SessionId, request.SourcePath, request.DestinationPath)

	promCounterForGRPCCalls.Inc()
	defer promCounterForGRPCCallReturns.Inc()

	session, fsClient, err := server.sessionManager.GetSessionAndIRODSFSClient(request.SessionId)
	if err != nil {
		logger.Errorf("%+v", err)
		return nil, commons.ErrorToStatus(err)
	}

	session.UpdateLastAccessTime()

	err = fsClient.RenameFileToFile(request.SourcePath, request.DestinationPath)
	if err != nil {
		logger.Errorf("%+v", err)
		return nil, commons.ErrorToStatus(err)
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

	logger.Infof("CreateFile request from pool session id %q, path %q, mode %q", request.SessionId, request.Path, request.Mode)
	defer logger.Infof("CreateFile response to pool session id %q, path %q, mode %q", request.SessionId, request.Path, request.Mode)

	promCounterForGRPCCalls.Inc()
	defer promCounterForGRPCCallReturns.Inc()

	session, fsClient, err := server.sessionManager.GetSessionAndIRODSFSClient(request.SessionId)
	if err != nil {
		logger.Errorf("%+v", err)
		return nil, commons.ErrorToStatus(err)
	}

	session.UpdateLastAccessTime()

	// clear cache for the path if exists
	server.cacheStore.DeleteAllEntriesForGroup(request.Path)

	irodsFsFileHandle, err := fsClient.CreateFile(request.Path, request.Resource, request.Mode)
	if err != nil {
		logger.Errorf("%+v", err)
		return nil, commons.ErrorToStatus(err)
	}

	fileOpenMode := irodsclient_types.FileOpenMode(request.Mode)
	if fileOpenMode.IsWrite() {
		// clear cache for the path if exists
		server.cacheStore.DeleteAllEntriesForGroup(request.Path)
	}

	poolFileHandle, err := NewPoolFileHandle(server, request.SessionId, irodsFsFileHandle, nil)
	if err != nil {
		logger.Errorf("%+v", err)
		return nil, commons.ErrorToStatus(err)
	}

	session.AddPoolFileHandle(poolFileHandle)

	fsEntry := irodsFsFileHandle.GetEntry()

	responseEntry := &api.Entry{
		Id:                fsEntry.ID,
		Type:              string(fsEntry.Type),
		Name:              fsEntry.Name,
		Path:              fsEntry.Path,
		Owner:             fsEntry.Owner,
		Size:              fsEntry.Size,
		DataType:          fsEntry.DataType,
		CreateTime:        irodsfs_common_utils.MakeTimeToString(fsEntry.CreateTime),
		ModifyTime:        irodsfs_common_utils.MakeTimeToString(fsEntry.ModifyTime),
		ChecksumAlgorithm: string(fsEntry.CheckSumAlgorithm),
		Checksum:          fsEntry.CheckSum,
	}

	response := &api.CreateFileResponse{
		FileHandleId: irodsFsFileHandle.GetID(),
		Entry:        responseEntry,
	}

	logger.Infof("CreateFile> pool session id %q, path %q, mode %q, handle id %q", request.SessionId, request.Path, request.Mode, irodsFsFileHandle.GetID())

	return response, nil
}

func (server *PoolServer) OpenFile(context context.Context, request *api.OpenFileRequest) (*api.OpenFileResponse, error) {
	logger := log.WithFields(log.Fields{
		"package":  "service",
		"struct":   "PoolServer",
		"function": "OpenFile",
	})

	defer irodsfs_common_utils.StackTraceFromPanic(logger)

	logger.Infof("OpenFile request from pool session id %q, path %q, mode %q", request.SessionId, request.Path, request.Mode)
	defer logger.Infof("OpenFile response to pool session id %q, path %q, mode %q", request.SessionId, request.Path, request.Mode)

	promCounterForGRPCCalls.Inc()
	defer promCounterForGRPCCallReturns.Inc()

	session, fsClient, err := server.sessionManager.GetSessionAndIRODSFSClient(request.SessionId)
	if err != nil {
		logger.Errorf("%+v", err)
		return nil, commons.ErrorToStatus(err)
	}

	session.UpdateLastAccessTime()

	fileOpenMode := irodsclient_types.FileOpenMode(request.Mode)

	irodsFsFileHandle, err := fsClient.OpenFile(request.Path, request.Resource, request.Mode)
	if err != nil {
		logger.Errorf("%+v", err)
		return nil, commons.ErrorToStatus(err)
	}

	if fileOpenMode.IsWrite() {
		// clear cache for the path if exists
		server.cacheStore.DeleteAllEntriesForGroup(request.Path)
	}

	poolFileHandle, err := NewPoolFileHandle(server, request.SessionId, irodsFsFileHandle, nil)
	if err != nil {
		logger.Errorf("%+v", err)
		return nil, commons.ErrorToStatus(err)
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
						poolFileHandle.AddExpectedFileHandlesForPrefetching(-1)
					} else {
						irodsFsFileHandlesForPrefetching := []irodsfs_common.IRODSFSFileHandle{prefetchingIrodsFsFileHandle}
						poolFileHandle.AddFileHandlesForPrefetching(irodsFsFileHandlesForPrefetching)
					}
				}
			}()
		}
	}

	fsEntry := irodsFsFileHandle.GetEntry()

	responseEntry := &api.Entry{
		Id:                fsEntry.ID,
		Type:              string(fsEntry.Type),
		Name:              fsEntry.Name,
		Path:              fsEntry.Path,
		Owner:             fsEntry.Owner,
		Size:              fsEntry.Size,
		DataType:          fsEntry.DataType,
		CreateTime:        irodsfs_common_utils.MakeTimeToString(fsEntry.CreateTime),
		ModifyTime:        irodsfs_common_utils.MakeTimeToString(fsEntry.ModifyTime),
		ChecksumAlgorithm: string(fsEntry.CheckSumAlgorithm),
		Checksum:          fsEntry.CheckSum,
	}

	response := &api.OpenFileResponse{
		FileHandleId: irodsFsFileHandle.GetID(),
		Entry:        responseEntry,
	}

	logger.Infof("OpenFile> pool session id %q, path %q, mode %q, handle id %q", request.SessionId, request.Path, request.Mode, irodsFsFileHandle.GetID())

	return response, nil
}

func (server *PoolServer) TruncateFile(context context.Context, request *api.TruncateFileRequest) (*api.Empty, error) {
	logger := log.WithFields(log.Fields{
		"package":  "service",
		"struct":   "PoolServer",
		"function": "TruncateFile",
	})

	defer irodsfs_common_utils.StackTraceFromPanic(logger)

	logger.Infof("TruncateFile request from pool session id %q, path %q", request.SessionId, request.Path)
	defer logger.Infof("TruncateFile response to pool session id %q, path %q", request.SessionId, request.Path)

	promCounterForGRPCCalls.Inc()
	defer promCounterForGRPCCallReturns.Inc()

	session, fsClient, err := server.sessionManager.GetSessionAndIRODSFSClient(request.SessionId)
	if err != nil {
		logger.Errorf("%+v", err)
		return nil, commons.ErrorToStatus(err)
	}

	session.UpdateLastAccessTime()

	// clear cache for the path if exists
	server.cacheStore.DeleteAllEntriesForGroup(request.Path)

	err = fsClient.TruncateFile(request.Path, request.Size)
	if err != nil {
		logger.Errorf("%+v", err)
		return nil, commons.ErrorToStatus(err)
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

	logger.Debugf("GetOffset request from pool session id %q, pool file handle id %q", request.SessionId, request.FileHandleId)
	defer logger.Debugf("GetOffset response to pool session id %q, pool file handle id %q", request.SessionId, request.FileHandleId)

	promCounterForGRPCCalls.Inc()
	defer promCounterForGRPCCallReturns.Inc()

	session, err := server.sessionManager.GetSession(request.SessionId)
	if err != nil {
		logger.Errorf("%+v", err)
		return nil, commons.ErrorToStatus(err)
	}

	session.UpdateLastAccessTime()

	handle, err := session.GetPoolFileHandle(request.FileHandleId)
	if err != nil {
		logger.Errorf("%+v", err)
		return nil, commons.ErrorToStatus(err)
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

	logger.Debugf("ReadAt request from pool session id %q, pool file handle id %q, offset %d, length %d", request.SessionId, request.FileHandleId, request.Offset, request.Length)
	defer logger.Debugf("ReadAt response to pool session id %q, pool file handle id %q, offset %d, length %d", request.SessionId, request.FileHandleId, request.Offset, request.Length)

	promCounterForGRPCCalls.Inc()
	defer promCounterForGRPCCallReturns.Inc()

	session, err := server.sessionManager.GetSession(request.SessionId)
	if err != nil {
		logger.Errorf("%+v", err)
		return nil, commons.ErrorToStatus(err)
	}

	session.UpdateLastAccessTime()

	handle, err := session.GetPoolFileHandle(request.FileHandleId)
	if err != nil {
		logger.Errorf("%+v", err)
		return nil, commons.ErrorToStatus(err)
	}

	buffer := make([]byte, request.Length)

	readLen, err := handle.ReadAt(buffer, request.Offset)
	if err != nil && err != io.EOF {
		logger.Errorf("%+v", err)
		return nil, commons.ErrorToStatus(err)
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

	logger.Debugf("WriteAt request from pool session id %q, pool file handle id %q, offset %d, length %d", request.SessionId, request.FileHandleId, request.Offset, len(request.Data))
	defer logger.Debugf("WriteAt response to pool session id %q, pool file handle id %q, offset %d, length %d", request.SessionId, request.FileHandleId, request.Offset, len(request.Data))

	promCounterForGRPCCalls.Inc()
	defer promCounterForGRPCCallReturns.Inc()

	session, err := server.sessionManager.GetSession(request.SessionId)
	if err != nil {
		logger.Errorf("%+v", err)
		return nil, commons.ErrorToStatus(err)
	}

	session.UpdateLastAccessTime()

	handle, err := session.GetPoolFileHandle(request.FileHandleId)
	if err != nil {
		logger.Errorf("%+v", err)
		return nil, commons.ErrorToStatus(err)
	}

	_, err = handle.WriteAt(request.Data, request.Offset)
	if err != nil {
		logger.Errorf("%+v", err)
		return nil, commons.ErrorToStatus(err)
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

	logger.Infof("Truncate request from pool session id %q, pool file handle id %q, size %d", request.SessionId, request.FileHandleId, request.Size)
	defer logger.Infof("Truncate response to pool session id %q, pool file handle id %q, size %d", request.SessionId, request.FileHandleId, request.Size)

	promCounterForGRPCCalls.Inc()
	defer promCounterForGRPCCallReturns.Inc()

	session, err := server.sessionManager.GetSession(request.SessionId)
	if err != nil {
		logger.Errorf("%+v", err)
		return nil, commons.ErrorToStatus(err)
	}

	session.UpdateLastAccessTime()

	handle, err := session.GetPoolFileHandle(request.FileHandleId)
	if err != nil {
		logger.Errorf("%+v", err)
		return nil, commons.ErrorToStatus(err)
	}

	err = handle.Truncate(request.Size)
	if err != nil {
		logger.Errorf("%+v", err)
		return nil, commons.ErrorToStatus(err)
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

	logger.Infof("Flush request from pool session id %q, pool file handle id %q", request.SessionId, request.FileHandleId)
	defer logger.Infof("Flush response to pool session id %q, pool file handle id %q", request.SessionId, request.FileHandleId)

	promCounterForGRPCCalls.Inc()
	defer promCounterForGRPCCallReturns.Inc()

	session, err := server.sessionManager.GetSession(request.SessionId)
	if err != nil {
		logger.Errorf("%+v", err)
		return nil, commons.ErrorToStatus(err)
	}

	session.UpdateLastAccessTime()

	handle, err := session.GetPoolFileHandle(request.FileHandleId)
	if err != nil {
		logger.Errorf("%+v", err)
		return nil, commons.ErrorToStatus(err)
	}

	err = handle.Flush()
	if err != nil {
		logger.Errorf("%+v", err)
		return nil, commons.ErrorToStatus(err)
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

	logger.Infof("Close request from pool session id %q, pool file handle id %q", request.SessionId, request.FileHandleId)
	defer logger.Infof("Close response to pool session id %q, pool file handle id %q", request.SessionId, request.FileHandleId)

	promCounterForGRPCCalls.Inc()
	defer promCounterForGRPCCallReturns.Inc()

	session, err := server.sessionManager.GetSession(request.SessionId)
	if err != nil {
		logger.Errorf("%+v", err)
		return nil, commons.ErrorToStatus(err)
	}

	session.UpdateLastAccessTime()

	handle, err := session.GetPoolFileHandle(request.FileHandleId)
	if err != nil {
		logger.Errorf("%+v", err)
		return nil, commons.ErrorToStatus(err)
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
		return nil, commons.ErrorToStatus(err)
	}

	return &api.Empty{}, nil
}
