package service

import (
	"context"
	"io"
	"time"

	"github.com/cockroachdb/errors"
	irodsclient_fs "github.com/cyverse/go-irodsclient/fs"
	irodsclient_types "github.com/cyverse/go-irodsclient/irods/types"
	irodsfs_common_util "github.com/cyverse/irodsfs-common/util"
	"github.com/cyverse/irodsfs-pool/commons"
	"github.com/cyverse/irodsfs-pool/service/api"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type PoolServerConfig struct {
	sessionTimeout                        time.Duration
	sessionTimeoutCheckInterval           time.Duration
	dataBlockSize                         int64
	maxDataMemCacheSize                   int64
	maxDataMemCacheBufferItems            int64
	dataMemCacheTTL                       time.Duration
	maxIOConnectionPerSession             int
	metadataCacheTimeoutSettings          []irodsclient_fs.MetadataCacheTimeoutSetting
	startNewTransaction                   bool
	maxMetadataCacheEntriesPerSession     int64
	maxMetadataCacheSizePerSession        int64
	maxMetadataCacheBufferItemsPerSession int64
	metadataCacheTTL                      time.Duration
	stagingRootPath                       string
	maxStagingDataSize                    int64
	maxCacheFileSize                      int64
	stagingDataGracePeriod                time.Duration
	sessionCloseGracePeriod               time.Duration
	logRootPath                           string
	logger                                *log.Entry
}

// PoolServer is a struct for PoolServer
type AccumulatedMetrics struct {
	Stat             uint64
	List             uint64
	Search           uint64
	CollectionCreate uint64
	CollectionDelete uint64
	CollectionRename uint64
	DataObjectCreate uint64
	DataObjectOpen   uint64
	DataObjectClose  uint64
	DataObjectDelete uint64
	DataObjectRename uint64
	DataObjectUpdate uint64
	DataObjectCopy   uint64
	DataObjectRead   uint64
	DataObjectWrite  uint64
	MetadataList     uint64
	MetadataCreate   uint64
	MetadataDelete   uint64
	MetadataUpdate   uint64
	AccessList       uint64
	AccessUpdate     uint64

	BytesSent              uint64
	BytesReceived          uint64
	CacheHit               uint64
	CacheMiss              uint64
	RequestFailures        uint64
	ConnectionFailures     uint64
	ConnectionPoolFailures uint64
}

type PoolServer struct {
	api.UnimplementedPoolAPIServer

	config              *PoolServerConfig
	sessionManager      *PoolSessionManager
	accumulatedMetrics  AccumulatedMetrics // sum of terminated sessions' final metrics
	lastReportedMetrics AccumulatedMetrics // last total reported to Prometheus (for delta)
	logger              *log.Entry
}

func NewPoolServer(config *PoolServerConfig) (*PoolServer, error) {
	if config == nil {
		return nil, errors.New("config is required")
	}

	var myLogger *log.Entry
	if config != nil && config.logger != nil {
		myLogger = config.logger
	} else {
		// create new logger object
		myLogger = log.StandardLogger().WithFields(log.Fields{})
	}

	sessionManager, err := NewPoolSessionManager(config)
	if err != nil {
		return nil, err
	}

	server := &PoolServer{
		config:         config,
		logger:         myLogger,
		sessionManager: sessionManager,
	}

	sessionManager.onBeforeSessionRelease = func(session *PoolSession) {
		server.CollectSessionMetrics(session)
	}

	return server, nil
}

func (server *PoolServer) Release() {
	defer irodsfs_common_util.StackTraceFromPanic(server.logger)

	server.logger.Info("Releasing the iRODS FUSE Pool server")
	defer server.logger.Info("Released the iRODS FUSE Pool server")

	server.sessionManager.Release()

}

func (server *PoolServer) GetSessionManager() *PoolSessionManager {
	return server.sessionManager
}

func (server *PoolServer) PrintConnectionStat() {
	server.logger.Infof("Total %d pool sessions, %d FS client instances, %d iRODS connections", server.GetPoolSessions(), server.GetIRODSFSClientInstances(), server.GetIRODSConnections())
}

func (server *PoolServer) Login(ctx context.Context, request *api.LoginRequest) (*api.LoginResponse, error) {
	defer irodsfs_common_util.StackTraceFromPanic(server.logger)

	serverLogger := server.logger.WithFields(log.Fields{
		"host":                 request.Account.Host,
		"port":                 request.Account.Port,
		"authenticationScheme": request.Account.AuthenticationScheme,
		"proxyUser":            request.Account.ProxyUser,
		"proxyZone":            request.Account.ProxyZone,
		"clientUser":           request.Account.ClientUser,
		"clientZone":           request.Account.ClientZone,
	})
	serverLogger.Infof("Login request")
	defer serverLogger.Infof("Login response")

	session, err := server.sessionManager.NewSession(request.Account, request.ApplicationName)
	if err != nil {
		sessionErr := errors.Wrapf(err, "Failed to create a new session for host %q, user %q", request.Account.Host, request.Account.ClientUser)
		server.logger.Error(sessionErr)
		return nil, commons.ErrorToStatus(err)
	}

	sessionLogger := session.logger.WithFields(log.Fields{
		"host":                 request.Account.Host,
		"port":                 request.Account.Port,
		"authenticationScheme": request.Account.AuthenticationScheme,
		"proxyUser":            request.Account.ProxyUser,
		"proxyZone":            request.Account.ProxyZone,
		"clientUser":           request.Account.ClientUser,
		"clientZone":           request.Account.ClientZone,
	})

	sessionLogger.Infof("Login request")
	defer sessionLogger.Infof("Login response")

	connID := ConnIDFromContext(ctx)
	if connID != "" {
		server.sessionManager.AddConnection(connID, session.GetID(), request.ApplicationName, request.Description)
	}

	response := &api.LoginResponse{
		SessionId: session.GetID(),
	}

	return response, nil
}

func (server *PoolServer) Logout(ctx context.Context, request *api.LogoutRequest) (*api.Empty, error) {
	defer irodsfs_common_util.StackTraceFromPanic(server.logger)

	serverLogger := server.logger.WithField("sessionID", request.SessionId)
	serverLogger.Infof("Logout request")
	defer serverLogger.Infof("Logout response")

	server.CollectPrometheusMetrics()

	session, err := server.sessionManager.GetSession(request.SessionId)
	if err != nil {
		sessionErr := errors.Wrapf(err, "failed to logout because the session for id %q is not found, ignoring...", request.SessionId)
		serverLogger.Error(sessionErr)
		return &api.Empty{}, nil
	}

	session.logger.Infof("Logout request")
	defer session.logger.Infof("Logout response")

	connID := ConnIDFromContext(ctx)
	if connID != "" {
		server.sessionManager.RemoveConnection(connID)
	}

	return &api.Empty{}, nil
}

func (server *PoolServer) LogoutAll() {
	defer irodsfs_common_util.StackTraceFromPanic(server.logger)

	server.logger.Info("LogoutAll - releasing all sessions")
	defer server.logger.Info("LogoutAll - released all sessions")

	server.sessionManager.ReleaseAllSessions()
}

func (server *PoolServer) KeepAlive(ctx context.Context, request *api.KeepAliveRequest) (*api.Empty, error) {
	defer irodsfs_common_util.StackTraceFromPanic(server.logger)

	session, err := server.sessionManager.GetSession(request.SessionId)
	if err != nil {
		sessionErr := errors.Wrapf(err, "failed to find the session for id %q", request.SessionId)
		server.logger.Debug(sessionErr)
		return nil, commons.ErrorToStatus(err)
	}

	session.logger.Debug("KeepAlive packet received")
	session.UpdateLastAccessTime()
	return &api.Empty{}, nil
}

func (server *PoolServer) getSessionAndLogger(sessionID string, fields log.Fields) (*PoolSession, *log.Entry, error) {
	session, err := server.sessionManager.GetSession(sessionID)
	if err != nil {
		server.logger.Error(err)
		return nil, nil, err
	}

	if fields == nil {
		fields = log.Fields{}
	}
	fields["sessionID"] = sessionID
	return session, session.logger.WithFields(fields), nil
}

func (server *PoolServer) GetPoolSessions() int {
	return server.sessionManager.GetTotalSessions()
}

func (server *PoolServer) GetIRODSFSClientInstances() int {
	return server.sessionManager.GetTotalIRODSFSClientInstances()
}

func (server *PoolServer) GetIRODSConnections() int {
	return server.sessionManager.GetTotalIRODSFSClientConnections()
}

func (server *PoolServer) List(ctx context.Context, request *api.ListRequest) (*api.ListResponse, error) {
	defer irodsfs_common_util.StackTraceFromPanic(server.logger)

	session, sessionLogger, err := server.getSessionAndLogger(request.SessionId, log.Fields{
		"path": request.Path,
	})
	if err != nil {
		return nil, commons.ErrorToStatus(err)
	}

	sessionLogger.Debugf("List request")
	defer sessionLogger.Debugf("List response")

	fsClient := session.GetIRODSFSClient()

	session.UpdateLastAccessTime()

	entries, err := fsClient.List(request.Path)
	if err != nil {
		sessionLogger.Error(err)
		return nil, commons.ErrorToStatus(err)
	}

	responseEntries := make([]*api.Entry, len(entries))
	for idx, entry := range entries {
		responseEntries[idx] = &api.Entry{
			Id:                entry.ID,
			Type:              string(entry.Type),
			Name:              entry.Name,
			Path:              entry.Path,
			Owner:             entry.Owner,
			Size:              entry.Size,
			DataType:          entry.DataType,
			CreateTime:        irodsfs_common_util.TimeString(entry.CreateTime),
			ModifyTime:        irodsfs_common_util.TimeString(entry.ModifyTime),
			AccessTime:        irodsfs_common_util.TimeString(entry.AccessTime),
			ChecksumAlgorithm: string(entry.CheckSumAlgorithm),
			Checksum:          entry.CheckSum,
		}
	}

	return &api.ListResponse{
		Entries: responseEntries,
	}, nil
}

func (server *PoolServer) Stat(ctx context.Context, request *api.StatRequest) (*api.StatResponse, error) {
	defer irodsfs_common_util.StackTraceFromPanic(server.logger)

	session, sessionLogger, err := server.getSessionAndLogger(request.SessionId, log.Fields{
		"path": request.Path,
	})
	if err != nil {
		return nil, commons.ErrorToStatus(err)
	}

	sessionLogger.Debugf("Stat request")
	defer sessionLogger.Debugf("Stat response")

	fsClient := session.GetIRODSFSClient()

	session.UpdateLastAccessTime()

	entry, err := fsClient.Stat(request.Path)
	if err != nil {
		if !irodsclient_types.IsFileNotFoundError(err) {
			sessionLogger.Error(err)
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
		CreateTime:        irodsfs_common_util.TimeString(entry.CreateTime),
		ModifyTime:        irodsfs_common_util.TimeString(entry.ModifyTime),
		AccessTime:        irodsfs_common_util.TimeString(entry.AccessTime),
		ChecksumAlgorithm: string(entry.CheckSumAlgorithm),
		Checksum:          entry.CheckSum,
	}

	return &api.StatResponse{
		Entry: responseEntry,
	}, nil
}

func (server *PoolServer) ExistsDir(ctx context.Context, request *api.ExistsDirRequest) (*api.ExistsDirResponse, error) {
	defer irodsfs_common_util.StackTraceFromPanic(server.logger)

	session, sessionLogger, err := server.getSessionAndLogger(request.SessionId, log.Fields{
		"path": request.Path,
	})
	if err != nil {
		return nil, commons.ErrorToStatus(err)
	}

	sessionLogger.Debugf("ExistsDir request")
	defer sessionLogger.Debugf("ExistsDir response")

	fsClient := session.GetIRODSFSClient()

	session.UpdateLastAccessTime()

	exist := fsClient.ExistsDir(request.Path)
	return &api.ExistsDirResponse{
		Exist: exist,
	}, nil
}

func (server *PoolServer) ExistsFile(ctx context.Context, request *api.ExistsFileRequest) (*api.ExistsFileResponse, error) {
	defer irodsfs_common_util.StackTraceFromPanic(server.logger)

	session, sessionLogger, err := server.getSessionAndLogger(request.SessionId, log.Fields{
		"path": request.Path,
	})
	if err != nil {
		return nil, commons.ErrorToStatus(err)
	}

	sessionLogger.Debugf("ExistsFile request")
	defer sessionLogger.Debugf("ExistsFile response")

	fsClient := session.GetIRODSFSClient()

	session.UpdateLastAccessTime()

	exist := fsClient.ExistsFile(request.Path)
	return &api.ExistsFileResponse{
		Exist: exist,
	}, nil
}

func (server *PoolServer) RemoveFile(ctx context.Context, request *api.RemoveFileRequest) (*api.Empty, error) {
	defer irodsfs_common_util.StackTraceFromPanic(server.logger)

	session, sessionLogger, err := server.getSessionAndLogger(request.SessionId, log.Fields{
		"path":  request.Path,
		"force": request.Force,
	})
	if err != nil {
		return nil, commons.ErrorToStatus(err)
	}

	sessionLogger.Debugf("RemoveFile request")
	defer sessionLogger.Debugf("RemoveFile response")

	fsClient := session.GetIRODSFSClient()

	session.UpdateLastAccessTime()

	err = fsClient.RemoveFile(request.Path, request.Force)
	if err != nil {
		sessionLogger.Error(err)
		return nil, commons.ErrorToStatus(err)
	}

	return &api.Empty{}, nil
}

func (server *PoolServer) RemoveDir(ctx context.Context, request *api.RemoveDirRequest) (*api.Empty, error) {
	defer irodsfs_common_util.StackTraceFromPanic(server.logger)

	session, sessionLogger, err := server.getSessionAndLogger(request.SessionId, log.Fields{
		"path":    request.Path,
		"recurse": request.Recurse,
		"force":   request.Force,
	})
	if err != nil {
		return nil, commons.ErrorToStatus(err)
	}

	sessionLogger.Debugf("RemoveDir request")
	defer sessionLogger.Debugf("RemoveDir response")

	fsClient := session.GetIRODSFSClient()

	session.UpdateLastAccessTime()

	err = fsClient.RemoveDir(request.Path, request.Recurse, request.Force)
	if err != nil {
		sessionLogger.Error(err)
		return nil, commons.ErrorToStatus(err)
	}

	return &api.Empty{}, nil
}

func (server *PoolServer) MakeDir(ctx context.Context, request *api.MakeDirRequest) (*api.Empty, error) {
	defer irodsfs_common_util.StackTraceFromPanic(server.logger)

	session, sessionLogger, err := server.getSessionAndLogger(request.SessionId, log.Fields{
		"path":    request.Path,
		"recurse": request.Recurse,
	})
	if err != nil {
		return nil, commons.ErrorToStatus(err)
	}

	sessionLogger.Debugf("MakeDir request")
	defer sessionLogger.Debugf("MakeDir response")

	fsClient := session.GetIRODSFSClient()

	session.UpdateLastAccessTime()

	err = fsClient.MakeDir(request.Path, request.Recurse)
	if err != nil {
		sessionLogger.Error(err)
		return nil, commons.ErrorToStatus(err)
	}

	return &api.Empty{}, nil
}

func (server *PoolServer) RenameDirToDir(ctx context.Context, request *api.RenameDirToDirRequest) (*api.Empty, error) {
	defer irodsfs_common_util.StackTraceFromPanic(server.logger)

	session, sessionLogger, err := server.getSessionAndLogger(request.SessionId, log.Fields{
		"sourcePath":      request.SourcePath,
		"destinationPath": request.DestinationPath,
	})
	if err != nil {
		return nil, commons.ErrorToStatus(err)
	}

	sessionLogger.Debugf("RenameDirToDir request")
	defer sessionLogger.Debugf("RenameDirToDir response")

	fsClient := session.GetIRODSFSClient()

	session.UpdateLastAccessTime()

	err = fsClient.RenameDirToDir(request.SourcePath, request.DestinationPath)
	if err != nil {
		sessionLogger.Error(err)
		return nil, commons.ErrorToStatus(err)
	}

	return &api.Empty{}, nil
}

func (server *PoolServer) RenameFileToFile(ctx context.Context, request *api.RenameFileToFileRequest) (*api.Empty, error) {
	defer irodsfs_common_util.StackTraceFromPanic(server.logger)

	session, sessionLogger, err := server.getSessionAndLogger(request.SessionId, log.Fields{
		"sourcePath":      request.SourcePath,
		"destinationPath": request.DestinationPath,
	})
	if err != nil {
		return nil, commons.ErrorToStatus(err)
	}

	sessionLogger.Debugf("RenameFileToFile request")
	defer sessionLogger.Debugf("RenameFileToFile response")

	fsClient := session.GetIRODSFSClient()

	session.UpdateLastAccessTime()

	err = fsClient.RenameFileToFile(request.SourcePath, request.DestinationPath)
	if err != nil {
		sessionLogger.Error(err)
		return nil, commons.ErrorToStatus(err)
	}

	return &api.Empty{}, nil
}

func (server *PoolServer) CreateFile(ctx context.Context, request *api.CreateFileRequest) (*api.CreateFileResponse, error) {
	defer irodsfs_common_util.StackTraceFromPanic(server.logger)

	session, sessionLogger, err := server.getSessionAndLogger(request.SessionId, log.Fields{
		"path": request.Path,
		"mode": request.Mode,
	})
	if err != nil {
		return nil, commons.ErrorToStatus(err)
	}

	sessionLogger.Debugf("CreateFile request")
	defer sessionLogger.Debugf("CreateFile response")

	fsClient := session.GetIRODSFSClient()

	session.UpdateLastAccessTime()

	irodsFsFileHandle, err := fsClient.CreateFile(request.Path, request.Mode)
	if err != nil {
		sessionLogger.Error(err)
		return nil, commons.ErrorToStatus(err)
	}

	poolFileHandle, err := NewPoolFileHandle(request.SessionId, irodsFsFileHandle)
	if err != nil {
		sessionLogger.Error(err)
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
		CreateTime:        irodsfs_common_util.TimeString(fsEntry.CreateTime),
		ModifyTime:        irodsfs_common_util.TimeString(fsEntry.ModifyTime),
		AccessTime:        irodsfs_common_util.TimeString(fsEntry.AccessTime),
		ChecksumAlgorithm: string(fsEntry.CheckSumAlgorithm),
		Checksum:          fsEntry.CheckSum,
	}

	return &api.CreateFileResponse{
		FileHandleId: irodsFsFileHandle.GetID(),
		Entry:        responseEntry,
	}, nil
}

func (server *PoolServer) OpenFile(ctx context.Context, request *api.OpenFileRequest) (*api.OpenFileResponse, error) {
	defer irodsfs_common_util.StackTraceFromPanic(server.logger)

	session, sessionLogger, err := server.getSessionAndLogger(request.SessionId, log.Fields{
		"path": request.Path,
		"mode": request.Mode,
	})
	if err != nil {
		return nil, commons.ErrorToStatus(err)
	}

	sessionLogger.Debugf("OpenFile request")
	defer sessionLogger.Debugf("OpenFile response")

	fsClient := session.GetIRODSFSClient()

	session.UpdateLastAccessTime()

	irodsFsFileHandle, err := fsClient.OpenFile(request.Path, request.Mode)
	if err != nil {
		sessionLogger.Error(err)
		return nil, commons.ErrorToStatus(err)
	}

	poolFileHandle, err := NewPoolFileHandle(request.SessionId, irodsFsFileHandle)
	if err != nil {
		sessionLogger.Error(err)
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
		CreateTime:        irodsfs_common_util.TimeString(fsEntry.CreateTime),
		ModifyTime:        irodsfs_common_util.TimeString(fsEntry.ModifyTime),
		AccessTime:        irodsfs_common_util.TimeString(fsEntry.AccessTime),
		ChecksumAlgorithm: string(fsEntry.CheckSumAlgorithm),
		Checksum:          fsEntry.CheckSum,
	}

	return &api.OpenFileResponse{
		FileHandleId: irodsFsFileHandle.GetID(),
		Entry:        responseEntry,
	}, nil
}

func (server *PoolServer) CreateFileBulk(ctx context.Context, request *api.CreateFileBulkRequest) (*api.CreateFileBulkResponse, error) {
	defer irodsfs_common_util.StackTraceFromPanic(server.logger)

	session, sessionLogger, err := server.getSessionAndLogger(request.SessionId, log.Fields{
		"path": request.Path,
		"mode": request.Mode,
	})
	if err != nil {
		return nil, commons.ErrorToStatus(err)
	}

	sessionLogger.Debugf("CreateFileBulk request")
	defer sessionLogger.Debugf("CreateFileBulk response")

	fsClient := session.GetIRODSFSClient()

	session.UpdateLastAccessTime()

	irodsFsFileHandle, err := fsClient.CreateFileBulk(request.Path, request.Mode)
	if err != nil {
		sessionLogger.Error(err)
		return nil, commons.ErrorToStatus(err)
	}

	poolFileHandle, err := NewPoolFileHandle(request.SessionId, irodsFsFileHandle)
	if err != nil {
		sessionLogger.Error(err)
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
		CreateTime:        irodsfs_common_util.TimeString(fsEntry.CreateTime),
		ModifyTime:        irodsfs_common_util.TimeString(fsEntry.ModifyTime),
		AccessTime:        irodsfs_common_util.TimeString(fsEntry.AccessTime),
		ChecksumAlgorithm: string(fsEntry.CheckSumAlgorithm),
		Checksum:          fsEntry.CheckSum,
	}

	return &api.CreateFileBulkResponse{
		FileHandleId: irodsFsFileHandle.GetID(),
		Entry:        responseEntry,
	}, nil
}

func (server *PoolServer) OpenFileBulk(ctx context.Context, request *api.OpenFileBulkRequest) (*api.OpenFileBulkResponse, error) {
	defer irodsfs_common_util.StackTraceFromPanic(server.logger)

	session, sessionLogger, err := server.getSessionAndLogger(request.SessionId, log.Fields{
		"path": request.Path,
		"mode": request.Mode,
	})
	if err != nil {
		return nil, commons.ErrorToStatus(err)
	}

	sessionLogger.Debugf("OpenFileBulk request")
	defer sessionLogger.Debugf("OpenFileBulk response")

	fsClient := session.GetIRODSFSClient()

	session.UpdateLastAccessTime()

	irodsFsFileHandle, err := fsClient.OpenFileBulk(request.Path, request.Mode)
	if err != nil {
		sessionLogger.Error(err)
		return nil, commons.ErrorToStatus(err)
	}

	poolFileHandle, err := NewPoolFileHandle(request.SessionId, irodsFsFileHandle)
	if err != nil {
		sessionLogger.Error(err)
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
		CreateTime:        irodsfs_common_util.TimeString(fsEntry.CreateTime),
		ModifyTime:        irodsfs_common_util.TimeString(fsEntry.ModifyTime),
		AccessTime:        irodsfs_common_util.TimeString(fsEntry.AccessTime),
		ChecksumAlgorithm: string(fsEntry.CheckSumAlgorithm),
		Checksum:          fsEntry.CheckSum,
	}

	return &api.OpenFileBulkResponse{
		FileHandleId: irodsFsFileHandle.GetID(),
		Entry:        responseEntry,
	}, nil
}

func (server *PoolServer) TruncateFile(ctx context.Context, request *api.TruncateFileRequest) (*api.Empty, error) {
	defer irodsfs_common_util.StackTraceFromPanic(server.logger)

	session, sessionLogger, err := server.getSessionAndLogger(request.SessionId, log.Fields{
		"path": request.Path,
		"size": request.Size,
	})
	if err != nil {
		return nil, commons.ErrorToStatus(err)
	}

	sessionLogger.Debugf("TruncateFile request")
	defer sessionLogger.Debugf("TruncateFile response")

	fsClient := session.GetIRODSFSClient()

	session.UpdateLastAccessTime()

	err = fsClient.TruncateFile(request.Path, request.Size)
	if err != nil {
		sessionLogger.Error(err)
		return nil, commons.ErrorToStatus(err)
	}

	return &api.Empty{}, nil
}

func (server *PoolServer) ReadAt(ctx context.Context, request *api.ReadAtRequest) (*api.ReadAtResponse, error) {
	defer irodsfs_common_util.StackTraceFromPanic(server.logger)

	session, sessionLogger, err := server.getSessionAndLogger(request.SessionId, log.Fields{
		"fileHandleID": request.FileHandleId,
		"offset":       request.Offset,
		"length":       request.Length,
	})
	if err != nil {
		return nil, commons.ErrorToStatus(err)
	}

	sessionLogger.Debugf("ReadAt request")
	defer sessionLogger.Debugf("ReadAt response")

	session.UpdateLastAccessTime()

	session.backgroundWg.Add(1)
	defer session.backgroundWg.Done()

	handle, err := session.GetPoolFileHandle(request.FileHandleId)
	if err != nil {
		sessionLogger.Error(err)
		return nil, commons.ErrorToStatus(err)
	}

	buffer := make([]byte, request.Length)

	readLen, err := handle.ReadAt(buffer, request.Offset)
	if err != nil && err != io.EOF {
		sessionLogger.Error(err)
		return nil, commons.ErrorToStatus(err)
	}

	return &api.ReadAtResponse{
		Data: buffer[:readLen],
	}, nil
}

func (server *PoolServer) WriteAt(ctx context.Context, request *api.WriteAtRequest) (*api.WriteAtResponse, error) {
	defer irodsfs_common_util.StackTraceFromPanic(server.logger)

	session, sessionLogger, err := server.getSessionAndLogger(request.SessionId, log.Fields{
		"fileHandleID": request.FileHandleId,
		"offset":       request.Offset,
		"length":       len(request.Data),
	})
	if err != nil {
		return nil, commons.ErrorToStatus(err)
	}

	sessionLogger.Debugf("WriteAt request")
	defer sessionLogger.Debugf("WriteAt response")

	session.UpdateLastAccessTime()

	session.backgroundWg.Add(1)
	defer session.backgroundWg.Done()

	handle, err := session.GetPoolFileHandle(request.FileHandleId)
	if err != nil {
		sessionLogger.Error(err)
		return nil, commons.ErrorToStatus(err)
	}

	written, err := handle.WriteAt(request.Data, request.Offset)
	if err != nil {
		sessionLogger.Error(err)
		return nil, commons.ErrorToStatus(err)
	}

	return &api.WriteAtResponse{
		Length: int32(written),
	}, nil
}

func (server *PoolServer) GetAvailable(ctx context.Context, request *api.GetAvailableRequest) (*api.GetAvailableResponse, error) {
	defer irodsfs_common_util.StackTraceFromPanic(server.logger)

	session, sessionLogger, err := server.getSessionAndLogger(request.SessionId, log.Fields{
		"fileHandleID": request.FileHandleId,
		"offset":       request.Offset,
	})
	if err != nil {
		return nil, commons.ErrorToStatus(err)
	}

	sessionLogger.Debugf("GetAvailable request")
	defer sessionLogger.Debugf("GetAvailable response")

	session.UpdateLastAccessTime()

	handle, err := session.GetPoolFileHandle(request.FileHandleId)
	if err != nil {
		sessionLogger.Error(err)
		return nil, commons.ErrorToStatus(err)
	}

	available := handle.GetAvailable(request.Offset)

	return &api.GetAvailableResponse{
		Available: available,
	}, nil
}

func (server *PoolServer) Truncate(ctx context.Context, request *api.TruncateRequest) (*api.Empty, error) {
	defer irodsfs_common_util.StackTraceFromPanic(server.logger)

	session, sessionLogger, err := server.getSessionAndLogger(request.SessionId, log.Fields{
		"fileHandleID": request.FileHandleId,
		"size":         request.Size,
	})
	if err != nil {
		return nil, commons.ErrorToStatus(err)
	}

	sessionLogger.Debugf("Truncate request")
	defer sessionLogger.Debugf("Truncate response")

	session.UpdateLastAccessTime()

	handle, err := session.GetPoolFileHandle(request.FileHandleId)
	if err != nil {
		sessionLogger.Error(err)
		return nil, commons.ErrorToStatus(err)
	}

	err = handle.Truncate(request.Size)
	if err != nil {
		sessionLogger.Error(err)
		return nil, commons.ErrorToStatus(err)
	}

	return &api.Empty{}, nil
}

func (server *PoolServer) Flush(ctx context.Context, request *api.FlushRequest) (*api.Empty, error) {
	defer irodsfs_common_util.StackTraceFromPanic(server.logger)

	session, sessionLogger, err := server.getSessionAndLogger(request.SessionId, log.Fields{
		"fileHandleID": request.FileHandleId,
	})
	if err != nil {
		return nil, commons.ErrorToStatus(err)
	}

	sessionLogger.Debugf("Flush request")
	defer sessionLogger.Debugf("Flush response")

	session.UpdateLastAccessTime()

	handle, err := session.GetPoolFileHandle(request.FileHandleId)
	if err != nil {
		sessionLogger.Error(err)
		return nil, commons.ErrorToStatus(err)
	}

	err = handle.Flush()
	if err != nil {
		sessionLogger.Error(err)
		return nil, commons.ErrorToStatus(err)
	}

	return &api.Empty{}, nil
}

func (server *PoolServer) Close(ctx context.Context, request *api.CloseRequest) (*api.Empty, error) {
	defer irodsfs_common_util.StackTraceFromPanic(server.logger)

	session, sessionLogger, err := server.getSessionAndLogger(request.SessionId, log.Fields{
		"fileHandleID": request.FileHandleId,
	})
	if err != nil {
		return nil, commons.ErrorToStatus(err)
	}

	sessionLogger.Debugf("Close request")
	defer sessionLogger.Debugf("Close response")

	session.UpdateLastAccessTime()

	handle, err := session.GetPoolFileHandle(request.FileHandleId)
	if err != nil {
		sessionLogger.Error(err)
		return nil, commons.ErrorToStatus(err)
	}

	session.RemovePoolFileHandle(request.FileHandleId)

	err = handle.Release()
	if err != nil {
		sessionLogger.Error(err)
		return nil, commons.ErrorToStatus(err)
	}

	return &api.Empty{}, nil
}

func (server *PoolServer) ReadStream(request *api.ReadStreamRequest, stream api.PoolAPI_ReadStreamServer) error {
	defer irodsfs_common_util.StackTraceFromPanic(server.logger)

	session, sessionLogger, err := server.getSessionAndLogger(request.SessionId, log.Fields{
		"irodsPath": request.IrodsPath,
		"blockSize": request.BlockSize,
		"numBlocks": request.NumBlocks,
	})
	if err != nil {
		return commons.ErrorToStatus(err)
	}

	sessionLogger.Debugf("ReadStream request")
	defer sessionLogger.Debugf("ReadStream response")

	session.UpdateLastAccessTime()

	session.backgroundWg.Add(1)
	defer session.backgroundWg.Done()

	client := session.GetIRODSFSClient()

	blockSize := int(request.BlockSize)
	if blockSize <= 0 {
		blockSize = int(server.config.dataBlockSize)
	}

	numBlocks := int(request.NumBlocks)
	if numBlocks <= 0 {
		numBlocks = 3
	}

	blockReadyCallback := func(data []byte, offset int64) error {
		return stream.Send(&api.ReadStreamResponse{
			Data:   data,
			Offset: offset,
		})
	}

	err = client.DownloadFileWithCallback(request.IrodsPath, blockSize, numBlocks, blockReadyCallback, nil)
	if err != nil {
		sessionLogger.Error(err)
		return commons.ErrorToStatus(err)
	}

	return nil
}

func (server *PoolServer) ReadStreamParallel(request *api.ReadStreamParallelRequest, stream api.PoolAPI_ReadStreamParallelServer) error {
	defer irodsfs_common_util.StackTraceFromPanic(server.logger)

	session, sessionLogger, err := server.getSessionAndLogger(request.SessionId, log.Fields{
		"irodsPath": request.IrodsPath,
		"blockSize": request.BlockSize,
		"numBlocks": request.NumBlocks,
		"taskNum":   request.TaskNum,
	})
	if err != nil {
		return commons.ErrorToStatus(err)
	}

	sessionLogger.Debugf("ReadStreamParallel request")
	defer sessionLogger.Debugf("ReadStreamParallel response")

	session.UpdateLastAccessTime()

	session.backgroundWg.Add(1)
	defer session.backgroundWg.Done()

	client := session.GetIRODSFSClient()

	blockSize := int(request.BlockSize)
	if blockSize <= 0 {
		blockSize = int(server.config.dataBlockSize)
	}

	taskNum := int(request.TaskNum)
	if taskNum <= 0 {
		taskNum = 4
	}

	numBlocks := int(request.NumBlocks)
	if numBlocks <= 0 {
		numBlocks = taskNum * 3
	}

	blockReadyCallback := func(data []byte, offset int64) error {
		return stream.Send(&api.ReadStreamParallelResponse{
			Data:   data,
			Offset: offset,
		})
	}

	err = client.DownloadFileParallelWithCallback(request.IrodsPath, blockSize, numBlocks, blockReadyCallback, taskNum, nil)
	if err != nil {
		sessionLogger.Error(err)
		return commons.ErrorToStatus(err)
	}

	return nil
}

func (server *PoolServer) WriteStream(stream api.PoolAPI_WriteStreamServer) error {
	defer irodsfs_common_util.StackTraceFromPanic(server.logger)

	request, err := stream.Recv()
	if err == io.EOF {
		return status.Error(codes.InvalidArgument, "write stream header is required")
	}
	if err != nil {
		return err
	}

	header := request.GetHeader()
	if header == nil {
		return status.Error(codes.InvalidArgument, "first write stream message must be a header")
	}

	session, sessionLogger, err := server.getSessionAndLogger(header.SessionId, log.Fields{
		"irodsPath": header.IrodsPath,
	})
	if err != nil {
		return commons.ErrorToStatus(err)
	}

	sessionLogger.Debugf("WriteStream request")
	defer sessionLogger.Debugf("WriteStream response")

	session.UpdateLastAccessTime()

	session.backgroundWg.Add(1)
	defer session.backgroundWg.Done()

	fsClient := session.GetIRODSFSClient()
	handle, err := fsClient.CreateFileBulk(header.IrodsPath, string(irodsclient_types.FileOpenModeWriteOnly))
	if err != nil {
		sessionLogger.Error(err)
		return commons.ErrorToStatus(err)
	}
	defer func() {
		if handle != nil {
			if closeErr := handle.Close(); closeErr != nil {
				sessionLogger.Error(closeErr)
			}
		}
	}()

	var totalWritten int64

	for {
		request, err = stream.Recv()
		if err == io.EOF {
			closeErr := handle.Close()
			handle = nil
			if closeErr != nil {
				sessionLogger.Error(closeErr)
				return commons.ErrorToStatus(closeErr)
			}

			return stream.SendAndClose(&api.WriteStreamResponse{
				Written: totalWritten,
			})
		}
		if err != nil {
			return err
		}

		block := request.GetBlock()
		if block == nil {
			if request.GetHeader() != nil {
				return status.Error(codes.InvalidArgument, "write stream header may only be sent once")
			}
			return status.Error(codes.InvalidArgument, "write stream block is required")
		}

		written, err := handle.WriteAt(block.Data, block.Offset)
		if err != nil {
			sessionLogger.Error(err)
			return commons.ErrorToStatus(err)
		}

		totalWritten += int64(written)
	}
}

func (server *PoolServer) CacheFile(ctx context.Context, request *api.CacheFileRequest) (*api.Empty, error) {
	defer irodsfs_common_util.StackTraceFromPanic(server.logger)

	session, sessionLogger, err := server.getSessionAndLogger(request.SessionId, log.Fields{
		"irodsPath": request.IrodsPath,
		"async":     request.Async,
	})
	if err != nil {
		return nil, commons.ErrorToStatus(err)
	}

	sessionLogger.Debugf("CacheFile request")
	defer sessionLogger.Debugf("CacheFile response")

	fsClient := session.GetIRODSFSClient()

	session.UpdateLastAccessTime()

	if request.Async {
		session.backgroundWg.Add(1)
		go func() {
			defer session.backgroundWg.Done()
			err := fsClient.CacheFile(request.IrodsPath, nil)
			if err != nil {
				sessionLogger.WithError(err).Errorf("async CacheFile %q", request.IrodsPath)
			}
		}()
		return &api.Empty{}, nil
	}

	session.backgroundWg.Add(1)
	defer session.backgroundWg.Done()

	err = fsClient.CacheFile(request.IrodsPath, nil)
	if err != nil {
		sessionLogger.Error(err)
		return nil, commons.ErrorToStatus(err)
	}

	return &api.Empty{}, nil
}

func (server *PoolServer) Sync(ctx context.Context, request *api.SyncRequest) (*api.Empty, error) {
	defer irodsfs_common_util.StackTraceFromPanic(server.logger)

	session, sessionLogger, err := server.getSessionAndLogger(request.SessionId, nil)
	if err != nil {
		return nil, commons.ErrorToStatus(err)
	}

	sessionLogger.Debugf("Sync request")
	defer sessionLogger.Debugf("Sync response")

	fsClient := session.GetIRODSFSClient()

	session.UpdateLastAccessTime()

	err = fsClient.Sync()
	if err != nil {
		sessionLogger.Error(err)
		return nil, commons.ErrorToStatus(err)
	}

	return &api.Empty{}, nil
}
