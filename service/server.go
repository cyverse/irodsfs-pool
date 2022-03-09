package service

import (
	context "context"
	"fmt"
	"runtime/debug"
	"sync"

	irodsclient_types "github.com/cyverse/go-irodsclient/irods/types"
	"github.com/cyverse/irodsfs-pool/commons"
	"github.com/cyverse/irodsfs-pool/service/api"
	"github.com/cyverse/irodsfs-pool/service/io"
	"github.com/cyverse/irodsfs-pool/utils"
	"github.com/rs/xid"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ServerConfig is a configuration for Server
type ServerConfig struct {
	BufferSizeMax        int64
	CacheSizeMax         int64
	CacheRootPath        string
	CacheTimeoutSettings []commons.MetadataCacheTimeoutSetting
}

// Server is a struct for Server
type Server struct {
	api.UnimplementedPoolAPIServer

	config *ServerConfig

	buffer io.Buffer
	cache  io.Cache

	sessions         map[string]*Session         // key: session id
	irodsConnections map[string]*IRODSConnection // key: connection id
	mutex            sync.RWMutex                // mutex to access Sessions
}

func NewServer(config *ServerConfig) (*Server, error) {
	var ramBuffer io.Buffer
	var err error
	if config.BufferSizeMax > 0 {
		ramBuffer = io.NewRAMBuffer(config.BufferSizeMax)
	}

	var diskCache io.Cache
	if config.CacheSizeMax > 0 {
		diskCache, err = io.NewDiskCache(config.CacheSizeMax, config.CacheRootPath)
		if err != nil {
			return nil, err
		}
	}

	return &Server{
		config: config,

		buffer: ramBuffer,
		cache:  diskCache,

		sessions:         map[string]*Session{},
		irodsConnections: map[string]*IRODSConnection{},
		mutex:            sync.RWMutex{},
	}, nil
}

func (server *Server) Release() {
	logger := log.WithFields(log.Fields{
		"package":  "service",
		"struct":   "Server",
		"function": "Release",
	})

	defer func() {
		if r := recover(); r != nil {
			logger.Errorf("stacktrace from panic: %s", string(debug.Stack()))
			logger.Panic(r)
		}
	}()

	logger.Info("Release")
	defer logger.Info("Released")

	server.mutex.Lock()

	for _, session := range server.sessions {
		logger.Infof("Logout session for client id %s", session.GetClientID())

		session.Release()
	}
	server.sessions = map[string]*Session{}

	for _, irodsConnection := range server.irodsConnections {
		logger.Infof("Logout connection for connection id %s", irodsConnection.GetID())

		// close file system for connection
		irodsConnection.Release()
	}
	server.irodsConnections = map[string]*IRODSConnection{}

	server.mutex.Unlock()

	if server.buffer != nil {
		server.buffer.Release()
		server.buffer = nil
	}

	if server.cache != nil {
		server.cache.Release()
		server.cache = nil
	}
}

func (server *Server) errorToStatus(err error) error {
	if irodsclient_types.IsFileNotFoundError(err) {
		return status.Error(codes.NotFound, err.Error())
	} else if irodsclient_types.IsCollectionNotEmptyError(err) {
		// there's no matching error type for not empty
		return status.Error(codes.AlreadyExists, err.Error())
	}

	return status.Error(codes.Internal, err.Error())
}

func (server *Server) Login(context context.Context, request *api.LoginRequest) (*api.LoginResponse, error) {
	logger := log.WithFields(log.Fields{
		"package":  "service",
		"struct":   "Server",
		"function": "Login",
	})

	defer func() {
		if r := recover(); r != nil {
			logger.Errorf("stacktrace from panic: %s", string(debug.Stack()))
			logger.Panic(r)
		}
	}()

	logger.Infof("Login request from client id %s, host %s, user %s", request.ClientId, request.Account.Host, request.Account.ClientUser)
	defer logger.Infof("Login response to client id %s, host %s, user %s", request.ClientId, request.Account.Host, request.Account.ClientUser)

	server.mutex.Lock()
	defer server.mutex.Unlock()

	connectionID := getConnectionID(request.Account)

	// create a session
	session := NewSession(request.ClientId, connectionID)
	sessionID := session.GetID()

	// find connection for the same account if exists
	if irodsConnection, ok := server.irodsConnections[connectionID]; ok {
		logger.Infof("Reusing existing connection: %s", connectionID)

		irodsConnection.AddSession(sessionID)
	} else {
		logger.Infof("Creating a new connection: %s", connectionID)

		// new connection
		newConn, err := NewIRODSConnection(connectionID, request.Account, request.ApplicationName, server.config.CacheTimeoutSettings)
		if err != nil {
			logger.WithError(err).Error("failed to create a new connection")
			return nil, server.errorToStatus(err)
		}

		newConn.AddSession(sessionID)

		server.irodsConnections[connectionID] = newConn
	}

	server.sessions[sessionID] = session

	response := &api.LoginResponse{
		SessionId: sessionID,
	}

	return response, nil
}

func (server *Server) Logout(context context.Context, request *api.LogoutRequest) (*api.Empty, error) {
	logger := log.WithFields(log.Fields{
		"package":  "service",
		"struct":   "Server",
		"function": "Logout",
	})

	defer func() {
		if r := recover(); r != nil {
			logger.Errorf("stacktrace from panic: %s", string(debug.Stack()))
			logger.Panic(r)
		}
	}()

	logger.Infof("Logout request from client, session id %s", request.SessionId)
	defer logger.Infof("Logout response to client, session id %s", request.SessionId)

	server.mutex.Lock()
	defer server.mutex.Unlock()

	if session, ok := server.sessions[request.SessionId]; ok {
		logger.Infof("Logout session for client id %s", session.GetClientID())

		connectionID := session.GetConnectionID()
		session.Release()
		delete(server.sessions, request.SessionId)

		if connection, ok := server.irodsConnections[connectionID]; ok {
			connection.RemoveSession(request.SessionId)
			if connection.ReleaseIfNoSession() {
				delete(server.irodsConnections, connectionID)
			}
		}

		return &api.Empty{}, nil
	}

	// session might be already closed due to timeout
	return &api.Empty{}, nil
}

func (server *Server) LogoutAll() {
	logger := log.WithFields(log.Fields{
		"package":  "service",
		"struct":   "Server",
		"function": "LogoutAll",
	})

	defer func() {
		if r := recover(); r != nil {
			logger.Errorf("stacktrace from panic: %s", string(debug.Stack()))
			logger.Panic(r)
		}
	}()

	logger.Info("Logout All")
	defer logger.Info("Logged-out All")

	server.mutex.Lock()
	defer server.mutex.Unlock()

	for _, session := range server.sessions {
		logger.Infof("Logout session for client id %s", session.GetClientID())

		session.Release()
	}
	server.sessions = map[string]*Session{}

	for _, irodsConnection := range server.irodsConnections {
		logger.Infof("Logout connection for connection id %s", irodsConnection.GetID())

		// close file system for connection
		irodsConnection.Release()
	}
	server.irodsConnections = map[string]*IRODSConnection{}
}

func (server *Server) GetSessions() int {
	server.mutex.Lock()
	defer server.mutex.Unlock()

	return len(server.sessions)
}

func (server *Server) GetIRODSFSCount() int {
	server.mutex.Lock()
	defer server.mutex.Unlock()

	return len(server.irodsConnections)
}

func (server *Server) GetIRODSConnections() int {
	logger := log.WithFields(log.Fields{
		"package":  "service",
		"struct":   "Server",
		"function": "LogoutAll",
	})

	defer func() {
		if r := recover(); r != nil {
			logger.Errorf("stacktrace from panic: %s", string(debug.Stack()))
			logger.Panic(r)
		}
	}()

	server.mutex.Lock()
	defer server.mutex.Unlock()

	connections := 0
	for _, connection := range server.irodsConnections {
		connections += connection.irodsFS.Connections()
	}

	return connections
}

func (server *Server) getSessionAndConnection(sessionID string) (*Session, *IRODSConnection, error) {
	logger := log.WithFields(log.Fields{
		"package":  "service",
		"struct":   "Server",
		"function": "getSessionAndConnection",
	})

	defer func() {
		if r := recover(); r != nil {
			logger.Errorf("stacktrace from panic: %s", string(debug.Stack()))
			logger.Panic(r)
		}
	}()

	server.mutex.RLock()
	defer server.mutex.RUnlock()

	session, ok := server.sessions[sessionID]
	if !ok {
		err := fmt.Errorf("cannot find session for id %s", sessionID)
		logger.Error(err)
		return nil, nil, err
	}

	connectionID := session.GetConnectionID()

	connection, ok := server.irodsConnections[connectionID]
	if !ok {
		err := fmt.Errorf("cannot find connection for id %s", connectionID)
		logger.Error(err)
		return nil, nil, err
	}

	return session, connection, nil
}

func (server *Server) getFileHandle(sessionID string, fileHandleID string) (*FileHandle, error) {
	logger := log.WithFields(log.Fields{
		"package":  "service",
		"struct":   "Server",
		"function": "getFileHandle",
	})

	defer func() {
		if r := recover(); r != nil {
			logger.Errorf("stacktrace from panic: %s", string(debug.Stack()))
			logger.Panic(r)
		}
	}()

	server.mutex.RLock()
	defer server.mutex.RUnlock()

	session, ok := server.sessions[sessionID]
	if !ok {
		err := fmt.Errorf("cannot find session for id %s", sessionID)
		logger.Error(err)
		return nil, err
	}

	session.UpdateLastAccessTime()

	fileHandle := session.GetFileHandle(fileHandleID)
	if fileHandle == nil {
		err := fmt.Errorf("failed to find file handle %s", fileHandleID)
		logger.Error(err)
		return nil, err
	}

	return fileHandle, nil
}

func (server *Server) List(context context.Context, request *api.ListRequest) (*api.ListResponse, error) {
	logger := log.WithFields(log.Fields{
		"package":  "service",
		"struct":   "Server",
		"function": "List",
	})

	defer func() {
		if r := recover(); r != nil {
			logger.Errorf("stacktrace from panic: %s", string(debug.Stack()))
			logger.Panic(r)
		}
	}()

	logger.Infof("List request from client session id %s, path %s", request.SessionId, request.Path)
	defer logger.Infof("List response to client session id %s, path %s", request.SessionId, request.Path)

	session, connection, err := server.getSessionAndConnection(request.SessionId)
	if err != nil {
		logger.Error(err)
		return nil, server.errorToStatus(err)
	}

	session.UpdateLastAccessTime()

	irodsFS := connection.GetIRODSFS()
	if irodsFS == nil {
		err = fmt.Errorf("failed to get iRODSFS from connection")
		logger.Error(err)
		return nil, server.errorToStatus(err)
	}

	entries, err := irodsFS.List(request.Path)
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
			CreateTime: utils.MakeTimeToString(entry.CreateTime),
			ModifyTime: utils.MakeTimeToString(entry.ModifyTime),
			Checksum:   entry.CheckSum,
		}
		responseEntries = append(responseEntries, responseEntry)
	}

	response := &api.ListResponse{
		Entries: responseEntries,
	}

	return response, nil
}

func (server *Server) Stat(context context.Context, request *api.StatRequest) (*api.StatResponse, error) {
	logger := log.WithFields(log.Fields{
		"package":  "service",
		"struct":   "Server",
		"function": "Stat",
	})

	defer func() {
		if r := recover(); r != nil {
			logger.Errorf("stacktrace from panic: %s", string(debug.Stack()))
			logger.Panic(r)
		}
	}()

	logger.Infof("Stat request from client session id %s, path %s", request.SessionId, request.Path)
	defer logger.Infof("Stat response to client session id %s, path %s", request.SessionId, request.Path)

	session, connection, err := server.getSessionAndConnection(request.SessionId)
	if err != nil {
		logger.Error(err)
		return nil, server.errorToStatus(err)
	}

	session.UpdateLastAccessTime()

	irodsFS := connection.GetIRODSFS()
	if irodsFS == nil {
		err = fmt.Errorf("failed to get iRODSFS from connection")
		logger.Error(err)
		return nil, server.errorToStatus(err)
	}

	entry, err := irodsFS.Stat(request.Path)
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
		CreateTime: utils.MakeTimeToString(entry.CreateTime),
		ModifyTime: utils.MakeTimeToString(entry.ModifyTime),
		Checksum:   entry.CheckSum,
	}

	response := &api.StatResponse{
		Entry: responseEntry,
	}

	return response, nil
}

func (server *Server) ExistsDir(context context.Context, request *api.ExistsDirRequest) (*api.ExistsDirResponse, error) {
	logger := log.WithFields(log.Fields{
		"package":  "service",
		"struct":   "Server",
		"function": "ExistsDir",
	})

	defer func() {
		if r := recover(); r != nil {
			logger.Errorf("stacktrace from panic: %s", string(debug.Stack()))
			logger.Panic(r)
		}
	}()

	logger.Infof("ExistsDir request from client session id %s, path %s", request.SessionId, request.Path)
	defer logger.Infof("ExistsDir response to client session id %s, path %s", request.SessionId, request.Path)

	session, connection, err := server.getSessionAndConnection(request.SessionId)
	if err != nil {
		logger.Error(err)
		return nil, server.errorToStatus(err)
	}

	session.UpdateLastAccessTime()

	irodsFS := connection.GetIRODSFS()
	if irodsFS == nil {
		err = fmt.Errorf("failed to get iRODSFS from connection")
		logger.Error(err)
		return nil, server.errorToStatus(err)
	}

	exist := irodsFS.ExistsDir(request.Path)
	return &api.ExistsDirResponse{
		Exist: exist,
	}, nil
}

func (server *Server) ExistsFile(context context.Context, request *api.ExistsFileRequest) (*api.ExistsFileResponse, error) {
	logger := log.WithFields(log.Fields{
		"package":  "service",
		"struct":   "Server",
		"function": "ExistsFile",
	})

	defer func() {
		if r := recover(); r != nil {
			logger.Errorf("stacktrace from panic: %s", string(debug.Stack()))
			logger.Panic(r)
		}
	}()

	logger.Infof("ExistsFile request from client session id %s, path %s", request.SessionId, request.Path)
	defer logger.Infof("ExistsFile response to client session id %s, path %s", request.SessionId, request.Path)

	session, connection, err := server.getSessionAndConnection(request.SessionId)
	if err != nil {
		logger.Error(err)
		return nil, server.errorToStatus(err)
	}

	session.UpdateLastAccessTime()

	irodsFS := connection.GetIRODSFS()
	if irodsFS == nil {
		err = fmt.Errorf("failed to get iRODSFS from connection")
		logger.Error(err)
		return nil, server.errorToStatus(err)
	}

	exist := irodsFS.ExistsFile(request.Path)
	return &api.ExistsFileResponse{
		Exist: exist,
	}, nil
}

func (server *Server) ListUserGroups(context context.Context, request *api.ListUserGroupsRequest) (*api.ListUserGroupsResponse, error) {
	logger := log.WithFields(log.Fields{
		"package":  "service",
		"struct":   "Server",
		"function": "ListUserGroups",
	})

	defer func() {
		if r := recover(); r != nil {
			logger.Errorf("stacktrace from panic: %s", string(debug.Stack()))
			logger.Panic(r)
		}
	}()

	logger.Infof("ListUserGroups request from client session id %s, user name %s", request.SessionId, request.UserName)
	defer logger.Infof("ListUserGroups response to client session id %s, user name %s", request.SessionId, request.UserName)

	session, connection, err := server.getSessionAndConnection(request.SessionId)
	if err != nil {
		logger.Error(err)
		return nil, server.errorToStatus(err)
	}

	session.UpdateLastAccessTime()

	irodsFS := connection.GetIRODSFS()
	if irodsFS == nil {
		err = fmt.Errorf("failed to get iRODSFS from connection")
		logger.Error(err)
		return nil, server.errorToStatus(err)
	}

	groups, err := irodsFS.ListUserGroups(request.UserName)
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

func (server *Server) ListDirACLs(context context.Context, request *api.ListDirACLsRequest) (*api.ListDirACLsResponse, error) {
	logger := log.WithFields(log.Fields{
		"package":  "service",
		"struct":   "Server",
		"function": "ListDirACLs",
	})

	defer func() {
		if r := recover(); r != nil {
			logger.Errorf("stacktrace from panic: %s", string(debug.Stack()))
			logger.Panic(r)
		}
	}()

	logger.Infof("ListDirACLs request from client session id %s, path %s", request.SessionId, request.Path)
	defer logger.Infof("ListDirACLs response to client session id %s, path %s", request.SessionId, request.Path)

	session, connection, err := server.getSessionAndConnection(request.SessionId)
	if err != nil {
		logger.Error(err)
		return nil, server.errorToStatus(err)
	}

	session.UpdateLastAccessTime()

	irodsFS := connection.GetIRODSFS()
	if irodsFS == nil {
		err = fmt.Errorf("failed to get iRODSFS from connection")
		logger.Error(err)
		return nil, server.errorToStatus(err)
	}

	accesses, err := irodsFS.ListDirACLs(request.Path)
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

func (server *Server) ListFileACLs(context context.Context, request *api.ListFileACLsRequest) (*api.ListFileACLsResponse, error) {
	logger := log.WithFields(log.Fields{
		"package":  "service",
		"struct":   "Server",
		"function": "ListFileACLs",
	})

	defer func() {
		if r := recover(); r != nil {
			logger.Errorf("stacktrace from panic: %s", string(debug.Stack()))
			logger.Panic(r)
		}
	}()

	logger.Infof("ListFileACLs request from client session id %s, path %s", request.SessionId, request.Path)
	defer logger.Infof("ListFileACLs response to client session id %s, path %s", request.SessionId, request.Path)

	session, connection, err := server.getSessionAndConnection(request.SessionId)
	if err != nil {
		logger.Error(err)
		return nil, server.errorToStatus(err)
	}

	session.UpdateLastAccessTime()

	irodsFS := connection.GetIRODSFS()
	if irodsFS == nil {
		err = fmt.Errorf("failed to get iRODSFS from connection")
		logger.Error(err)
		return nil, server.errorToStatus(err)
	}

	accesses, err := irodsFS.ListFileACLs(request.Path)
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

func (server *Server) RemoveFile(context context.Context, request *api.RemoveFileRequest) (*api.Empty, error) {
	logger := log.WithFields(log.Fields{
		"package":  "service",
		"struct":   "Server",
		"function": "RemoveFile",
	})

	defer func() {
		if r := recover(); r != nil {
			logger.Errorf("stacktrace from panic: %s", string(debug.Stack()))
			logger.Panic(r)
		}
	}()

	logger.Infof("RemoveFile request from client session id %s, path %s", request.SessionId, request.Path)
	defer logger.Infof("RemoveFile response to client session id %s, path %s", request.SessionId, request.Path)

	session, connection, err := server.getSessionAndConnection(request.SessionId)
	if err != nil {
		logger.Error(err)
		return nil, server.errorToStatus(err)
	}

	session.UpdateLastAccessTime()

	irodsFS := connection.GetIRODSFS()
	if irodsFS == nil {
		err = fmt.Errorf("failed to get iRODSFS from connection")
		logger.Error(err)
		return nil, server.errorToStatus(err)
	}

	err = irodsFS.RemoveFile(request.Path, request.Force)
	if err != nil {
		logger.Error(err)
		return nil, server.errorToStatus(err)
	}

	// clear cache for the path if exists
	server.cache.DeleteAllEntriesForGroup(request.Path)

	return &api.Empty{}, nil
}

func (server *Server) RemoveDir(context context.Context, request *api.RemoveDirRequest) (*api.Empty, error) {
	logger := log.WithFields(log.Fields{
		"package":  "service",
		"struct":   "Server",
		"function": "RemoveDir",
	})

	defer func() {
		if r := recover(); r != nil {
			logger.Errorf("stacktrace from panic: %s", string(debug.Stack()))
			logger.Panic(r)
		}
	}()

	logger.Infof("RemoveDir request from client session id %s, path %s", request.SessionId, request.Path)
	defer logger.Infof("RemoveDir response to client session id %s, path %s", request.SessionId, request.Path)

	session, connection, err := server.getSessionAndConnection(request.SessionId)
	if err != nil {
		logger.Error(err)
		return nil, fmt.Errorf("failed to get iRODSFS from connection")
	}

	session.UpdateLastAccessTime()

	irodsFS := connection.GetIRODSFS()
	if irodsFS == nil {
		err = fmt.Errorf("failed to get iRODSFS from connection")
		logger.Error(err)
		return nil, server.errorToStatus(err)
	}

	err = irodsFS.RemoveDir(request.Path, request.Recurse, request.Force)
	if err != nil {
		logger.Error(err)
		return nil, server.errorToStatus(err)
	}

	return &api.Empty{}, nil
}

func (server *Server) MakeDir(context context.Context, request *api.MakeDirRequest) (*api.Empty, error) {
	logger := log.WithFields(log.Fields{
		"package":  "service",
		"struct":   "Server",
		"function": "MakeDir",
	})

	defer func() {
		if r := recover(); r != nil {
			logger.Errorf("stacktrace from panic: %s", string(debug.Stack()))
			logger.Panic(r)
		}
	}()

	logger.Infof("MakeDir request from client session id %s, path %s", request.SessionId, request.Path)
	defer logger.Infof("MakeDir response to client session id %s, path %s", request.SessionId, request.Path)

	session, connection, err := server.getSessionAndConnection(request.SessionId)
	if err != nil {
		logger.Error(err)
		return nil, server.errorToStatus(err)
	}

	session.UpdateLastAccessTime()

	irodsFS := connection.GetIRODSFS()
	if irodsFS == nil {
		err = fmt.Errorf("failed to get iRODSFS from connection")
		logger.Error(err)
		return nil, server.errorToStatus(err)
	}

	err = irodsFS.MakeDir(request.Path, request.Recurse)
	if err != nil {
		logger.Error(err)
		return nil, server.errorToStatus(err)
	}

	return &api.Empty{}, nil
}

func (server *Server) RenameDirToDir(context context.Context, request *api.RenameDirToDirRequest) (*api.Empty, error) {
	logger := log.WithFields(log.Fields{
		"package":  "service",
		"struct":   "Server",
		"function": "RenameDirToDir",
	})

	defer func() {
		if r := recover(); r != nil {
			logger.Errorf("stacktrace from panic: %s", string(debug.Stack()))
			logger.Panic(r)
		}
	}()

	logger.Infof("RenameDirToDir request from client session id %s, source path %s -> destination path %s", request.SessionId, request.SourcePath, request.DestinationPath)
	defer logger.Infof("RenameDirToDir response to client session id %s, source path %s -> destination path %s", request.SessionId, request.SourcePath, request.DestinationPath)

	session, connection, err := server.getSessionAndConnection(request.SessionId)
	if err != nil {
		logger.Error(err)
		return nil, server.errorToStatus(err)
	}

	session.UpdateLastAccessTime()

	irodsFS := connection.GetIRODSFS()
	if irodsFS == nil {
		err = fmt.Errorf("failed to get iRODSFS from connection")
		logger.Error(err)
		return nil, server.errorToStatus(err)
	}

	err = irodsFS.RenameDirToDir(request.SourcePath, request.DestinationPath)
	if err != nil {
		logger.Error(err)
		return nil, server.errorToStatus(err)
	}

	return &api.Empty{}, nil
}

func (server *Server) RenameFileToFile(context context.Context, request *api.RenameFileToFileRequest) (*api.Empty, error) {
	logger := log.WithFields(log.Fields{
		"package":  "service",
		"struct":   "Server",
		"function": "RenameFileToFile",
	})

	defer func() {
		if r := recover(); r != nil {
			logger.Errorf("stacktrace from panic: %s", string(debug.Stack()))
			logger.Panic(r)
		}
	}()

	logger.Infof("RenameFileToFile request from client session id %s, source path %s -> destination path %s", request.SessionId, request.SourcePath, request.DestinationPath)
	defer logger.Infof("RenameFileToFile response to client session id %s, source path %s -> destination path %s", request.SessionId, request.SourcePath, request.DestinationPath)

	session, connection, err := server.getSessionAndConnection(request.SessionId)
	if err != nil {
		logger.Error(err)
		return nil, server.errorToStatus(err)
	}

	session.UpdateLastAccessTime()

	irodsFS := connection.GetIRODSFS()
	if irodsFS == nil {
		err = fmt.Errorf("failed to get iRODSFS from connection")
		logger.Error(err)
		return nil, server.errorToStatus(err)
	}

	err = irodsFS.RenameFileToFile(request.SourcePath, request.DestinationPath)
	if err != nil {
		logger.Error(err)
		return nil, server.errorToStatus(err)
	}

	// clear cache for the path if exists
	server.cache.DeleteAllEntriesForGroup(request.SourcePath)

	return &api.Empty{}, nil
}

func (server *Server) CreateFile(context context.Context, request *api.CreateFileRequest) (*api.CreateFileResponse, error) {
	logger := log.WithFields(log.Fields{
		"package":  "service",
		"struct":   "Server",
		"function": "CreateFile",
	})

	defer func() {
		if r := recover(); r != nil {
			logger.Errorf("stacktrace from panic: %s", string(debug.Stack()))
			logger.Panic(r)
		}
	}()

	logger.Infof("CreateFile request from client session id %s, path %s", request.SessionId, request.Path)
	defer logger.Infof("CreateFile response to client session id %s, path %s", request.SessionId, request.Path)

	session, connection, err := server.getSessionAndConnection(request.SessionId)
	if err != nil {
		logger.Error(err)
		return nil, server.errorToStatus(err)
	}

	session.UpdateLastAccessTime()

	irodsFS := connection.GetIRODSFS()
	if irodsFS == nil {
		err = fmt.Errorf("failed to get iRODSFS from connection")
		logger.Error(err)
		return nil, server.errorToStatus(err)
	}

	// clear cache for the path if exists
	server.cache.DeleteAllEntriesForGroup(request.Path)

	handle, err := irodsFS.CreateFile(request.Path, request.Resource, request.Mode)
	if err != nil {
		logger.Error(err)
		return nil, server.errorToStatus(err)
	}

	fileHandleID := xid.New().String()
	handleMutex := &sync.Mutex{}
	var writer io.Writer = io.NewSyncWriter(request.Path, handle, handleMutex)
	var reader io.Reader = io.NewSyncReader(request.Path, handle, handleMutex)

	fileHandle := NewFileHandle(fileHandleID, request.SessionId, connection.GetID(), writer, reader, handle, handleMutex)

	session.AddFileHandle(fileHandle)

	entry := handle.GetEntry()
	responseEntry := &api.Entry{
		Id:         entry.ID,
		Type:       string(entry.Type),
		Name:       entry.Name,
		Path:       entry.Path,
		Owner:      entry.Owner,
		Size:       entry.Size,
		CreateTime: utils.MakeTimeToString(entry.CreateTime),
		ModifyTime: utils.MakeTimeToString(entry.ModifyTime),
		Checksum:   entry.CheckSum,
	}

	response := &api.CreateFileResponse{
		FileHandleId: fileHandleID,
		Entry:        responseEntry,
	}

	return response, nil
}

func (server *Server) OpenFile(context context.Context, request *api.OpenFileRequest) (*api.OpenFileResponse, error) {
	logger := log.WithFields(log.Fields{
		"package":  "service",
		"struct":   "Server",
		"function": "OpenFile",
	})

	defer func() {
		if r := recover(); r != nil {
			logger.Errorf("stacktrace from panic: %s", string(debug.Stack()))
			logger.Panic(r)
		}
	}()

	logger.Infof("OpenFile request from client session id %s, path %s, mode(%s)", request.SessionId, request.Path, request.Mode)
	defer logger.Infof("OpenFile response to client session id %s, path %s, mode(%s)", request.SessionId, request.Path, request.Mode)

	session, connection, err := server.getSessionAndConnection(request.SessionId)
	if err != nil {
		logger.Error(err)
		return nil, server.errorToStatus(err)
	}

	session.UpdateLastAccessTime()

	irodsFS := connection.GetIRODSFS()
	if irodsFS == nil {
		err = fmt.Errorf("failed to get iRODSFS from connection")
		logger.Error(err)
		return nil, server.errorToStatus(err)
	}

	handle, err := irodsFS.OpenFile(request.Path, request.Resource, request.Mode)
	if err != nil {
		logger.Error(err)
		return nil, server.errorToStatus(err)
	}

	entry := handle.GetEntry()

	fileHandleID := xid.New().String()
	handleMutex := &sync.Mutex{}

	var writer io.Writer
	var reader io.Reader

	switch irodsclient_types.FileOpenMode(request.Mode) {
	case irodsclient_types.FileOpenModeAppend, irodsclient_types.FileOpenModeWriteOnly, irodsclient_types.FileOpenModeWriteTruncate:
		// clear cache for the path if exists
		server.cache.DeleteAllEntriesForGroup(request.Path)

		// writer
		if server.buffer != nil {
			asyncWriter := io.NewAsyncWriter(request.Path, fileHandleID, handle, handleMutex, server.buffer)
			writer = io.NewBufferedWriter(request.Path, asyncWriter)
		} else {
			syncWriter := io.NewSyncWriter(request.Path, handle, handleMutex)
			writer = io.NewBufferedWriter(request.Path, syncWriter)
		}

		// reader
		reader = io.NewSyncReader(request.Path, handle, handleMutex)
	case irodsclient_types.FileOpenModeReadOnly:
		// writer
		writer = io.NewSyncWriter(request.Path, handle, handleMutex)

		// reader
		if server.cache != nil {
			syncReader := io.NewSyncReader(request.Path, handle, handleMutex)
			reader = io.NewCacheReader(request.Path, entry.CheckSum, server.cache, syncReader)
		} else {
			reader = io.NewSyncReader(request.Path, handle, handleMutex)
		}
	default:
		// clear cache for the path if exists
		server.cache.DeleteAllEntriesForGroup(request.Path)

		writer = io.NewSyncWriter(request.Path, handle, handleMutex)
		reader = io.NewSyncReader(request.Path, handle, handleMutex)
	}

	fileHandle := NewFileHandle(fileHandleID, request.SessionId, connection.GetID(), writer, reader, handle, handleMutex)

	session.AddFileHandle(fileHandle)

	responseEntry := &api.Entry{
		Id:         entry.ID,
		Type:       string(entry.Type),
		Name:       entry.Name,
		Path:       entry.Path,
		Owner:      entry.Owner,
		Size:       entry.Size,
		CreateTime: utils.MakeTimeToString(entry.CreateTime),
		ModifyTime: utils.MakeTimeToString(entry.ModifyTime),
		Checksum:   entry.CheckSum,
	}

	response := &api.OpenFileResponse{
		FileHandleId: fileHandleID,
		Entry:        responseEntry,
	}

	return response, nil
}

func (server *Server) TruncateFile(context context.Context, request *api.TruncateFileRequest) (*api.Empty, error) {
	logger := log.WithFields(log.Fields{
		"package":  "service",
		"struct":   "Server",
		"function": "TruncateFile",
	})

	defer func() {
		if r := recover(); r != nil {
			logger.Errorf("stacktrace from panic: %s", string(debug.Stack()))
			logger.Panic(r)
		}
	}()

	logger.Infof("TruncateFile request from client session id %s, path %s", request.SessionId, request.Path)
	defer logger.Infof("TruncateFile response to client session id %s, path %s", request.SessionId, request.Path)

	session, connection, err := server.getSessionAndConnection(request.SessionId)
	if err != nil {
		logger.Error(err)
		return nil, server.errorToStatus(err)
	}

	session.UpdateLastAccessTime()

	irodsFS := connection.GetIRODSFS()
	if irodsFS == nil {
		err = fmt.Errorf("failed to get iRODSFS from connection")
		logger.Error(err)
		return nil, server.errorToStatus(err)
	}

	// clear cache for the path if exists
	server.cache.DeleteAllEntriesForGroup(request.Path)

	err = irodsFS.TruncateFile(request.Path, request.Size)
	if err != nil {
		logger.Error(err)
		return nil, server.errorToStatus(err)
	}

	return &api.Empty{}, nil
}

func (server *Server) GetOffset(context context.Context, request *api.GetOffsetRequest) (*api.GetOffsetResponse, error) {
	logger := log.WithFields(log.Fields{
		"package":  "service",
		"struct":   "Server",
		"function": "GetOffset",
	})

	defer func() {
		if r := recover(); r != nil {
			logger.Errorf("stacktrace from panic: %s", string(debug.Stack()))
			logger.Panic(r)
		}
	}()

	logger.Infof("GetOffset request from client session id %s, file handle id %s", request.SessionId, request.FileHandleId)
	defer logger.Infof("GetOffset response to client session id %s, file handle id %s", request.SessionId, request.FileHandleId)

	fileHandle, err := server.getFileHandle(request.SessionId, request.FileHandleId)
	if err != nil {
		logger.Error(err)
		return nil, server.errorToStatus(err)
	}

	response := &api.GetOffsetResponse{
		Offset: fileHandle.GetOffset(),
	}

	return response, nil
}

func (server *Server) ReadAt(context context.Context, request *api.ReadAtRequest) (*api.ReadAtResponse, error) {
	logger := log.WithFields(log.Fields{
		"package":  "service",
		"struct":   "Server",
		"function": "ReadAt",
	})

	defer func() {
		if r := recover(); r != nil {
			logger.Errorf("stacktrace from panic: %s", string(debug.Stack()))
			logger.Panic(r)
		}
	}()

	logger.Infof("ReadAt request from client session id %s, file handle id %s, offset %d, length %d", request.SessionId, request.FileHandleId, request.Offset, request.Length)
	defer logger.Infof("ReadAt response to client session id %s, file handle id %s, offset %d, length %d", request.SessionId, request.FileHandleId, request.Offset, request.Length)

	fileHandle, err := server.getFileHandle(request.SessionId, request.FileHandleId)
	if err != nil {
		logger.Error(err)
		return nil, server.errorToStatus(err)
	}

	data, err := fileHandle.ReadAt(request.Offset, int(request.Length))
	if err != nil {
		logger.Error(err)
		return nil, server.errorToStatus(err)
	}

	response := &api.ReadAtResponse{
		Data: data,
	}

	return response, nil
}

func (server *Server) WriteAt(context context.Context, request *api.WriteAtRequest) (*api.Empty, error) {
	logger := log.WithFields(log.Fields{
		"package":  "service",
		"struct":   "Server",
		"function": "WriteAt",
	})

	defer func() {
		if r := recover(); r != nil {
			logger.Errorf("stacktrace from panic: %s", string(debug.Stack()))
			logger.Panic(r)
		}
	}()

	logger.Infof("WriteAt request from client session id %s, file handle id %s, offset %d, length %d", request.SessionId, request.FileHandleId, request.Offset, len(request.Data))
	defer logger.Infof("WriteAt response to client session id %s, file handle id %s, offset %d, length %d", request.SessionId, request.FileHandleId, request.Offset, len(request.Data))

	fileHandle, err := server.getFileHandle(request.SessionId, request.FileHandleId)
	if err != nil {
		logger.Error(err)
		return nil, server.errorToStatus(err)
	}

	err = fileHandle.WriteAt(request.Offset, request.Data)
	if err != nil {
		logger.Error(err)
		return nil, server.errorToStatus(err)
	}

	return &api.Empty{}, nil
}

func (server *Server) Flush(context context.Context, request *api.FlushRequest) (*api.Empty, error) {
	logger := log.WithFields(log.Fields{
		"package":  "service",
		"struct":   "Server",
		"function": "Flush",
	})

	defer func() {
		if r := recover(); r != nil {
			logger.Errorf("stacktrace from panic: %s", string(debug.Stack()))
			logger.Panic(r)
		}
	}()

	logger.Infof("Flush request from client session id %s, file handle id %s", request.SessionId, request.FileHandleId)
	defer logger.Infof("Flush response to client session id %s, file handle id %s", request.SessionId, request.FileHandleId)

	fileHandle, err := server.getFileHandle(request.SessionId, request.FileHandleId)
	if err != nil {
		logger.Error(err)
		return nil, server.errorToStatus(err)
	}

	err = fileHandle.Flush()
	if err != nil {
		logger.Error(err)
		return nil, server.errorToStatus(err)
	}

	return &api.Empty{}, nil
}

func (server *Server) Close(context context.Context, request *api.CloseRequest) (*api.Empty, error) {
	logger := log.WithFields(log.Fields{
		"package":  "service",
		"struct":   "Server",
		"function": "Close",
	})

	defer func() {
		if r := recover(); r != nil {
			logger.Errorf("stacktrace from panic: %s", string(debug.Stack()))
			logger.Panic(r)
		}
	}()

	logger.Infof("Close request from client session id %s, file handle id %s", request.SessionId, request.FileHandleId)
	defer logger.Infof("Close response to client session id %s, file handle id %s", request.SessionId, request.FileHandleId)

	session, _, err := server.getSessionAndConnection(request.SessionId)
	if err != nil {
		logger.Error(err)
		return nil, server.errorToStatus(err)
	}

	session.UpdateLastAccessTime()

	fileHandle := session.GetFileHandle(request.FileHandleId)
	if fileHandle == nil {
		err := fmt.Errorf("failed to find file handle %s", request.FileHandleId)
		logger.Error(err)
		return nil, server.errorToStatus(err)
	}

	session.RemoveFileHandle(request.FileHandleId)

	if irodsclient_types.FileOpenMode(fileHandle.GetFileOpenMode()) != irodsclient_types.FileOpenModeReadOnly {
		// not read-only
		// clear cache for the path if exists
		server.cache.DeleteAllEntriesForGroup(fileHandle.GetEntryPath())
	}

	err = fileHandle.Release()
	if err != nil {
		logger.Error(err)
		return nil, server.errorToStatus(err)
	}

	return &api.Empty{}, nil
}
