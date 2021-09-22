package service

import (
	context "context"
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"sync"
	"time"

	irodsclient_fs "github.com/cyverse/go-irodsclient/fs"
	irodsclient_types "github.com/cyverse/go-irodsclient/irods/types"
	"github.com/cyverse/irodsfs-pool/service/api"
	"github.com/cyverse/irodsfs-pool/service/asyncwrite"
	"github.com/cyverse/irodsfs-pool/utils"
	"github.com/rs/xid"
	log "github.com/sirupsen/logrus"
)

type Server struct {
	api.UnimplementedPoolAPIServer
	Mutex    sync.RWMutex // mutex to access Sessions
	Sessions map[string]*Session
	Buffer   asyncwrite.Buffer
}

type Session struct {
	ID               string
	Account          *irodsclient_types.IRODSAccount
	IRODSFS          *irodsclient_fs.FileSystem
	ReferenceCount   int
	LastActivityTime time.Time
	FileHandles      map[string]*FileHandle
	Mutex            sync.Mutex // mutex to access FileHandles, ReferenceCount and LastActivityTime
}

type FileHandle struct {
	ID          string
	SessionID   string
	Writer      asyncwrite.Writer
	IRODSHandle *irodsclient_fs.FileHandle
	Mutex       *sync.Mutex // mutex to access IRODSHandle
}

func NewServer(bufferSizeMax int64) *Server {
	var ramBuffer asyncwrite.Buffer
	if bufferSizeMax > 0 {
		ramBuffer = asyncwrite.NewRAMBuffer(bufferSizeMax)
	}

	return &Server{
		Sessions: map[string]*Session{},
		Buffer:   ramBuffer,
	}
}

func (server *Server) generateSessionID(account *api.Account) string {
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

func (server *Server) Login(context context.Context, request *api.LoginRequest) (*api.LoginResponse, error) {
	logger := log.WithFields(log.Fields{
		"package":  "service",
		"struct":   "Server",
		"function": "Login",
	})

	logger.Infof("Login request from client: %s - %s", request.Account.Host, request.Account.ClientUser)

	sessionID := server.generateSessionID(request.Account)

	server.Mutex.Lock()
	defer server.Mutex.Unlock()

	if session, ok := server.Sessions[sessionID]; ok {
		logger.Infof("Reusing existing session: %s", sessionID)

		session.Mutex.Lock()
		defer session.Mutex.Unlock()

		session.ReferenceCount++
		session.LastActivityTime = time.Now()
	} else {
		logger.Infof("Creating a new session: %s", sessionID)

		// new session
		account := &irodsclient_types.IRODSAccount{
			AuthenticationScheme:    irodsclient_types.AuthScheme(request.Account.AuthenticationScheme),
			ClientServerNegotiation: request.Account.ClientServerNegotiation,
			CSNegotiationPolicy:     irodsclient_types.CSNegotiationRequire(request.Account.CsNegotiationPolicy),
			Host:                    request.Account.Host,
			Port:                    int(request.Account.Port),
			ClientUser:              request.Account.ClientUser,
			ClientZone:              request.Account.ClientZone,
			ProxyUser:               request.Account.ProxyUser,
			ProxyZone:               request.Account.ProxyZone,
			ServerDN:                request.Account.ServerDn,
			Password:                request.Account.Password,
			Ticket:                  request.Account.Ticket,
			PamTTL:                  int(request.Account.PamTtl),
		}

		fs, err := irodsclient_fs.NewFileSystemWithDefault(account, request.ApplicationName)
		if err != nil {
			logger.Error(err)
			return nil, err
		}

		session = &Session{
			ID:               sessionID,
			Account:          account,
			IRODSFS:          fs,
			ReferenceCount:   1,
			LastActivityTime: time.Now(),
			FileHandles:      map[string]*FileHandle{},
		}

		session.Mutex.Lock()
		defer session.Mutex.Unlock()

		server.Sessions[sessionID] = session
	}

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

	logger.Infof("Logout request from client: %s", request.SessionId)

	server.Mutex.Lock()
	defer server.Mutex.Unlock()

	session, ok := server.Sessions[request.SessionId]
	if !ok {
		//err := fmt.Errorf("cannot find session %s", request.SessionId)
		//logger.Error(err)
		//return nil, err

		// no problem, session might be already closed due to timeout
		return &api.Empty{}, nil
	}

	session.Mutex.Lock()
	defer session.Mutex.Unlock()

	session.ReferenceCount--
	session.LastActivityTime = time.Now()

	if session.ReferenceCount <= 0 {
		logger.Infof("Deleting session: %s", session.ID)
		// find opened file handles
		for _, handle := range session.FileHandles {
			if handle.IRODSHandle != nil {
				handle.IRODSHandle.Close()
			}
		}

		// empty
		session.FileHandles = map[string]*FileHandle{}

		delete(server.Sessions, request.SessionId)

		if session.IRODSFS != nil {
			session.IRODSFS.Release()
			session.IRODSFS = nil
		}
	}

	return &api.Empty{}, nil
}

func (server *Server) LogoutAll() {
	logger := log.WithFields(log.Fields{
		"package":  "service",
		"struct":   "Server",
		"function": "LogoutAll",
	})

	logger.Info("Logout All")

	server.Mutex.Lock()
	defer server.Mutex.Unlock()

	for _, session := range server.Sessions {
		session.Mutex.Lock()

		session.ReferenceCount = 0
		session.LastActivityTime = time.Now()

		logger.Infof("Deleting session: %s", session.ID)
		// find opened file handles
		for _, handle := range session.FileHandles {
			if handle.IRODSHandle != nil {
				handle.IRODSHandle.Close()
			}
		}

		// empty
		session.FileHandles = map[string]*FileHandle{}

		if session.IRODSFS != nil {
			session.IRODSFS.Release()
			session.IRODSFS = nil
		}

		session.Mutex.Unlock()
	}

	server.Sessions = map[string]*Session{}
}

func (server *Server) Connections() int {
	connections := 0

	server.Mutex.Lock()
	defer server.Mutex.Unlock()

	for _, session := range server.Sessions {
		session.Mutex.Lock()

		if session.IRODSFS != nil {
			connections += session.IRODSFS.Connections()
		}

		session.Mutex.Unlock()
	}

	return connections
}

func (server *Server) getSession(sessionID string) (*Session, error) {
	logger := log.WithFields(log.Fields{
		"package":  "service",
		"struct":   "Server",
		"function": "getSession",
	})

	server.Mutex.RLock()
	defer server.Mutex.RUnlock()

	session, ok := server.Sessions[sessionID]
	if !ok {
		err := fmt.Errorf("cannot find session %s", sessionID)
		logger.Error(err)
		return nil, err
	}

	if session.IRODSFS == nil {
		err := fmt.Errorf("session not logged in %s", sessionID)
		logger.Error(err)
		return nil, err
	}

	return session, nil
}

func (server *Server) getFileHandle(sessionID string, fileHandleID string) (*FileHandle, error) {
	logger := log.WithFields(log.Fields{
		"package":  "service",
		"struct":   "Server",
		"function": "getFileHandle",
	})

	session, err := server.getSession(sessionID)
	if err != nil {
		logger.Error(err)
		return nil, err
	}

	session.Mutex.Lock()
	defer session.Mutex.Unlock()

	session.LastActivityTime = time.Now()

	fileHandle, ok := session.FileHandles[fileHandleID]
	if !ok {
		err := fmt.Errorf("cannot find file handle %s", fileHandleID)
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

	logger.Infof("List request from client %s: %s", request.SessionId, request.Path)

	session, err := server.getSession(request.SessionId)
	if err != nil {
		logger.Error(err)
		return nil, err
	}

	session.Mutex.Lock()
	session.LastActivityTime = time.Now()
	session.Mutex.Unlock()

	entries, err := session.IRODSFS.List(request.Path)
	if err != nil {
		logger.Error(err)
		return nil, err
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

	logger.Infof("Stat request from client %s: %s", request.SessionId, request.Path)

	session, err := server.getSession(request.SessionId)
	if err != nil {
		logger.Error(err)
		return nil, err
	}

	session.Mutex.Lock()
	session.LastActivityTime = time.Now()
	session.Mutex.Unlock()

	entry, err := session.IRODSFS.Stat(request.Path)
	if err != nil {
		if irodsclient_types.IsFileNotFoundError(err) {
			return &api.StatResponse{
				Error: &api.SoftError{
					Type:    api.ErrorType_FILENOTFOUND,
					Message: err.Error(),
				},
			}, nil
		}

		logger.Error(err)
		return nil, err
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
		Error: nil,
	}

	return response, nil
}

func (server *Server) ExistsDir(context context.Context, request *api.ExistsDirRequest) (*api.ExistsDirResponse, error) {
	logger := log.WithFields(log.Fields{
		"package":  "service",
		"struct":   "Server",
		"function": "ExistsDir",
	})

	logger.Infof("ExistsDir request from client %s: %s", request.SessionId, request.Path)

	session, err := server.getSession(request.SessionId)
	if err != nil {
		logger.Error(err)
		return nil, err
	}

	session.Mutex.Lock()
	session.LastActivityTime = time.Now()
	session.Mutex.Unlock()

	exist := session.IRODSFS.ExistsDir(request.Path)
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

	logger.Infof("ExistsFile request from client %s: %s", request.SessionId, request.Path)

	session, err := server.getSession(request.SessionId)
	if err != nil {
		logger.Error(err)
		return nil, err
	}

	session.Mutex.Lock()
	session.LastActivityTime = time.Now()
	session.Mutex.Unlock()

	exist := session.IRODSFS.ExistsFile(request.Path)
	return &api.ExistsFileResponse{
		Exist: exist,
	}, nil
}

func (server *Server) ListDirACLsWithGroupUsers(context context.Context, request *api.ListDirACLsWithGroupUsersRequest) (*api.ListDirACLsWithGroupUsersResponse, error) {
	logger := log.WithFields(log.Fields{
		"package":  "service",
		"struct":   "Server",
		"function": "ListDirACLsWithGroupUsers",
	})

	logger.Infof("ListDirACLsWithGroupUsers request from client %s: %s", request.SessionId, request.Path)

	session, err := server.getSession(request.SessionId)
	if err != nil {
		logger.Error(err)
		return nil, err
	}

	session.Mutex.Lock()
	session.LastActivityTime = time.Now()
	session.Mutex.Unlock()

	accesses, err := session.IRODSFS.ListDirACLsWithGroupUsers(request.Path)
	if err != nil {
		logger.Error(err)
		return nil, err
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

	response := &api.ListDirACLsWithGroupUsersResponse{
		Accesses: responseAccesses,
	}

	return response, nil
}

func (server *Server) ListFileACLsWithGroupUsers(context context.Context, request *api.ListFileACLsWithGroupUsersRequest) (*api.ListFileACLsWithGroupUsersResponse, error) {
	logger := log.WithFields(log.Fields{
		"package":  "service",
		"struct":   "Server",
		"function": "ListFileACLsWithGroupUsers",
	})

	logger.Infof("ListFileACLsWithGroupUsers request from client %s: %s", request.SessionId, request.Path)

	session, err := server.getSession(request.SessionId)
	if err != nil {
		logger.Error(err)
		return nil, err
	}

	session.Mutex.Lock()
	session.LastActivityTime = time.Now()
	session.Mutex.Unlock()

	accesses, err := session.IRODSFS.ListFileACLsWithGroupUsers(request.Path)
	if err != nil {
		logger.Error(err)
		return nil, err
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

	response := &api.ListFileACLsWithGroupUsersResponse{
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

	logger.Infof("RemoveFile request from client %s: %s", request.SessionId, request.Path)

	session, err := server.getSession(request.SessionId)
	if err != nil {
		logger.Error(err)
		return nil, err
	}

	session.Mutex.Lock()
	session.LastActivityTime = time.Now()
	session.Mutex.Unlock()

	err = session.IRODSFS.RemoveFile(request.Path, request.Force)
	if err != nil {
		logger.Error(err)
		return nil, err
	}

	return &api.Empty{}, nil
}

func (server *Server) RemoveDir(context context.Context, request *api.RemoveDirRequest) (*api.Empty, error) {
	logger := log.WithFields(log.Fields{
		"package":  "service",
		"struct":   "Server",
		"function": "RemoveDir",
	})

	logger.Infof("RemoveDir request from client %s: %s", request.SessionId, request.Path)

	session, err := server.getSession(request.SessionId)
	if err != nil {
		logger.Error(err)
		return nil, err
	}

	session.Mutex.Lock()
	session.LastActivityTime = time.Now()
	session.Mutex.Unlock()

	err = session.IRODSFS.RemoveDir(request.Path, request.Recurse, request.Force)
	if err != nil {
		logger.Error(err)
		return nil, err
	}

	return &api.Empty{}, nil
}

func (server *Server) MakeDir(context context.Context, request *api.MakeDirRequest) (*api.Empty, error) {
	logger := log.WithFields(log.Fields{
		"package":  "service",
		"struct":   "Server",
		"function": "MakeDir",
	})

	logger.Infof("MakeDir request from client %s: %s", request.SessionId, request.Path)

	session, err := server.getSession(request.SessionId)
	if err != nil {
		logger.Error(err)
		return nil, err
	}

	session.Mutex.Lock()
	session.LastActivityTime = time.Now()
	session.Mutex.Unlock()

	err = session.IRODSFS.MakeDir(request.Path, request.Recurse)
	if err != nil {
		logger.Error(err)
		return nil, err
	}

	return &api.Empty{}, nil
}

func (server *Server) RenameDirToDir(context context.Context, request *api.RenameDirToDirRequest) (*api.Empty, error) {
	logger := log.WithFields(log.Fields{
		"package":  "service",
		"struct":   "Server",
		"function": "RenameDirToDir",
	})

	logger.Infof("RenameDirToDir request from client %s: %s -> %s", request.SessionId, request.SourcePath, request.DestinationPath)

	session, err := server.getSession(request.SessionId)
	if err != nil {
		logger.Error(err)
		return nil, err
	}

	session.Mutex.Lock()
	session.LastActivityTime = time.Now()
	session.Mutex.Unlock()

	err = session.IRODSFS.RenameDirToDir(request.SourcePath, request.DestinationPath)
	if err != nil {
		logger.Error(err)
		return nil, err
	}

	return &api.Empty{}, nil
}

func (server *Server) RenameFileToFile(context context.Context, request *api.RenameFileToFileRequest) (*api.Empty, error) {
	logger := log.WithFields(log.Fields{
		"package":  "service",
		"struct":   "Server",
		"function": "RenameFileToFile",
	})

	logger.Infof("RenameFileToFile request from client %s: %s -> %s", request.SessionId, request.SourcePath, request.DestinationPath)

	session, err := server.getSession(request.SessionId)
	if err != nil {
		logger.Error(err)
		return nil, err
	}

	session.Mutex.Lock()
	session.LastActivityTime = time.Now()
	session.Mutex.Unlock()

	err = session.IRODSFS.RenameFileToFile(request.SourcePath, request.DestinationPath)
	if err != nil {
		logger.Error(err)
		return nil, err
	}

	return &api.Empty{}, nil
}

func (server *Server) CreateFile(context context.Context, request *api.CreateFileRequest) (*api.CreateFileResponse, error) {
	logger := log.WithFields(log.Fields{
		"package":  "service",
		"struct":   "Server",
		"function": "CreateFile",
	})

	logger.Infof("CreateFile request from client %s: %s", request.SessionId, request.Path)

	session, err := server.getSession(request.SessionId)
	if err != nil {
		logger.Error(err)
		return nil, err
	}

	handle, err := session.IRODSFS.CreateFile(request.Path, request.Resource)
	if err != nil {
		logger.Error(err)
		return nil, err
	}

	handleMutex := &sync.Mutex{}
	fileHandleID := xid.New().String()

	var writer asyncwrite.Writer

	if server.Buffer != nil {
		asyncWriter := asyncwrite.NewAsyncWriter(request.Path, fileHandleID, handle, handleMutex, server.Buffer)
		writer = asyncwrite.NewBufferedWriter(request.Path, asyncWriter)
	} else {
		syncWriter := asyncwrite.NewSyncWriter(request.Path, handle, handleMutex)
		writer = asyncwrite.NewBufferedWriter(request.Path, syncWriter)
	}

	fileHandle := &FileHandle{
		ID:          fileHandleID,
		SessionID:   request.SessionId,
		Writer:      writer,
		IRODSHandle: handle,
		Mutex:       handleMutex,
	}

	session.Mutex.Lock()
	defer session.Mutex.Unlock()

	// add
	session.FileHandles[fileHandleID] = fileHandle
	session.LastActivityTime = time.Now()

	responseEntry := &api.Entry{
		Id:         handle.Entry.ID,
		Type:       string(handle.Entry.Type),
		Name:       handle.Entry.Name,
		Path:       handle.Entry.Path,
		Owner:      handle.Entry.Owner,
		Size:       handle.Entry.Size,
		CreateTime: utils.MakeTimeToString(handle.Entry.CreateTime),
		ModifyTime: utils.MakeTimeToString(handle.Entry.ModifyTime),
		Checksum:   handle.Entry.CheckSum,
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

	logger.Infof("OpenFile request from client %s: %s", request.SessionId, request.Path)

	session, err := server.getSession(request.SessionId)
	if err != nil {
		logger.Error(err)
		return nil, err
	}

	handle, err := session.IRODSFS.OpenFile(request.Path, request.Resource, request.Mode)
	if err != nil {
		logger.Error(err)
		return nil, err
	}

	handleMutex := &sync.Mutex{}
	fileHandleID := xid.New().String()

	asyncWriter := asyncwrite.NewAsyncWriter(request.Path, fileHandleID, handle, handleMutex, server.Buffer)
	writer := asyncwrite.NewBufferedWriter(request.Path, asyncWriter)

	fileHandle := &FileHandle{
		ID:          fileHandleID,
		SessionID:   request.SessionId,
		Writer:      writer,
		IRODSHandle: handle,
		Mutex:       handleMutex,
	}

	session.Mutex.Lock()
	defer session.Mutex.Unlock()

	// add
	session.FileHandles[fileHandleID] = fileHandle
	session.LastActivityTime = time.Now()

	responseEntry := &api.Entry{
		Id:         handle.Entry.ID,
		Type:       string(handle.Entry.Type),
		Name:       handle.Entry.Name,
		Path:       handle.Entry.Path,
		Owner:      handle.Entry.Owner,
		Size:       handle.Entry.Size,
		CreateTime: utils.MakeTimeToString(handle.Entry.CreateTime),
		ModifyTime: utils.MakeTimeToString(handle.Entry.ModifyTime),
		Checksum:   handle.Entry.CheckSum,
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

	logger.Infof("TruncateFile request from client %s: %s", request.SessionId, request.Path)

	session, err := server.getSession(request.SessionId)
	if err != nil {
		logger.Error(err)
		return nil, err
	}

	session.Mutex.Lock()
	session.LastActivityTime = time.Now()
	session.Mutex.Unlock()

	err = session.IRODSFS.TruncateFile(request.Path, request.Size)
	if err != nil {
		logger.Error(err)
		return nil, err
	}

	return &api.Empty{}, nil
}

func (server *Server) GetOffset(context context.Context, request *api.GetOffsetRequest) (*api.GetOffsetResponse, error) {
	logger := log.WithFields(log.Fields{
		"package":  "service",
		"struct":   "Server",
		"function": "GetOffset",
	})

	logger.Infof("GetOffset request from client sessionID: %s, fileHandleID: %s", request.SessionId, request.FileHandleId)

	fileHandle, err := server.getFileHandle(request.SessionId, request.FileHandleId)
	if err != nil {
		logger.Error(err)
		return nil, err
	}

	err = fileHandle.Writer.Flush()
	if err != nil {
		logger.Error(err)
		return nil, err
	}

	fileHandle.Mutex.Lock()
	defer fileHandle.Mutex.Unlock()

	offset := fileHandle.IRODSHandle.GetOffset()
	response := &api.GetOffsetResponse{
		Offset: offset,
	}

	return response, nil
}

func (server *Server) ReadAt(context context.Context, request *api.ReadAtRequest) (*api.ReadAtResponse, error) {
	logger := log.WithFields(log.Fields{
		"package":  "service",
		"struct":   "Server",
		"function": "ReadAt",
	})

	logger.Infof("ReadAt request from client sessionID: %s, fileHandleID: %s", request.SessionId, request.FileHandleId)

	fileHandle, err := server.getFileHandle(request.SessionId, request.FileHandleId)
	if err != nil {
		logger.Error(err)
		return nil, err
	}

	fileHandle.Mutex.Lock()
	defer fileHandle.Mutex.Unlock()

	data, err := fileHandle.IRODSHandle.ReadAt(request.Offset, int(request.Length))
	if err != nil {
		logger.Error(err)
		return nil, err
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

	logger.Infof("WriteAt request from client sessionID: %s, fileHandleID: %s", request.SessionId, request.FileHandleId)

	fileHandle, err := server.getFileHandle(request.SessionId, request.FileHandleId)
	if err != nil {
		logger.Error(err)
		return nil, err
	}

	err = fileHandle.Writer.WriteAt(request.Offset, request.Data)
	//err = fileHandle.IRODSHandle.WriteAt(request.Offset, request.Data)
	if err != nil {
		logger.Error(err)
		return nil, err
	}

	return &api.Empty{}, nil
}

func (server *Server) Flush(context context.Context, request *api.FlushRequest) (*api.Empty, error) {
	logger := log.WithFields(log.Fields{
		"package":  "service",
		"struct":   "Server",
		"function": "Flush",
	})

	logger.Infof("Flush request from client sessionID: %s, fileHandleID: %s", request.SessionId, request.FileHandleId)

	session, err := server.getSession(request.SessionId)
	if err != nil {
		logger.Error(err)
		return nil, err
	}

	session.Mutex.Lock()
	defer session.Mutex.Unlock()

	session.LastActivityTime = time.Now()

	fileHandle, ok := session.FileHandles[request.FileHandleId]
	if !ok {
		err := fmt.Errorf("cannot find file handle %s", request.FileHandleId)
		logger.Error(err)
		return nil, err
	}

	err = fileHandle.Writer.Flush()
	if err != nil {
		logger.Error(err)
		return nil, err
	}

	return &api.Empty{}, nil
}

func (server *Server) Close(context context.Context, request *api.CloseRequest) (*api.Empty, error) {
	logger := log.WithFields(log.Fields{
		"package":  "service",
		"struct":   "Server",
		"function": "Close",
	})

	logger.Infof("Close request from client sessionID: %s, fileHandleID: %s", request.SessionId, request.FileHandleId)

	session, err := server.getSession(request.SessionId)
	if err != nil {
		logger.Error(err)
		return nil, err
	}

	session.Mutex.Lock()
	defer session.Mutex.Unlock()

	session.LastActivityTime = time.Now()

	fileHandle, ok := session.FileHandles[request.FileHandleId]
	if !ok {
		err := fmt.Errorf("cannot find file handle %s", request.FileHandleId)
		logger.Error(err)
		return nil, err
	}

	delete(session.FileHandles, request.FileHandleId)

	if fileHandle.Writer != nil {
		// wait until all queued tasks complete
		fileHandle.Writer.Release()

		err := fileHandle.Writer.GetPendingError()
		if err != nil {
			logger.WithError(err)
			return nil, err
		}
	}

	fileHandle.Mutex.Lock()
	defer fileHandle.Mutex.Unlock()

	err = fileHandle.IRODSHandle.Close()
	if err != nil {
		logger.Error(err)
		return nil, err
	}

	return &api.Empty{}, nil
}
