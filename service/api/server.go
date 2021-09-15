package api

import (
	context "context"
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"sync"
	"time"

	irodsfs "github.com/cyverse/go-irodsclient/fs"
	irodsfs_clienttype "github.com/cyverse/go-irodsclient/irods/types"
	"github.com/cyverse/irodsfs-proxy/utils"
	"github.com/rs/xid"
	log "github.com/sirupsen/logrus"
)

type Server struct {
	UnimplementedProxyAPIServer
	Mutex    sync.RWMutex // mutex to access Sessions
	Sessions map[string]*Session
}

type Session struct {
	ID               string
	Account          *irodsfs_clienttype.IRODSAccount
	IRODSFS          *irodsfs.FileSystem
	ReferenceCount   int
	LastActivityTime time.Time
	FileHandles      map[string]*FileHandle
	Mutex            sync.Mutex // mutex to access FileHandles, ReferenceCount and LastActivityTime
}

type FileHandle struct {
	ID          string
	SessionID   string
	IRODSHandle *irodsfs.FileHandle
}

func NewServer() *Server {
	return &Server{
		Sessions: map[string]*Session{},
	}
}

func (server *Server) generateSessionID(account *Account) string {
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

func (server *Server) Login(context context.Context, request *LoginRequest) (*LoginResponse, error) {
	logger := log.WithFields(log.Fields{
		"package":  "api",
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
		account := &irodsfs_clienttype.IRODSAccount{
			AuthenticationScheme:    irodsfs_clienttype.AuthScheme(request.Account.AuthenticationScheme),
			ClientServerNegotiation: request.Account.ClientServerNegotiation,
			CSNegotiationPolicy:     irodsfs_clienttype.CSNegotiationRequire(request.Account.CsNegotiationPolicy),
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

		session = &Session{
			ID:               sessionID,
			Account:          account,
			IRODSFS:          nil, // placeholder
			ReferenceCount:   1,
			LastActivityTime: time.Now(),
			FileHandles:      map[string]*FileHandle{},
		}

		session.Mutex.Lock()
		defer session.Mutex.Unlock()

		server.Sessions[sessionID] = session

		fs, err := irodsfs.NewFileSystemWithDefault(account, request.ApplicationName)
		if err != nil {
			logger.Error(err)
			return nil, err
		}

		session.IRODSFS = fs
	}

	response := &LoginResponse{
		SessionId: sessionID,
	}

	return response, nil
}

func (server *Server) Logout(context context.Context, request *LogoutRequest) (*Empty, error) {
	logger := log.WithFields(log.Fields{
		"package":  "api",
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
		return &Empty{}, nil
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

	return &Empty{}, nil
}

func (server *Server) LogoutAll() {
	logger := log.WithFields(log.Fields{
		"package":  "api",
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

func (server *Server) getSession(sessionID string) (*Session, error) {
	logger := log.WithFields(log.Fields{
		"package":  "api",
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
		"package":  "api",
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

func (server *Server) List(context context.Context, request *ListRequest) (*ListResponse, error) {
	logger := log.WithFields(log.Fields{
		"package":  "api",
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

	responseEntries := []*Entry{}
	for _, entry := range entries {
		responseEntry := &Entry{
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

	response := &ListResponse{
		Entries: responseEntries,
	}

	return response, nil
}

func (server *Server) Stat(context context.Context, request *StatRequest) (*StatResponse, error) {
	logger := log.WithFields(log.Fields{
		"package":  "api",
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
		if irodsfs_clienttype.IsFileNotFoundError(err) {
			return &StatResponse{
				Error: &SoftError{
					Type:    ErrorType_FILENOTFOUND,
					Message: err.Error(),
				},
			}, nil
		}

		logger.Error(err)
		return nil, err
	}

	responseEntry := &Entry{
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

	response := &StatResponse{
		Entry: responseEntry,
		Error: nil,
	}

	return response, nil
}

func (server *Server) ExistsDir(context context.Context, request *ExistsDirRequest) (*ExistsDirResponse, error) {
	logger := log.WithFields(log.Fields{
		"package":  "api",
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
	return &ExistsDirResponse{
		Exist: exist,
	}, nil
}

func (server *Server) ExistsFile(context context.Context, request *ExistsFileRequest) (*ExistsFileResponse, error) {
	logger := log.WithFields(log.Fields{
		"package":  "api",
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
	return &ExistsFileResponse{
		Exist: exist,
	}, nil
}

func (server *Server) ListDirACLsWithGroupUsers(context context.Context, request *ListDirACLsWithGroupUsersRequest) (*ListDirACLsWithGroupUsersResponse, error) {
	logger := log.WithFields(log.Fields{
		"package":  "api",
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

	responseAccesses := []*Access{}
	for _, access := range accesses {
		responseAccess := &Access{
			Path:        access.Path,
			UserName:    access.UserName,
			UserZone:    access.UserZone,
			UserType:    string(access.UserType),
			AccessLevel: string(access.AccessLevel),
		}
		responseAccesses = append(responseAccesses, responseAccess)
	}

	response := &ListDirACLsWithGroupUsersResponse{
		Accesses: responseAccesses,
	}

	return response, nil
}

func (server *Server) ListFileACLsWithGroupUsers(context context.Context, request *ListFileACLsWithGroupUsersRequest) (*ListFileACLsWithGroupUsersResponse, error) {
	logger := log.WithFields(log.Fields{
		"package":  "api",
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

	responseAccesses := []*Access{}
	for _, access := range accesses {
		responseAccess := &Access{
			Path:        access.Path,
			UserName:    access.UserName,
			UserZone:    access.UserZone,
			UserType:    string(access.UserType),
			AccessLevel: string(access.AccessLevel),
		}
		responseAccesses = append(responseAccesses, responseAccess)
	}

	response := &ListFileACLsWithGroupUsersResponse{
		Accesses: responseAccesses,
	}

	return response, nil
}

func (server *Server) RemoveFile(context context.Context, request *RemoveFileRequest) (*Empty, error) {
	logger := log.WithFields(log.Fields{
		"package":  "api",
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

	return &Empty{}, nil
}

func (server *Server) RemoveDir(context context.Context, request *RemoveDirRequest) (*Empty, error) {
	logger := log.WithFields(log.Fields{
		"package":  "api",
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

	return &Empty{}, nil
}

func (server *Server) MakeDir(context context.Context, request *MakeDirRequest) (*Empty, error) {
	logger := log.WithFields(log.Fields{
		"package":  "api",
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

	return &Empty{}, nil
}

func (server *Server) RenameDirToDir(context context.Context, request *RenameDirToDirRequest) (*Empty, error) {
	logger := log.WithFields(log.Fields{
		"package":  "api",
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

	return &Empty{}, nil
}

func (server *Server) RenameFileToFile(context context.Context, request *RenameFileToFileRequest) (*Empty, error) {
	logger := log.WithFields(log.Fields{
		"package":  "api",
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

	return &Empty{}, nil
}

func (server *Server) CreateFile(context context.Context, request *CreateFileRequest) (*CreateFileResponse, error) {
	logger := log.WithFields(log.Fields{
		"package":  "api",
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

	fileHandleID := xid.New().String()
	fileHandle := &FileHandle{
		ID:          fileHandleID,
		SessionID:   request.SessionId,
		IRODSHandle: handle,
	}

	session.Mutex.Lock()
	defer session.Mutex.Unlock()

	// add
	session.FileHandles[fileHandleID] = fileHandle
	session.LastActivityTime = time.Now()

	responseEntry := &Entry{
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

	response := &CreateFileResponse{
		FileHandleId: fileHandleID,
		Entry:        responseEntry,
	}

	return response, nil
}

func (server *Server) OpenFile(context context.Context, request *OpenFileRequest) (*OpenFileResponse, error) {
	logger := log.WithFields(log.Fields{
		"package":  "api",
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

	fileHandleID := xid.New().String()
	fileHandle := &FileHandle{
		ID:          fileHandleID,
		SessionID:   request.SessionId,
		IRODSHandle: handle,
	}

	session.Mutex.Lock()
	defer session.Mutex.Unlock()

	// add
	session.FileHandles[fileHandleID] = fileHandle
	session.LastActivityTime = time.Now()

	responseEntry := &Entry{
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

	response := &OpenFileResponse{
		FileHandleId: fileHandleID,
		Entry:        responseEntry,
	}

	return response, nil
}

func (server *Server) TruncateFile(context context.Context, request *TruncateFileRequest) (*Empty, error) {
	logger := log.WithFields(log.Fields{
		"package":  "api",
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

	return &Empty{}, nil
}

func (server *Server) GetOffset(context context.Context, request *GetOffsetRequest) (*GetOffsetResponse, error) {
	logger := log.WithFields(log.Fields{
		"package":  "api",
		"struct":   "Server",
		"function": "GetOffset",
	})

	logger.Infof("GetOffset request from client sessionID: %s, fileHandleID: %s", request.SessionId, request.FileHandleId)

	fileHandle, err := server.getFileHandle(request.SessionId, request.FileHandleId)
	if err != nil {
		logger.Error(err)
		return nil, err
	}

	offset := fileHandle.IRODSHandle.GetOffset()
	response := &GetOffsetResponse{
		Offset: offset,
	}

	return response, nil
}

func (server *Server) ReadAt(context context.Context, request *ReadAtRequest) (*ReadAtResponse, error) {
	logger := log.WithFields(log.Fields{
		"package":  "api",
		"struct":   "Server",
		"function": "ReadAt",
	})

	logger.Infof("ReadAt request from client sessionID: %s, fileHandleID: %s", request.SessionId, request.FileHandleId)

	fileHandle, err := server.getFileHandle(request.SessionId, request.FileHandleId)
	if err != nil {
		logger.Error(err)
		return nil, err
	}

	data, err := fileHandle.IRODSHandle.ReadAt(request.Offset, int(request.Length))
	if err != nil {
		logger.Error(err)
		return nil, err
	}

	response := &ReadAtResponse{
		Data: data,
	}

	return response, nil
}

func (server *Server) WriteAt(context context.Context, request *WriteAtRequest) (*Empty, error) {
	logger := log.WithFields(log.Fields{
		"package":  "api",
		"struct":   "Server",
		"function": "WriteAt",
	})

	logger.Infof("WriteAt request from client sessionID: %s, fileHandleID: %s", request.SessionId, request.FileHandleId)

	fileHandle, err := server.getFileHandle(request.SessionId, request.FileHandleId)
	if err != nil {
		logger.Error(err)
		return nil, err
	}

	err = fileHandle.IRODSHandle.WriteAt(request.Offset, request.Data)
	if err != nil {
		logger.Error(err)
		return nil, err
	}

	return &Empty{}, nil
}

func (server *Server) Close(context context.Context, request *CloseRequest) (*Empty, error) {
	logger := log.WithFields(log.Fields{
		"package":  "api",
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

	err = fileHandle.IRODSHandle.Close()
	if err != nil {
		logger.Error(err)
		return nil, err
	}

	return &Empty{}, nil
}
