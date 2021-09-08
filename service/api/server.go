package api

import (
	context "context"
	"fmt"

	irodsfs "github.com/cyverse/go-irodsclient/fs"
	irodsfs_clienttype "github.com/cyverse/go-irodsclient/irods/types"
	"github.com/cyverse/irodsfs-proxy/utils"
	"github.com/rs/xid"
	log "github.com/sirupsen/logrus"
)

type Server struct {
	UnimplementedProxyAPIServer
	Sessions map[string]*Session

	FileHandles map[string]map[string]*FileHandle // key = sessionID, second key = handleID
}

type Session struct {
	ID      string
	Account *irodsfs_clienttype.IRODSAccount
	FS      *irodsfs.FileSystem
}

type FileHandle struct {
	ID          string
	SessionID   string
	IRODSHandle *irodsfs.FileHandle
}

func (server *Server) Login(context context.Context, request *LoginRequest) (*LoginResponse, error) {
	logger := log.WithFields(log.Fields{
		"package":  "api",
		"function": "Server.Login",
	})

	logger.Infof("Login request from client: %s - %s", request.Account.Host, request.Account.ClientUser)

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

	fs, err := irodsfs.NewFileSystemWithDefault(account, request.ApplicationName)
	if err != nil {
		logger.Error(err)
		return nil, err
	}

	sessionID := xid.New().String()
	session := &Session{
		ID:      sessionID,
		Account: account,
		FS:      fs,
	}

	server.Sessions[sessionID] = session
	response := &LoginResponse{
		SessionId: sessionID,
	}

	return response, nil
}

func (server *Server) Logout(context context.Context, request *LogoutRequest) (*Empty, error) {
	logger := log.WithFields(log.Fields{
		"package":  "api",
		"function": "Server.Logout",
	})

	logger.Infof("Logout request from client: %s", request.SessionId)

	session, ok := server.Sessions[request.SessionId]
	if !ok {
		err := fmt.Errorf("cannot find session %s", request.SessionId)
		logger.Error(err)
		return nil, err
	}

	// find opened file handles
	if handles, ok := server.FileHandles[request.SessionId]; ok {
		for _, handle := range handles {
			if handle.IRODSHandle != nil {
				handle.IRODSHandle.Close()
			}
		}
		delete(server.FileHandles, request.SessionId)
	}

	delete(server.Sessions, request.SessionId)

	session.FS.Release()
	return &Empty{}, nil
}

func (server *Server) List(context context.Context, request *ListRequest) (*ListResponse, error) {
	logger := log.WithFields(log.Fields{
		"package":  "api",
		"function": "Server.List",
	})

	logger.Infof("List request from client %s: %s", request.SessionId, request.Path)

	session, ok := server.Sessions[request.SessionId]
	if !ok {
		err := fmt.Errorf("cannot find session %s", request.SessionId)
		logger.Error(err)
		return nil, err
	}

	entries, err := session.FS.List(request.Path)
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
		"function": "Server.Stat",
	})

	logger.Infof("Stat request from client %s: %s", request.SessionId, request.Path)

	session, ok := server.Sessions[request.SessionId]
	if !ok {
		err := fmt.Errorf("cannot find session %s", request.SessionId)
		logger.Error(err)
		return nil, err
	}

	entry, err := session.FS.Stat(request.Path)
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

func (server *Server) ListDirACLsWithGroupUsers(context context.Context, request *ListDirACLsWithGroupUsersRequest) (*ListDirACLsWithGroupUsersResponse, error) {
	logger := log.WithFields(log.Fields{
		"package":  "api",
		"function": "Server.ListDirACLsWithGroupUsers",
	})

	logger.Infof("ListDirACLsWithGroupUsers request from client %s: %s", request.SessionId, request.Path)

	session, ok := server.Sessions[request.SessionId]
	if !ok {
		err := fmt.Errorf("cannot find session %s", request.SessionId)
		logger.Error(err)
		return nil, err
	}

	accesses, err := session.FS.ListDirACLsWithGroupUsers(request.Path)
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
		"function": "Server.ListFileACLsWithGroupUsers",
	})

	logger.Infof("ListFileACLsWithGroupUsers request from client %s: %s", request.SessionId, request.Path)

	session, ok := server.Sessions[request.SessionId]
	if !ok {
		err := fmt.Errorf("cannot find session %s", request.SessionId)
		logger.Error(err)
		return nil, err
	}

	accesses, err := session.FS.ListFileACLsWithGroupUsers(request.Path)
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
		"function": "Server.RemoveFile",
	})

	logger.Infof("RemoveFile request from client %s: %s", request.SessionId, request.Path)

	session, ok := server.Sessions[request.SessionId]
	if !ok {
		err := fmt.Errorf("cannot find session %s", request.SessionId)
		logger.Error(err)
		return nil, err
	}

	err := session.FS.RemoveFile(request.Path, request.Force)
	if err != nil {
		logger.Error(err)
		return nil, err
	}

	return &Empty{}, nil
}

func (server *Server) RemoveDir(context context.Context, request *RemoveDirRequest) (*Empty, error) {
	logger := log.WithFields(log.Fields{
		"package":  "api",
		"function": "Server.RemoveDir",
	})

	logger.Infof("RemoveDir request from client %s: %s", request.SessionId, request.Path)

	session, ok := server.Sessions[request.SessionId]
	if !ok {
		err := fmt.Errorf("cannot find session %s", request.SessionId)
		logger.Error(err)
		return nil, err
	}

	err := session.FS.RemoveDir(request.Path, request.Recurse, request.Force)
	if err != nil {
		logger.Error(err)
		return nil, err
	}

	return &Empty{}, nil
}

func (server *Server) MakeDir(context context.Context, request *MakeDirRequest) (*Empty, error) {
	logger := log.WithFields(log.Fields{
		"package":  "api",
		"function": "Server.MakeDir",
	})

	logger.Infof("MakeDir request from client %s: %s", request.SessionId, request.Path)

	session, ok := server.Sessions[request.SessionId]
	if !ok {
		err := fmt.Errorf("cannot find session %s", request.SessionId)
		logger.Error(err)
		return nil, err
	}

	err := session.FS.MakeDir(request.Path, request.Recurse)
	if err != nil {
		logger.Error(err)
		return nil, err
	}

	return &Empty{}, nil
}

func (server *Server) RenameDirToDir(context context.Context, request *RenameDirToDirRequest) (*Empty, error) {
	logger := log.WithFields(log.Fields{
		"package":  "api",
		"function": "Server.RenameDirToDir",
	})

	logger.Infof("RenameDirToDir request from client %s: %s -> %s", request.SessionId, request.SourcePath, request.DestinationPath)

	session, ok := server.Sessions[request.SessionId]
	if !ok {
		err := fmt.Errorf("cannot find session %s", request.SessionId)
		logger.Error(err)
		return nil, err
	}

	err := session.FS.RenameDirToDir(request.SourcePath, request.DestinationPath)
	if err != nil {
		logger.Error(err)
		return nil, err
	}

	return &Empty{}, nil
}

func (server *Server) RenameFileToFile(context context.Context, request *RenameFileToFileRequest) (*Empty, error) {
	logger := log.WithFields(log.Fields{
		"package":  "api",
		"function": "Server.RenameFileToFile",
	})

	logger.Infof("RenameFileToFile request from client %s: %s -> %s", request.SessionId, request.SourcePath, request.DestinationPath)

	session, ok := server.Sessions[request.SessionId]
	if !ok {
		err := fmt.Errorf("cannot find session %s", request.SessionId)
		logger.Error(err)
		return nil, err
	}

	err := session.FS.RenameFileToFile(request.SourcePath, request.DestinationPath)
	if err != nil {
		logger.Error(err)
		return nil, err
	}

	return &Empty{}, nil
}

func (server *Server) CreateFile(context context.Context, request *CreateFileRequest) (*CreateFileResponse, error) {
	logger := log.WithFields(log.Fields{
		"package":  "api",
		"function": "Server.CreateFile",
	})

	logger.Infof("CreateFile request from client %s: %s", request.SessionId, request.Path)

	session, ok := server.Sessions[request.SessionId]
	if !ok {
		err := fmt.Errorf("cannot find session %s", request.SessionId)
		logger.Error(err)
		return nil, err
	}

	handle, err := session.FS.CreateFile(request.Path, request.Resource)
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

	if handles, ok := server.FileHandles[request.SessionId]; ok {
		// add
		handles[fileHandleID] = fileHandle
	} else {
		// create
		handles = map[string]*FileHandle{}
		server.FileHandles[request.SessionId] = handles
		// add
		handles[fileHandleID] = fileHandle
	}

	response := &CreateFileResponse{
		FileHandleId: fileHandleID,
	}

	return response, nil
}

func (server *Server) OpenFile(context context.Context, request *OpenFileRequest) (*OpenFileResponse, error) {
	logger := log.WithFields(log.Fields{
		"package":  "api",
		"function": "Server.OpenFile",
	})

	logger.Infof("OpenFile request from client %s: %s", request.SessionId, request.Path)

	session, ok := server.Sessions[request.SessionId]
	if !ok {
		err := fmt.Errorf("cannot find session %s", request.SessionId)
		logger.Error(err)
		return nil, err
	}

	handle, err := session.FS.OpenFile(request.Path, request.Resource, request.Mode)
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

	if handles, ok := server.FileHandles[request.SessionId]; ok {
		// add
		handles[fileHandleID] = fileHandle
	} else {
		// create
		handles = map[string]*FileHandle{}
		server.FileHandles[request.SessionId] = handles
		// add
		handles[fileHandleID] = fileHandle
	}

	response := &OpenFileResponse{
		FileHandleId: fileHandleID,
	}

	return response, nil
}

func (server *Server) TruncateFile(context context.Context, request *TruncateFileRequest) (*Empty, error) {
	logger := log.WithFields(log.Fields{
		"package":  "api",
		"function": "Server.TruncateFile",
	})

	logger.Infof("TruncateFile request from client %s: %s", request.SessionId, request.Path)

	session, ok := server.Sessions[request.SessionId]
	if !ok {
		err := fmt.Errorf("cannot find session %s", request.SessionId)
		logger.Error(err)
		return nil, err
	}

	err := session.FS.TruncateFile(request.Path, request.Size)
	if err != nil {
		logger.Error(err)
		return nil, err
	}

	return &Empty{}, nil
}

func (server *Server) Seek(context context.Context, request *SeekRequest) (*SeekResponse, error) {
	logger := log.WithFields(log.Fields{
		"package":  "api",
		"function": "Server.Seek",
	})

	logger.Infof("Seek request from client sessionID: %s, fileHandleID: %s", request.SessionId, request.FileHandleId)

	fileHandles, ok := server.FileHandles[request.SessionId]
	if !ok {
		err := fmt.Errorf("cannot find handles for session %s", request.SessionId)
		logger.Error(err)
		return nil, err
	}

	fileHandle, ok := fileHandles[request.FileHandleId]
	if !ok {
		err := fmt.Errorf("cannot find file handle %s", request.FileHandleId)
		logger.Error(err)
		return nil, err
	}

	newOffset, err := fileHandle.IRODSHandle.Seek(request.Offset, irodsfs_clienttype.Whence(request.Whence))
	if err != nil {
		logger.Error(err)
		return nil, err
	}

	response := &SeekResponse{
		Offset: newOffset,
	}

	return response, nil
}

func (server *Server) Read(context context.Context, request *ReadRequest) (*ReadResponse, error) {
	logger := log.WithFields(log.Fields{
		"package":  "api",
		"function": "Server.Read",
	})

	logger.Infof("Read request from client sessionID: %s, fileHandleID: %s", request.SessionId, request.FileHandleId)

	fileHandles, ok := server.FileHandles[request.SessionId]
	if !ok {
		err := fmt.Errorf("cannot find handles for session %s", request.SessionId)
		logger.Error(err)
		return nil, err
	}

	fileHandle, ok := fileHandles[request.FileHandleId]
	if !ok {
		err := fmt.Errorf("cannot find file handle %s", request.FileHandleId)
		logger.Error(err)
		return nil, err
	}

	data, err := fileHandle.IRODSHandle.Read(int(request.Length))
	if err != nil {
		logger.Error(err)
		return nil, err
	}

	response := &ReadResponse{
		Data: data,
	}

	return response, nil
}

func (server *Server) Write(context context.Context, request *WriteRequest) (*Empty, error) {
	logger := log.WithFields(log.Fields{
		"package":  "api",
		"function": "Server.Write",
	})

	logger.Infof("Write request from client sessionID: %s, fileHandleID: %s", request.SessionId, request.FileHandleId)

	fileHandles, ok := server.FileHandles[request.SessionId]
	if !ok {
		err := fmt.Errorf("cannot find handles for session %s", request.SessionId)
		logger.Error(err)
		return nil, err
	}

	fileHandle, ok := fileHandles[request.FileHandleId]
	if !ok {
		err := fmt.Errorf("cannot find file handle %s", request.FileHandleId)
		logger.Error(err)
		return nil, err
	}

	err := fileHandle.IRODSHandle.Write(request.Data)
	if err != nil {
		logger.Error(err)
		return nil, err
	}

	return &Empty{}, nil
}

func (server *Server) Close(context context.Context, request *CloseRequest) (*Empty, error) {
	logger := log.WithFields(log.Fields{
		"package":  "api",
		"function": "Server.Close",
	})

	logger.Infof("Close request from client sessionID: %s, fileHandleID: %s", request.SessionId, request.FileHandleId)

	fileHandles, ok := server.FileHandles[request.SessionId]
	if !ok {
		err := fmt.Errorf("cannot find handles for session %s", request.SessionId)
		logger.Error(err)
		return nil, err
	}

	fileHandle, ok := fileHandles[request.FileHandleId]
	if !ok {
		err := fmt.Errorf("cannot find file handle %s", request.FileHandleId)
		logger.Error(err)
		return nil, err
	}

	delete(fileHandles, request.FileHandleId)

	err := fileHandle.IRODSHandle.Close()
	if err != nil {
		logger.Error(err)
		return nil, err
	}

	return &Empty{}, nil
}
