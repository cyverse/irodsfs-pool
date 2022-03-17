package client

import (
	"context"
	"fmt"
	"runtime/debug"
	"time"

	irodsclient_fs "github.com/cyverse/go-irodsclient/fs"
	irodsclient_types "github.com/cyverse/go-irodsclient/irods/types"
	"github.com/cyverse/irodsfs-pool/service/api"
	"github.com/cyverse/irodsfs-pool/utils"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	FileRWLengthMax int32 = 1024 * 1024 * 2 // 2MB
)

// PoolServiceClient is a struct that holds connection information
type PoolServiceClient struct {
	host             string // host:port
	operationTimeout time.Duration
	connection       *grpc.ClientConn
	apiClient        api.PoolAPIClient
}

type PoolServiceSession struct {
	id              string
	account         *irodsclient_types.IRODSAccount
	applicationName string
	clientID        string
}

type PoolServiceFileHandle struct {
	sessionID    string
	entry        *irodsclient_fs.Entry
	openMode     string
	fileHandleID string
}

// GetSessionID returns session ID
func (handle *PoolServiceFileHandle) GetSessionID() string {
	return handle.sessionID
}

// IsReadMode returns true if file is opened with read mode
func (handle *PoolServiceFileHandle) IsReadMode() bool {
	return irodsclient_types.IsFileOpenFlagRead(irodsclient_types.FileOpenMode(handle.openMode))
}

// IsWriteMode returns true if file is opened with write mode
func (handle *PoolServiceFileHandle) IsWriteMode() bool {
	return irodsclient_types.IsFileOpenFlagWrite(irodsclient_types.FileOpenMode(handle.openMode))
}

// GetOpenMode returns file open mode
func (handle *PoolServiceFileHandle) GetOpenMode() string {
	return handle.openMode
}

// GetEntry returns file entry
func (handle *PoolServiceFileHandle) GetEntry() *irodsclient_fs.Entry {
	return handle.entry
}

// GetFileHandleID returns file handle ID
func (handle *PoolServiceFileHandle) GetFileHandleID() string {
	return handle.fileHandleID
}

// NewPoolServiceClient creates a new pool service client
func NewPoolServiceClient(poolHost string, operationTimeout time.Duration) *PoolServiceClient {
	return &PoolServiceClient{
		host:             poolHost,
		operationTimeout: operationTimeout,
		connection:       nil,
	}
}

// Disconnect connects to pool service
func (client *PoolServiceClient) Connect() error {
	logger := log.WithFields(log.Fields{
		"package":  "client",
		"struct":   "PoolServiceClient",
		"function": "Connect",
	})

	defer func() {
		if r := recover(); r != nil {
			logger.Errorf("stacktrace from panic: %s", string(debug.Stack()))
			logger.Panic(r)
		}
	}()

	conn, err := grpc.Dial(client.host, grpc.WithInsecure())
	if err != nil {
		logger.Error(err)
		return err
	}

	client.connection = conn
	client.apiClient = api.NewPoolAPIClient(conn)
	return nil
}

// Disconnect disconnects connection from pool service
func (client *PoolServiceClient) Disconnect() {
	if client.apiClient != nil {
		client.apiClient = nil
	}

	if client.connection != nil {
		client.connection.Close()
		client.connection = nil
	}
}

func (client *PoolServiceClient) statusToError(err error) error {
	st, ok := status.FromError(err)
	if ok {
		switch st.Code() {
		case codes.NotFound:
			return irodsclient_types.NewFileNotFoundError(st.Message())
		case codes.AlreadyExists:
			// there's no matching error type for not empty
			return irodsclient_types.NewCollectionNotEmptyError(st.Message())
		case codes.Internal:
			return fmt.Errorf(err.Error())
		default:
			return fmt.Errorf(err.Error())
		}
	}

	return err
}

func (client *PoolServiceClient) getContextWithDeadline() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), client.operationTimeout)
}

func (client *PoolServiceClient) getLargeReadOption() grpc.CallOption {
	// set to 128MB
	return grpc.MaxCallRecvMsgSize(128 * 1024 * 1024)
}

func (client *PoolServiceClient) getLargeWriteOption() grpc.CallOption {
	// set to 128MB
	return grpc.MaxCallSendMsgSize(128 * 1024 * 1024)
}

// Login logins to iRODS service using account info
func (client *PoolServiceClient) Login(account *irodsclient_types.IRODSAccount, applicationName string, clientID string) (*PoolServiceSession, error) {
	logger := log.WithFields(log.Fields{
		"package":  "client",
		"struct":   "PoolServiceClient",
		"function": "Login",
	})

	defer func() {
		if r := recover(); r != nil {
			logger.Errorf("stacktrace from panic: %s", string(debug.Stack()))
			logger.Panic(r)
		}
	}()

	request := &api.LoginRequest{
		Account: &api.Account{
			AuthenticationScheme:    string(account.AuthenticationScheme),
			ClientServerNegotiation: account.ClientServerNegotiation,
			CsNegotiationPolicy:     string(account.CSNegotiationPolicy),
			Host:                    account.Host,
			Port:                    int32(account.Port),
			ClientUser:              account.ClientUser,
			ClientZone:              account.ClientZone,
			ProxyUser:               account.ProxyUser,
			ProxyZone:               account.ProxyZone,
			Password:                account.Password,
			Ticket:                  account.Ticket,
			Resource:                account.DefaultResource,
			PamTtl:                  int32(account.PamTTL),
		},
		ApplicationName: applicationName,
		ClientId:        clientID,
	}

	ctx, cancel := client.getContextWithDeadline()
	defer cancel()

	response, err := client.apiClient.Login(ctx, request)
	if err != nil {
		logger.Error(err)
		return nil, client.statusToError(err)
	}

	return &PoolServiceSession{
		id:              response.SessionId,
		account:         account,
		applicationName: applicationName,
		clientID:        clientID,
	}, nil
}

// Logout logouts from iRODS service
func (client *PoolServiceClient) Logout(session *PoolServiceSession) error {
	logger := log.WithFields(log.Fields{
		"package":  "client",
		"struct":   "PoolServiceClient",
		"function": "Logout",
	})

	defer func() {
		if r := recover(); r != nil {
			logger.Errorf("stacktrace from panic: %s", string(debug.Stack()))
			logger.Panic(r)
		}
	}()

	request := &api.LogoutRequest{
		SessionId: session.id,
	}

	ctx, cancel := client.getContextWithDeadline()
	defer cancel()

	_, err := client.apiClient.Logout(ctx, request)
	if err != nil {
		logger.Error(err)
		return client.statusToError(err)
	}

	return nil
}

// List lists iRODS collection entries
func (client *PoolServiceClient) List(session *PoolServiceSession, path string) ([]*irodsclient_fs.Entry, error) {
	logger := log.WithFields(log.Fields{
		"package":  "client",
		"struct":   "PoolServiceClient",
		"function": "List",
	})

	defer func() {
		if r := recover(); r != nil {
			logger.Errorf("stacktrace from panic: %s", string(debug.Stack()))
			logger.Panic(r)
		}
	}()

	request := &api.ListRequest{
		SessionId: session.id,
		Path:      path,
	}

	ctx, cancel := client.getContextWithDeadline()
	defer cancel()

	response, err := client.apiClient.List(ctx, request, client.getLargeReadOption())
	if err != nil {
		logger.Error(err)
		return nil, client.statusToError(err)
	}

	irodsEntries := []*irodsclient_fs.Entry{}

	for _, entry := range response.Entries {
		createTime, err := utils.ParseTime(entry.CreateTime)
		if err != nil {
			logger.Error(err)
			return nil, err
		}

		modifyTime, err := utils.ParseTime(entry.ModifyTime)
		if err != nil {
			logger.Error(err)
			return nil, err
		}

		irodsEntry := &irodsclient_fs.Entry{
			ID:         entry.Id,
			Type:       irodsclient_fs.EntryType(entry.Type),
			Name:       entry.Name,
			Path:       entry.Path,
			Owner:      entry.Owner,
			Size:       entry.Size,
			CreateTime: createTime,
			ModifyTime: modifyTime,
			CheckSum:   entry.Checksum,
		}

		irodsEntries = append(irodsEntries, irodsEntry)
	}

	return irodsEntries, nil
}

// Stat stats iRODS entry
func (client *PoolServiceClient) Stat(session *PoolServiceSession, path string) (*irodsclient_fs.Entry, error) {
	logger := log.WithFields(log.Fields{
		"package":  "client",
		"struct":   "PoolServiceClient",
		"function": "Stat",
	})

	defer func() {
		if r := recover(); r != nil {
			logger.Errorf("stacktrace from panic: %s", string(debug.Stack()))
			logger.Panic(r)
		}
	}()

	request := &api.StatRequest{
		SessionId: session.id,
		Path:      path,
	}

	ctx, cancel := client.getContextWithDeadline()
	defer cancel()

	response, err := client.apiClient.Stat(ctx, request)
	if err != nil {
		logger.Error(err)
		return nil, client.statusToError(err)
	}

	createTime, err := utils.ParseTime(response.Entry.CreateTime)
	if err != nil {
		logger.Error(err)
		return nil, err
	}

	modifyTime, err := utils.ParseTime(response.Entry.ModifyTime)
	if err != nil {
		logger.Error(err)
		return nil, err
	}

	irodsEntry := &irodsclient_fs.Entry{
		ID:         response.Entry.Id,
		Type:       irodsclient_fs.EntryType(response.Entry.Type),
		Name:       response.Entry.Name,
		Path:       response.Entry.Path,
		Owner:      response.Entry.Owner,
		Size:       response.Entry.Size,
		CreateTime: createTime,
		ModifyTime: modifyTime,
		CheckSum:   response.Entry.Checksum,
	}

	return irodsEntry, nil
}

// ExistsDir checks existence of Dir
func (client *PoolServiceClient) ExistsDir(session *PoolServiceSession, path string) bool {
	logger := log.WithFields(log.Fields{
		"package":  "client",
		"struct":   "PoolServiceClient",
		"function": "ExistsDir",
	})

	defer func() {
		if r := recover(); r != nil {
			logger.Errorf("stacktrace from panic: %s", string(debug.Stack()))
			logger.Panic(r)
		}
	}()

	request := &api.ExistsDirRequest{
		SessionId: session.id,
		Path:      path,
	}

	ctx, cancel := client.getContextWithDeadline()
	defer cancel()

	response, err := client.apiClient.ExistsDir(ctx, request)
	if err != nil {
		logger.Error(err)
		return false
	}

	return response.Exist
}

// ExistsFile checks existence of File
func (client *PoolServiceClient) ExistsFile(session *PoolServiceSession, path string) bool {
	logger := log.WithFields(log.Fields{
		"package":  "client",
		"struct":   "PoolServiceClient",
		"function": "ExistsFile",
	})

	defer func() {
		if r := recover(); r != nil {
			logger.Errorf("stacktrace from panic: %s", string(debug.Stack()))
			logger.Panic(r)
		}
	}()

	request := &api.ExistsFileRequest{
		SessionId: session.id,
		Path:      path,
	}

	ctx, cancel := client.getContextWithDeadline()
	defer cancel()

	response, err := client.apiClient.ExistsFile(ctx, request)
	if err != nil {
		logger.Error(err)
		return false
	}

	return response.Exist
}

// ListUserGroups lists iRODS Groups that a user belongs to
func (client *PoolServiceClient) ListUserGroups(session *PoolServiceSession, user string) ([]*irodsclient_types.IRODSUser, error) {
	logger := log.WithFields(log.Fields{
		"package":  "client",
		"struct":   "PoolServiceClient",
		"function": "ListUserGroups",
	})

	defer func() {
		if r := recover(); r != nil {
			logger.Errorf("stacktrace from panic: %s", string(debug.Stack()))
			logger.Panic(r)
		}
	}()

	request := &api.ListUserGroupsRequest{
		SessionId: session.id,
		UserName:  user,
	}

	ctx, cancel := client.getContextWithDeadline()
	defer cancel()

	response, err := client.apiClient.ListUserGroups(ctx, request, client.getLargeReadOption())
	if err != nil {
		logger.Error(err)
		return nil, client.statusToError(err)
	}

	irodsUsers := []*irodsclient_types.IRODSUser{}

	for _, user := range response.Users {
		irodsUser := &irodsclient_types.IRODSUser{
			Name: user.Name,
			Zone: user.Zone,
			Type: irodsclient_types.IRODSUserType(user.Type),
		}

		irodsUsers = append(irodsUsers, irodsUser)
	}

	return irodsUsers, nil
}

// ListDirACLs lists iRODS collection ACLs
func (client *PoolServiceClient) ListDirACLs(session *PoolServiceSession, path string) ([]*irodsclient_types.IRODSAccess, error) {
	logger := log.WithFields(log.Fields{
		"package":  "client",
		"struct":   "PoolServiceClient",
		"function": "ListDirACLs",
	})

	defer func() {
		if r := recover(); r != nil {
			logger.Errorf("stacktrace from panic: %s", string(debug.Stack()))
			logger.Panic(r)
		}
	}()

	request := &api.ListDirACLsRequest{
		SessionId: session.id,
		Path:      path,
	}

	ctx, cancel := client.getContextWithDeadline()
	defer cancel()

	response, err := client.apiClient.ListDirACLs(ctx, request, client.getLargeReadOption())
	if err != nil {
		logger.Error(err)
		return nil, client.statusToError(err)
	}

	irodsAccesses := []*irodsclient_types.IRODSAccess{}

	for _, access := range response.Accesses {
		irodsAccess := &irodsclient_types.IRODSAccess{
			Path:        access.Path,
			UserName:    access.UserName,
			UserZone:    access.UserZone,
			UserType:    irodsclient_types.IRODSUserType(access.UserType),
			AccessLevel: irodsclient_types.IRODSAccessLevelType(access.AccessLevel),
		}

		irodsAccesses = append(irodsAccesses, irodsAccess)
	}

	return irodsAccesses, nil
}

// ListFileACLs lists iRODS data object ACLs
func (client *PoolServiceClient) ListFileACLs(session *PoolServiceSession, path string) ([]*irodsclient_types.IRODSAccess, error) {
	logger := log.WithFields(log.Fields{
		"package":  "client",
		"struct":   "PoolServiceClient",
		"function": "ListFileACLs",
	})

	defer func() {
		if r := recover(); r != nil {
			logger.Errorf("stacktrace from panic: %s", string(debug.Stack()))
			logger.Panic(r)
		}
	}()

	request := &api.ListFileACLsRequest{
		SessionId: session.id,
		Path:      path,
	}

	ctx, cancel := client.getContextWithDeadline()
	defer cancel()

	response, err := client.apiClient.ListFileACLs(ctx, request, client.getLargeReadOption())
	if err != nil {
		logger.Error(err)
		return nil, client.statusToError(err)
	}

	irodsAccesses := []*irodsclient_types.IRODSAccess{}

	for _, access := range response.Accesses {
		irodsAccess := &irodsclient_types.IRODSAccess{
			Path:        access.Path,
			UserName:    access.UserName,
			UserZone:    access.UserZone,
			UserType:    irodsclient_types.IRODSUserType(access.UserType),
			AccessLevel: irodsclient_types.IRODSAccessLevelType(access.AccessLevel),
		}

		irodsAccesses = append(irodsAccesses, irodsAccess)
	}

	return irodsAccesses, nil
}

// RemoveFile removes iRODS data object
func (client *PoolServiceClient) RemoveFile(session *PoolServiceSession, path string, force bool) error {
	logger := log.WithFields(log.Fields{
		"package":  "client",
		"struct":   "PoolServiceClient",
		"function": "RemoveFile",
	})

	defer func() {
		if r := recover(); r != nil {
			logger.Errorf("stacktrace from panic: %s", string(debug.Stack()))
			logger.Panic(r)
		}
	}()

	request := &api.RemoveFileRequest{
		SessionId: session.id,
		Path:      path,
		Force:     force,
	}

	ctx, cancel := client.getContextWithDeadline()
	defer cancel()

	_, err := client.apiClient.RemoveFile(ctx, request)
	if err != nil {
		logger.Error(err)
		return client.statusToError(err)
	}

	return nil
}

// RemoveDir removes iRODS collection
func (client *PoolServiceClient) RemoveDir(session *PoolServiceSession, path string, recurse bool, force bool) error {
	logger := log.WithFields(log.Fields{
		"package":  "client",
		"struct":   "PoolServiceClient",
		"function": "RemoveDir",
	})

	defer func() {
		if r := recover(); r != nil {
			logger.Errorf("stacktrace from panic: %s", string(debug.Stack()))
			logger.Panic(r)
		}
	}()

	request := &api.RemoveDirRequest{
		SessionId: session.id,
		Path:      path,
		Recurse:   recurse,
		Force:     force,
	}

	ctx, cancel := client.getContextWithDeadline()
	defer cancel()

	_, err := client.apiClient.RemoveDir(ctx, request)
	if err != nil {
		logger.Error(err)
		return client.statusToError(err)
	}

	return nil
}

// MakeDir creates a new iRODS collection
func (client *PoolServiceClient) MakeDir(session *PoolServiceSession, path string, recurse bool) error {
	logger := log.WithFields(log.Fields{
		"package":  "client",
		"struct":   "PoolServiceClient",
		"function": "MakeDir",
	})

	defer func() {
		if r := recover(); r != nil {
			logger.Errorf("stacktrace from panic: %s", string(debug.Stack()))
			logger.Panic(r)
		}
	}()

	request := &api.MakeDirRequest{
		SessionId: session.id,
		Path:      path,
		Recurse:   recurse,
	}

	ctx, cancel := client.getContextWithDeadline()
	defer cancel()

	_, err := client.apiClient.MakeDir(ctx, request)
	if err != nil {
		logger.Error(err)
		return client.statusToError(err)
	}

	return nil
}

// RenameDirToDir renames iRODS collection
func (client *PoolServiceClient) RenameDirToDir(session *PoolServiceSession, srcPath string, destPath string) error {
	logger := log.WithFields(log.Fields{
		"package":  "client",
		"struct":   "PoolServiceClient",
		"function": "RenameDirToDir",
	})

	defer func() {
		if r := recover(); r != nil {
			logger.Errorf("stacktrace from panic: %s", string(debug.Stack()))
			logger.Panic(r)
		}
	}()

	request := &api.RenameDirToDirRequest{
		SessionId:       session.id,
		SourcePath:      srcPath,
		DestinationPath: destPath,
	}

	ctx, cancel := client.getContextWithDeadline()
	defer cancel()

	_, err := client.apiClient.RenameDirToDir(ctx, request)
	if err != nil {
		logger.Error(err)
		return client.statusToError(err)
	}

	return nil
}

// RenameFileToFile renames iRODS data object
func (client *PoolServiceClient) RenameFileToFile(session *PoolServiceSession, srcPath string, destPath string) error {
	logger := log.WithFields(log.Fields{
		"package":  "client",
		"struct":   "PoolServiceClient",
		"function": "RenameFileToFile",
	})

	defer func() {
		if r := recover(); r != nil {
			logger.Errorf("stacktrace from panic: %s", string(debug.Stack()))
			logger.Panic(r)
		}
	}()

	request := &api.RenameFileToFileRequest{
		SessionId:       session.id,
		SourcePath:      srcPath,
		DestinationPath: destPath,
	}

	ctx, cancel := client.getContextWithDeadline()
	defer cancel()

	_, err := client.apiClient.RenameFileToFile(ctx, request)
	if err != nil {
		logger.Error(err)
		return client.statusToError(err)
	}

	return nil
}

// CreateFile creates a new iRODS data object
func (client *PoolServiceClient) CreateFile(session *PoolServiceSession, path string, resource string, mode string) (*PoolServiceFileHandle, error) {
	logger := log.WithFields(log.Fields{
		"package":  "client",
		"struct":   "PoolServiceClient",
		"function": "CreateFile",
	})

	defer func() {
		if r := recover(); r != nil {
			logger.Errorf("stacktrace from panic: %s", string(debug.Stack()))
			logger.Panic(r)
		}
	}()

	request := &api.CreateFileRequest{
		SessionId: session.id,
		Path:      path,
		Resource:  resource,
		Mode:      mode,
	}

	ctx, cancel := client.getContextWithDeadline()
	defer cancel()

	response, err := client.apiClient.CreateFile(ctx, request)
	if err != nil {
		logger.Error(err)
		return nil, client.statusToError(err)
	}

	createTime, err := utils.ParseTime(response.Entry.CreateTime)
	if err != nil {
		logger.Error(err)
		return nil, err
	}

	modifyTime, err := utils.ParseTime(response.Entry.ModifyTime)
	if err != nil {
		logger.Error(err)
		return nil, err
	}

	irodsEntry := &irodsclient_fs.Entry{
		ID:         response.Entry.Id,
		Type:       irodsclient_fs.EntryType(response.Entry.Type),
		Name:       response.Entry.Name,
		Path:       response.Entry.Path,
		Owner:      response.Entry.Owner,
		Size:       response.Entry.Size,
		CreateTime: createTime,
		ModifyTime: modifyTime,
		CheckSum:   response.Entry.Checksum,
	}

	return &PoolServiceFileHandle{
		sessionID:    session.id,
		entry:        irodsEntry,
		openMode:     mode,
		fileHandleID: response.FileHandleId,
	}, nil
}

// OpenFile opens iRODS data object
func (client *PoolServiceClient) OpenFile(session *PoolServiceSession, path string, resource string, mode string) (*PoolServiceFileHandle, error) {
	logger := log.WithFields(log.Fields{
		"package":  "client",
		"struct":   "PoolServiceClient",
		"function": "OpenFile",
	})

	defer func() {
		if r := recover(); r != nil {
			logger.Errorf("stacktrace from panic: %s", string(debug.Stack()))
			logger.Panic(r)
		}
	}()

	request := &api.OpenFileRequest{
		SessionId: session.id,
		Path:      path,
		Resource:  resource,
		Mode:      mode,
	}

	ctx, cancel := client.getContextWithDeadline()
	defer cancel()

	response, err := client.apiClient.OpenFile(ctx, request)
	if err != nil {
		logger.Error(err)
		return nil, client.statusToError(err)
	}

	createTime, err := utils.ParseTime(response.Entry.CreateTime)
	if err != nil {
		logger.Error(err)
		return nil, err
	}

	modifyTime, err := utils.ParseTime(response.Entry.ModifyTime)
	if err != nil {
		logger.Error(err)
		return nil, err
	}

	irodsEntry := &irodsclient_fs.Entry{
		ID:         response.Entry.Id,
		Type:       irodsclient_fs.EntryType(response.Entry.Type),
		Name:       response.Entry.Name,
		Path:       response.Entry.Path,
		Owner:      response.Entry.Owner,
		Size:       response.Entry.Size,
		CreateTime: createTime,
		ModifyTime: modifyTime,
		CheckSum:   response.Entry.Checksum,
	}

	return &PoolServiceFileHandle{
		sessionID:    session.id,
		entry:        irodsEntry,
		openMode:     mode,
		fileHandleID: response.FileHandleId,
	}, nil
}

// TruncateFile truncates iRODS data object
func (client *PoolServiceClient) TruncateFile(session *PoolServiceSession, path string, size int64) error {
	logger := log.WithFields(log.Fields{
		"package":  "client",
		"struct":   "PoolServiceClient",
		"function": "TruncateFile",
	})

	defer func() {
		if r := recover(); r != nil {
			logger.Errorf("stacktrace from panic: %s", string(debug.Stack()))
			logger.Panic(r)
		}
	}()

	request := &api.TruncateFileRequest{
		SessionId: session.id,
		Path:      path,
		Size:      size,
	}

	ctx, cancel := client.getContextWithDeadline()
	defer cancel()

	_, err := client.apiClient.TruncateFile(ctx, request)
	if err != nil {
		logger.Error(err)
		return client.statusToError(err)
	}

	return nil
}

// GetOffset returns current offset
func (client *PoolServiceClient) GetOffset(handle *PoolServiceFileHandle) int64 {
	logger := log.WithFields(log.Fields{
		"package":  "client",
		"struct":   "PoolServiceClient",
		"function": "GetOffset",
	})

	defer func() {
		if r := recover(); r != nil {
			logger.Errorf("stacktrace from panic: %s", string(debug.Stack()))
			logger.Panic(r)
		}
	}()

	request := &api.GetOffsetRequest{
		SessionId:    handle.sessionID,
		FileHandleId: handle.fileHandleID,
	}

	ctx, cancel := client.getContextWithDeadline()
	defer cancel()

	response, err := client.apiClient.GetOffset(ctx, request)
	if err != nil {
		logger.Error(err)
		return -1
	}

	return response.Offset
}

// ReadAt reads iRODS data object
func (client *PoolServiceClient) ReadAt(handle *PoolServiceFileHandle, offset int64, length int32) ([]byte, error) {
	logger := log.WithFields(log.Fields{
		"package":  "client",
		"struct":   "PoolServiceClient",
		"function": "ReadAt",
	})

	defer func() {
		if r := recover(); r != nil {
			logger.Errorf("stacktrace from panic: %s", string(debug.Stack()))
			logger.Panic(r)
		}
	}()

	if length <= FileRWLengthMax {
		// do zero copy
		request := &api.ReadAtRequest{
			SessionId:    handle.sessionID,
			FileHandleId: handle.fileHandleID,
			Offset:       offset,
			Length:       length,
		}

		ctx, cancel := client.getContextWithDeadline()
		defer cancel()

		response, err := client.apiClient.ReadAt(ctx, request, client.getLargeReadOption())
		if err != nil {
			logger.Error(err)
			return nil, client.statusToError(err)
		}

		return response.Data, nil
	}

	// large data, use a buffer
	remainLength := length
	curOffset := offset
	outputData := make([]byte, length)
	totalReadLength := 0

	for remainLength > 0 {
		curLength := remainLength
		if remainLength > FileRWLengthMax {
			curLength = FileRWLengthMax
		}

		request := &api.ReadAtRequest{
			SessionId:    handle.sessionID,
			FileHandleId: handle.fileHandleID,
			Offset:       curOffset,
			Length:       curLength,
		}

		ctx, cancel := client.getContextWithDeadline()
		defer cancel()

		response, err := client.apiClient.ReadAt(ctx, request)
		if err != nil {
			logger.Error(err)
			return nil, client.statusToError(err)
		}

		copy(outputData[totalReadLength:], response.Data)

		remainLength -= int32(len(response.Data))
		curOffset += int64(len(response.Data))
		totalReadLength += len(response.Data)

		if len(response.Data) == 0 || len(response.Data) != int(curLength) {
			// EOF
			break
		}
	}

	return outputData[:totalReadLength], nil
}

// WriteAt writes iRODS data object
func (client *PoolServiceClient) WriteAt(handle *PoolServiceFileHandle, offset int64, data []byte) error {
	logger := log.WithFields(log.Fields{
		"package":  "client",
		"struct":   "PoolServiceClient",
		"function": "WriteAt",
	})

	defer func() {
		if r := recover(); r != nil {
			logger.Errorf("stacktrace from panic: %s", string(debug.Stack()))
			logger.Panic(r)
		}
	}()

	remainLength := len(data)
	curOffset := offset
	totalWriteLength := 0

	for remainLength > 0 {
		curLength := remainLength
		if remainLength > int(FileRWLengthMax) {
			curLength = int(FileRWLengthMax)
		}

		request := &api.WriteAtRequest{
			SessionId:    handle.sessionID,
			FileHandleId: handle.fileHandleID,
			Offset:       curOffset,
			Data:         data[totalWriteLength : totalWriteLength+curLength],
		}

		ctx, cancel := client.getContextWithDeadline()
		defer cancel()

		_, err := client.apiClient.WriteAt(ctx, request, client.getLargeWriteOption())
		if err != nil {
			logger.Error(err)
			return client.statusToError(err)
		}

		remainLength -= curLength
		curOffset += int64(curLength)
		totalWriteLength += int(curLength)
	}

	return nil
}

// Flush flushes iRODS data object handle
func (client *PoolServiceClient) Flush(handle *PoolServiceFileHandle) error {
	logger := log.WithFields(log.Fields{
		"package":  "client",
		"struct":   "PoolServiceClient",
		"function": "Flush",
	})

	defer func() {
		if r := recover(); r != nil {
			logger.Errorf("stacktrace from panic: %s", string(debug.Stack()))
			logger.Panic(r)
		}
	}()

	request := &api.FlushRequest{
		SessionId:    handle.sessionID,
		FileHandleId: handle.fileHandleID,
	}

	ctx, cancel := client.getContextWithDeadline()
	defer cancel()

	_, err := client.apiClient.Flush(ctx, request)
	if err != nil {
		logger.Error(err)
		return client.statusToError(err)
	}

	return nil
}

// Close closes iRODS data object handle
func (client *PoolServiceClient) Close(handle *PoolServiceFileHandle) error {
	logger := log.WithFields(log.Fields{
		"package":  "client",
		"struct":   "PoolServiceClient",
		"function": "Close",
	})

	defer func() {
		if r := recover(); r != nil {
			logger.Errorf("stacktrace from panic: %s", string(debug.Stack()))
			logger.Panic(r)
		}
	}()

	request := &api.CloseRequest{
		SessionId:    handle.sessionID,
		FileHandleId: handle.fileHandleID,
	}

	ctx, cancel := client.getContextWithDeadline()
	defer cancel()

	_, err := client.apiClient.Close(ctx, request)
	if err != nil {
		logger.Error(err)
		return client.statusToError(err)
	}

	return nil
}
