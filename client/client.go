package client

import (
	"context"
	"fmt"
	"time"

	irodsclient_fs "github.com/cyverse/go-irodsclient/fs"
	irodsclient_types "github.com/cyverse/go-irodsclient/irods/types"
	"github.com/cyverse/irodsfs-pool/service/api"
	"github.com/cyverse/irodsfs-pool/utils"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
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

// IsReadMode returns true if file is opened with read mode
func (handle *PoolServiceFileHandle) IsReadMode() bool {
	return irodsclient_types.IsFileOpenFlagRead(irodsclient_types.FileOpenMode(handle.openMode))
}

// IsWriteMode returns true if file is opened with write mode
func (handle *PoolServiceFileHandle) IsWriteMode() bool {
	return irodsclient_types.IsFileOpenFlagWrite(irodsclient_types.FileOpenMode(handle.openMode))
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

func (client *PoolServiceClient) getContextWithDeadline() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), client.operationTimeout)
}

// Login logins to iRODS service using account info
func (client *PoolServiceClient) Login(account *irodsclient_types.IRODSAccount, applicationName string, clientID string) (*PoolServiceSession, error) {
	logger := log.WithFields(log.Fields{
		"package":  "client",
		"struct":   "PoolServiceClient",
		"function": "Login",
	})

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
			ServerDn:                account.ServerDN,
			Password:                account.Password,
			Ticket:                  account.Ticket,
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
		return nil, err
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

	request := &api.LogoutRequest{
		SessionId: session.id,
	}

	ctx, cancel := client.getContextWithDeadline()
	defer cancel()

	_, err := client.apiClient.Logout(ctx, request)
	if err != nil {
		logger.Error(err)
		return err
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

	request := &api.ListRequest{
		SessionId: session.id,
		Path:      path,
	}

	ctx, cancel := client.getContextWithDeadline()
	defer cancel()

	response, err := client.apiClient.List(ctx, request)
	if err != nil {
		logger.Error(err)
		return nil, err
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

	request := &api.StatRequest{
		SessionId: session.id,
		Path:      path,
	}

	ctx, cancel := client.getContextWithDeadline()
	defer cancel()

	response, err := client.apiClient.Stat(ctx, request)
	if err != nil {
		logger.Error(err)
		return nil, err
	}

	if response.Error != nil && response.Error.Type != api.ErrorType_NONE {
		// has soft error
		if response.Error.Type == api.ErrorType_FILENOTFOUND {
			// file not found
			return nil, irodsclient_types.NewFileNotFoundError(response.Error.Message)
		}
		return nil, fmt.Errorf(response.Error.Message)
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

// ListDirACLsWithGroupUsers lists iRODS collection ACLs with group users
func (client *PoolServiceClient) ListDirACLsWithGroupUsers(session *PoolServiceSession, path string) ([]*irodsclient_types.IRODSAccess, error) {
	logger := log.WithFields(log.Fields{
		"package":  "client",
		"struct":   "PoolServiceClient",
		"function": "ListDirACLsWithGroupUsers",
	})

	request := &api.ListDirACLsWithGroupUsersRequest{
		SessionId: session.id,
		Path:      path,
	}

	ctx, cancel := client.getContextWithDeadline()
	defer cancel()

	response, err := client.apiClient.ListDirACLsWithGroupUsers(ctx, request)
	if err != nil {
		logger.Error(err)
		return nil, err
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

// ListFileACLsWithGroupUsers lists iRODS data object ACLs with group users
func (client *PoolServiceClient) ListFileACLsWithGroupUsers(session *PoolServiceSession, path string) ([]*irodsclient_types.IRODSAccess, error) {
	logger := log.WithFields(log.Fields{
		"package":  "client",
		"struct":   "PoolServiceClient",
		"function": "ListFileACLsWithGroupUsers",
	})

	request := &api.ListFileACLsWithGroupUsersRequest{
		SessionId: session.id,
		Path:      path,
	}

	ctx, cancel := client.getContextWithDeadline()
	defer cancel()

	response, err := client.apiClient.ListFileACLsWithGroupUsers(ctx, request)
	if err != nil {
		logger.Error(err)
		return nil, err
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
		return err
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
		return err
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
		return err
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
		return err
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
		return err
	}

	return nil
}

// CreateFile creates a new iRODS data object
func (client *PoolServiceClient) CreateFile(session *PoolServiceSession, path string, resource string) (*PoolServiceFileHandle, error) {
	logger := log.WithFields(log.Fields{
		"package":  "client",
		"struct":   "PoolServiceClient",
		"function": "CreateFile",
	})

	request := &api.CreateFileRequest{
		SessionId: session.id,
		Path:      path,
		Resource:  resource,
	}

	ctx, cancel := client.getContextWithDeadline()
	defer cancel()

	response, err := client.apiClient.CreateFile(ctx, request)
	if err != nil {
		logger.Error(err)
		return nil, err
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
		openMode:     string(irodsclient_types.FileOpenModeWriteOnly),
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
		return nil, err
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
		return err
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

		response, err := client.apiClient.ReadAt(ctx, request)
		if err != nil {
			logger.Error(err)
			return nil, err
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
			return nil, err
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

		_, err := client.apiClient.WriteAt(ctx, request)
		if err != nil {
			logger.Error(err)
			return err
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

	request := &api.FlushRequest{
		SessionId:    handle.sessionID,
		FileHandleId: handle.fileHandleID,
	}

	ctx, cancel := client.getContextWithDeadline()
	defer cancel()

	_, err := client.apiClient.Flush(ctx, request)
	if err != nil {
		logger.Error(err)
		return err
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

	request := &api.CloseRequest{
		SessionId:    handle.sessionID,
		FileHandleId: handle.fileHandleID,
	}

	ctx, cancel := client.getContextWithDeadline()
	defer cancel()

	_, err := client.apiClient.Close(ctx, request)
	if err != nil {
		logger.Error(err)
		return err
	}

	return nil
}
