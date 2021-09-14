package client

import (
	"context"
	"fmt"

	irodsfs "github.com/cyverse/go-irodsclient/fs"
	irodsfs_clienttype "github.com/cyverse/go-irodsclient/irods/types"
	"github.com/cyverse/irodsfs-proxy/service/api"
	"github.com/cyverse/irodsfs-proxy/utils"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
)

const (
	FileRWLengthMax int32 = 1024 * 1024 * 2 // 2MB
)

// ProxyServiceClient is a struct that holds connection information
type ProxyServiceClient struct {
	Host       string // host:port
	Connection *grpc.ClientConn
	APIClient  api.ProxyAPIClient
}

type ProxyServiceSession struct {
	ID              string
	Account         *irodsfs_clienttype.IRODSAccount
	ApplicationName string
}

type ProxyServiceFileHandle struct {
	SessionID    string
	Entry        *irodsfs.Entry
	OpenMode     string
	FileHandleID string
}

// IsReadMode returns true if file is opened with read mode
func (handle *ProxyServiceFileHandle) IsReadMode() bool {
	return irodsfs_clienttype.IsFileOpenFlagRead(irodsfs_clienttype.FileOpenMode(handle.OpenMode))
}

// IsWriteMode returns true if file is opened with write mode
func (handle *ProxyServiceFileHandle) IsWriteMode() bool {
	return irodsfs_clienttype.IsFileOpenFlagWrite(irodsfs_clienttype.FileOpenMode(handle.OpenMode))
}

// NewProxyServiceClient creates a new proxy service client
func NewProxyServiceClient(proxyHost string) *ProxyServiceClient {
	return &ProxyServiceClient{
		Host:       proxyHost,
		Connection: nil,
	}
}

// Disconnect connects to proxy service
func (client *ProxyServiceClient) Connect() error {
	logger := log.WithFields(log.Fields{
		"package":  "client",
		"struct":   "ProxyServiceClient",
		"function": "Connect",
	})

	conn, err := grpc.Dial(client.Host, grpc.WithInsecure())
	if err != nil {
		logger.Error(err)
		return err
	}

	client.Connection = conn
	client.APIClient = api.NewProxyAPIClient(conn)
	return nil
}

// Disconnect disconnects connection from proxy service
func (client *ProxyServiceClient) Disconnect() {
	if client.APIClient != nil {
		client.APIClient = nil
	}

	if client.Connection != nil {
		client.Connection.Close()
		client.Connection = nil
	}
}

// Login logins to iRODS service using account info
func (client *ProxyServiceClient) Login(account *irodsfs_clienttype.IRODSAccount, applicationName string) (*ProxyServiceSession, error) {
	logger := log.WithFields(log.Fields{
		"package":  "client",
		"struct":   "ProxyServiceClient",
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
	}

	response, err := client.APIClient.Login(context.Background(), request)
	if err != nil {
		logger.Error(err)
		return nil, err
	}

	return &ProxyServiceSession{
		ID:              response.SessionId,
		Account:         account,
		ApplicationName: applicationName,
	}, nil
}

// Logout logouts from iRODS service
func (client *ProxyServiceClient) Logout(session *ProxyServiceSession) error {
	logger := log.WithFields(log.Fields{
		"package":  "client",
		"struct":   "ProxyServiceClient",
		"function": "Logout",
	})

	request := &api.LogoutRequest{
		SessionId: session.ID,
	}

	_, err := client.APIClient.Logout(context.Background(), request)
	if err != nil {
		logger.Error(err)
		return err
	}

	return nil
}

// List lists iRODS collection entries
func (client *ProxyServiceClient) List(session *ProxyServiceSession, path string) ([]*irodsfs.Entry, error) {
	logger := log.WithFields(log.Fields{
		"package":  "client",
		"struct":   "ProxyServiceClient",
		"function": "List",
	})

	request := &api.ListRequest{
		SessionId: session.ID,
		Path:      path,
	}

	response, err := client.APIClient.List(context.Background(), request)
	if err != nil {
		logger.Error(err)
		return nil, err
	}

	irodsEntries := []*irodsfs.Entry{}

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

		irodsEntry := &irodsfs.Entry{
			ID:         entry.Id,
			Type:       irodsfs.EntryType(entry.Type),
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
func (client *ProxyServiceClient) Stat(session *ProxyServiceSession, path string) (*irodsfs.Entry, error) {
	logger := log.WithFields(log.Fields{
		"package":  "client",
		"struct":   "ProxyServiceClient",
		"function": "Stat",
	})

	request := &api.StatRequest{
		SessionId: session.ID,
		Path:      path,
	}

	response, err := client.APIClient.Stat(context.Background(), request)
	if err != nil {
		logger.Error(err)
		return nil, err
	}

	if response.Error != nil && response.Error.Type != api.ErrorType_NONE {
		// has soft error
		if response.Error.Type == api.ErrorType_FILENOTFOUND {
			// file not found
			return nil, irodsfs_clienttype.NewFileNotFoundError(response.Error.Message)
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

	irodsEntry := &irodsfs.Entry{
		ID:         response.Entry.Id,
		Type:       irodsfs.EntryType(response.Entry.Type),
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
func (client *ProxyServiceClient) ExistsDir(session *ProxyServiceSession, path string) bool {
	logger := log.WithFields(log.Fields{
		"package":  "client",
		"struct":   "ProxyServiceClient",
		"function": "ExistsDir",
	})

	request := &api.ExistsDirRequest{
		SessionId: session.ID,
		Path:      path,
	}

	response, err := client.APIClient.ExistsDir(context.Background(), request)
	if err != nil {
		logger.Error(err)
		return false
	}

	return response.Exist
}

// ExistsFile checks existence of File
func (client *ProxyServiceClient) ExistsFile(session *ProxyServiceSession, path string) bool {
	logger := log.WithFields(log.Fields{
		"package":  "client",
		"struct":   "ProxyServiceClient",
		"function": "ExistsFile",
	})

	request := &api.ExistsFileRequest{
		SessionId: session.ID,
		Path:      path,
	}

	response, err := client.APIClient.ExistsFile(context.Background(), request)
	if err != nil {
		logger.Error(err)
		return false
	}

	return response.Exist
}

// ListDirACLsWithGroupUsers lists iRODS collection ACLs with group users
func (client *ProxyServiceClient) ListDirACLsWithGroupUsers(session *ProxyServiceSession, path string) ([]*irodsfs_clienttype.IRODSAccess, error) {
	logger := log.WithFields(log.Fields{
		"package":  "client",
		"struct":   "ProxyServiceClient",
		"function": "ListDirACLsWithGroupUsers",
	})

	request := &api.ListDirACLsWithGroupUsersRequest{
		SessionId: session.ID,
		Path:      path,
	}

	response, err := client.APIClient.ListDirACLsWithGroupUsers(context.Background(), request)
	if err != nil {
		logger.Error(err)
		return nil, err
	}

	irodsAccesses := []*irodsfs_clienttype.IRODSAccess{}

	for _, access := range response.Accesses {
		irodsAccess := &irodsfs_clienttype.IRODSAccess{
			Path:        access.Path,
			UserName:    access.UserName,
			UserZone:    access.UserZone,
			UserType:    irodsfs_clienttype.IRODSUserType(access.UserType),
			AccessLevel: irodsfs_clienttype.IRODSAccessLevelType(access.AccessLevel),
		}

		irodsAccesses = append(irodsAccesses, irodsAccess)
	}

	return irodsAccesses, nil
}

// ListFileACLsWithGroupUsers lists iRODS data object ACLs with group users
func (client *ProxyServiceClient) ListFileACLsWithGroupUsers(session *ProxyServiceSession, path string) ([]*irodsfs_clienttype.IRODSAccess, error) {
	logger := log.WithFields(log.Fields{
		"package":  "client",
		"struct":   "ProxyServiceClient",
		"function": "ListFileACLsWithGroupUsers",
	})

	request := &api.ListFileACLsWithGroupUsersRequest{
		SessionId: session.ID,
		Path:      path,
	}

	response, err := client.APIClient.ListFileACLsWithGroupUsers(context.Background(), request)
	if err != nil {
		logger.Error(err)
		return nil, err
	}

	irodsAccesses := []*irodsfs_clienttype.IRODSAccess{}

	for _, access := range response.Accesses {
		irodsAccess := &irodsfs_clienttype.IRODSAccess{
			Path:        access.Path,
			UserName:    access.UserName,
			UserZone:    access.UserZone,
			UserType:    irodsfs_clienttype.IRODSUserType(access.UserType),
			AccessLevel: irodsfs_clienttype.IRODSAccessLevelType(access.AccessLevel),
		}

		irodsAccesses = append(irodsAccesses, irodsAccess)
	}

	return irodsAccesses, nil
}

// RemoveFile removes iRODS data object
func (client *ProxyServiceClient) RemoveFile(session *ProxyServiceSession, path string, force bool) error {
	logger := log.WithFields(log.Fields{
		"package":  "client",
		"struct":   "ProxyServiceClient",
		"function": "RemoveFile",
	})

	request := &api.RemoveFileRequest{
		SessionId: session.ID,
		Path:      path,
		Force:     force,
	}

	_, err := client.APIClient.RemoveFile(context.Background(), request)
	if err != nil {
		logger.Error(err)
		return err
	}

	return nil
}

// RemoveDir removes iRODS collection
func (client *ProxyServiceClient) RemoveDir(session *ProxyServiceSession, path string, recurse bool, force bool) error {
	logger := log.WithFields(log.Fields{
		"package":  "client",
		"struct":   "ProxyServiceClient",
		"function": "RemoveDir",
	})

	request := &api.RemoveDirRequest{
		SessionId: session.ID,
		Path:      path,
		Recurse:   recurse,
		Force:     force,
	}

	_, err := client.APIClient.RemoveDir(context.Background(), request)
	if err != nil {
		logger.Error(err)
		return err
	}

	return nil
}

// MakeDir creates a new iRODS collection
func (client *ProxyServiceClient) MakeDir(session *ProxyServiceSession, path string, recurse bool) error {
	logger := log.WithFields(log.Fields{
		"package":  "client",
		"struct":   "ProxyServiceClient",
		"function": "MakeDir",
	})

	request := &api.MakeDirRequest{
		SessionId: session.ID,
		Path:      path,
		Recurse:   recurse,
	}

	_, err := client.APIClient.MakeDir(context.Background(), request)
	if err != nil {
		logger.Error(err)
		return err
	}

	return nil
}

// RenameDirToDir renames iRODS collection
func (client *ProxyServiceClient) RenameDirToDir(session *ProxyServiceSession, srcPath string, destPath string) error {
	logger := log.WithFields(log.Fields{
		"package":  "client",
		"struct":   "ProxyServiceClient",
		"function": "RenameDirToDir",
	})

	request := &api.RenameDirToDirRequest{
		SessionId:       session.ID,
		SourcePath:      srcPath,
		DestinationPath: destPath,
	}

	_, err := client.APIClient.RenameDirToDir(context.Background(), request)
	if err != nil {
		logger.Error(err)
		return err
	}

	return nil
}

// RenameFileToFile renames iRODS data object
func (client *ProxyServiceClient) RenameFileToFile(session *ProxyServiceSession, srcPath string, destPath string) error {
	logger := log.WithFields(log.Fields{
		"package":  "client",
		"struct":   "ProxyServiceClient",
		"function": "RenameFileToFile",
	})

	request := &api.RenameFileToFileRequest{
		SessionId:       session.ID,
		SourcePath:      srcPath,
		DestinationPath: destPath,
	}

	_, err := client.APIClient.RenameFileToFile(context.Background(), request)
	if err != nil {
		logger.Error(err)
		return err
	}

	return nil
}

// CreateFile creates a new iRODS data object
func (client *ProxyServiceClient) CreateFile(session *ProxyServiceSession, path string, resource string) (*ProxyServiceFileHandle, error) {
	logger := log.WithFields(log.Fields{
		"package":  "client",
		"struct":   "ProxyServiceClient",
		"function": "CreateFile",
	})

	request := &api.CreateFileRequest{
		SessionId: session.ID,
		Path:      path,
		Resource:  resource,
	}

	response, err := client.APIClient.CreateFile(context.Background(), request)
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

	irodsEntry := &irodsfs.Entry{
		ID:         response.Entry.Id,
		Type:       irodsfs.EntryType(response.Entry.Type),
		Name:       response.Entry.Name,
		Path:       response.Entry.Path,
		Owner:      response.Entry.Owner,
		Size:       response.Entry.Size,
		CreateTime: createTime,
		ModifyTime: modifyTime,
		CheckSum:   response.Entry.Checksum,
	}

	return &ProxyServiceFileHandle{
		SessionID:    session.ID,
		Entry:        irodsEntry,
		OpenMode:     string(irodsfs_clienttype.FileOpenModeWriteOnly),
		FileHandleID: response.FileHandleId,
	}, nil
}

// OpenFile opens iRODS data object
func (client *ProxyServiceClient) OpenFile(session *ProxyServiceSession, path string, resource string, mode string) (*ProxyServiceFileHandle, error) {
	logger := log.WithFields(log.Fields{
		"package":  "client",
		"struct":   "ProxyServiceClient",
		"function": "OpenFile",
	})

	request := &api.OpenFileRequest{
		SessionId: session.ID,
		Path:      path,
		Resource:  resource,
		Mode:      mode,
	}

	response, err := client.APIClient.OpenFile(context.Background(), request)
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

	irodsEntry := &irodsfs.Entry{
		ID:         response.Entry.Id,
		Type:       irodsfs.EntryType(response.Entry.Type),
		Name:       response.Entry.Name,
		Path:       response.Entry.Path,
		Owner:      response.Entry.Owner,
		Size:       response.Entry.Size,
		CreateTime: createTime,
		ModifyTime: modifyTime,
		CheckSum:   response.Entry.Checksum,
	}

	return &ProxyServiceFileHandle{
		SessionID:    session.ID,
		Entry:        irodsEntry,
		OpenMode:     mode,
		FileHandleID: response.FileHandleId,
	}, nil
}

// TruncateFile truncates iRODS data object
func (client *ProxyServiceClient) TruncateFile(session *ProxyServiceSession, path string, size int64) error {
	logger := log.WithFields(log.Fields{
		"package":  "client",
		"struct":   "ProxyServiceClient",
		"function": "TruncateFile",
	})

	request := &api.TruncateFileRequest{
		SessionId: session.ID,
		Path:      path,
		Size:      size,
	}

	_, err := client.APIClient.TruncateFile(context.Background(), request)
	if err != nil {
		logger.Error(err)
		return err
	}

	return nil
}

// GetOffset returns current offset
func (client *ProxyServiceClient) GetOffset(handle *ProxyServiceFileHandle) int64 {
	logger := log.WithFields(log.Fields{
		"package":  "client",
		"struct":   "ProxyServiceClient",
		"function": "GetOffset",
	})

	request := &api.GetOffsetRequest{
		SessionId:    handle.SessionID,
		FileHandleId: handle.FileHandleID,
	}

	response, err := client.APIClient.GetOffset(context.Background(), request)
	if err != nil {
		logger.Error(err)
		return -1
	}

	return response.Offset
}

// ReadAt reads iRODS data object
func (client *ProxyServiceClient) ReadAt(handle *ProxyServiceFileHandle, offset int64, length int32) ([]byte, error) {
	logger := log.WithFields(log.Fields{
		"package":  "client",
		"struct":   "ProxyServiceClient",
		"function": "ReadAt",
	})

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
			SessionId:    handle.SessionID,
			FileHandleId: handle.FileHandleID,
			Offset:       curOffset,
			Length:       curLength,
		}

		response, err := client.APIClient.ReadAt(context.Background(), request)
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
func (client *ProxyServiceClient) WriteAt(handle *ProxyServiceFileHandle, offset int64, data []byte) error {
	logger := log.WithFields(log.Fields{
		"package":  "client",
		"struct":   "ProxyServiceClient",
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
			SessionId:    handle.SessionID,
			FileHandleId: handle.FileHandleID,
			Offset:       curOffset,
			Data:         data[totalWriteLength : totalWriteLength+curLength],
		}

		_, err := client.APIClient.WriteAt(context.Background(), request)
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

// Close cloess iRODS data object handle
func (client *ProxyServiceClient) Close(handle *ProxyServiceFileHandle) error {
	logger := log.WithFields(log.Fields{
		"package":  "client",
		"struct":   "ProxyServiceClient",
		"function": "Close",
	})

	request := &api.CloseRequest{
		SessionId:    handle.SessionID,
		FileHandleId: handle.FileHandleID,
	}

	_, err := client.APIClient.Close(context.Background(), request)
	if err != nil {
		logger.Error(err)
		return err
	}

	return nil
}
