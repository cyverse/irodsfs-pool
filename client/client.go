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
	Path         string
	FileHandleID string
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
		"function": "ProxyServiceClient.Connect",
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
		"function": "ProxyServiceClient.Login",
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
		"function": "ProxyServiceClient.Logout",
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
		"function": "ProxyServiceClient.List",
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
		"function": "ProxyServiceClient.Stat",
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

// ListDirACLsWithGroupUsers lists iRODS collection ACLs with group users
func (client *ProxyServiceClient) ListDirACLsWithGroupUsers(session *ProxyServiceSession, path string) ([]*irodsfs_clienttype.IRODSAccess, error) {
	logger := log.WithFields(log.Fields{
		"package":  "client",
		"function": "ProxyServiceClient.ListDirACLsWithGroupUsers",
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
		"function": "ProxyServiceClient.ListFileACLsWithGroupUsers",
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
		"function": "ProxyServiceClient.RemoveFile",
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
		"function": "ProxyServiceClient.RemoveDir",
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
		"function": "ProxyServiceClient.MakeDir",
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
		"function": "ProxyServiceClient.RenameDirToDir",
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
		"function": "ProxyServiceClient.RenameFileToFile",
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
		"function": "ProxyServiceClient.CreateFile",
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

	return &ProxyServiceFileHandle{
		SessionID:    session.ID,
		Path:         path,
		FileHandleID: response.FileHandleId,
	}, nil
}

// OpenFile opens iRODS data object
func (client *ProxyServiceClient) OpenFile(session *ProxyServiceSession, path string, resource string, mode string) (*ProxyServiceFileHandle, error) {
	logger := log.WithFields(log.Fields{
		"package":  "client",
		"function": "ProxyServiceClient.OpenFile",
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

	return &ProxyServiceFileHandle{
		SessionID:    session.ID,
		Path:         path,
		FileHandleID: response.FileHandleId,
	}, nil
}

// TruncateFile truncates iRODS data object
func (client *ProxyServiceClient) TruncateFile(session *ProxyServiceSession, path string, size int64) error {
	logger := log.WithFields(log.Fields{
		"package":  "client",
		"function": "ProxyServiceClient.TruncateFile",
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

// Seek seeks iRODS data object
func (client *ProxyServiceClient) Seek(handle *ProxyServiceFileHandle, offset int64, whence irodsfs_clienttype.Whence) (int64, error) {
	logger := log.WithFields(log.Fields{
		"package":  "client",
		"function": "ProxyServiceClient.Seek",
	})

	request := &api.SeekRequest{
		SessionId:    handle.SessionID,
		FileHandleId: handle.FileHandleID,
		Offset:       offset,
		Whence:       int32(whence),
	}

	response, err := client.APIClient.Seek(context.Background(), request)
	if err != nil {
		logger.Error(err)
		return 0, err
	}

	return response.Offset, nil
}

// Read reads iRODS data object
func (client *ProxyServiceClient) Read(handle *ProxyServiceFileHandle, length int32) ([]byte, error) {
	logger := log.WithFields(log.Fields{
		"package":  "client",
		"function": "ProxyServiceClient.Read",
	})

	request := &api.ReadRequest{
		SessionId:    handle.SessionID,
		FileHandleId: handle.FileHandleID,
		Length:       length,
	}

	response, err := client.APIClient.Read(context.Background(), request)
	if err != nil {
		logger.Error(err)
		return nil, err
	}

	return response.Data, nil
}

// Write writes iRODS data object
func (client *ProxyServiceClient) Write(handle *ProxyServiceFileHandle, data []byte) error {
	logger := log.WithFields(log.Fields{
		"package":  "client",
		"function": "ProxyServiceClient.Write",
	})

	request := &api.WriteRequest{
		SessionId:    handle.SessionID,
		FileHandleId: handle.FileHandleID,
		Data:         data,
	}

	_, err := client.APIClient.Write(context.Background(), request)
	if err != nil {
		logger.Error(err)
		return err
	}

	return nil
}

// Close cloess iRODS data object handle
func (client *ProxyServiceClient) Close(handle *ProxyServiceFileHandle) error {
	logger := log.WithFields(log.Fields{
		"package":  "client",
		"function": "ProxyServiceClient.Close",
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
