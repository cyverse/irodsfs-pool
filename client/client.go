package client

import (
	"context"
	"fmt"
	"io"
	"time"

	irodsclient_fs "github.com/cyverse/go-irodsclient/fs"
	irodsclient_types "github.com/cyverse/go-irodsclient/irods/types"
	irodsfs_common_irods "github.com/cyverse/irodsfs-common/irods"
	irodsfs_common_utils "github.com/cyverse/irodsfs-common/utils"
	"github.com/cyverse/irodsfs-pool/service/api"
	"github.com/rs/xid"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	fileRWLengthMax    int = 1024 * 1024     // 1MB
	messageRWLengthMax int = 8 * 1024 * 1024 // 8MB
)

// PoolServiceClient is a client of pool service
type PoolServiceClient struct {
	id               string
	address          string // host:port
	operationTimeout time.Duration
	grpcConnection   *grpc.ClientConn
	apiClient        api.PoolAPIClient
}

// PoolServiceSession is a service session
// implements irodsfs-common/irods/interface.go
type PoolServiceSession struct {
	id                string
	poolServiceClient *PoolServiceClient
	account           *irodsclient_types.IRODSAccount
	applicationName   string
}

// NewPoolServiceClient creates a new pool service client
func NewPoolServiceClient(address string, operationTimeout time.Duration, clientID string) *PoolServiceClient {
	if len(clientID) == 0 {
		clientID = xid.New().String()
	}

	return &PoolServiceClient{
		id:               clientID,
		address:          address,
		operationTimeout: operationTimeout,
		grpcConnection:   nil,
	}
}

// Connect connects to pool service
func (client *PoolServiceClient) Connect() error {
	logger := log.WithFields(log.Fields{
		"package":  "client",
		"struct":   "PoolServiceClient",
		"function": "Connect",
	})

	defer irodsfs_common_utils.StackTraceFromPanic(logger)

	conn, err := grpc.Dial(client.address, grpc.WithInsecure())
	if err != nil {
		logger.Error(err)
		return err
	}

	client.grpcConnection = conn
	client.apiClient = api.NewPoolAPIClient(conn)
	return nil
}

// Disconnect disconnects connection from pool service
func (client *PoolServiceClient) Disconnect() {
	if client.apiClient != nil {
		client.apiClient = nil
	}

	if client.grpcConnection != nil {
		client.grpcConnection.Close()
		client.grpcConnection = nil
	}
}

func (client *PoolServiceClient) getContextWithDeadline() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), client.operationTimeout)
}

func getLargeReadOption() grpc.CallOption {
	return grpc.MaxCallRecvMsgSize(messageRWLengthMax)
}

func getLargeWriteOption() grpc.CallOption {
	return grpc.MaxCallSendMsgSize(messageRWLengthMax)
}
func statusToError(err error) error {
	if err == nil {
		return nil
	}

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

// NewSession creates a new session for iRODS service using account info
func (client *PoolServiceClient) NewSession(account *irodsclient_types.IRODSAccount, applicationName string) (irodsfs_common_irods.IRODSFSClient, error) {
	logger := log.WithFields(log.Fields{
		"package":  "client",
		"struct":   "PoolServiceClient",
		"function": "NewSession",
	})

	defer irodsfs_common_utils.StackTraceFromPanic(logger)

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
			DefaultResource:         account.DefaultResource,
			PamTtl:                  int32(account.PamTTL),
		},
		ApplicationName: applicationName,
		ClientId:        client.id,
	}

	ctx, cancel := client.getContextWithDeadline()
	defer cancel()

	response, err := client.apiClient.Login(ctx, request)
	if err != nil {
		logger.Error(err)
		return nil, statusToError(err)
	}

	return &PoolServiceSession{
		poolServiceClient: client,
		id:                response.SessionId,
		account:           account,
		applicationName:   applicationName,
	}, nil
}

// Release logouts from iRODS service session
func (session *PoolServiceSession) Release() {
	logger := log.WithFields(log.Fields{
		"package":  "client",
		"struct":   "PoolServiceSession",
		"function": "Release",
	})

	defer irodsfs_common_utils.StackTraceFromPanic(logger)

	request := &api.LogoutRequest{
		SessionId: session.id,
	}

	ctx, cancel := session.poolServiceClient.getContextWithDeadline()
	defer cancel()

	_, err := session.poolServiceClient.apiClient.Logout(ctx, request)
	if err != nil {
		logger.Error(err)
		return
	}
}

func (session *PoolServiceSession) GetAccount() *irodsclient_types.IRODSAccount {
	return session.account
}

func (session *PoolServiceSession) GetApplicationName() string {
	return session.applicationName
}

func (session *PoolServiceSession) GetConnections() int {
	// return just 1, proxy connection
	return 1
}

func (session *PoolServiceSession) GetTransferMetrics() irodsclient_types.TransferMetrics {
	// return empty
	return irodsclient_types.TransferMetrics{}
}

// List lists iRODS collection entries
func (session *PoolServiceSession) List(path string) ([]*irodsclient_fs.Entry, error) {
	logger := log.WithFields(log.Fields{
		"package":  "client",
		"struct":   "PoolServiceSession",
		"function": "List",
	})

	defer irodsfs_common_utils.StackTraceFromPanic(logger)

	request := &api.ListRequest{
		SessionId: session.id,
		Path:      path,
	}

	ctx, cancel := session.poolServiceClient.getContextWithDeadline()
	defer cancel()

	response, err := session.poolServiceClient.apiClient.List(ctx, request, getLargeReadOption())
	if err != nil {
		logger.Error(err)
		return nil, statusToError(err)
	}

	irodsEntries := []*irodsclient_fs.Entry{}

	for _, entry := range response.Entries {
		createTime, err := irodsfs_common_utils.ParseTime(entry.CreateTime)
		if err != nil {
			logger.Error(err)
			return nil, err
		}

		modifyTime, err := irodsfs_common_utils.ParseTime(entry.ModifyTime)
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
func (session *PoolServiceSession) Stat(path string) (*irodsclient_fs.Entry, error) {
	logger := log.WithFields(log.Fields{
		"package":  "client",
		"struct":   "PoolServiceSession",
		"function": "Stat",
	})

	defer irodsfs_common_utils.StackTraceFromPanic(logger)

	request := &api.StatRequest{
		SessionId: session.id,
		Path:      path,
	}

	ctx, cancel := session.poolServiceClient.getContextWithDeadline()
	defer cancel()

	response, err := session.poolServiceClient.apiClient.Stat(ctx, request)
	if err != nil {
		logger.Error(err)
		return nil, statusToError(err)
	}

	createTime, err := irodsfs_common_utils.ParseTime(response.Entry.CreateTime)
	if err != nil {
		logger.Error(err)
		return nil, err
	}

	modifyTime, err := irodsfs_common_utils.ParseTime(response.Entry.ModifyTime)
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
func (session *PoolServiceSession) ExistsDir(path string) bool {
	logger := log.WithFields(log.Fields{
		"package":  "client",
		"struct":   "PoolServiceSession",
		"function": "ExistsDir",
	})

	defer irodsfs_common_utils.StackTraceFromPanic(logger)

	request := &api.ExistsDirRequest{
		SessionId: session.id,
		Path:      path,
	}

	ctx, cancel := session.poolServiceClient.getContextWithDeadline()
	defer cancel()

	response, err := session.poolServiceClient.apiClient.ExistsDir(ctx, request)
	if err != nil {
		logger.Error(err)
		return false
	}

	return response.Exist
}

// ExistsFile checks existence of File
func (session *PoolServiceSession) ExistsFile(path string) bool {
	logger := log.WithFields(log.Fields{
		"package":  "client",
		"struct":   "PoolServiceSession",
		"function": "ExistsFile",
	})

	defer irodsfs_common_utils.StackTraceFromPanic(logger)

	request := &api.ExistsFileRequest{
		SessionId: session.id,
		Path:      path,
	}

	ctx, cancel := session.poolServiceClient.getContextWithDeadline()
	defer cancel()

	response, err := session.poolServiceClient.apiClient.ExistsFile(ctx, request)
	if err != nil {
		logger.Error(err)
		return false
	}

	return response.Exist
}

// ListUserGroups lists iRODS Groups that a user belongs to
func (session *PoolServiceSession) ListUserGroups(user string) ([]*irodsclient_types.IRODSUser, error) {
	logger := log.WithFields(log.Fields{
		"package":  "client",
		"struct":   "PoolServiceSession",
		"function": "ListUserGroups",
	})

	defer irodsfs_common_utils.StackTraceFromPanic(logger)

	request := &api.ListUserGroupsRequest{
		SessionId: session.id,
		UserName:  user,
	}

	ctx, cancel := session.poolServiceClient.getContextWithDeadline()
	defer cancel()

	response, err := session.poolServiceClient.apiClient.ListUserGroups(ctx, request, getLargeReadOption())
	if err != nil {
		logger.Error(err)
		return nil, statusToError(err)
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
func (session *PoolServiceSession) ListDirACLs(path string) ([]*irodsclient_types.IRODSAccess, error) {
	logger := log.WithFields(log.Fields{
		"package":  "client",
		"struct":   "PoolServiceSession",
		"function": "ListDirACLs",
	})

	defer irodsfs_common_utils.StackTraceFromPanic(logger)

	request := &api.ListDirACLsRequest{
		SessionId: session.id,
		Path:      path,
	}

	ctx, cancel := session.poolServiceClient.getContextWithDeadline()
	defer cancel()

	response, err := session.poolServiceClient.apiClient.ListDirACLs(ctx, request, getLargeReadOption())
	if err != nil {
		logger.Error(err)
		return nil, statusToError(err)
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
func (session *PoolServiceSession) ListFileACLs(path string) ([]*irodsclient_types.IRODSAccess, error) {
	logger := log.WithFields(log.Fields{
		"package":  "client",
		"struct":   "PoolServiceSession",
		"function": "ListFileACLs",
	})

	defer irodsfs_common_utils.StackTraceFromPanic(logger)

	request := &api.ListFileACLsRequest{
		SessionId: session.id,
		Path:      path,
	}

	ctx, cancel := session.poolServiceClient.getContextWithDeadline()
	defer cancel()

	response, err := session.poolServiceClient.apiClient.ListFileACLs(ctx, request, getLargeReadOption())
	if err != nil {
		logger.Error(err)
		return nil, statusToError(err)
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
func (session *PoolServiceSession) RemoveFile(path string, force bool) error {
	logger := log.WithFields(log.Fields{
		"package":  "client",
		"struct":   "PoolServiceSession",
		"function": "RemoveFile",
	})

	defer irodsfs_common_utils.StackTraceFromPanic(logger)

	request := &api.RemoveFileRequest{
		SessionId: session.id,
		Path:      path,
		Force:     force,
	}

	ctx, cancel := session.poolServiceClient.getContextWithDeadline()
	defer cancel()

	_, err := session.poolServiceClient.apiClient.RemoveFile(ctx, request)
	if err != nil {
		logger.Error(err)
		return statusToError(err)
	}

	return nil
}

// RemoveDir removes iRODS collection
func (session *PoolServiceSession) RemoveDir(path string, recurse bool, force bool) error {
	logger := log.WithFields(log.Fields{
		"package":  "client",
		"struct":   "PoolServiceSession",
		"function": "RemoveDir",
	})

	defer irodsfs_common_utils.StackTraceFromPanic(logger)

	request := &api.RemoveDirRequest{
		SessionId: session.id,
		Path:      path,
		Recurse:   recurse,
		Force:     force,
	}

	ctx, cancel := session.poolServiceClient.getContextWithDeadline()
	defer cancel()

	_, err := session.poolServiceClient.apiClient.RemoveDir(ctx, request)
	if err != nil {
		logger.Error(err)
		return statusToError(err)
	}

	return nil
}

// MakeDir creates a new iRODS collection
func (session *PoolServiceSession) MakeDir(path string, recurse bool) error {
	logger := log.WithFields(log.Fields{
		"package":  "client",
		"struct":   "PoolServiceSession",
		"function": "MakeDir",
	})

	defer irodsfs_common_utils.StackTraceFromPanic(logger)

	request := &api.MakeDirRequest{
		SessionId: session.id,
		Path:      path,
		Recurse:   recurse,
	}

	ctx, cancel := session.poolServiceClient.getContextWithDeadline()
	defer cancel()

	_, err := session.poolServiceClient.apiClient.MakeDir(ctx, request)
	if err != nil {
		logger.Error(err)
		return statusToError(err)
	}

	return nil
}

// RenameDirToDir renames iRODS collection
func (session *PoolServiceSession) RenameDirToDir(srcPath string, destPath string) error {
	logger := log.WithFields(log.Fields{
		"package":  "client",
		"struct":   "PoolServiceSession",
		"function": "RenameDirToDir",
	})

	defer irodsfs_common_utils.StackTraceFromPanic(logger)

	request := &api.RenameDirToDirRequest{
		SessionId:       session.id,
		SourcePath:      srcPath,
		DestinationPath: destPath,
	}

	ctx, cancel := session.poolServiceClient.getContextWithDeadline()
	defer cancel()

	_, err := session.poolServiceClient.apiClient.RenameDirToDir(ctx, request)
	if err != nil {
		logger.Error(err)
		return statusToError(err)
	}

	return nil
}

// RenameFileToFile renames iRODS data object
func (session *PoolServiceSession) RenameFileToFile(srcPath string, destPath string) error {
	logger := log.WithFields(log.Fields{
		"package":  "client",
		"struct":   "PoolServiceSession",
		"function": "RenameFileToFile",
	})

	defer irodsfs_common_utils.StackTraceFromPanic(logger)

	request := &api.RenameFileToFileRequest{
		SessionId:       session.id,
		SourcePath:      srcPath,
		DestinationPath: destPath,
	}

	ctx, cancel := session.poolServiceClient.getContextWithDeadline()
	defer cancel()

	_, err := session.poolServiceClient.apiClient.RenameFileToFile(ctx, request)
	if err != nil {
		logger.Error(err)
		return statusToError(err)
	}

	return nil
}

// CreateFile creates a new iRODS data object
func (session *PoolServiceSession) CreateFile(path string, resource string, mode string) (irodsfs_common_irods.IRODSFSFileHandle, error) {
	logger := log.WithFields(log.Fields{
		"package":  "client",
		"struct":   "PoolServiceSession",
		"function": "CreateFile",
	})

	defer irodsfs_common_utils.StackTraceFromPanic(logger)

	request := &api.CreateFileRequest{
		SessionId: session.id,
		Path:      path,
		Resource:  resource,
		Mode:      mode,
	}

	ctx, cancel := session.poolServiceClient.getContextWithDeadline()
	defer cancel()

	response, err := session.poolServiceClient.apiClient.CreateFile(ctx, request)
	if err != nil {
		logger.Error(err)
		return nil, statusToError(err)
	}

	createTime, err := irodsfs_common_utils.ParseTime(response.Entry.CreateTime)
	if err != nil {
		logger.Error(err)
		return nil, err
	}

	modifyTime, err := irodsfs_common_utils.ParseTime(response.Entry.ModifyTime)
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
		id:                 response.FileHandleId,
		poolServiceClient:  session.poolServiceClient,
		poolServiceSession: session,
		entry:              irodsEntry,
		openMode:           irodsclient_types.FileOpenMode(mode),
	}, nil
}

// OpenFile opens iRODS data object
func (session *PoolServiceSession) OpenFile(path string, resource string, mode string) (irodsfs_common_irods.IRODSFSFileHandle, error) {
	logger := log.WithFields(log.Fields{
		"package":  "client",
		"struct":   "PoolServiceSession",
		"function": "OpenFile",
	})

	defer irodsfs_common_utils.StackTraceFromPanic(logger)

	request := &api.OpenFileRequest{
		SessionId: session.id,
		Path:      path,
		Resource:  resource,
		Mode:      mode,
	}

	ctx, cancel := session.poolServiceClient.getContextWithDeadline()
	defer cancel()

	response, err := session.poolServiceClient.apiClient.OpenFile(ctx, request)
	if err != nil {
		logger.Error(err)
		return nil, statusToError(err)
	}

	createTime, err := irodsfs_common_utils.ParseTime(response.Entry.CreateTime)
	if err != nil {
		logger.Error(err)
		return nil, err
	}

	modifyTime, err := irodsfs_common_utils.ParseTime(response.Entry.ModifyTime)
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
		id:                 response.FileHandleId,
		poolServiceClient:  session.poolServiceClient,
		poolServiceSession: session,
		entry:              irodsEntry,
		openMode:           irodsclient_types.FileOpenMode(mode),
	}, nil
}

// TruncateFile truncates iRODS data object
func (session *PoolServiceSession) TruncateFile(path string, size int64) error {
	logger := log.WithFields(log.Fields{
		"package":  "client",
		"struct":   "PoolServiceSession",
		"function": "TruncateFile",
	})

	defer irodsfs_common_utils.StackTraceFromPanic(logger)

	request := &api.TruncateFileRequest{
		SessionId: session.id,
		Path:      path,
		Size:      size,
	}

	ctx, cancel := session.poolServiceClient.getContextWithDeadline()
	defer cancel()

	_, err := session.poolServiceClient.apiClient.TruncateFile(ctx, request)
	if err != nil {
		logger.Error(err)
		return statusToError(err)
	}

	return nil
}

// PoolServiceFileHandle implements IRODSFSFileHandle
type PoolServiceFileHandle struct {
	id                 string
	poolServiceClient  *PoolServiceClient
	poolServiceSession *PoolServiceSession
	entry              *irodsclient_fs.Entry
	openMode           irodsclient_types.FileOpenMode

	availableOffset int64
	availableLen    int64
}

func (handle *PoolServiceFileHandle) GetID() string {
	return handle.id
}

func (handle *PoolServiceFileHandle) GetEntry() *irodsclient_fs.Entry {
	return handle.entry
}

func (handle *PoolServiceFileHandle) GetOpenMode() irodsclient_types.FileOpenMode {
	return handle.openMode
}

// GetOffset returns current offset
func (handle *PoolServiceFileHandle) GetOffset() int64 {
	logger := log.WithFields(log.Fields{
		"package":  "client",
		"struct":   "PoolServiceFileHandle",
		"function": "GetOffset",
	})

	defer irodsfs_common_utils.StackTraceFromPanic(logger)

	request := &api.GetOffsetRequest{
		SessionId:    handle.poolServiceSession.id,
		FileHandleId: handle.id,
	}

	ctx, cancel := handle.poolServiceClient.getContextWithDeadline()
	defer cancel()

	response, err := handle.poolServiceClient.apiClient.GetOffset(ctx, request)
	if err != nil {
		logger.Error(err)
		return -1
	}

	return response.Offset
}

func (handle *PoolServiceFileHandle) IsReadMode() bool {
	return handle.openMode.IsRead()
}

func (handle *PoolServiceFileHandle) IsWriteMode() bool {
	return handle.openMode.IsWrite()
}

// ReadAt reads iRODS data object
func (handle *PoolServiceFileHandle) ReadAt(buffer []byte, offset int64) (int, error) {
	logger := log.WithFields(log.Fields{
		"package":  "client",
		"struct":   "PoolServiceFileHandle",
		"function": "ReadAt",
	})

	defer irodsfs_common_utils.StackTraceFromPanic(logger)

	remainLength := len(buffer)
	curOffset := offset
	totalReadLength := 0

	for remainLength > 0 {
		curLength := remainLength
		if remainLength > fileRWLengthMax {
			curLength = fileRWLengthMax
		}

		request := &api.ReadAtRequest{
			SessionId:    handle.poolServiceSession.id,
			FileHandleId: handle.id,
			Offset:       curOffset,
			Length:       int32(curLength),
		}

		ctx, cancel := handle.poolServiceClient.getContextWithDeadline()
		defer cancel()

		response, err := handle.poolServiceClient.apiClient.ReadAt(ctx, request, getLargeReadOption())
		if err != nil {
			logger.Error(err)
			return 0, statusToError(err)
		}

		if len(response.Data) > 0 {
			copyLen := copy(buffer[totalReadLength:], response.Data)

			remainLength -= copyLen
			curOffset += int64(copyLen)
			totalReadLength += copyLen
		}

		if response.Available > 0 {
			handle.availableOffset = curOffset
			handle.availableLen = response.Available
		} else {
			handle.availableOffset = -1
			handle.availableLen = -1
		}

		if len(response.Data) < curLength {
			// EOF
			return totalReadLength, io.EOF
		}
	}

	return totalReadLength, nil
}

// GetAvailable returns available len for read
func (handle *PoolServiceFileHandle) GetAvailable(offset int64) int64 {
	if handle.availableOffset == offset {
		return handle.availableLen
	}

	return -1
}

// WriteAt writes iRODS data object
func (handle *PoolServiceFileHandle) WriteAt(data []byte, offset int64) (int, error) {
	logger := log.WithFields(log.Fields{
		"package":  "client",
		"struct":   "PoolServiceFileHandle",
		"function": "WriteAt",
	})

	defer irodsfs_common_utils.StackTraceFromPanic(logger)

	remainLength := len(data)
	curOffset := offset
	totalWriteLength := 0

	for remainLength > 0 {
		curLength := remainLength
		if remainLength > fileRWLengthMax {
			curLength = fileRWLengthMax
		}

		request := &api.WriteAtRequest{
			SessionId:    handle.poolServiceSession.id,
			FileHandleId: handle.id,
			Offset:       curOffset,
			Data:         data[totalWriteLength : totalWriteLength+curLength],
		}

		ctx, cancel := handle.poolServiceClient.getContextWithDeadline()
		defer cancel()

		_, err := handle.poolServiceClient.apiClient.WriteAt(ctx, request, getLargeWriteOption())
		svcErr := statusToError(err)
		if err != nil {
			logger.Error(err)
			return 0, svcErr
		}

		remainLength -= curLength
		curOffset += int64(curLength)
		totalWriteLength += int(curLength)
	}

	return totalWriteLength, nil
}

// Truncate truncates iRODS data object
func (handle *PoolServiceFileHandle) Truncate(size int64) error {
	logger := log.WithFields(log.Fields{
		"package":  "client",
		"struct":   "PoolServiceFileHandle",
		"function": "Truncate",
	})

	defer irodsfs_common_utils.StackTraceFromPanic(logger)

	request := &api.TruncateRequest{
		SessionId:    handle.poolServiceSession.id,
		FileHandleId: handle.id,
		Size:         size,
	}

	ctx, cancel := handle.poolServiceClient.getContextWithDeadline()
	defer cancel()

	_, err := handle.poolServiceClient.apiClient.Truncate(ctx, request)
	if err != nil {
		logger.Error(err)
		return statusToError(err)
	}

	return nil
}

// Flush flushes iRODS data object handle
func (handle *PoolServiceFileHandle) Flush() error {
	logger := log.WithFields(log.Fields{
		"package":  "client",
		"struct":   "PoolServiceFileHandle",
		"function": "Flush",
	})

	defer irodsfs_common_utils.StackTraceFromPanic(logger)

	request := &api.FlushRequest{
		SessionId:    handle.poolServiceSession.id,
		FileHandleId: handle.id,
	}

	ctx, cancel := handle.poolServiceClient.getContextWithDeadline()
	defer cancel()

	_, err := handle.poolServiceClient.apiClient.Flush(ctx, request)
	if err != nil {
		logger.Error(err)
		return statusToError(err)
	}

	return nil
}

// Close closes iRODS data object handle
func (handle *PoolServiceFileHandle) Close() error {
	logger := log.WithFields(log.Fields{
		"package":  "client",
		"struct":   "PoolServiceFileHandle",
		"function": "Close",
	})

	defer irodsfs_common_utils.StackTraceFromPanic(logger)

	request := &api.CloseRequest{
		SessionId:    handle.poolServiceSession.id,
		FileHandleId: handle.id,
	}

	ctx, cancel := handle.poolServiceClient.getContextWithDeadline()
	defer cancel()

	_, err := handle.poolServiceClient.apiClient.Close(ctx, request)
	if err != nil {
		logger.Error(err)
		return statusToError(err)
	}

	return nil
}
