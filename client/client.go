package client

import (
	"context"
	"io"
	"sync"
	"time"

	irodsclient_fs "github.com/cyverse/go-irodsclient/fs"
	irodsclient_metrics "github.com/cyverse/go-irodsclient/irods/metrics"
	irodsclient_types "github.com/cyverse/go-irodsclient/irods/types"
	irodsfs_common_irods "github.com/cyverse/irodsfs-common/irods"
	irodsfs_common_utils "github.com/cyverse/irodsfs-common/utils"
	"github.com/cyverse/irodsfs-pool/service/api"
	"github.com/rs/xid"
	log "github.com/sirupsen/logrus"
	"golang.org/x/xerrors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	fileRWLengthMax    int = 1024 * 1024     // 1MB
	messageRWLengthMax int = 8 * 1024 * 1024 // 8MB

	localMetadataCacheTiemout time.Duration = 1 * time.Minute
)

// PoolServiceClient is a client of pool service
type PoolServiceClient struct {
	id               string
	address          string // host:port
	operationTimeout time.Duration
	grpcConnection   *grpc.ClientConn
	apiClient        api.PoolAPIClient
	fsCache          *MetadataCache
}

// PoolServiceSession is a service session
// implements irodsfs-common/irods/interface.go
type PoolServiceSession struct {
	id                string
	poolServiceClient *PoolServiceClient
	account           *irodsclient_types.IRODSAccount
	applicationName   string

	cacheEventHandlers map[string]irodsclient_fs.FilesystemCacheEventHandler
	mutex              sync.RWMutex // mutex to access PoolServiceSession
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
		fsCache:          NewMetadataCache(localMetadataCacheTiemout, localMetadataCacheTiemout),
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
		grpcErr := xerrors.Errorf("failed to dial to %s: %w", client.address, err)
		logger.Errorf("%+v", grpcErr)
		return grpcErr
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

func isReloginRequiredError(err error) bool {
	if err == nil {
		return false
	}

	st, ok := status.FromError(err)
	if ok {
		switch st.Code() {
		case codes.Unauthenticated:
			return true
		default:
			return false
		}
	}
	return false
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
			return xerrors.Errorf(err.Error())
		default:
			return xerrors.Errorf(err.Error())
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

	ctx, cancel := client.getContextWithDeadline()
	defer cancel()

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

	response, err := client.apiClient.Login(ctx, request)
	if err != nil {
		logger.Error(err)
		return nil, statusToError(err)
	}

	// subscribe cache update event
	subscribeRequest := &api.SubscribeCacheEventsRequest{
		SessionId: response.SessionId,
	}

	cacheEventSubscriptionClient, err := client.apiClient.SubscribeCacheEvents(ctx, subscribeRequest)
	if err != nil {
		logger.Error(err)
		return nil, statusToError(err)
	}

	session := &PoolServiceSession{
		poolServiceClient: client,
		id:                response.SessionId,
		account:           account,
		applicationName:   applicationName,

		cacheEventHandlers: map[string]irodsclient_fs.FilesystemCacheEventHandler{},
		mutex:              sync.RWMutex{},
	}

	go session.cacheEventHandler(cacheEventSubscriptionClient)

	return session, nil
}

func (session *PoolServiceSession) cacheEventHandler(eventClient api.PoolAPI_SubscribeCacheEventsClient) {
	logger := log.WithFields(log.Fields{
		"package":  "client",
		"struct":   "PoolServiceSession",
		"function": "cacheEventHandler",
	})

	// keep receiving the message
	for {
		eventResp := api.CacheEventsResponse{}
		err := eventClient.RecvMsg(&eventResp)
		if err != nil {
			if err == io.EOF {
				// abort
				logger.Info("End of stream")
				return
			}

			logger.WithError(err).Error("failed to receive a message, aborting the handler")
			return
		}

		// distribute events to handlers registered
		session.mutex.RLock()
		for _, handler := range session.cacheEventHandlers {
			handler(eventResp.Path, irodsclient_fs.FilesystemCacheEventType(eventResp.EventType))
		}
		session.mutex.RUnlock()
	}
}

// Release logouts from iRODS service session
func (session *PoolServiceSession) Release() {
	logger := log.WithFields(log.Fields{
		"package":  "client",
		"struct":   "PoolServiceSession",
		"function": "Release",
	})

	defer irodsfs_common_utils.StackTraceFromPanic(logger)

	ctx, cancel := session.poolServiceClient.getContextWithDeadline()
	defer cancel()

	request := &api.LogoutRequest{
		SessionId: session.id,
	}

	session.mutex.Lock()
	session.cacheEventHandlers = map[string]irodsclient_fs.FilesystemCacheEventHandler{}
	session.mutex.Unlock()

	_, err := session.poolServiceClient.apiClient.Logout(ctx, request)
	if err != nil {
		logger.Error(err)
		return
	}
}

// Relogin re-login iRODS service session
func (session *PoolServiceSession) Relogin() error {
	logger := log.WithFields(log.Fields{
		"package":  "client",
		"struct":   "PoolServiceSession",
		"function": "Relogin",
	})

	defer irodsfs_common_utils.StackTraceFromPanic(logger)

	client := session.poolServiceClient

	newSession, err := client.NewSession(session.account, session.applicationName)
	if err != nil {
		logger.Error(err)
		return err
	}

	if newPoolServiceSession, ok := newSession.(*PoolServiceSession); ok {
		// update session ID
		session.id = newPoolServiceSession.id
		return nil
	}

	return xerrors.Errorf("cannot convert new session to PoolServiceSession type")
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

func (session *PoolServiceSession) GetMetrics() *irodsclient_metrics.IRODSMetrics {
	// return empty
	return &irodsclient_metrics.IRODSMetrics{}
}

// List lists iRODS collection entries
func (session *PoolServiceSession) List(path string) ([]*irodsclient_fs.Entry, error) {
	logger := log.WithFields(log.Fields{
		"package":  "client",
		"struct":   "PoolServiceSession",
		"function": "List",
	})

	defer irodsfs_common_utils.StackTraceFromPanic(logger)

	ctx, cancel := session.poolServiceClient.getContextWithDeadline()
	defer cancel()

	// if there's a cache
	cachedDirEntries := session.poolServiceClient.fsCache.GetDirCache(path)
	if cachedDirEntries != nil {
		return cachedDirEntries, nil
	}

	// no cache
	request := &api.ListRequest{
		SessionId: session.id,
		Path:      path,
	}

	irodsEntries := []*irodsclient_fs.Entry{}

	response, err := session.poolServiceClient.apiClient.List(ctx, request, getLargeReadOption())
	if err != nil {
		logger.Error(err)

		if isReloginRequiredError(err) {
			// relogin
			err2 := session.Relogin()
			if err2 != nil {
				logger.Error(err)
				return nil, err2
			}

			// retry
			response, err = session.poolServiceClient.apiClient.List(ctx, request, getLargeReadOption())
			if err != nil {
				logger.Error(err)
				return nil, statusToError(err)
			}
		} else {
			return nil, statusToError(err)
		}
	}

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
			DataType:   entry.DataType,
			CreateTime: createTime,
			ModifyTime: modifyTime,
			CheckSum:   entry.Checksum,
		}

		irodsEntries = append(irodsEntries, irodsEntry)
	}

	// put to cache
	session.poolServiceClient.fsCache.AddDirCache(path, irodsEntries)
	for _, irodsEntry := range irodsEntries {
		session.poolServiceClient.fsCache.AddEntryCache(irodsEntry)
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

	ctx, cancel := session.poolServiceClient.getContextWithDeadline()
	defer cancel()

	// if there's a cache
	cachedEntry := session.poolServiceClient.fsCache.GetEntryCache(path)
	if cachedEntry != nil {
		return cachedEntry, nil
	}

	// no cache
	request := &api.StatRequest{
		SessionId: session.id,
		Path:      path,
	}

	response, err := session.poolServiceClient.apiClient.Stat(ctx, request)
	if err != nil {
		logger.Error(err)

		if isReloginRequiredError(err) {
			// relogin
			err2 := session.Relogin()
			if err2 != nil {
				logger.Error(err)
				return nil, err2
			}

			// retry
			response, err = session.poolServiceClient.apiClient.Stat(ctx, request)
			if err != nil {
				logger.Error(err)
				return nil, statusToError(err)
			}
		} else {
			return nil, statusToError(err)
		}
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
		DataType:   response.Entry.DataType,
		CreateTime: createTime,
		ModifyTime: modifyTime,
		CheckSum:   response.Entry.Checksum,
	}

	// put to cache
	session.poolServiceClient.fsCache.AddEntryCache(irodsEntry)

	return irodsEntry, nil
}

// ListXattr lists iRODS metadata (xattr)
func (session *PoolServiceSession) ListXattr(path string) ([]*irodsclient_types.IRODSMeta, error) {
	logger := log.WithFields(log.Fields{
		"package":  "client",
		"struct":   "PoolServiceSession",
		"function": "ListXattr",
	})

	defer irodsfs_common_utils.StackTraceFromPanic(logger)

	ctx, cancel := session.poolServiceClient.getContextWithDeadline()
	defer cancel()

	request := &api.ListXattrRequest{
		SessionId: session.id,
		Path:      path,
	}

	irodsMetadata := []*irodsclient_types.IRODSMeta{}

	response, err := session.poolServiceClient.apiClient.ListXattr(ctx, request, getLargeReadOption())
	if err != nil {
		logger.Error(err)

		if isReloginRequiredError(err) {
			// relogin
			err2 := session.Relogin()
			if err2 != nil {
				logger.Error(err)
				return nil, err2
			}

			// retry
			response, err = session.poolServiceClient.apiClient.ListXattr(ctx, request, getLargeReadOption())
			if err != nil {
				logger.Error(err)
				return nil, statusToError(err)
			}
		} else {
			return nil, statusToError(err)
		}
	}

	for _, metadata := range response.Metadata {
		irodsMeta := &irodsclient_types.IRODSMeta{
			AVUID: metadata.Id,
			Name:  metadata.Name,
			Value: metadata.Value,
			Units: metadata.Unit,
		}

		irodsMetadata = append(irodsMetadata, irodsMeta)
	}

	return irodsMetadata, nil
}

// GetXattr returns iRODS metadata (xattr)
func (session *PoolServiceSession) GetXattr(path string, name string) (*irodsclient_types.IRODSMeta, error) {
	logger := log.WithFields(log.Fields{
		"package":  "client",
		"struct":   "PoolServiceSession",
		"function": "GetXattr",
	})

	defer irodsfs_common_utils.StackTraceFromPanic(logger)

	ctx, cancel := session.poolServiceClient.getContextWithDeadline()
	defer cancel()

	request := &api.GetXattrRequest{
		SessionId: session.id,
		Path:      path,
		Name:      name,
	}

	response, err := session.poolServiceClient.apiClient.GetXattr(ctx, request)
	if err != nil {
		logger.Error(err)

		st, ok := status.FromError(err)
		if ok {
			if st.Code() == codes.NotFound {
				// xattr not found
				return nil, nil
			}
		}

		if isReloginRequiredError(err) {
			// relogin
			err2 := session.Relogin()
			if err2 != nil {
				logger.Error(err)
				return nil, err2
			}

			// retry
			response, err = session.poolServiceClient.apiClient.GetXattr(ctx, request)
			if err != nil {
				logger.Error(err)

				st, ok := status.FromError(err)
				if ok {
					if st.Code() == codes.NotFound {
						// xattr not found
						return nil, nil
					}
				}

				return nil, statusToError(err)
			}
		} else {
			return nil, statusToError(err)
		}
	}

	irodsMeta := &irodsclient_types.IRODSMeta{
		AVUID: response.Metadata.Id,
		Name:  response.Metadata.Name,
		Value: response.Metadata.Value,
		Units: response.Metadata.Unit,
	}

	return irodsMeta, nil
}

// SetXattr sets iRODS metadata (xattr)
func (session *PoolServiceSession) SetXattr(path string, name string, value string) error {
	logger := log.WithFields(log.Fields{
		"package":  "client",
		"struct":   "PoolServiceSession",
		"function": "SetXattr",
	})

	defer irodsfs_common_utils.StackTraceFromPanic(logger)

	ctx, cancel := session.poolServiceClient.getContextWithDeadline()
	defer cancel()

	request := &api.SetXattrRequest{
		SessionId: session.id,
		Path:      path,
		Name:      name,
		Value:     value,
	}

	_, err := session.poolServiceClient.apiClient.SetXattr(ctx, request)
	if err != nil {
		logger.Error(err)

		if isReloginRequiredError(err) {
			// relogin
			err2 := session.Relogin()
			if err2 != nil {
				logger.Error(err)
				return err2
			}

			// retry
			_, err = session.poolServiceClient.apiClient.SetXattr(ctx, request)
			if err != nil {
				logger.Error(err)
				return statusToError(err)
			}
		} else {
			return statusToError(err)
		}
	}

	return nil
}

// RemoveXattr removes iRODS metadata (xattr)
func (session *PoolServiceSession) RemoveXattr(path string, name string) error {
	logger := log.WithFields(log.Fields{
		"package":  "client",
		"struct":   "PoolServiceSession",
		"function": "RemoveXattr",
	})

	defer irodsfs_common_utils.StackTraceFromPanic(logger)

	ctx, cancel := session.poolServiceClient.getContextWithDeadline()
	defer cancel()

	request := &api.RemoveXattrRequest{
		SessionId: session.id,
		Path:      path,
		Name:      name,
	}

	_, err := session.poolServiceClient.apiClient.RemoveXattr(ctx, request)
	if err != nil {
		logger.Error(err)

		if isReloginRequiredError(err) {
			// relogin
			err2 := session.Relogin()
			if err2 != nil {
				logger.Error(err)
				return err2
			}

			// retry
			_, err := session.poolServiceClient.apiClient.RemoveXattr(ctx, request)
			if err != nil {
				logger.Error(err)
				return statusToError(err)
			}
		} else {
			return statusToError(err)
		}
	}

	return nil
}

// ExistsDir checks existence of Dir
func (session *PoolServiceSession) ExistsDir(path string) bool {
	logger := log.WithFields(log.Fields{
		"package":  "client",
		"struct":   "PoolServiceSession",
		"function": "ExistsDir",
	})

	defer irodsfs_common_utils.StackTraceFromPanic(logger)

	ctx, cancel := session.poolServiceClient.getContextWithDeadline()
	defer cancel()

	// if there's a cache
	cachedEntry := session.poolServiceClient.fsCache.GetEntryCache(path)
	if cachedEntry != nil && cachedEntry.Type == irodsclient_fs.DirectoryEntry {
		return true
	}

	// no cache
	request := &api.ExistsDirRequest{
		SessionId: session.id,
		Path:      path,
	}

	response, err := session.poolServiceClient.apiClient.ExistsDir(ctx, request)
	if err != nil {
		logger.Error(err)

		if isReloginRequiredError(err) {
			// relogin
			err2 := session.Relogin()
			if err2 != nil {
				logger.Error(err)
				return false
			}

			// retry
			response, err = session.poolServiceClient.apiClient.ExistsDir(ctx, request)
			if err != nil {
				logger.Error(err)
				return false
			}
		} else {
			return false
		}
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

	ctx, cancel := session.poolServiceClient.getContextWithDeadline()
	defer cancel()

	// if there's a cache
	cachedEntry := session.poolServiceClient.fsCache.GetEntryCache(path)
	if cachedEntry != nil && cachedEntry.Type == irodsclient_fs.FileEntry {
		return true
	}

	// no cache
	request := &api.ExistsFileRequest{
		SessionId: session.id,
		Path:      path,
	}

	response, err := session.poolServiceClient.apiClient.ExistsFile(ctx, request)
	if err != nil {
		logger.Error(err)

		if isReloginRequiredError(err) {
			// relogin
			err2 := session.Relogin()
			if err2 != nil {
				logger.Error(err)
				return false
			}

			// retry
			response, err = session.poolServiceClient.apiClient.ExistsFile(ctx, request)
			if err != nil {
				logger.Error(err)
				return false
			}
		} else {
			return false
		}
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

	ctx, cancel := session.poolServiceClient.getContextWithDeadline()
	defer cancel()

	request := &api.ListUserGroupsRequest{
		SessionId: session.id,
		UserName:  user,
	}

	response, err := session.poolServiceClient.apiClient.ListUserGroups(ctx, request, getLargeReadOption())
	if err != nil {
		logger.Error(err)

		if isReloginRequiredError(err) {
			// relogin
			err2 := session.Relogin()
			if err2 != nil {
				logger.Error(err)
				return nil, err2
			}

			// retry
			response, err = session.poolServiceClient.apiClient.ListUserGroups(ctx, request, getLargeReadOption())
			if err != nil {
				logger.Error(err)
				return nil, statusToError(err)
			}
		} else {
			return nil, statusToError(err)
		}
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

	ctx, cancel := session.poolServiceClient.getContextWithDeadline()
	defer cancel()

	// if there's a cache
	cachedACLs := session.poolServiceClient.fsCache.GetACLsCache(path)
	if cachedACLs != nil {
		return cachedACLs, nil
	}

	// no cache
	request := &api.ListDirACLsRequest{
		SessionId: session.id,
		Path:      path,
	}

	irodsAccesses := []*irodsclient_types.IRODSAccess{}

	response, err := session.poolServiceClient.apiClient.ListDirACLs(ctx, request, getLargeReadOption())
	if err != nil {
		logger.Error(err)

		if isReloginRequiredError(err) {
			// relogin
			err2 := session.Relogin()
			if err2 != nil {
				logger.Error(err)
				return nil, err2
			}

			// retry
			response, err = session.poolServiceClient.apiClient.ListDirACLs(ctx, request, getLargeReadOption())
			if err != nil {
				logger.Error(err)
				return nil, statusToError(err)
			}
		} else {
			return nil, statusToError(err)
		}
	}

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

	// put to cache
	session.poolServiceClient.fsCache.AddACLsCache(path, irodsAccesses)

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

	ctx, cancel := session.poolServiceClient.getContextWithDeadline()
	defer cancel()

	// if there's a cache
	cachedACLs := session.poolServiceClient.fsCache.GetACLsCache(path)
	if cachedACLs != nil {
		return cachedACLs, nil
	}

	// no cache
	request := &api.ListFileACLsRequest{
		SessionId: session.id,
		Path:      path,
	}

	irodsAccesses := []*irodsclient_types.IRODSAccess{}

	response, err := session.poolServiceClient.apiClient.ListFileACLs(ctx, request, getLargeReadOption())
	if err != nil {
		logger.Error(err)

		if isReloginRequiredError(err) {
			// relogin
			err2 := session.Relogin()
			if err2 != nil {
				logger.Error(err)
				return nil, err2
			}

			// retry
			response, err = session.poolServiceClient.apiClient.ListFileACLs(ctx, request, getLargeReadOption())
			if err != nil {
				logger.Error(err)
				return nil, statusToError(err)
			}
		} else {
			return nil, statusToError(err)
		}
	}

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

	// put to cache
	session.poolServiceClient.fsCache.AddACLsCache(path, irodsAccesses)

	return irodsAccesses, nil
}

// ListACLsForEntries lists ACLs for entries in an iRODS collection
func (session *PoolServiceSession) ListACLsForEntries(path string) ([]*irodsclient_types.IRODSAccess, error) {
	logger := log.WithFields(log.Fields{
		"package":  "client",
		"struct":   "PoolServiceSession",
		"function": "ListACLsForEntries",
	})

	defer irodsfs_common_utils.StackTraceFromPanic(logger)

	ctx, cancel := session.poolServiceClient.getContextWithDeadline()
	defer cancel()

	// if there's a cache
	cachedACLs := session.poolServiceClient.fsCache.GetDirEntryACLsCache(path)
	if cachedACLs != nil {
		return cachedACLs, nil
	}

	// no cache
	request := &api.ListACLsForEntriesRequest{
		SessionId: session.id,
		Path:      path,
	}

	response, err := session.poolServiceClient.apiClient.ListACLsForEntries(ctx, request, getLargeReadOption())
	if err != nil {
		logger.Error(err)

		if isReloginRequiredError(err) {
			// relogin
			err2 := session.Relogin()
			if err2 != nil {
				logger.Error(err)
				return nil, err2
			}

			// retry
			response, err = session.poolServiceClient.apiClient.ListACLsForEntries(ctx, request, getLargeReadOption())
			if err != nil {
				logger.Error(err)
				return nil, statusToError(err)
			}
		} else {
			return nil, statusToError(err)
		}
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

	// put to cache
	session.poolServiceClient.fsCache.AddDirEntryACLsCache(path, irodsAccesses)
	session.poolServiceClient.fsCache.AddACLsCacheMulti(irodsAccesses)

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

	ctx, cancel := session.poolServiceClient.getContextWithDeadline()
	defer cancel()

	request := &api.RemoveFileRequest{
		SessionId: session.id,
		Path:      path,
		Force:     force,
	}

	_, err := session.poolServiceClient.apiClient.RemoveFile(ctx, request)
	if err != nil {
		logger.Error(err)

		if isReloginRequiredError(err) {
			// relogin
			err2 := session.Relogin()
			if err2 != nil {
				logger.Error(err)
				return err2
			}

			// retry
			_, err = session.poolServiceClient.apiClient.RemoveFile(ctx, request)
			if err != nil {
				logger.Error(err)
				return statusToError(err)
			}
		} else {
			return statusToError(err)
		}
	}

	// remove cache
	parentDirPath := irodsfs_common_utils.GetDirname(path)
	session.poolServiceClient.fsCache.RemoveDirCache(parentDirPath)
	session.poolServiceClient.fsCache.RemoveEntryCache(path)
	session.poolServiceClient.fsCache.RemoveACLsCache(path)

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

	ctx, cancel := session.poolServiceClient.getContextWithDeadline()
	defer cancel()

	request := &api.RemoveDirRequest{
		SessionId: session.id,
		Path:      path,
		Recurse:   recurse,
		Force:     force,
	}

	_, err := session.poolServiceClient.apiClient.RemoveDir(ctx, request)
	if err != nil {
		logger.Error(err)

		if isReloginRequiredError(err) {
			// relogin
			err2 := session.Relogin()
			if err2 != nil {
				logger.Error(err)
				return err2
			}

			// retry
			_, err := session.poolServiceClient.apiClient.RemoveDir(ctx, request)
			if err != nil {
				logger.Error(err)
				return statusToError(err)
			}
		} else {
			return statusToError(err)
		}
	}

	// remove cache
	removeTarget := []*irodsclient_fs.Entry{}
	dirCache := session.poolServiceClient.fsCache.GetDirCache(path)
	if dirCache != nil {
		removeTarget = append(removeTarget, dirCache...)
	}

	for len(removeTarget) > 0 {
		front := removeTarget[0]

		if front.Type == irodsclient_fs.DirectoryEntry {
			frontDirCache := session.poolServiceClient.fsCache.GetDirCache(front.Path)
			if frontDirCache != nil {
				removeTarget = append(removeTarget, frontDirCache...)
			}

			session.poolServiceClient.fsCache.RemoveDirCache(front.Path)
			session.poolServiceClient.fsCache.RemoveDirEntryACLsCache(front.Path)
		}

		session.poolServiceClient.fsCache.RemoveEntryCache(front.Path)
		session.poolServiceClient.fsCache.RemoveACLsCache(front.Path)

		removeTarget = removeTarget[1:]
	}

	parentDirPath := irodsfs_common_utils.GetDirname(path)
	session.poolServiceClient.fsCache.RemoveDirCache(parentDirPath)
	session.poolServiceClient.fsCache.RemoveEntryCache(path)
	session.poolServiceClient.fsCache.RemoveACLsCache(path)

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

		if isReloginRequiredError(err) {
			// relogin
			err2 := session.Relogin()
			if err2 != nil {
				logger.Error(err)
				return err2
			}

			// retry
			_, err = session.poolServiceClient.apiClient.MakeDir(ctx, request)
			if err != nil {
				logger.Error(err)
				return statusToError(err)
			}
		} else {
			return statusToError(err)
		}
	}

	// remove cache
	parentDirPath := irodsfs_common_utils.GetDirname(path)
	session.poolServiceClient.fsCache.RemoveDirCache(parentDirPath)
	session.poolServiceClient.fsCache.RemoveEntryCache(path)
	session.poolServiceClient.fsCache.RemoveACLsCache(path)

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

		if isReloginRequiredError(err) {
			// relogin
			err2 := session.Relogin()
			if err2 != nil {
				logger.Error(err)
				return err2
			}

			// retry
			_, err := session.poolServiceClient.apiClient.RenameDirToDir(ctx, request)
			if err != nil {
				logger.Error(err)
				return statusToError(err)
			}
		} else {
			return statusToError(err)
		}
	}

	// remove cache
	removeTarget := []*irodsclient_fs.Entry{}
	dirCache := session.poolServiceClient.fsCache.GetDirCache(srcPath)
	if dirCache != nil {
		removeTarget = append(removeTarget, dirCache...)
	}

	for len(removeTarget) > 0 {
		front := removeTarget[0]

		if front.Type == irodsclient_fs.DirectoryEntry {
			frontDirCache := session.poolServiceClient.fsCache.GetDirCache(front.Path)
			if frontDirCache != nil {
				removeTarget = append(removeTarget, frontDirCache...)
			}

			session.poolServiceClient.fsCache.RemoveDirCache(front.Path)
		}

		session.poolServiceClient.fsCache.RemoveEntryCache(front.Path)
		session.poolServiceClient.fsCache.RemoveACLsCache(front.Path)

		removeTarget = removeTarget[1:]
	}

	srcParentDirPath := irodsfs_common_utils.GetDirname(srcPath)
	session.poolServiceClient.fsCache.RemoveDirCache(srcParentDirPath)
	session.poolServiceClient.fsCache.RemoveDirCache(srcPath)
	session.poolServiceClient.fsCache.RemoveEntryCache(srcPath)
	session.poolServiceClient.fsCache.RemoveACLsCache(srcPath)

	destParentDirPath := irodsfs_common_utils.GetDirname(destPath)
	session.poolServiceClient.fsCache.RemoveDirCache(destParentDirPath)
	session.poolServiceClient.fsCache.RemoveDirCache(destPath)
	session.poolServiceClient.fsCache.RemoveEntryCache(destPath)
	session.poolServiceClient.fsCache.RemoveACLsCache(destPath)

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

	ctx, cancel := session.poolServiceClient.getContextWithDeadline()
	defer cancel()

	request := &api.RenameFileToFileRequest{
		SessionId:       session.id,
		SourcePath:      srcPath,
		DestinationPath: destPath,
	}

	_, err := session.poolServiceClient.apiClient.RenameFileToFile(ctx, request)
	if err != nil {
		logger.Error(err)

		if isReloginRequiredError(err) {
			// relogin
			err2 := session.Relogin()
			if err2 != nil {
				logger.Error(err)
				return err2
			}

			// retry
			_, err = session.poolServiceClient.apiClient.RenameFileToFile(ctx, request)
			if err != nil {
				logger.Error(err)
				return statusToError(err)
			}
		} else {
			return statusToError(err)
		}
	}

	// remove cache
	srcParentDirPath := irodsfs_common_utils.GetDirname(srcPath)
	session.poolServiceClient.fsCache.RemoveDirCache(srcParentDirPath)
	session.poolServiceClient.fsCache.RemoveEntryCache(srcPath)
	session.poolServiceClient.fsCache.RemoveACLsCache(srcPath)

	destParentDirPath := irodsfs_common_utils.GetDirname(destPath)
	session.poolServiceClient.fsCache.RemoveDirCache(destParentDirPath)
	session.poolServiceClient.fsCache.RemoveEntryCache(destPath)
	session.poolServiceClient.fsCache.RemoveACLsCache(destPath)

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

	ctx, cancel := session.poolServiceClient.getContextWithDeadline()
	defer cancel()

	request := &api.CreateFileRequest{
		SessionId: session.id,
		Path:      path,
		Resource:  resource,
		Mode:      mode,
	}

	response, err := session.poolServiceClient.apiClient.CreateFile(ctx, request)
	if err != nil {
		logger.Error(err)

		if isReloginRequiredError(err) {
			// relogin
			err2 := session.Relogin()
			if err2 != nil {
				logger.Error(err)
				return nil, err2
			}

			// retry
			response, err = session.poolServiceClient.apiClient.CreateFile(ctx, request)
			if err != nil {
				logger.Error(err)
				return nil, statusToError(err)
			}
		} else {
			return nil, statusToError(err)
		}
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
		DataType:   response.Entry.DataType,
		CreateTime: createTime,
		ModifyTime: modifyTime,
		CheckSum:   response.Entry.Checksum,
	}

	// remove cache
	parentDirPath := irodsfs_common_utils.GetDirname(path)
	session.poolServiceClient.fsCache.RemoveDirCache(parentDirPath)
	session.poolServiceClient.fsCache.RemoveEntryCache(path)
	session.poolServiceClient.fsCache.RemoveACLsCache(path)

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

	ctx, cancel := session.poolServiceClient.getContextWithDeadline()
	defer cancel()

	request := &api.OpenFileRequest{
		SessionId: session.id,
		Path:      path,
		Resource:  resource,
		Mode:      mode,
	}

	response, err := session.poolServiceClient.apiClient.OpenFile(ctx, request)
	if err != nil {
		logger.Error(err)

		if isReloginRequiredError(err) {
			// relogin
			err2 := session.Relogin()
			if err2 != nil {
				logger.Error(err)
				return nil, err2
			}

			// retry
			response, err = session.poolServiceClient.apiClient.OpenFile(ctx, request)
			if err != nil {
				logger.Error(err)
				return nil, statusToError(err)
			}
		} else {
			return nil, statusToError(err)
		}
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
		DataType:   response.Entry.DataType,
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

		if isReloginRequiredError(err) {
			// relogin
			err2 := session.Relogin()
			if err2 != nil {
				logger.Error(err)
				return err2
			}

			// retry
			_, err = session.poolServiceClient.apiClient.TruncateFile(ctx, request)
			if err != nil {
				logger.Error(err)
				return statusToError(err)
			}
		} else {
			return statusToError(err)
		}
	}

	parentDirPath := irodsfs_common_utils.GetDirname(path)
	session.poolServiceClient.fsCache.RemoveDirCache(parentDirPath)
	session.poolServiceClient.fsCache.RemoveEntryCache(path)

	return nil
}

// AddCacheEventHandler adds a cache event handler
func (session *PoolServiceSession) AddCacheEventHandler(handler irodsclient_fs.FilesystemCacheEventHandler) (string, error) {
	logger := log.WithFields(log.Fields{
		"package":  "client",
		"struct":   "PoolServiceSession",
		"function": "AddCacheEventHandler",
	})

	defer irodsfs_common_utils.StackTraceFromPanic(logger)

	handlerID := xid.New().String()

	session.mutex.Lock()
	defer session.mutex.Unlock()

	session.cacheEventHandlers[handlerID] = handler

	return handlerID, nil
}

// RemoveCacheEventHandler removes a cache event handler with the given handler ID
func (session *PoolServiceSession) RemoveCacheEventHandler(handlerID string) error {
	logger := log.WithFields(log.Fields{
		"package":  "client",
		"struct":   "PoolServiceSession",
		"function": "RemoveCacheEventHandler",
	})

	defer irodsfs_common_utils.StackTraceFromPanic(logger)

	session.mutex.Lock()
	defer session.mutex.Unlock()

	delete(session.cacheEventHandlers, handlerID)
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

	ctx, cancel := handle.poolServiceClient.getContextWithDeadline()
	defer cancel()

	request := &api.TruncateRequest{
		SessionId:    handle.poolServiceSession.id,
		FileHandleId: handle.id,
		Size:         size,
	}

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

	ctx, cancel := handle.poolServiceClient.getContextWithDeadline()
	defer cancel()

	request := &api.FlushRequest{
		SessionId:    handle.poolServiceSession.id,
		FileHandleId: handle.id,
	}

	_, err := handle.poolServiceClient.apiClient.Flush(ctx, request)
	if err != nil {
		logger.Error(err)
		return statusToError(err)
	}

	parentDirPath := irodsfs_common_utils.GetDirname(handle.entry.Path)
	handle.poolServiceClient.fsCache.RemoveDirCache(parentDirPath)
	handle.poolServiceClient.fsCache.RemoveEntryCache(handle.entry.Path)

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

	if handle.openMode.IsWrite() {
		parentDirPath := irodsfs_common_utils.GetDirname(handle.entry.Path)
		handle.poolServiceClient.fsCache.RemoveDirCache(parentDirPath)
		handle.poolServiceClient.fsCache.RemoveEntryCache(handle.entry.Path)
	}

	return nil
}
