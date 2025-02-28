package client

import (
	"context"
	"io"
	"net"
	"sync"
	"time"

	irodsclient_fs "github.com/cyverse/go-irodsclient/fs"
	irodsclient_metrics "github.com/cyverse/go-irodsclient/irods/metrics"
	irodsclient_types "github.com/cyverse/go-irodsclient/irods/types"
	irodsfs_common_irods "github.com/cyverse/irodsfs-common/irods"
	irodsfs_common_utils "github.com/cyverse/irodsfs-common/utils"
	"github.com/cyverse/irodsfs-pool/commons"
	"github.com/cyverse/irodsfs-pool/service/api"
	"github.com/rs/xid"
	log "github.com/sirupsen/logrus"
	"golang.org/x/xerrors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
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
	connected        bool
}

// PoolServiceSession is a service session
// implements irodsfs-common/irods/interface.go
type PoolServiceSession struct {
	id                string
	poolServiceClient *PoolServiceClient
	account           *irodsclient_types.IRODSAccount
	applicationName   string

	loggedIn      bool
	mutex         sync.RWMutex // mutex to access PoolServiceSession
	terminateChan chan bool
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
		connected:        false,
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

	if client.connected {
		return xerrors.Errorf("already connected to %q", client.address)
	}

	scheme, endpoint, err := commons.ParsePoolServiceEndpoint(client.address)
	if err != nil {
		return err
	}

	logger.Infof("scheme: %s, endpoint: %s", scheme, endpoint)

	if scheme != "unix" && scheme != "tcp" {
		logger.Errorf("unknown protocol %q", scheme)
		return xerrors.Errorf("unknown protocol %q", scheme)
	}

	logger.Infof("Connecting to %s endpoint: %q", scheme, endpoint)

	dialer := func(ctx context.Context, address string) (net.Conn, error) {
		return net.Dial(scheme, address)
	}

	conn, err := grpc.Dial(endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(dialer))
	if err != nil {
		grpcErr := xerrors.Errorf("failed to dial to %q: %w", client.address, err)
		logger.Errorf("%+v", grpcErr)
		return grpcErr
	}

	client.grpcConnection = conn
	client.apiClient = api.NewPoolAPIClient(client.grpcConnection)
	client.connected = true
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

	client.connected = false
}

// disconnected unintentionally
func (client *PoolServiceClient) disconnected() {
	client.connected = false
	client.apiClient = nil
	client.grpcConnection = nil

	// clear all cache
	client.fsCache.ClearDirCache()
	client.fsCache.ClearEntryCache()
	client.fsCache.ClearACLsCache()
	client.fsCache.ClearDirEntryACLsCache()
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

	var sslConf *api.SSLConfiguration
	if account.SSLConfiguration != nil {
		sslConf = &api.SSLConfiguration{
			CaCertificateFile:       account.SSLConfiguration.CACertificateFile,
			CaCertificatePath:       account.SSLConfiguration.CACertificatePath,
			EncryptionKeySize:       int32(account.SSLConfiguration.EncryptionKeySize),
			EncryptionAlgorithm:     account.SSLConfiguration.EncryptionAlgorithm,
			EncryptionSaltSize:      int32(account.SSLConfiguration.EncryptionSaltSize),
			EncryptionNumHashRounds: int32(account.SSLConfiguration.EncryptionNumHashRounds),
			VerifyServer:            string(account.SSLConfiguration.VerifyServer),
			DhParamsFile:            account.SSLConfiguration.DHParamsFile,
			ServerName:              account.SSLConfiguration.ServerName,
		}
	}

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
			DefaultHashScheme:       account.DefaultHashScheme,
			PamTtl:                  int32(account.PamTTL),
			PamToken:                account.PAMToken,
			SslConfiguration:        sslConf,
		},
		ApplicationName: applicationName,
		ClientId:        client.id,
	}

	response, err := client.apiClient.Login(ctx, request)
	if err != nil {
		logger.Errorf("%+v", err)
		return nil, commons.StatusToError(err)
	}

	session := &PoolServiceSession{
		poolServiceClient: client,
		id:                response.SessionId,
		account:           account,
		applicationName:   applicationName,
		loggedIn:          true,
		mutex:             sync.RWMutex{},
		terminateChan:     make(chan bool),
	}

	// run a goroutine to send keepalive
	go func() {
		tickerKeepalive := time.NewTicker(5 * time.Minute)
		defer tickerKeepalive.Stop()

		for {
			select {
			case <-session.terminateChan:
				// terminate
				return
			case <-tickerKeepalive.C:
				// send keep alive
				session.mutex.RLock()
				loggedIn := session.loggedIn
				session.mutex.RUnlock()

				if loggedIn {
					request := &api.KeepAliveRequest{
						SessionId: session.id,
					}

					_, err := session.poolServiceClient.apiClient.KeepAlive(context.Background(), request)
					if err != nil {
						session.mutex.Lock()
						session.loggedIn = false
						session.mutex.Unlock()

						logger.Errorf("%+v", err)
					}
				}
			}
		}
	}()

	return session, nil
}

// Release logouts from iRODS service session
func (session *PoolServiceSession) Release() {
	logger := log.WithFields(log.Fields{
		"package":  "client",
		"struct":   "PoolServiceSession",
		"function": "Release",
	})

	defer irodsfs_common_utils.StackTraceFromPanic(logger)

	session.terminateChan <- true

	ctx, cancel := session.poolServiceClient.getContextWithDeadline()
	defer cancel()

	request := &api.LogoutRequest{
		SessionId: session.id,
	}

	session.mutex.Lock()
	session.loggedIn = false
	session.mutex.Unlock()

	_, err := session.poolServiceClient.apiClient.Logout(ctx, request)
	if err != nil {
		logger.Errorf("%+v", err)
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

	session.mutex.Lock()
	defer session.mutex.Unlock()

	ctx, cancel := session.poolServiceClient.getContextWithDeadline()
	defer cancel()

	var sslConf *api.SSLConfiguration
	if session.account.SSLConfiguration != nil {
		sslConf = &api.SSLConfiguration{
			CaCertificateFile:       session.account.SSLConfiguration.CACertificateFile,
			CaCertificatePath:       session.account.SSLConfiguration.CACertificatePath,
			EncryptionKeySize:       int32(session.account.SSLConfiguration.EncryptionKeySize),
			EncryptionAlgorithm:     session.account.SSLConfiguration.EncryptionAlgorithm,
			EncryptionSaltSize:      int32(session.account.SSLConfiguration.EncryptionSaltSize),
			EncryptionNumHashRounds: int32(session.account.SSLConfiguration.EncryptionNumHashRounds),
			VerifyServer:            string(session.account.SSLConfiguration.VerifyServer),
			DhParamsFile:            session.account.SSLConfiguration.DHParamsFile,
			ServerName:              session.account.SSLConfiguration.ServerName,
		}
	}

	request := &api.LoginRequest{
		Account: &api.Account{
			AuthenticationScheme:    string(session.account.AuthenticationScheme),
			ClientServerNegotiation: session.account.ClientServerNegotiation,
			CsNegotiationPolicy:     string(session.account.CSNegotiationPolicy),
			Host:                    session.account.Host,
			Port:                    int32(session.account.Port),
			ClientUser:              session.account.ClientUser,
			ClientZone:              session.account.ClientZone,
			ProxyUser:               session.account.ProxyUser,
			ProxyZone:               session.account.ProxyZone,
			Password:                session.account.Password,
			Ticket:                  session.account.Ticket,
			DefaultResource:         session.account.DefaultResource,
			DefaultHashScheme:       session.account.DefaultHashScheme,
			PamTtl:                  int32(session.account.PamTTL),
			PamToken:                session.account.PAMToken,
			SslConfiguration:        sslConf,
		},
		ApplicationName: session.applicationName,
		ClientId:        session.poolServiceClient.id,
	}

	response, err := session.poolServiceClient.apiClient.Login(ctx, request)
	if err != nil {
		logger.Errorf("%+v", err)
		return commons.StatusToError(err)
	}

	// update session ID
	session.id = response.SessionId
	session.loggedIn = true
	return nil
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

func (session *PoolServiceSession) doWithRelogin(f func() (interface{}, error)) (interface{}, error) {
	session.mutex.RLock()
	loggedIn := session.loggedIn
	session.mutex.RUnlock()

	if !loggedIn {
		// keepalive detected logged out
		// relogin first
		err := session.Relogin()
		if err != nil {
			return nil, err
		}
	}

	// now let's go
	res, err := f()
	if err != nil {
		// relogin required
		if commons.IsReloginRequiredError(err) {
			session.mutex.Lock()
			session.loggedIn = false
			session.mutex.Unlock()

			// relogin
			err2 := session.Relogin()
			if err2 != nil {
				return nil, err2
			}

			// retry
			res, err = f()
			if commons.IsReloginRequiredError(err) {
				// logged out
				session.mutex.Lock()
				session.loggedIn = false
				session.mutex.Unlock()
			}

			return res, err
		}

		return res, err
	}
	return res, nil
}

// List lists iRODS collection entries
func (session *PoolServiceSession) List(path string) ([]*irodsclient_fs.Entry, error) {
	logger := log.WithFields(log.Fields{
		"package":  "client",
		"struct":   "PoolServiceSession",
		"function": "List",
	})

	defer irodsfs_common_utils.StackTraceFromPanic(logger)

	// if there's a cache
	cachedEntries := []*irodsclient_fs.Entry{}
	useCached := false

	cachedDirEntryPaths := session.poolServiceClient.fsCache.GetDirCache(path)
	if cachedDirEntryPaths != nil {
		useCached = true
		for _, cachedDirEntryPath := range cachedDirEntryPaths {
			cachedEntry := session.poolServiceClient.fsCache.GetEntryCache(cachedDirEntryPath)
			if cachedEntry != nil {
				cachedEntries = append(cachedEntries, cachedEntry)
			} else {
				useCached = false
				break
			}
		}
	}

	if useCached {
		return cachedEntries, nil
	}

	// otherwise, retrieve it and add it to cache
	irodsEntries := []*irodsclient_fs.Entry{}
	irodsEntryPaths := []string{}

	listFunc := func() (interface{}, error) {
		ctx, cancel := session.poolServiceClient.getContextWithDeadline()
		defer cancel()

		request := &api.ListRequest{
			SessionId: session.id,
			Path:      path,
		}

		return session.poolServiceClient.apiClient.List(ctx, request, getLargeReadOption())
	}

	res, err := session.doWithRelogin(listFunc)
	if err != nil {
		logger.Errorf("%+v", err)
		return nil, commons.StatusToError(err)
	}

	response, ok := res.(*api.ListResponse)
	if !ok {
		logger.Error("failed to convert interface to ListResponse")
		return nil, xerrors.Errorf("failed to convert interface to ListResponse")
	}

	for _, entry := range response.Entries {
		createTime, err := irodsfs_common_utils.ParseTime(entry.CreateTime)
		if err != nil {
			logger.Errorf("%+v", err)
			return nil, err
		}

		modifyTime, err := irodsfs_common_utils.ParseTime(entry.ModifyTime)
		if err != nil {
			logger.Errorf("%+v", err)
			return nil, err
		}

		irodsEntry := &irodsclient_fs.Entry{
			ID:                entry.Id,
			Type:              irodsclient_fs.EntryType(entry.Type),
			Name:              entry.Name,
			Path:              entry.Path,
			Owner:             entry.Owner,
			Size:              entry.Size,
			DataType:          entry.DataType,
			CreateTime:        createTime,
			ModifyTime:        modifyTime,
			CheckSumAlgorithm: irodsclient_types.ChecksumAlgorithm(entry.ChecksumAlgorithm),
			CheckSum:          entry.Checksum,
		}

		irodsEntries = append(irodsEntries, irodsEntry)
		irodsEntryPaths = append(irodsEntryPaths, irodsEntry.Path)
	}

	// put to cache
	for _, irodsEntry := range irodsEntries {
		session.poolServiceClient.fsCache.AddEntryCache(irodsEntry)
	}
	session.poolServiceClient.fsCache.AddDirCache(path, irodsEntryPaths)

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

	// if there's a cache
	cachedEntry := session.poolServiceClient.fsCache.GetEntryCache(path)
	if cachedEntry != nil {
		return cachedEntry, nil
	}

	// no cache
	statFunc := func() (interface{}, error) {
		ctx, cancel := session.poolServiceClient.getContextWithDeadline()
		defer cancel()

		request := &api.StatRequest{
			SessionId: session.id,
			Path:      path,
		}

		return session.poolServiceClient.apiClient.Stat(ctx, request)
	}

	res, err := session.doWithRelogin(statFunc)
	if err != nil {
		logger.Errorf("%+v", err)
		return nil, commons.StatusToError(err)
	}

	response, ok := res.(*api.StatResponse)
	if !ok {
		logger.Error("failed to convert interface to StatResponse")
		return nil, xerrors.Errorf("failed to convert interface to StatResponse")
	}

	createTime, err := irodsfs_common_utils.ParseTime(response.Entry.CreateTime)
	if err != nil {
		logger.Errorf("%+v", err)
		return nil, err
	}

	modifyTime, err := irodsfs_common_utils.ParseTime(response.Entry.ModifyTime)
	if err != nil {
		logger.Errorf("%+v", err)
		return nil, err
	}

	irodsEntry := &irodsclient_fs.Entry{
		ID:                response.Entry.Id,
		Type:              irodsclient_fs.EntryType(response.Entry.Type),
		Name:              response.Entry.Name,
		Path:              response.Entry.Path,
		Owner:             response.Entry.Owner,
		Size:              response.Entry.Size,
		DataType:          response.Entry.DataType,
		CreateTime:        createTime,
		ModifyTime:        modifyTime,
		CheckSumAlgorithm: irodsclient_types.ChecksumAlgorithm(response.Entry.ChecksumAlgorithm),
		CheckSum:          response.Entry.Checksum,
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

	irodsMetadata := []*irodsclient_types.IRODSMeta{}

	listXattrFunc := func() (interface{}, error) {
		ctx, cancel := session.poolServiceClient.getContextWithDeadline()
		defer cancel()

		request := &api.ListXattrRequest{
			SessionId: session.id,
			Path:      path,
		}

		return session.poolServiceClient.apiClient.ListXattr(ctx, request, getLargeReadOption())
	}

	res, err := session.doWithRelogin(listXattrFunc)
	if err != nil {
		logger.Errorf("%+v", err)
		return nil, commons.StatusToError(err)
	}

	response, ok := res.(*api.ListXattrResponse)
	if !ok {
		logger.Error("failed to convert interface to ListXattrResponse")
		return nil, xerrors.Errorf("failed to convert interface to ListXattrResponse")
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

	getXattrFunc := func() (interface{}, error) {
		ctx, cancel := session.poolServiceClient.getContextWithDeadline()
		defer cancel()

		request := &api.GetXattrRequest{
			SessionId: session.id,
			Path:      path,
			Name:      name,
		}

		return session.poolServiceClient.apiClient.GetXattr(ctx, request)
	}

	res, err := session.doWithRelogin(getXattrFunc)
	if err != nil {
		logger.Errorf("%+v", err)
		err2 := commons.StatusToError(err)
		if irodsclient_types.IsFileNotFoundError(err2) {
			// xattr not found
			return nil, nil
		}

		return nil, err
	}

	response, ok := res.(*api.GetXattrResponse)
	if !ok {
		logger.Error("failed to convert interface to GetXattrResponse")
		return nil, xerrors.Errorf("failed to convert interface to GetXattrResponse")
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

	setXattrFunc := func() (interface{}, error) {
		ctx, cancel := session.poolServiceClient.getContextWithDeadline()
		defer cancel()

		request := &api.SetXattrRequest{
			SessionId: session.id,
			Path:      path,
			Name:      name,
			Value:     value,
		}

		return session.poolServiceClient.apiClient.SetXattr(ctx, request)
	}

	_, err := session.doWithRelogin(setXattrFunc)
	if err != nil {
		logger.Errorf("%+v", err)
		return commons.StatusToError(err)
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

	removeXattrFunc := func() (interface{}, error) {
		ctx, cancel := session.poolServiceClient.getContextWithDeadline()
		defer cancel()

		request := &api.RemoveXattrRequest{
			SessionId: session.id,
			Path:      path,
			Name:      name,
		}

		return session.poolServiceClient.apiClient.RemoveXattr(ctx, request)
	}

	_, err := session.doWithRelogin(removeXattrFunc)
	if err != nil {
		logger.Errorf("%+v", err)
		return commons.StatusToError(err)
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

	// if there's a cache
	cachedEntry := session.poolServiceClient.fsCache.GetEntryCache(path)
	if cachedEntry != nil && cachedEntry.IsDir() {
		return true
	}

	// no cache
	existsDirFunc := func() (interface{}, error) {
		ctx, cancel := session.poolServiceClient.getContextWithDeadline()
		defer cancel()

		request := &api.ExistsDirRequest{
			SessionId: session.id,
			Path:      path,
		}

		return session.poolServiceClient.apiClient.ExistsDir(ctx, request)
	}

	res, err := session.doWithRelogin(existsDirFunc)
	if err != nil {
		logger.Errorf("%+v", err)
		return false
	}

	response, ok := res.(*api.ExistsDirResponse)
	if !ok {
		logger.Error("failed to convert interface to ExistsDirResponse")
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

	// if there's a cache
	cachedEntry := session.poolServiceClient.fsCache.GetEntryCache(path)
	if cachedEntry != nil && cachedEntry.Type == irodsclient_fs.FileEntry {
		return true
	}

	// no cache
	existsFileFunc := func() (interface{}, error) {
		ctx, cancel := session.poolServiceClient.getContextWithDeadline()
		defer cancel()

		request := &api.ExistsFileRequest{
			SessionId: session.id,
			Path:      path,
		}

		return session.poolServiceClient.apiClient.ExistsFile(ctx, request)
	}

	res, err := session.doWithRelogin(existsFileFunc)
	if err != nil {
		logger.Errorf("%+v", err)
		return false
	}

	response, ok := res.(*api.ExistsFileResponse)
	if !ok {
		logger.Error("failed to convert interface to ExistsFileResponse")
		return false
	}

	return response.Exist
}

// ListUserGroups lists iRODS Groups that a user belongs to
func (session *PoolServiceSession) ListUserGroups(zone string, user string) ([]*irodsclient_types.IRODSUser, error) {
	logger := log.WithFields(log.Fields{
		"package":  "client",
		"struct":   "PoolServiceSession",
		"function": "ListUserGroups",
	})

	defer irodsfs_common_utils.StackTraceFromPanic(logger)

	listUserGroupsFunc := func() (interface{}, error) {
		ctx, cancel := session.poolServiceClient.getContextWithDeadline()
		defer cancel()

		request := &api.ListUserGroupsRequest{
			SessionId: session.id,
			Zone:      zone,
			UserName:  user,
		}

		return session.poolServiceClient.apiClient.ListUserGroups(ctx, request, getLargeReadOption())
	}

	res, err := session.doWithRelogin(listUserGroupsFunc)
	if err != nil {
		logger.Errorf("%+v", err)
		return nil, commons.StatusToError(err)
	}

	response, ok := res.(*api.ListUserGroupsResponse)
	if !ok {
		logger.Error("failed to convert interface to ListUserGroupsResponse")
		return nil, xerrors.Errorf("failed to convert interface to ListUserGroupsResponse")
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

	// if there's a cache
	cachedACLs := session.poolServiceClient.fsCache.GetACLsCache(path)
	if cachedACLs != nil {
		return cachedACLs, nil
	}

	// no cache
	irodsAccesses := []*irodsclient_types.IRODSAccess{}

	listDirACLsFunc := func() (interface{}, error) {
		ctx, cancel := session.poolServiceClient.getContextWithDeadline()
		defer cancel()

		request := &api.ListDirACLsRequest{
			SessionId: session.id,
			Path:      path,
		}

		return session.poolServiceClient.apiClient.ListDirACLs(ctx, request, getLargeReadOption())
	}

	res, err := session.doWithRelogin(listDirACLsFunc)
	if err != nil {
		logger.Errorf("%+v", err)
		return nil, commons.StatusToError(err)
	}

	response, ok := res.(*api.ListDirACLsResponse)
	if !ok {
		logger.Error("failed to convert interface to ListDirACLsResponse")
		return nil, xerrors.Errorf("failed to convert interface to ListDirACLsResponse")
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

	// if there's a cache
	cachedACLs := session.poolServiceClient.fsCache.GetACLsCache(path)
	if cachedACLs != nil {
		return cachedACLs, nil
	}

	// no cache
	irodsAccesses := []*irodsclient_types.IRODSAccess{}

	listFileACLsFunc := func() (interface{}, error) {
		ctx, cancel := session.poolServiceClient.getContextWithDeadline()
		defer cancel()

		request := &api.ListFileACLsRequest{
			SessionId: session.id,
			Path:      path,
		}

		return session.poolServiceClient.apiClient.ListFileACLs(ctx, request, getLargeReadOption())
	}

	res, err := session.doWithRelogin(listFileACLsFunc)
	if err != nil {
		logger.Errorf("%+v", err)
		return nil, commons.StatusToError(err)
	}

	response, ok := res.(*api.ListFileACLsResponse)
	if !ok {
		logger.Error("failed to convert interface to ListFileACLsResponse")
		return nil, xerrors.Errorf("failed to convert interface to ListFileACLsResponse")
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

	// if there's a cache
	cachedACLs := session.poolServiceClient.fsCache.GetDirEntryACLsCache(path)
	if cachedACLs != nil {
		return cachedACLs, nil
	}

	// no cache
	listACLsForEntriesFunc := func() (interface{}, error) {
		ctx, cancel := session.poolServiceClient.getContextWithDeadline()
		defer cancel()

		request := &api.ListACLsForEntriesRequest{
			SessionId: session.id,
			Path:      path,
		}

		return session.poolServiceClient.apiClient.ListACLsForEntries(ctx, request, getLargeReadOption())
	}

	res, err := session.doWithRelogin(listACLsForEntriesFunc)
	if err != nil {
		logger.Errorf("%+v", err)
		return nil, commons.StatusToError(err)
	}

	response, ok := res.(*api.ListACLsForEntriesResponse)
	if !ok {
		logger.Error("failed to convert interface to ListACLsForEntriesResponse")
		return nil, xerrors.Errorf("failed to convert interface to ListACLsForEntriesResponse")
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

	removeFileFunc := func() (interface{}, error) {
		ctx, cancel := session.poolServiceClient.getContextWithDeadline()
		defer cancel()

		request := &api.RemoveFileRequest{
			SessionId: session.id,
			Path:      path,
			Force:     force,
		}

		return session.poolServiceClient.apiClient.RemoveFile(ctx, request)
	}

	_, err := session.doWithRelogin(removeFileFunc)
	if err != nil {
		logger.Errorf("%+v", err)
		return commons.StatusToError(err)
	}

	// remove cache
	session.InvalidateCacheForRemoveFile(path)

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

	removeDirFunc := func() (interface{}, error) {
		ctx, cancel := session.poolServiceClient.getContextWithDeadline()
		defer cancel()

		request := &api.RemoveDirRequest{
			SessionId: session.id,
			Path:      path,
			Recurse:   recurse,
			Force:     force,
		}

		return session.poolServiceClient.apiClient.RemoveDir(ctx, request)
	}

	_, err := session.doWithRelogin(removeDirFunc)
	if err != nil {
		logger.Errorf("%+v", err)
		return commons.StatusToError(err)
	}

	// remove cache
	session.InvalidateCacheForRemoveDir(path, recurse)

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

	makeDirFunc := func() (interface{}, error) {
		ctx, cancel := session.poolServiceClient.getContextWithDeadline()
		defer cancel()

		request := &api.MakeDirRequest{
			SessionId: session.id,
			Path:      path,
			Recurse:   recurse,
		}

		return session.poolServiceClient.apiClient.MakeDir(ctx, request)
	}

	_, err := session.doWithRelogin(makeDirFunc)
	if err != nil {
		logger.Errorf("%+v", err)
		return commons.StatusToError(err)
	}

	// remove cache
	session.InvalidateCacheForMakeDir(path)

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

	renameDirToDirFunc := func() (interface{}, error) {
		ctx, cancel := session.poolServiceClient.getContextWithDeadline()
		defer cancel()

		request := &api.RenameDirToDirRequest{
			SessionId:       session.id,
			SourcePath:      srcPath,
			DestinationPath: destPath,
		}

		return session.poolServiceClient.apiClient.RenameDirToDir(ctx, request)
	}

	_, err := session.doWithRelogin(renameDirToDirFunc)
	if err != nil {
		logger.Errorf("%+v", err)
		return commons.StatusToError(err)
	}

	// remove cache
	session.InvalidateCacheForRenameDir(srcPath, destPath)

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

	renameFileToFileFunc := func() (interface{}, error) {
		ctx, cancel := session.poolServiceClient.getContextWithDeadline()
		defer cancel()

		request := &api.RenameFileToFileRequest{
			SessionId:       session.id,
			SourcePath:      srcPath,
			DestinationPath: destPath,
		}

		return session.poolServiceClient.apiClient.RenameFileToFile(ctx, request)
	}

	_, err := session.doWithRelogin(renameFileToFileFunc)
	if err != nil {
		logger.Errorf("%+v", err)
		return commons.StatusToError(err)
	}

	// remove cache
	session.InvalidateCacheForRenameFile(srcPath, destPath)

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

	createFileFunc := func() (interface{}, error) {
		ctx, cancel := session.poolServiceClient.getContextWithDeadline()
		defer cancel()

		request := &api.CreateFileRequest{
			SessionId: session.id,
			Path:      path,
			Resource:  resource,
			Mode:      mode,
		}

		return session.poolServiceClient.apiClient.CreateFile(ctx, request)
	}

	res, err := session.doWithRelogin(createFileFunc)
	if err != nil {
		logger.Errorf("%+v", err)
		return nil, commons.StatusToError(err)
	}

	response, ok := res.(*api.CreateFileResponse)
	if !ok {
		logger.Error("failed to convert interface to CreateFileResponse")
		return nil, xerrors.Errorf("failed to convert interface to CreateFileResponse")
	}

	createTime, err := irodsfs_common_utils.ParseTime(response.Entry.CreateTime)
	if err != nil {
		logger.Errorf("%+v", err)
		return nil, err
	}

	modifyTime, err := irodsfs_common_utils.ParseTime(response.Entry.ModifyTime)
	if err != nil {
		logger.Errorf("%+v", err)
		return nil, err
	}

	irodsEntry := &irodsclient_fs.Entry{
		ID:                response.Entry.Id,
		Type:              irodsclient_fs.EntryType(response.Entry.Type),
		Name:              response.Entry.Name,
		Path:              response.Entry.Path,
		Owner:             response.Entry.Owner,
		Size:              response.Entry.Size,
		DataType:          response.Entry.DataType,
		CreateTime:        createTime,
		ModifyTime:        modifyTime,
		CheckSumAlgorithm: irodsclient_types.ChecksumAlgorithm(response.Entry.ChecksumAlgorithm),
		CheckSum:          response.Entry.Checksum,
	}

	// remove cache
	session.InvalidateCacheForCreateFile(path)

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

	openFileFunc := func() (interface{}, error) {
		ctx, cancel := session.poolServiceClient.getContextWithDeadline()
		defer cancel()

		request := &api.OpenFileRequest{
			SessionId: session.id,
			Path:      path,
			Resource:  resource,
			Mode:      mode,
		}

		return session.poolServiceClient.apiClient.OpenFile(ctx, request)
	}

	res, err := session.doWithRelogin(openFileFunc)
	if err != nil {
		logger.Errorf("%+v", err)
		return nil, commons.StatusToError(err)
	}

	response, ok := res.(*api.OpenFileResponse)
	if !ok {
		logger.Error("failed to convert interface to OpenFileResponse")
		return nil, xerrors.Errorf("failed to convert interface to OpenFileResponse")
	}

	createTime, err := irodsfs_common_utils.ParseTime(response.Entry.CreateTime)
	if err != nil {
		logger.Errorf("%+v", err)
		return nil, err
	}

	modifyTime, err := irodsfs_common_utils.ParseTime(response.Entry.ModifyTime)
	if err != nil {
		logger.Errorf("%+v", err)
		return nil, err
	}

	irodsEntry := &irodsclient_fs.Entry{
		ID:                response.Entry.Id,
		Type:              irodsclient_fs.EntryType(response.Entry.Type),
		Name:              response.Entry.Name,
		Path:              response.Entry.Path,
		Owner:             response.Entry.Owner,
		Size:              response.Entry.Size,
		DataType:          response.Entry.DataType,
		CreateTime:        createTime,
		ModifyTime:        modifyTime,
		CheckSumAlgorithm: irodsclient_types.ChecksumAlgorithm(response.Entry.ChecksumAlgorithm),
		CheckSum:          response.Entry.Checksum,
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

	truncateFileFunc := func() (interface{}, error) {
		ctx, cancel := session.poolServiceClient.getContextWithDeadline()
		defer cancel()

		request := &api.TruncateFileRequest{
			SessionId: session.id,
			Path:      path,
			Size:      size,
		}

		return session.poolServiceClient.apiClient.TruncateFile(ctx, request)
	}

	_, err := session.doWithRelogin(truncateFileFunc)
	if err != nil {
		logger.Errorf("%+v", err)
		return commons.StatusToError(err)
	}

	// remove cache
	session.InvalidateCacheForUpdateFile(path)

	return nil
}

// InvalidateCacheForRemoveFile removes caches for file
func (session *PoolServiceSession) InvalidateCacheForRemoveFile(path string) {
	// remove cache
	parentDirPath := irodsfs_common_utils.GetDirname(path)
	session.poolServiceClient.fsCache.RemoveDirCache(parentDirPath)
	session.poolServiceClient.fsCache.RemoveEntryCache(path)
	session.poolServiceClient.fsCache.RemoveACLsCache(path)
}

// InvalidateCacheForCreateFile removes caches for file
func (session *PoolServiceSession) InvalidateCacheForCreateFile(path string) {
	// remove cache
	parentDirPath := irodsfs_common_utils.GetDirname(path)
	session.poolServiceClient.fsCache.RemoveDirCache(parentDirPath)
	session.poolServiceClient.fsCache.RemoveEntryCache(path)
	session.poolServiceClient.fsCache.RemoveACLsCache(path)
}

// InvalidateCacheForUpdateFile removes caches for file
func (session *PoolServiceSession) InvalidateCacheForUpdateFile(path string) {
	// remove cache
	parentDirPath := irodsfs_common_utils.GetDirname(path)
	session.poolServiceClient.fsCache.RemoveDirCache(parentDirPath)
	session.poolServiceClient.fsCache.RemoveEntryCache(path)
	session.poolServiceClient.fsCache.RemoveACLsCache(path)
}

// InvalidateCacheForRenameFile removes caches for file
func (session *PoolServiceSession) InvalidateCacheForRenameFile(srcPath string, destPath string) {
	session.InvalidateCacheForRemoveFile(srcPath)
	session.InvalidateCacheForCreateFile(destPath)
}

func (session *PoolServiceSession) invalidateCacheForRemoveDirInternal(path string, recurse bool) {
	session.poolServiceClient.fsCache.RemoveEntryCache(path)
	session.poolServiceClient.fsCache.RemoveACLsCache(path)
	session.poolServiceClient.fsCache.RemoveDirEntryACLsCache(path)

	if recurse {
		dirEntries := session.poolServiceClient.fsCache.GetDirCache(path)
		for _, dirEntry := range dirEntries {
			// do it recursively
			session.invalidateCacheForRemoveDirInternal(dirEntry, recurse)
		}
	}

	session.poolServiceClient.fsCache.RemoveDirCache(path)
}

// InvalidateCacheForRemoveDir removes caches for dir
func (session *PoolServiceSession) InvalidateCacheForRemoveDir(path string, recurse bool) {
	// remove cache
	if recurse {
		dirCache := session.poolServiceClient.fsCache.GetDirCache(path)
		for _, dirEntry := range dirCache {
			// do it recursively
			session.invalidateCacheForRemoveDirInternal(dirEntry, recurse)
		}
	}

	parentDirPath := irodsfs_common_utils.GetDirname(path)
	session.poolServiceClient.fsCache.RemoveDirCache(parentDirPath)

	session.poolServiceClient.fsCache.RemoveDirCache(path)
	session.poolServiceClient.fsCache.RemoveDirEntryACLsCache(path)
	session.poolServiceClient.fsCache.RemoveEntryCache(path)
	session.poolServiceClient.fsCache.RemoveACLsCache(path)
}

// InvalidateCacheForMakeDir removes caches for dir
func (session *PoolServiceSession) InvalidateCacheForMakeDir(path string) {
	// remove cache
	parentDirPath := irodsfs_common_utils.GetDirname(path)
	session.poolServiceClient.fsCache.RemoveDirCache(parentDirPath)
	session.poolServiceClient.fsCache.RemoveDirCache(path)
	session.poolServiceClient.fsCache.RemoveEntryCache(path)
	session.poolServiceClient.fsCache.RemoveACLsCache(path)
}

// InvalidateCacheForRenameDir removes caches for dir
func (session *PoolServiceSession) InvalidateCacheForRenameDir(srcPath string, destPath string) {
	session.InvalidateCacheForRemoveDir(srcPath, true)
	session.InvalidateCacheForRemoveDir(destPath, true)
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

	getOffsetFunc := func() (interface{}, error) {
		ctx, cancel := handle.poolServiceClient.getContextWithDeadline()
		defer cancel()

		request := &api.GetOffsetRequest{
			SessionId:    handle.poolServiceSession.id,
			FileHandleId: handle.id,
		}

		return handle.poolServiceClient.apiClient.GetOffset(ctx, request)
	}

	res, err := handle.poolServiceSession.doWithRelogin(getOffsetFunc)
	if err != nil {
		logger.Errorf("%+v", err)
		return -1
	}

	response, ok := res.(*api.GetOffsetResponse)
	if !ok {
		logger.Error("failed to convert interface to GetOffsetResponse")
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

		readAtFunc := func() (interface{}, error) {
			ctx, cancel := handle.poolServiceClient.getContextWithDeadline()
			defer cancel()

			request := &api.ReadAtRequest{
				SessionId:    handle.poolServiceSession.id,
				FileHandleId: handle.id,
				Offset:       curOffset,
				Length:       int32(curLength),
			}

			return handle.poolServiceClient.apiClient.ReadAt(ctx, request, getLargeReadOption())
		}

		res, err := handle.poolServiceSession.doWithRelogin(readAtFunc)
		if err != nil {
			logger.Errorf("%+v", err)
			return 0, commons.StatusToError(err)
		}

		response, ok := res.(*api.ReadAtResponse)
		if !ok {
			logger.Error("failed to convert interface to ReadAtResponse")
			return 0, xerrors.Errorf("failed to convert interface to ReadAtResponse")
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

		writeAtFunc := func() (interface{}, error) {
			ctx, cancel := handle.poolServiceClient.getContextWithDeadline()
			defer cancel()

			request := &api.WriteAtRequest{
				SessionId:    handle.poolServiceSession.id,
				FileHandleId: handle.id,
				Offset:       curOffset,
				Data:         data[totalWriteLength : totalWriteLength+curLength],
			}

			return handle.poolServiceClient.apiClient.WriteAt(ctx, request, getLargeWriteOption())
		}

		_, err := handle.poolServiceSession.doWithRelogin(writeAtFunc)
		if err != nil {
			logger.Errorf("%+v", err)
			return 0, commons.StatusToError(err)
		}

		remainLength -= curLength
		curOffset += int64(curLength)
		totalWriteLength += int(curLength)

		// update entry size
		if handle.entry.Size < curOffset+int64(curLength) {
			handle.entry.Size = curOffset + int64(curLength)
		}
	}

	return totalWriteLength, nil
}

// Lock locks iRODS data object
func (handle *PoolServiceFileHandle) Lock(wait bool) error {
	logger := log.WithFields(log.Fields{
		"package":  "client",
		"struct":   "PoolServiceFileHandle",
		"function": "Lock",
	})

	defer irodsfs_common_utils.StackTraceFromPanic(logger)

	lockFunc := func() (interface{}, error) {
		ctx, cancel := handle.poolServiceClient.getContextWithDeadline()
		defer cancel()

		request := &api.LockRequest{
			SessionId:    handle.poolServiceSession.id,
			FileHandleId: handle.id,
			Wait:         wait,
		}

		return handle.poolServiceClient.apiClient.Lock(ctx, request)
	}

	_, err := handle.poolServiceSession.doWithRelogin(lockFunc)
	if err != nil {
		logger.Errorf("%+v", err)
		return commons.StatusToError(err)
	}

	return nil
}

// RLock locks iRODS data object with read lock
func (handle *PoolServiceFileHandle) RLock(wait bool) error {
	logger := log.WithFields(log.Fields{
		"package":  "client",
		"struct":   "PoolServiceFileHandle",
		"function": "RLock",
	})

	defer irodsfs_common_utils.StackTraceFromPanic(logger)

	rlockFunc := func() (interface{}, error) {
		ctx, cancel := handle.poolServiceClient.getContextWithDeadline()
		defer cancel()

		request := &api.LockRequest{
			SessionId:    handle.poolServiceSession.id,
			FileHandleId: handle.id,
			Wait:         wait,
		}

		return handle.poolServiceClient.apiClient.RLock(ctx, request)
	}

	_, err := handle.poolServiceSession.doWithRelogin(rlockFunc)
	if err != nil {
		logger.Errorf("%+v", err)
		return commons.StatusToError(err)
	}

	return nil
}

// Unlock unlocks iRODS data object with read lock
func (handle *PoolServiceFileHandle) Unlock() error {
	logger := log.WithFields(log.Fields{
		"package":  "client",
		"struct":   "PoolServiceFileHandle",
		"function": "Unlock",
	})

	defer irodsfs_common_utils.StackTraceFromPanic(logger)

	unlockFunc := func() (interface{}, error) {
		ctx, cancel := handle.poolServiceClient.getContextWithDeadline()
		defer cancel()

		request := &api.UnlockRequest{
			SessionId:    handle.poolServiceSession.id,
			FileHandleId: handle.id,
		}

		return handle.poolServiceClient.apiClient.Unlock(ctx, request)
	}

	_, err := handle.poolServiceSession.doWithRelogin(unlockFunc)
	if err != nil {
		logger.Errorf("%+v", err)
		return commons.StatusToError(err)
	}

	return nil
}

// Truncate truncates iRODS data object
func (handle *PoolServiceFileHandle) Truncate(size int64) error {
	logger := log.WithFields(log.Fields{
		"package":  "client",
		"struct":   "PoolServiceFileHandle",
		"function": "Truncate",
	})

	defer irodsfs_common_utils.StackTraceFromPanic(logger)

	truncateFunc := func() (interface{}, error) {
		ctx, cancel := handle.poolServiceClient.getContextWithDeadline()
		defer cancel()

		request := &api.TruncateRequest{
			SessionId:    handle.poolServiceSession.id,
			FileHandleId: handle.id,
			Size:         size,
		}

		return handle.poolServiceClient.apiClient.Truncate(ctx, request)
	}

	_, err := handle.poolServiceSession.doWithRelogin(truncateFunc)
	if err != nil {
		logger.Errorf("%+v", err)
		return commons.StatusToError(err)
	}

	// update entry size
	if handle.entry.Size < size {
		handle.entry.Size = size
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

	flushFunc := func() (interface{}, error) {
		ctx, cancel := handle.poolServiceClient.getContextWithDeadline()
		defer cancel()

		request := &api.FlushRequest{
			SessionId:    handle.poolServiceSession.id,
			FileHandleId: handle.id,
		}

		return handle.poolServiceClient.apiClient.Flush(ctx, request)
	}

	_, err := handle.poolServiceSession.doWithRelogin(flushFunc)
	if err != nil {
		logger.Errorf("%+v", err)
		return commons.StatusToError(err)
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

	closeFunc := func() (interface{}, error) {
		ctx, cancel := handle.poolServiceClient.getContextWithDeadline()
		defer cancel()

		request := &api.CloseRequest{
			SessionId:    handle.poolServiceSession.id,
			FileHandleId: handle.id,
		}

		return handle.poolServiceClient.apiClient.Close(ctx, request)
	}

	_, err := handle.poolServiceSession.doWithRelogin(closeFunc)
	if err != nil {
		logger.Errorf("%+v", err)
		return commons.StatusToError(err)
	}

	if handle.openMode.IsWrite() {
		parentDirPath := irodsfs_common_utils.GetDirname(handle.entry.Path)
		handle.poolServiceClient.fsCache.RemoveDirCache(parentDirPath)
		handle.poolServiceClient.fsCache.RemoveEntryCache(handle.entry.Path)
	}

	return nil
}
