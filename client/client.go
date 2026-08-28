package client

import (
	"context"
	"io"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/errors"
	irodsclient_fs "github.com/cyverse/go-irodsclient/fs"
	irodsclient_common "github.com/cyverse/go-irodsclient/irods/common"
	irodsclient_metrics "github.com/cyverse/go-irodsclient/irods/metrics"
	irodsclient_types "github.com/cyverse/go-irodsclient/irods/types"
	irodsclient_util "github.com/cyverse/go-irodsclient/irods/util"
	irodsfs_common_irods "github.com/cyverse/irodsfs-common/irods"
	irodsfs_common_util "github.com/cyverse/irodsfs-common/util"
	"github.com/cyverse/irodsfs-pool/commons"
	"github.com/cyverse/irodsfs-pool/service/api"
	"github.com/rs/xid"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

const (
	fileRWLengthMax        int   = 1024 * 1024      // 1MB
	messageRWLengthMax     int   = 8 * 1024 * 1024  // 8MB
	microBufferSize        int   = 1024 * 1024      // 1MB - micro buffering threshold for WRONLY
	prefetchBlockSize      int   = 4 * 1024 * 1024  // 4MB - prefetch block size for RDONLY
	prefetchCacheThreshold int64 = 16 * 1024 * 1024 // 16MB - switch to server-side memory cache after this much data is read

	localMetadataCacheTiemout time.Duration = 10 * time.Second

	reconnectInitialInterval time.Duration = 1 * time.Second
	reconnectMaxInterval     time.Duration = 1 * time.Minute
	reconnectTimeout         time.Duration = 1 * time.Hour
)

// prefetchState manages double-buffered read-ahead for ReadOnly file handles
type prefetchState struct {
	mu sync.Mutex

	buf      []byte
	bufStart int64
	bufSize  int

	nextBuf   []byte
	nextStart int64
	nextSize  int
	nextReady chan struct{}
	nextErr   error
	fetching  bool

	bytesRead int64
	disabled  bool
	closed    bool
}

// PoolServiceClient is a client of pool service
type PoolServiceClient struct {
	id               string
	address          string // host:port
	operationTimeout time.Duration
	grpcConnection   *grpc.ClientConn
	apiClient        api.PoolAPIClient
	fsCache          *MetadataCache
	connected        bool
	autoReconnect    bool
	reconnectingFlag int32 // atomic: 0=normal, 1=reconnect in progress

	bgCancelMu sync.Mutex
	bgCancel   context.CancelFunc // cancels the running backgroundReconnect goroutine

	logger *log.Entry
}

// PoolServiceSession is a service session
// implements irodsfs-common/irods/interface.go
const (
	maxPrefetchHandles      int = 10
	maxWriteBufferedHandles int = 10
)

type PoolServiceSession struct {
	id                string
	poolServiceClient *PoolServiceClient
	account           *irodsclient_types.IRODSAccount
	applicationName   string

	loggedIn             bool
	openReadOnlyHandles  int32
	openWriteOnlyHandles int32
	mutex                sync.RWMutex // mutex to access PoolServiceSession
	terminateChan        chan bool
	logger               *log.Entry
}

// NewPoolServiceClient creates a new pool service client
func NewPoolServiceClient(address string, operationTimeout time.Duration, autoReconnect bool, logger *log.Entry) *PoolServiceClient {
	clientID := xid.New().String()

	if logger == nil {
		logger = log.WithFields(log.Fields{
			"clientID": clientID,
		})
	} else {
		logger = logger.WithFields(log.Fields{
			"clientID": clientID,
		})
	}

	return &PoolServiceClient{
		id:               clientID,
		address:          address,
		operationTimeout: operationTimeout,
		grpcConnection:   nil,
		fsCache:          NewMetadataCache(localMetadataCacheTiemout, localMetadataCacheTiemout),
		connected:        false,
		autoReconnect:    autoReconnect,

		logger: logger,
	}
}

// isTransportError returns true for gRPC errors that indicate the server is unreachable.
func isTransportError(err error) bool {
	st, ok := status.FromError(err)
	if !ok {
		return false
	}
	return st.Code() == codes.Unavailable
}

// waitForReady blocks until conn reaches connectivity.Ready, or the context
// expires, or the connection is shut down.
func waitForReady(ctx context.Context, conn *grpc.ClientConn) bool {
	for {
		state := conn.GetState()
		if state == connectivity.Ready {
			return true
		}
		if state == connectivity.Shutdown {
			return false
		}
		if !conn.WaitForStateChange(ctx, state) {
			return false
		}
	}
}

// startBackgroundReconnect creates a cancellable context, stores the cancel
// func so Disconnect() can stop the goroutine, then starts backgroundReconnect.
func (client *PoolServiceClient) startBackgroundReconnect() {
	bgCtx, bgCancel := context.WithCancel(context.Background())

	client.bgCancelMu.Lock()
	if client.bgCancel != nil {
		client.bgCancel() // cancel any previous (should not happen, but be safe)
	}
	client.bgCancel = bgCancel
	client.bgCancelMu.Unlock()

	go client.backgroundReconnect(bgCtx)
}

// backgroundReconnect runs until ctx is cancelled (by Disconnect) or the
// connection is re-established.  It uses exponential backoff capped at 1 min
// and gives up after 1 hr.  While running, reconnectingFlag == 1 and every
// API call returns an error immediately.
func (client *PoolServiceClient) backgroundReconnect(ctx context.Context) {
	defer func() {
		atomic.StoreInt32(&client.reconnectingFlag, 0)
		client.bgCancelMu.Lock()
		client.bgCancel = nil
		client.bgCancelMu.Unlock()
	}()

	interval := reconnectInitialInterval
	deadline := time.Now().Add(reconnectTimeout)

	for time.Now().Before(deadline) {
		if ctx.Err() != nil {
			client.logger.Info("backgroundReconnect: stopped by Disconnect")
			return
		}

		// Use disconnectConn (not Disconnect) to avoid cancelling bgCtx or
		// resetting reconnectingFlag from within the reconnect loop itself.
		client.disconnectConn()
		if err := client.Connect(); err != nil {
			client.logger.WithError(err).Warn("backgroundReconnect: failed to create connection")
		} else {
			conn := client.grpcConnection
			conn.Connect()

			remaining := time.Until(deadline)
			waitDur := interval
			if waitDur > remaining {
				waitDur = remaining
			}

			waitCtx, cancel := context.WithTimeout(ctx, waitDur)
			ready := waitForReady(waitCtx, conn)
			cancel()

			if ready {
				client.logger.Info("Reconnected to pool service")
				return
			}
		}

		if interval < reconnectMaxInterval {
			interval *= 2
			if interval > reconnectMaxInterval {
				interval = reconnectMaxInterval
			}
		}
	}

	client.logger.Error("Reconnect timed out after 1 hour, giving up")
}

// Connect connects to pool service
func (client *PoolServiceClient) Connect() error {
	defer irodsfs_common_util.StackTraceFromPanic(client.logger)

	if client.connected {
		return errors.Errorf("already connected to %q", client.address)
	}

	scheme, endpoint, err := commons.ParsePoolServiceEndpoint(client.address)
	if err != nil {
		return err
	}

	client.logger.Infof("scheme: %s, endpoint: %s", scheme, endpoint)

	if scheme != "unix" && scheme != "tcp" {
		schemeErr := errors.Newf("unknown protocol %q", scheme)
		client.logger.Error(schemeErr)
		return schemeErr
	}

	client.logger.Infof("Connecting to %s endpoint: %q", scheme, endpoint)

	dialer := func(ctx context.Context, address string) (net.Conn, error) {
		return net.Dial(scheme, address)
	}

	conn, err := grpc.NewClient(endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(dialer))
	if err != nil {
		grpcErr := errors.Wrapf(err, "failed to dial to %q", client.address)
		client.logger.Error(grpcErr)
		return grpcErr
	}

	client.grpcConnection = conn
	client.apiClient = api.NewPoolAPIClient(client.grpcConnection)
	client.connected = true
	return nil
}

// disconnectConn tears down the gRPC connection without touching bgCancel or
// reconnectingFlag.  Used internally by the reconnect paths so they don't
// accidentally cancel themselves or reset the in-progress flag.
func (client *PoolServiceClient) disconnectConn() {
	client.apiClient = nil
	if client.grpcConnection != nil {
		client.grpcConnection.Close()
		client.grpcConnection = nil
	}
	client.connected = false
}

// Disconnect disconnects connection from pool service
func (client *PoolServiceClient) Disconnect() {
	// Stop any running background reconnect goroutine.
	client.bgCancelMu.Lock()
	if client.bgCancel != nil {
		client.bgCancel()
		client.bgCancel = nil
	}
	client.bgCancelMu.Unlock()
	atomic.StoreInt32(&client.reconnectingFlag, 0)

	client.disconnectConn()
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
func (client *PoolServiceClient) NewSession(account *irodsclient_types.IRODSAccount, applicationName string, description string) (irodsfs_common_irods.IRODSFSClient, error) {
	defer irodsfs_common_util.StackTraceFromPanic(client.logger)

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
		Description:     description,
	}

	response, err := client.apiClient.Login(ctx, request)
	if err != nil {
		client.logger.Error(err)
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

		logger: client.logger.WithFields(log.Fields{"session_id": response.SessionId}),
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

						client.logger.Error(err)
					}
				}
			}
		}
	}()

	return session, nil
}

// Release logouts from iRODS service session
func (session *PoolServiceSession) Release() {
	defer irodsfs_common_util.StackTraceFromPanic(session.logger)

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
		session.logger.Error(err)
		return
	}
}

// Relogin re-login iRODS service session
func (session *PoolServiceSession) Relogin() error {
	defer irodsfs_common_util.StackTraceFromPanic(session.logger)

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
	}

	response, err := session.poolServiceClient.apiClient.Login(ctx, request)
	if err != nil {
		session.logger.Error(err)
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

func (session *PoolServiceSession) GetOpenConnections() int {
	// return just 1, proxy connection
	return 1
}

func (session *PoolServiceSession) GetMetrics() *irodsclient_metrics.IRODSMetrics {
	// return empty
	return &irodsclient_metrics.IRODSMetrics{}
}

func (session *PoolServiceSession) doWithRelogin(f func() (interface{}, error)) (interface{}, error) {
	client := session.poolServiceClient

	// While background reconnect is running every call fails immediately.
	if atomic.LoadInt32(&client.reconnectingFlag) == 1 {
		return nil, errors.New("pool server reconnect in progress, please retry later")
	}

	session.mutex.RLock()
	loggedIn := session.loggedIn
	session.mutex.RUnlock()

	if !loggedIn {
		// keepalive detected logged out — relogin first
		err := session.Relogin()
		if err != nil {
			if client.autoReconnect && isTransportError(err) {
				// Transport error during relogin: trigger background reconnect if not already running.
				if atomic.CompareAndSwapInt32(&client.reconnectingFlag, 0, 1) {
					client.logger.Warn("Transport error on re-login, starting background reconnect")
					client.startBackgroundReconnect()
				}
			}
			return nil, err
		}
	}

	// now let's go
	res, err := f()
	if err != nil {
		// Check for transport error FIRST: IsReloginRequiredError also returns
		// true for codes.Unavailable, so it must not intercept transport errors.
		if client.autoReconnect && isTransportError(err) {
			session.logger.Warnf("Transport error detected: %v", err)

			session.mutex.Lock()
			session.loggedIn = false
			session.mutex.Unlock()

			// Only one goroutine handles the reconnect; others return error immediately.
			if !atomic.CompareAndSwapInt32(&client.reconnectingFlag, 0, 1) {
				return res, err
			}

			// One immediate attempt: recreate connection and test with Relogin.
			// Use disconnectConn (not Disconnect) to preserve reconnectingFlag==1.
			client.disconnectConn()
			_ = client.Connect()
			if conn := client.grpcConnection; conn != nil {
				conn.Connect()
				waitCtx, cancel := context.WithTimeout(context.Background(), reconnectInitialInterval)
				ready := waitForReady(waitCtx, conn)
				cancel()

				if ready {
					if reloginErr := session.Relogin(); reloginErr == nil {
						// Server is back, resume normally.
						atomic.StoreInt32(&client.reconnectingFlag, 0)
						res, err = f()
						return res, err
					} else if !isTransportError(reloginErr) {
						// Server reachable but auth/other error — don't background reconnect.
						atomic.StoreInt32(&client.reconnectingFlag, 0)
						return nil, reloginErr
					}
				}
			}

			// Immediate attempt failed — hand off to background reconnect loop.
			client.logger.Warn("Immediate reconnect failed, starting background reconnect")
			client.startBackgroundReconnect()
			return res, err
		}

		// relogin required (session expired, not a transport failure)
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
	defer irodsfs_common_util.StackTraceFromPanic(session.logger)

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
		session.logger.Error(err)
		return nil, commons.StatusToError(err)
	}

	response, ok := res.(*api.ListResponse)
	if !ok {
		session.logger.Error("failed to convert interface to ListResponse")
		return nil, errors.Errorf("failed to convert interface to ListResponse")
	}

	for _, entry := range response.Entries {
		createTime, err := irodsfs_common_util.ParseTime(entry.CreateTime)
		if err != nil {
			session.logger.Error(err)
			return nil, err
		}

		modifyTime, err := irodsfs_common_util.ParseTime(entry.ModifyTime)
		if err != nil {
			session.logger.Error(err)
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
	defer irodsfs_common_util.StackTraceFromPanic(session.logger)

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
		session.logger.Error(err)
		return nil, commons.StatusToError(err)
	}

	response, ok := res.(*api.StatResponse)
	if !ok {
		session.logger.Error("failed to convert interface to StatResponse")
		return nil, errors.Errorf("failed to convert interface to StatResponse")
	}

	createTime, err := irodsfs_common_util.ParseTime(response.Entry.CreateTime)
	if err != nil {
		session.logger.Error(err)
		return nil, err
	}

	modifyTime, err := irodsfs_common_util.ParseTime(response.Entry.ModifyTime)
	if err != nil {
		session.logger.Error(err)
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

// ExistsDir checks existence of Dir
func (session *PoolServiceSession) ExistsDir(path string) bool {
	defer irodsfs_common_util.StackTraceFromPanic(session.logger)

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
		session.logger.Error(err)
		return false
	}

	response, ok := res.(*api.ExistsDirResponse)
	if !ok {
		session.logger.Error("failed to convert interface to ExistsDirResponse")
		return false
	}

	return response.Exist
}

// ExistsFile checks existence of File
func (session *PoolServiceSession) ExistsFile(path string) bool {
	defer irodsfs_common_util.StackTraceFromPanic(session.logger)

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
		session.logger.Error(err)
		return false
	}

	response, ok := res.(*api.ExistsFileResponse)
	if !ok {
		session.logger.Error("failed to convert interface to ExistsFileResponse")
		return false
	}

	return response.Exist
}

// RemoveFile removes iRODS data object
func (session *PoolServiceSession) RemoveFile(path string, force bool) error {
	defer irodsfs_common_util.StackTraceFromPanic(session.logger)

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
		session.logger.Error(err)
		return commons.StatusToError(err)
	}

	// remove cache
	session.InvalidateCacheForRemoveFile(path)

	return nil
}

// RemoveDir removes iRODS collection
func (session *PoolServiceSession) RemoveDir(path string, recurse bool, force bool) error {
	defer irodsfs_common_util.StackTraceFromPanic(session.logger)

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
		session.logger.Error(err)
		return commons.StatusToError(err)
	}

	// remove cache
	session.InvalidateCacheForRemoveDir(path, recurse)

	return nil
}

// MakeDir creates a new iRODS collection
func (session *PoolServiceSession) MakeDir(path string, recurse bool) error {
	defer irodsfs_common_util.StackTraceFromPanic(session.logger)

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
		session.logger.Error(err)
		return commons.StatusToError(err)
	}

	// remove cache
	session.InvalidateCacheForMakeDir(path)

	return nil
}

// RenameDirToDir renames iRODS collection
func (session *PoolServiceSession) RenameDirToDir(srcPath string, destPath string) error {
	defer irodsfs_common_util.StackTraceFromPanic(session.logger)

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
		session.logger.Error(err)
		return commons.StatusToError(err)
	}

	// remove cache
	session.InvalidateCacheForRenameDir(srcPath, destPath)

	return nil
}

// RenameFileToFile renames iRODS data object
func (session *PoolServiceSession) RenameFileToFile(srcPath string, destPath string) error {
	defer irodsfs_common_util.StackTraceFromPanic(session.logger)

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
		session.logger.Error(err)
		return commons.StatusToError(err)
	}

	// remove cache
	session.InvalidateCacheForRenameFile(srcPath, destPath)

	return nil
}

// CreateFile creates a new iRODS data object
func (session *PoolServiceSession) CreateFile(path string, mode string) (irodsfs_common_irods.IRODSFSFileHandle, error) {
	defer irodsfs_common_util.StackTraceFromPanic(session.logger)

	createFileFunc := func() (interface{}, error) {
		ctx, cancel := session.poolServiceClient.getContextWithDeadline()
		defer cancel()

		request := &api.CreateFileRequest{
			SessionId: session.id,
			Path:      path,
			Mode:      mode,
		}

		return session.poolServiceClient.apiClient.CreateFile(ctx, request)
	}

	res, err := session.doWithRelogin(createFileFunc)
	if err != nil {
		session.logger.Error(err)
		return nil, commons.StatusToError(err)
	}

	response, ok := res.(*api.CreateFileResponse)
	if !ok {
		session.logger.Error("failed to convert interface to CreateFileResponse")
		return nil, errors.Errorf("failed to convert interface to CreateFileResponse")
	}

	createTime, err := irodsfs_common_util.ParseTime(response.Entry.CreateTime)
	if err != nil {
		session.logger.Error(err)
		return nil, err
	}

	modifyTime, err := irodsfs_common_util.ParseTime(response.Entry.ModifyTime)
	if err != nil {
		session.logger.Error(err)
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
	session.poolServiceClient.fsCache.AddEntryCache(irodsEntry)

	handle := &PoolServiceFileHandle{
		id:                 response.FileHandleId,
		poolServiceClient:  session.poolServiceClient,
		poolServiceSession: session,
		entry:              irodsEntry,
		openMode:           irodsclient_types.FileOpenMode(mode),
		logger:             session.logger.WithFields(log.Fields{"handle_id": response.FileHandleId}),
	}

	if irodsclient_types.FileOpenMode(mode).IsWriteOnly() {
		count := atomic.AddInt32(&session.openWriteOnlyHandles, 1)
		if int(count) <= maxWriteBufferedHandles {
			handle.writeBuffered = true
		}
	}

	return handle, nil
}

// OpenFile opens iRODS data object
func (session *PoolServiceSession) OpenFile(path string, mode string) (irodsfs_common_irods.IRODSFSFileHandle, error) {
	defer irodsfs_common_util.StackTraceFromPanic(session.logger)

	openFileFunc := func() (interface{}, error) {
		ctx, cancel := session.poolServiceClient.getContextWithDeadline()
		defer cancel()

		request := &api.OpenFileRequest{
			SessionId: session.id,
			Path:      path,
			Mode:      mode,
		}

		return session.poolServiceClient.apiClient.OpenFile(ctx, request)
	}

	res, err := session.doWithRelogin(openFileFunc)
	if err != nil {
		session.logger.Error(err)
		return nil, commons.StatusToError(err)
	}

	response, ok := res.(*api.OpenFileResponse)
	if !ok {
		session.logger.Error("failed to convert interface to OpenFileResponse")
		return nil, errors.Errorf("failed to convert interface to OpenFileResponse")
	}

	createTime, err := irodsfs_common_util.ParseTime(response.Entry.CreateTime)
	if err != nil {
		session.logger.Error(err)
		return nil, err
	}

	modifyTime, err := irodsfs_common_util.ParseTime(response.Entry.ModifyTime)
	if err != nil {
		session.logger.Error(err)
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

	handle := &PoolServiceFileHandle{
		id:                 response.FileHandleId,
		poolServiceClient:  session.poolServiceClient,
		poolServiceSession: session,
		entry:              irodsEntry,
		openMode:           irodsclient_types.FileOpenMode(mode),

		logger: session.logger.WithFields(log.Fields{"handle_id": response.FileHandleId}),
	}

	// The server-side open observes the current size of staged files. Publish
	// that snapshot so a following getattr cannot reuse metadata cached before
	// the file was written.
	session.poolServiceClient.fsCache.AddEntryCache(irodsEntry)

	if irodsclient_types.FileOpenMode(mode).IsReadOnly() {
		count := atomic.AddInt32(&session.openReadOnlyHandles, 1)
		if int(count) <= maxPrefetchHandles {
			handle.prefetch = &prefetchState{
				buf:     make([]byte, prefetchBlockSize),
				nextBuf: make([]byte, prefetchBlockSize),
			}
		} else {
			session.CacheFileAsync(irodsEntry.Path)
		}
	} else if irodsclient_types.FileOpenMode(mode).IsWriteOnly() {
		count := atomic.AddInt32(&session.openWriteOnlyHandles, 1)
		if int(count) <= maxWriteBufferedHandles {
			handle.writeBuffered = true
		}
	}

	return handle, nil
}

// CreateFileBulk creates an iRODS data object for bulk upload (file is synced and deleted after close)
func (session *PoolServiceSession) CreateFileBulk(path string, mode string) (irodsfs_common_irods.IRODSFSFileHandle, error) {
	defer irodsfs_common_util.StackTraceFromPanic(session.logger)

	createFileBulkFunc := func() (interface{}, error) {
		ctx, cancel := session.poolServiceClient.getContextWithDeadline()
		defer cancel()

		request := &api.CreateFileBulkRequest{
			SessionId: session.id,
			Path:      path,
			Mode:      mode,
		}

		return session.poolServiceClient.apiClient.CreateFileBulk(ctx, request)
	}

	res, err := session.doWithRelogin(createFileBulkFunc)
	if err != nil {
		session.logger.Error(err)
		return nil, commons.StatusToError(err)
	}

	response, ok := res.(*api.CreateFileBulkResponse)
	if !ok {
		session.logger.Error("failed to convert interface to CreateFileBulkResponse")
		return nil, errors.Errorf("failed to convert interface to CreateFileBulkResponse")
	}

	createTime, err := irodsfs_common_util.ParseTime(response.Entry.CreateTime)
	if err != nil {
		session.logger.Error(err)
		return nil, err
	}

	modifyTime, err := irodsfs_common_util.ParseTime(response.Entry.ModifyTime)
	if err != nil {
		session.logger.Error(err)
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

	handle := &PoolServiceFileHandle{
		id:                 response.FileHandleId,
		poolServiceClient:  session.poolServiceClient,
		poolServiceSession: session,
		entry:              irodsEntry,
		openMode:           irodsclient_types.FileOpenMode(mode),
		logger:             session.logger.WithFields(log.Fields{"handle_id": response.FileHandleId}),
	}

	if irodsclient_types.FileOpenMode(mode).IsWriteOnly() {
		count := atomic.AddInt32(&session.openWriteOnlyHandles, 1)
		if int(count) <= maxWriteBufferedHandles {
			handle.writeBuffered = true
		}
	}

	return handle, nil
}

// OpenFileBulk opens an iRODS data object for bulk upload (file is synced and deleted after close)
func (session *PoolServiceSession) OpenFileBulk(path string, mode string) (irodsfs_common_irods.IRODSFSFileHandle, error) {
	defer irodsfs_common_util.StackTraceFromPanic(session.logger)

	openFileBulkFunc := func() (interface{}, error) {
		ctx, cancel := session.poolServiceClient.getContextWithDeadline()
		defer cancel()

		request := &api.OpenFileBulkRequest{
			SessionId: session.id,
			Path:      path,
			Mode:      mode,
		}

		return session.poolServiceClient.apiClient.OpenFileBulk(ctx, request)
	}

	res, err := session.doWithRelogin(openFileBulkFunc)
	if err != nil {
		session.logger.Error(err)
		return nil, commons.StatusToError(err)
	}

	response, ok := res.(*api.OpenFileBulkResponse)
	if !ok {
		session.logger.Error("failed to convert interface to OpenFileBulkResponse")
		return nil, errors.Errorf("failed to convert interface to OpenFileBulkResponse")
	}

	createTime, err := irodsfs_common_util.ParseTime(response.Entry.CreateTime)
	if err != nil {
		session.logger.Error(err)
		return nil, err
	}

	modifyTime, err := irodsfs_common_util.ParseTime(response.Entry.ModifyTime)
	if err != nil {
		session.logger.Error(err)
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

	handle := &PoolServiceFileHandle{
		id:                 response.FileHandleId,
		poolServiceClient:  session.poolServiceClient,
		poolServiceSession: session,
		entry:              irodsEntry,
		openMode:           irodsclient_types.FileOpenMode(mode),
		logger:             session.logger.WithFields(log.Fields{"handle_id": response.FileHandleId}),
	}

	if irodsclient_types.FileOpenMode(mode).IsReadOnly() {
		count := atomic.AddInt32(&session.openReadOnlyHandles, 1)
		if int(count) <= maxPrefetchHandles {
			handle.prefetch = &prefetchState{
				buf:     make([]byte, prefetchBlockSize),
				nextBuf: make([]byte, prefetchBlockSize),
			}
		} else {
			session.CacheFileAsync(irodsEntry.Path)
		}
	} else if irodsclient_types.FileOpenMode(mode).IsWriteOnly() {
		count := atomic.AddInt32(&session.openWriteOnlyHandles, 1)
		if int(count) <= maxWriteBufferedHandles {
			handle.writeBuffered = true
		}
	}

	return handle, nil
}

// TruncateFile truncates iRODS data object
func (session *PoolServiceSession) TruncateFile(path string, size int64) error {
	defer irodsfs_common_util.StackTraceFromPanic(session.logger)

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
		session.logger.Error(err)
		return commons.StatusToError(err)
	}

	// remove cache
	session.InvalidateCacheForUpdateFile(path)

	return nil
}

// Sync synchronizes data
func (session *PoolServiceSession) Sync() error {
	defer irodsfs_common_util.StackTraceFromPanic(session.logger)

	syncFunc := func() (interface{}, error) {
		ctx, cancel := session.poolServiceClient.getContextWithDeadline()
		defer cancel()

		request := &api.SyncRequest{
			SessionId: session.id,
		}

		return session.poolServiceClient.apiClient.Sync(ctx, request)
	}

	_, err := session.doWithRelogin(syncFunc)
	if err != nil {
		session.logger.Error(err)
		return commons.StatusToError(err)
	}

	return nil
}

// DownloadFile downloads iRODS file to local path using ReadStream
func (session *PoolServiceSession) DownloadFile(irodsPath string, localPath string, transferCallback irodsclient_common.TransferTrackerCallback) error {
	defer irodsfs_common_util.StackTraceFromPanic(session.logger)

	var fileSize int64
	if transferCallback != nil {
		entry, err := session.Stat(irodsPath)
		if err != nil {
			return err
		}
		fileSize = entry.Size
	}

	localFile, err := os.Create(localPath)
	if err != nil {
		return errors.Wrapf(err, "failed to create local file %q", localPath)
	}
	defer localFile.Close()

	ctx, cancel := session.poolServiceClient.getContextWithDeadline()
	defer cancel()

	request := &api.ReadStreamRequest{
		SessionId: session.id,
		IrodsPath: irodsPath,
	}

	stream, err := session.poolServiceClient.apiClient.ReadStream(ctx, request, getLargeReadOption())
	if err != nil {
		return commons.StatusToError(err)
	}

	for {
		response, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return commons.StatusToError(err)
		}

		_, writeErr := localFile.WriteAt(response.Data, response.Offset)
		if writeErr != nil {
			return errors.Wrapf(writeErr, "failed to write to local file %q", localPath)
		}

		if transferCallback != nil {
			newOffset := response.Offset + int64(len(response.Data))
			transferCallback("download", newOffset, fileSize)
		}
	}

	return nil
}

// DownloadFileParallel downloads iRODS file to local path (uses single stream)
func (session *PoolServiceSession) DownloadFileParallel(irodsPath string, localPath string, taskNum int, transferCallback irodsclient_common.TransferTrackerCallback) error {
	return session.DownloadFile(irodsPath, localPath, transferCallback)
}

// DownloadFileWithCallback downloads iRODS file and calls blockReadyCallback for each block
func (session *PoolServiceSession) DownloadFileWithCallback(irodsPath string, blockSize int, numBlocks int, blockReadyCallback irodsclient_common.DataObjectBlockCallback, transferCallback irodsclient_common.TransferTrackerCallback) error {
	defer irodsfs_common_util.StackTraceFromPanic(session.logger)

	var fileSize int64
	if transferCallback != nil {
		entry, err := session.Stat(irodsPath)
		if err != nil {
			return err
		}
		fileSize = entry.Size
	}

	ctx, cancel := session.poolServiceClient.getContextWithDeadline()
	defer cancel()

	request := &api.ReadStreamRequest{
		SessionId: session.id,
		IrodsPath: irodsPath,
		BlockSize: int32(blockSize),
		NumBlocks: int32(numBlocks),
	}

	stream, err := session.poolServiceClient.apiClient.ReadStream(ctx, request, getLargeReadOption())
	if err != nil {
		return commons.StatusToError(err)
	}

	for {
		response, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return commons.StatusToError(err)
		}

		if blockReadyCallback != nil {
			callbackErr := blockReadyCallback(response.Data, response.Offset)
			if callbackErr != nil {
				return callbackErr
			}
		}

		if transferCallback != nil {
			newOffset := response.Offset + int64(len(response.Data))
			transferCallback("download", newOffset, fileSize)
		}
	}

	return nil
}

// DownloadFileParallelWithCallback downloads iRODS file with callback using parallel reading on the server
func (session *PoolServiceSession) DownloadFileParallelWithCallback(irodsPath string, blockSize int, numBlocks int, blockReadyCallback irodsclient_common.DataObjectBlockCallback, taskNum int, transferCallback irodsclient_common.TransferTrackerCallback) error {
	defer irodsfs_common_util.StackTraceFromPanic(session.logger)

	ctx, cancel := session.poolServiceClient.getContextWithDeadline()
	defer cancel()

	request := &api.ReadStreamParallelRequest{
		SessionId: session.id,
		IrodsPath: irodsPath,
		BlockSize: int32(blockSize),
		NumBlocks: int32(numBlocks),
		TaskNum:   int32(taskNum),
	}

	stream, err := session.poolServiceClient.apiClient.ReadStreamParallel(ctx, request, getLargeReadOption())
	if err != nil {
		return commons.StatusToError(err)
	}

	for {
		response, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return commons.StatusToError(err)
		}

		if blockReadyCallback != nil {
			callbackErr := blockReadyCallback(response.Data, response.Offset)
			if callbackErr != nil {
				return callbackErr
			}
		}

		if transferCallback != nil {
			newOffset := response.Offset + int64(len(response.Data))
			transferCallback("download", newOffset, 0)
		}
	}

	return nil
}

// UploadFile uploads local file to iRODS path using WriteStream
func (session *PoolServiceSession) UploadFile(localPath string, irodsPath string, transferCallback irodsclient_common.TransferTrackerCallback) error {
	defer irodsfs_common_util.StackTraceFromPanic(session.logger)

	fileInfo, err := os.Stat(localPath)
	if err != nil {
		return errors.Wrapf(err, "failed to stat local file %q", localPath)
	}

	// open local file
	localFile, err := os.Open(localPath)
	if err != nil {
		return errors.Wrapf(err, "failed to open local file %q", localPath)
	}
	defer localFile.Close()

	// use WriteStream
	ctx, cancel := session.poolServiceClient.getContextWithDeadline()
	defer cancel()

	stream, err := session.poolServiceClient.apiClient.WriteStream(ctx, getLargeWriteOption())
	if err != nil {
		return commons.StatusToError(err)
	}

	err = stream.Send(&api.WriteStreamRequest{
		Payload: &api.WriteStreamRequest_Header{
			Header: &api.WriteStreamHeader{
				SessionId: session.id,
				IrodsPath: irodsPath,
			},
		},
	})
	if err != nil {
		return commons.StatusToError(err)
	}

	buffer := make([]byte, fileRWLengthMax)
	var offset int64

	for {
		n, readErr := localFile.Read(buffer)
		if n > 0 {
			sendErr := stream.Send(&api.WriteStreamRequest{
				Payload: &api.WriteStreamRequest_Block{
					Block: &api.WriteStreamBlock{
						Offset: offset,
						Data:   buffer[:n],
					},
				},
			})
			if sendErr != nil {
				return commons.StatusToError(sendErr)
			}
			offset += int64(n)

			if transferCallback != nil {
				transferCallback("upload", offset, fileInfo.Size())
			}
		}

		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			return errors.Wrapf(readErr, "failed to read local file %q", localPath)
		}
	}

	_, err = stream.CloseAndRecv()
	if err != nil {
		return commons.StatusToError(err)
	}

	session.InvalidateCacheForCreateFile(irodsPath)

	return nil
}

// UploadFileParallel uploads local file to iRODS path (uses single stream)
func (session *PoolServiceSession) UploadFileParallel(localPath string, irodsPath string, taskNum int, transferCallback irodsclient_common.TransferTrackerCallback) error {
	return session.UploadFile(localPath, irodsPath, transferCallback)
}

// CacheFile requests the pool server to cache file content (synchronous)
func (session *PoolServiceSession) CacheFile(irodsPath string, transferCallback irodsclient_common.TransferTrackerCallback) error {
	defer irodsfs_common_util.StackTraceFromPanic(session.logger)

	cacheFileFunc := func() (interface{}, error) {
		ctx, cancel := session.poolServiceClient.getContextWithDeadline()
		defer cancel()

		request := &api.CacheFileRequest{
			SessionId: session.id,
			IrodsPath: irodsPath,
		}

		return session.poolServiceClient.apiClient.CacheFile(ctx, request)
	}

	_, err := session.doWithRelogin(cacheFileFunc)
	if err != nil {
		session.logger.Error(err)
		return commons.StatusToError(err)
	}

	return nil
}

// CacheFileAsync requests the pool server to cache file content in background.
// The server returns immediately and caches asynchronously.
// Session release on the server waits for the caching to complete.
func (session *PoolServiceSession) CacheFileAsync(irodsPath string) {
	defer irodsfs_common_util.StackTraceFromPanic(session.logger)

	cacheFileFunc := func() (interface{}, error) {
		ctx, cancel := session.poolServiceClient.getContextWithDeadline()
		defer cancel()

		request := &api.CacheFileRequest{
			SessionId: session.id,
			IrodsPath: irodsPath,
			Async:     true,
		}

		return session.poolServiceClient.apiClient.CacheFile(ctx, request)
	}

	_, err := session.doWithRelogin(cacheFileFunc)
	if err != nil {
		session.logger.Error(err)
	}
}

// InvalidateAllCache removes all caches
func (session *PoolServiceSession) InvalidateAllCache() {
	session.poolServiceClient.fsCache.ClearDirCache()
	session.poolServiceClient.fsCache.ClearEntryCache()
}

// InvalidateCacheForRemoveFile removes caches for file
func (session *PoolServiceSession) InvalidateCacheForRemoveFile(path string) {
	// remove cache
	parentDirPath := irodsclient_util.GetIRODSPathDirname(path)
	session.poolServiceClient.fsCache.RemoveDirCache(parentDirPath)
	session.poolServiceClient.fsCache.RemoveEntryCache(path)
}

// InvalidateCacheForCreateFile removes caches for file
func (session *PoolServiceSession) InvalidateCacheForCreateFile(path string) {
	// remove cache
	parentDirPath := irodsclient_util.GetIRODSPathDirname(path)
	session.poolServiceClient.fsCache.RemoveDirCache(parentDirPath)
	session.poolServiceClient.fsCache.RemoveEntryCache(path)
}

// InvalidateCacheForUpdateFile removes caches for file
func (session *PoolServiceSession) InvalidateCacheForUpdateFile(path string) {
	// remove cache
	parentDirPath := irodsclient_util.GetIRODSPathDirname(path)
	session.poolServiceClient.fsCache.RemoveDirCache(parentDirPath)
	session.poolServiceClient.fsCache.RemoveEntryCache(path)
}

// InvalidateCacheForRenameFile removes caches for file
func (session *PoolServiceSession) InvalidateCacheForRenameFile(srcPath string, destPath string) {
	session.InvalidateCacheForRemoveFile(srcPath)
	session.InvalidateCacheForCreateFile(destPath)
}

func (session *PoolServiceSession) invalidateCacheForRemoveDirInternal(path string, recurse bool) {
	session.poolServiceClient.fsCache.RemoveEntryCache(path)

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

	parentDirPath := irodsclient_util.GetIRODSPathDirname(path)
	session.poolServiceClient.fsCache.RemoveDirCache(parentDirPath)

	session.poolServiceClient.fsCache.RemoveDirCache(path)
	session.poolServiceClient.fsCache.RemoveEntryCache(path)
}

// InvalidateCacheForMakeDir removes caches for dir
func (session *PoolServiceSession) InvalidateCacheForMakeDir(path string) {
	// remove cache
	parentDirPath := irodsclient_util.GetIRODSPathDirname(path)
	session.poolServiceClient.fsCache.RemoveDirCache(parentDirPath)
	session.poolServiceClient.fsCache.RemoveDirCache(path)
	session.poolServiceClient.fsCache.RemoveEntryCache(path)
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

	writeBuffered     bool
	writeBuffer       []byte
	writeBufferOffset int64
	writeBufferSize   int

	prefetch *prefetchState

	readMutex sync.Mutex
	closed    bool
	mutex     sync.Mutex

	logger *log.Entry
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

func (handle *PoolServiceFileHandle) IsReadMode() bool {
	return handle.openMode.IsRead()
}

func (handle *PoolServiceFileHandle) IsWriteMode() bool {
	return handle.openMode.IsWrite()
}

// ReadAt reads iRODS data object
func (handle *PoolServiceFileHandle) ReadAt(buffer []byte, offset int64) (int, error) {
	defer irodsfs_common_util.StackTraceFromPanic(handle.logger)

	if handle.prefetch == nil {
		return handle.readFromServer(buffer, offset)
	}

	// FUSE may issue concurrent reads for a single open file handle. The
	// prefetch state uses a current and a next buffer and deliberately drops
	// its own lock while fetching a cache miss. Serialize callers here so two
	// misses cannot overwrite that shared state or start background fetches
	// into the same next buffer.
	handle.readMutex.Lock()
	defer handle.readMutex.Unlock()

	n, err := handle.readWithPrefetch(buffer, offset)

	pf := handle.prefetch
	pf.mu.Lock()
	requestCache := false
	if !pf.disabled {
		pf.bytesRead += int64(n)
		if pf.bytesRead > prefetchCacheThreshold {
			pf.disabled = true
			pf.buf = nil
			pf.nextBuf = nil
			pf.bufSize = 0
			pf.nextSize = 0
			requestCache = true
		}
	}
	pf.mu.Unlock()

	if requestCache {
		handle.poolServiceSession.CacheFileAsync(handle.entry.Path)
	}

	return n, err
}

func (handle *PoolServiceFileHandle) readWithPrefetch(buffer []byte, offset int64) (int, error) {
	totalRead := 0
	for totalRead < len(buffer) {
		currentOffset := offset + int64(totalRead)
		if currentOffset >= handle.entry.Size {
			return totalRead, io.EOF
		}

		n, err := handle.readFromPrefetchBlock(buffer[totalRead:], currentOffset)
		totalRead += n
		if err != nil && err != io.EOF {
			return totalRead, err
		}
		if n == 0 {
			return totalRead, io.EOF
		}
	}

	return totalRead, nil
}

// readFromPrefetchBlock reads from one prefetch block. A short read with
// io.EOF can mean the end of this internal block rather than the end of the
// file; readWithPrefetch continues with the following block in that case.
func (handle *PoolServiceFileHandle) readFromPrefetchBlock(buffer []byte, offset int64) (int, error) {
	pf := handle.prefetch
	pf.mu.Lock()

	if pf.disabled {
		pf.mu.Unlock()
		return handle.readFromServer(buffer, offset)
	}

	bufEnd := pf.bufStart + int64(pf.bufSize)
	if pf.bufSize > 0 && offset >= pf.bufStart && offset < bufEnd {
		start := int(offset - pf.bufStart)
		n := copy(buffer, pf.buf[start:pf.bufSize])
		readEnd := offset + int64(n)

		// trigger prefetch if consumed past 50%
		if !pf.fetching && pf.bufSize > 0 && int(readEnd-pf.bufStart) > pf.bufSize/2 {
			handle.triggerPrefetch(bufEnd)
		}

		pf.mu.Unlock()

		if n < len(buffer) {
			return n, io.EOF
		}
		return n, nil
	}

	// check if data is in next buffer (being fetched or already ready)
	if pf.fetching {
		nextReady := pf.nextReady
		pf.mu.Unlock()
		<-nextReady
		pf.mu.Lock()
	}

	if pf.nextSize > 0 {
		nextEnd := pf.nextStart + int64(pf.nextSize)
		if offset >= pf.nextStart && offset < nextEnd {
			// swap next → current
			pf.buf, pf.nextBuf = pf.nextBuf, pf.buf
			pf.bufStart = pf.nextStart
			pf.bufSize = pf.nextSize
			pf.nextSize = 0
			pf.nextErr = nil

			start := int(offset - pf.bufStart)
			n := copy(buffer, pf.buf[start:pf.bufSize])
			readEnd := offset + int64(n)

			bufEnd = pf.bufStart + int64(pf.bufSize)
			if !pf.fetching && int(readEnd-pf.bufStart) > pf.bufSize/2 {
				handle.triggerPrefetch(bufEnd)
			}

			pf.mu.Unlock()

			if n < len(buffer) {
				return n, io.EOF
			}
			return n, nil
		}
	}
	pf.mu.Unlock()

	// fetch block synchronously
	blockStart := (offset / int64(prefetchBlockSize)) * int64(prefetchBlockSize)
	buf := make([]byte, prefetchBlockSize)
	n, err := handle.readFromServer(buf, blockStart)

	pf.mu.Lock()
	pf.buf = buf
	pf.bufStart = blockStart
	pf.bufSize = n
	pf.nextSize = 0
	pf.nextErr = nil
	pf.fetching = false

	if n > 0 && offset >= blockStart && offset < blockStart+int64(n) {
		start := int(offset - blockStart)
		copied := copy(buffer, pf.buf[start:n])
		readEnd := offset + int64(copied)
		bufEnd = blockStart + int64(n)

		if !pf.fetching && int(readEnd-pf.bufStart) > pf.bufSize/2 {
			handle.triggerPrefetch(bufEnd)
		}

		pf.mu.Unlock()

		if copied < len(buffer) {
			return copied, io.EOF
		}
		return copied, nil
	}
	pf.mu.Unlock()

	if err != nil {
		return 0, err
	}
	return 0, io.EOF
}

// triggerPrefetch starts background fetch of the next block. Must be called with pf.mu held.
func (handle *PoolServiceFileHandle) triggerPrefetch(nextOffset int64) {
	pf := handle.prefetch
	if nextOffset >= handle.entry.Size || pf.fetching || pf.nextSize > 0 || pf.closed {
		return
	}

	pf.fetching = true
	pf.nextStart = nextOffset
	ready := make(chan struct{})
	pf.nextReady = ready

	// Remove the target buffer from shared state while the background read is
	// mutating it. It is published again only after the complete read finishes.
	nextBuffer := pf.nextBuf
	pf.nextBuf = nil
	if cap(nextBuffer) < prefetchBlockSize {
		nextBuffer = make([]byte, prefetchBlockSize)
	} else {
		nextBuffer = nextBuffer[:prefetchBlockSize]
	}

	go func() {
		n, err := handle.readFromServer(nextBuffer, nextOffset)

		pf.mu.Lock()
		if !pf.closed && !pf.disabled {
			pf.nextBuf = nextBuffer
			pf.nextSize = n
			pf.nextErr = err
		}
		pf.fetching = false
		close(ready)
		pf.mu.Unlock()
	}()
}

func (handle *PoolServiceFileHandle) readFromServer(buffer []byte, offset int64) (int, error) {
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
			handle.logger.Error(err)
			return 0, commons.StatusToError(err)
		}

		response, ok := res.(*api.ReadAtResponse)
		if !ok {
			handle.logger.Error("failed to convert interface to ReadAtResponse")
			return 0, errors.Errorf("failed to convert interface to ReadAtResponse")
		}

		if len(response.Data) > 0 {
			copyLen := copy(buffer[totalReadLength:], response.Data)

			remainLength -= copyLen
			curOffset += int64(copyLen)
			totalReadLength += copyLen
		}

		if len(response.Data) < curLength {
			return totalReadLength, io.EOF
		}
	}

	return totalReadLength, nil
}

func (handle *PoolServiceFileHandle) GetAvailable(offset int64) int64 {
	defer irodsfs_common_util.StackTraceFromPanic(handle.logger)

	ctx, cancel := handle.poolServiceClient.getContextWithDeadline()
	defer cancel()

	request := &api.GetAvailableRequest{
		SessionId:    handle.poolServiceSession.id,
		FileHandleId: handle.id,
		Offset:       offset,
	}

	response, err := handle.poolServiceClient.apiClient.GetAvailable(ctx, request)
	if err != nil {
		return -1
	}

	return response.Available
}

// WriteAt writes iRODS data object
func (handle *PoolServiceFileHandle) WriteAt(data []byte, offset int64) (int, error) {
	defer irodsfs_common_util.StackTraceFromPanic(handle.logger)

	if !handle.writeBuffered {
		return handle.sendToServer(data, offset)
	}

	// micro buffering for WriteOnly mode
	dataLen := len(data)

	if handle.writeBufferSize > 0 && offset != handle.writeBufferOffset+int64(handle.writeBufferSize) {
		if err := handle.flushWriteBuffer(); err != nil {
			return 0, err
		}
	}

	if handle.writeBuffer == nil {
		handle.writeBuffer = make([]byte, microBufferSize)
	}

	if handle.writeBufferSize == 0 {
		handle.writeBufferOffset = offset
	}

	copied := 0
	for copied < dataLen {
		space := microBufferSize - handle.writeBufferSize
		n := dataLen - copied
		if n > space {
			n = space
		}

		copy(handle.writeBuffer[handle.writeBufferSize:], data[copied:copied+n])
		handle.writeBufferSize += n
		copied += n

		if handle.writeBufferSize >= microBufferSize {
			if err := handle.flushWriteBuffer(); err != nil {
				return copied, err
			}
			if copied < dataLen {
				handle.writeBufferOffset = offset + int64(copied)
			}
		}
	}

	endOffset := offset + int64(dataLen)
	if handle.entry.Size < endOffset {
		handle.entry.Size = endOffset
	}
	handle.poolServiceClient.fsCache.AddEntryCache(handle.entry)

	return dataLen, nil
}

func (handle *PoolServiceFileHandle) flushWriteBuffer() error {
	if handle.writeBufferSize == 0 {
		return nil
	}

	_, err := handle.sendToServer(handle.writeBuffer[:handle.writeBufferSize], handle.writeBufferOffset)
	handle.writeBufferSize = 0
	return err
}

func (handle *PoolServiceFileHandle) sendToServer(data []byte, offset int64) (int, error) {
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
			handle.logger.Error(err)
			return 0, commons.StatusToError(err)
		}

		remainLength -= curLength
		curOffset += int64(curLength)
		totalWriteLength += curLength

		if handle.entry.Size < curOffset {
			handle.entry.Size = curOffset
		}
	}
	handle.poolServiceClient.fsCache.AddEntryCache(handle.entry)

	return totalWriteLength, nil
}

// Truncate truncates iRODS data object
func (handle *PoolServiceFileHandle) Truncate(size int64) error {
	defer irodsfs_common_util.StackTraceFromPanic(handle.logger)

	if err := handle.flushWriteBuffer(); err != nil {
		return err
	}

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
		handle.logger.Error(err)
		return commons.StatusToError(err)
	}

	handle.entry.Size = size
	handle.poolServiceClient.fsCache.AddEntryCache(handle.entry)

	return nil
}

// Flush flushes iRODS data object handle
func (handle *PoolServiceFileHandle) Flush() error {
	defer irodsfs_common_util.StackTraceFromPanic(handle.logger)

	if err := handle.flushWriteBuffer(); err != nil {
		return err
	}

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
		handle.logger.Error(err)
		return commons.StatusToError(err)
	}

	parentDirPath := irodsclient_util.GetIRODSPathDirname(handle.entry.Path)
	handle.poolServiceClient.fsCache.RemoveDirCache(parentDirPath)
	handle.poolServiceClient.fsCache.RemoveEntryCache(handle.entry.Path)

	return nil
}

// Close closes iRODS data object handle
func (handle *PoolServiceFileHandle) Close() error {
	defer irodsfs_common_util.StackTraceFromPanic(handle.logger)

	handle.mutex.Lock()
	if handle.closed {
		handle.mutex.Unlock()
		return nil
	}
	handle.closed = true
	handle.mutex.Unlock()

	if err := handle.flushWriteBuffer(); err != nil {
		return err
	}

	if handle.prefetch != nil {
		handle.prefetch.mu.Lock()
		handle.prefetch.closed = true
		handle.prefetch.mu.Unlock()
	}

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
		handle.logger.Error(err)
		return commons.StatusToError(err)
	}

	if handle.openMode.IsReadOnly() {
		atomic.AddInt32(&handle.poolServiceSession.openReadOnlyHandles, -1)
	} else if handle.openMode.IsWriteOnly() {
		atomic.AddInt32(&handle.poolServiceSession.openWriteOnlyHandles, -1)
	}

	if handle.openMode.IsWrite() {
		parentDirPath := irodsclient_util.GetIRODSPathDirname(handle.entry.Path)
		handle.poolServiceClient.fsCache.RemoveDirCache(parentDirPath)
		handle.poolServiceClient.fsCache.RemoveEntryCache(handle.entry.Path)
	}

	return nil
}
