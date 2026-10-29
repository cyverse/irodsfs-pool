package service

import (
	"context"
	"net"
	"net/http"
	"os"
	"time"

	"github.com/cockroachdb/errors"
	irodsfs_common_util "github.com/cyverse/irodsfs-common/util"
	"github.com/cyverse/irodsfs-pool/commons"
	"github.com/cyverse/irodsfs-pool/service/api"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
)

const (
	// gRPC keepalive pings are HTTP/2 PING frames. They tell whether a
	// connection is alive, not whether a session is, so they do not replace
	// the KeepAlive call that holds a session open.

	// grpcKeepaliveMinTime is the shortest interval clients may ping at. It must
	// stay below the client's ping interval, or the server would close client
	// connections for pinging too often.
	grpcKeepaliveMinTime time.Duration = 10 * time.Second
	// grpcKeepaliveTime is how long a connection is idle before the server
	// pings the client, and grpcKeepaliveTimeout how long it waits for the
	// answer before closing the connection to a client that is gone.
	grpcKeepaliveTime    time.Duration = 1 * time.Minute
	grpcKeepaliveTimeout time.Duration = 20 * time.Second
)

// grpcKeepaliveServerOptions returns the gRPC server options for keepalive
// pings: clients may ping while they have a call in flight, or when idle, and
// the server pings idle clients to find the ones that are gone.
func grpcKeepaliveServerOptions() []grpc.ServerOption {
	return []grpc.ServerOption{
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			MinTime:             grpcKeepaliveMinTime,
			PermitWithoutStream: true,
		}),
		grpc.KeepaliveParams(keepalive.ServerParameters{
			Time:    grpcKeepaliveTime,
			Timeout: grpcKeepaliveTimeout,
		}),
	}
}

// PoolService is a service object
type PoolService struct {
	config *commons.Config

	poolServer       *PoolServer
	grpcServer       *grpc.Server
	statHandler      *PoolServiceStatHandler
	monitoringServer *http.Server
	logger           *log.Entry

	terminateChan chan bool
}

// NewPoolService creates a new pool service
func NewPoolService(config *commons.Config) (*PoolService, error) {
	logger := log.WithFields(log.Fields{})

	defer irodsfs_common_util.StackTraceFromPanic(logger)

	recoveryEncryptionKey, err := config.GetRecoveryEncryptionKey()
	if err != nil {
		return nil, err
	}

	poolServerConfig := &PoolServerConfig{
		dataRootPath:                          config.DataRootPath,
		recoveryEncryptionKey:                 recoveryEncryptionKey,
		sessionTimeout:                        time.Duration(config.SessionTimeout),
		sessionTimeoutCheckInterval:           time.Duration(config.SessionTimeoutCheckInterval),
		dataBlockSize:                         config.DataBlockSize,
		maxDataMemCacheSize:                   config.MaxDataMemCacheSize,
		maxDataMemCacheBufferItems:            config.MaxDataMemCacheBufferItems,
		dataMemCacheTTL:                       time.Duration(config.DataMemCacheTTL),
		maxIOConnectionPerSession:             config.MaxIOConnectionPerSession,
		metadataCacheTimeoutSettings:          config.MetadataCacheTimeoutSettings,
		startNewTransaction:                   config.StartNewTransaction,
		maxMetadataCacheEntriesPerSession:     config.MaxMetadataCacheEntriesPerSession,
		maxMetadataCacheSizePerSession:        config.MaxMetadataCacheSizePerSession,
		maxMetadataCacheBufferItemsPerSession: config.MaxMetadataCacheBufferItemsPerSession,
		metadataCacheTTL:                      time.Duration(config.MetadataCacheTTL),
		stagingRootPath:                       config.StagingRootPath,
		maxStagingDataSize:                    config.MaxStagingDataSize,
		maxCacheFileSize:                      config.MaxCacheFileSize,
		stagingDataGracePeriod:                time.Duration(config.StagingDataGracePeriod),
		sessionCloseGracePeriod:               time.Duration(config.SessionCloseGracePeriod),
		packedDirectories:                     config.PackedDirectories.ToPackedFSConfig(),
		logRootPath:                           config.GetLogRootPath(),
		logMaxBackups:                         config.LogMaxBackups,
	}

	poolServer, err := NewPoolServer(poolServerConfig)
	if err != nil {
		poolErr := errors.Wrap(err, "failed to create a new pool server")
		logger.Error(poolErr)
		return nil, err
	}

	statHandler := &PoolServiceStatHandler{
		liveConnections: 0,
		poolServer:      poolServer,
	}

	serverOptions := []grpc.ServerOption{grpc.StatsHandler(statHandler), grpc.UnaryInterceptor(statHandler.UnaryInterceptor), grpc.MaxConcurrentStreams(0)}
	serverOptions = append(serverOptions, grpcKeepaliveServerOptions()...)
	grpcServer := grpc.NewServer(serverOptions...)
	api.RegisterPoolAPIServer(grpcServer, poolServer)

	service := &PoolService{
		config: config,

		poolServer:  poolServer,
		grpcServer:  grpcServer,
		statHandler: statHandler,
		logger:      logger,

		terminateChan: make(chan bool),
	}

	return service, nil
}

// Release releases the service
func (svc *PoolService) Release() {
	defer irodsfs_common_util.StackTraceFromPanic(svc.logger)

	svc.logger.Info("Releasing the iRODS FUSE Pool service")
	defer svc.logger.Info("Released the iRODS FUSE Pool service")

	if svc.grpcServer != nil {
		svc.grpcServer = nil
	}

	if svc.poolServer != nil {
		svc.poolServer.Release()
		svc.poolServer = nil
	}

	scheme, endpoint, err := commons.ParsePoolServiceEndpoint(svc.config.GetServiceEndpoint())
	if err == nil {
		if scheme == "unix" {
			os.Remove(endpoint)
		}
	}
}

// Start starts the service
func (svc *PoolService) Start() error {
	defer irodsfs_common_util.StackTraceFromPanic(svc.logger)

	svc.logger.Info("Starting the iRODS FUSE Pool service")

	svc.checkResourceAvailability()

	var listener net.Listener
	scheme, endpoint, err := commons.ParsePoolServiceEndpoint(svc.config.GetServiceEndpoint())
	if err != nil {
		svc.logger.Error(err)
		return err
	}

	svc.logger.Infof("scheme: %s, endpoint: %s", scheme, endpoint)

	switch scheme {
	case "unix":
		unixListener, err := net.Listen("unix", endpoint)
		if err != nil {
			listenErr := errors.Wrapf(err, "failed to listen to unix socket %q", endpoint)
			svc.logger.Error(listenErr)
			return listenErr
		}

		svc.logger.Infof("Listening unix socket: %q", endpoint)
		listener = unixListener
	case "tcp":
		tcpListener, err := net.Listen("tcp", endpoint)
		if err != nil {
			listenErr := errors.Wrapf(err, "failed to listen to tcp socket %q", endpoint)
			svc.logger.Error(listenErr)
			return listenErr
		}

		svc.logger.Infof("Listening tcp socket: %q", endpoint)
		listener = tcpListener
	default:
		err = errors.Newf("unknown protocol %q", scheme)
		svc.logger.Error(err)
		return err
	}

	go func() {
		tickerMetricsCollection := time.NewTicker(10 * time.Second)
		defer tickerMetricsCollection.Stop()

		for {
			select {
			case <-svc.terminateChan:
				// terminate
				return
			case <-tickerMetricsCollection.C:
				svc.poolServer.CollectPrometheusMetrics()
			}
		}
	}()

	go func() {
		err = svc.grpcServer.Serve(listener)
		if err != nil {
			grpcServerErr := errors.Wrap(err, "failed to serve")
			svc.logger.Error(grpcServerErr)
		}
	}()

	if managementServiceAddr, err := commons.ParseManagementServiceEndpoint(svc.config.GetManagementServiceEndpoint()); err != nil {
		return errors.Wrap(err, "invalid management service endpoint")
	} else if managementServiceAddr != "" {
		startTime := time.Now()
		monitoringHandler := newMonitoringHandler(svc.poolServer, svc.config, startTime)
		apiHandler := newRESTAPIHandler(svc.poolServer, svc.config, startTime)
		mux := http.NewServeMux()
		mux.Handle("/monitor", monitoringHandler)
		mux.Handle("/monitor/", monitoringHandler)
		mux.Handle("/metrics", promhttp.Handler())
		apiHandler.RegisterRoutes(mux)

		svc.monitoringServer = &http.Server{Addr: managementServiceAddr, Handler: mux}

		go func() {
			svc.logger.Infof("Starting monitoring service at %s (endpoints: /healthz, /readyz, /monitor, /metrics, /api/sysinfo, /api/sessions, /api/recovery-sessions)", svc.config.GetManagementServiceEndpoint())
			if err := svc.monitoringServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				svc.logger.WithError(err).Error("monitoring service error")
			}
		}()
	}

	return nil
}

func (svc *PoolService) checkResourceAvailability() {
	// Check system memory vs configured cache size
	sysTotal, sysAvail := getSystemMemoryInfo()
	requiredMem := uint64(svc.config.MaxDataMemCacheSize)
	if sysAvail > 0 && sysAvail < requiredMem {
		svc.logger.Warnf("Insufficient system memory: available %s, but cache requires %s (total system RAM: %s)",
			formatBytes(int64(sysAvail)), formatBytes(int64(requiredMem)), formatBytes(int64(sysTotal)))
	}

	// Check staging disk space
	var stagingPath string
	if len(svc.config.StagingRootPath) > 0 {
		stagingPath = svc.config.StagingRootPath
	} else {
		stagingPath = svc.config.GetDataStagingRootPath()
	}

	_, diskFree := getDiskInfo(stagingPath)
	requiredDisk := uint64(svc.config.MaxStagingDataSize)
	if diskFree > 0 && diskFree < requiredDisk {
		svc.logger.Warnf("Insufficient disk space for staging at %q: available %s, but staging requires %s",
			stagingPath, formatBytes(int64(diskFree)), formatBytes(int64(requiredDisk)))
	}
}

// Stop stops the service
func (svc *PoolService) Stop() {
	svc.logger.Info("Stopping the iRODS FUSE Pool service")

	defer irodsfs_common_util.StackTraceFromPanic(svc.logger)

	svc.terminateChan <- true

	if svc.monitoringServer != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		svc.monitoringServer.Shutdown(ctx)
	}

	if svc.grpcServer != nil {
		svc.grpcServer.Stop()
	}
}
