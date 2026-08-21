package service

import (
	"net"
	"os"
	"time"

	"github.com/cockroachdb/errors"
	irodsfs_common_util "github.com/cyverse/irodsfs-common/util"
	"github.com/cyverse/irodsfs-pool/commons"
	"github.com/cyverse/irodsfs-pool/service/api"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
)

// PoolService is a service object
type PoolService struct {
	config *commons.Config

	poolServer  *PoolServer
	grpcServer  *grpc.Server
	statHandler *PoolServiceStatHandler
	logger      *log.Entry

	terminateChan chan bool
}

// NewPoolService creates a new pool service
func NewPoolService(config *commons.Config) (*PoolService, error) {
	logger := log.WithFields(log.Fields{})

	defer irodsfs_common_util.StackTraceFromPanic(logger)

	poolServerConfig := &PoolServerConfig{
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
		stagingDataGracePeriod:                time.Duration(config.StagingDataGracePeriod),
	}

	poolServer, err := NewPoolServer(poolServerConfig)
	if err != nil {
		poolErr := errors.Errorf("failed to create a new pool server: %w", err)
		logger.Errorf("%+v", poolErr)
		return nil, err
	}

	statHandler := &PoolServiceStatHandler{
		liveConnections: 0,
		poolServer:      poolServer,
	}

	grpcServer := grpc.NewServer(grpc.StatsHandler(statHandler), grpc.UnaryInterceptor(statHandler.UnaryInterceptor), grpc.MaxConcurrentStreams(0))
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

	svc.logger.Info("Releasing the iRODS FUSE Lite Pool service")
	defer svc.logger.Info("Released the iRODS FUSE Lite Pool service")

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

	svc.logger.Info("Starting the iRODS FUSE Lite Pool service")

	var listener net.Listener
	scheme, endpoint, err := commons.ParsePoolServiceEndpoint(svc.config.GetServiceEndpoint())
	if err != nil {
		svc.logger.Errorf("%+v", err)
		return err
	}

	svc.logger.Infof("scheme: %s, endpoint: %s", scheme, endpoint)

	switch scheme {
	case "unix":
		unixListener, err := net.Listen("unix", endpoint)
		if err != nil {
			listenErr := errors.Errorf("failed to listen to unix socket %q: %w", endpoint, err)
			svc.logger.Errorf("%+v", listenErr)
			return listenErr
		}

		svc.logger.Infof("Listening unix socket: %q", endpoint)
		listener = unixListener
	case "tcp":
		tcpListener, err := net.Listen("tcp", endpoint)
		if err != nil {
			listenErr := errors.Errorf("failed to listen to tcp socket %q: %w", endpoint, err)
			svc.logger.Errorf("%+v", listenErr)
			return listenErr
		}

		svc.logger.Infof("Listening tcp socket: %q", endpoint)
		listener = tcpListener
	default:
		svc.logger.Errorf("unknown protocol %q", scheme)
		return errors.Errorf("unknown protocol %q", scheme)
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
				svc.poolServer.PrintConnectionStat()
				svc.poolServer.CollectPrometheusMetrics()
			}
		}
	}()

	go func() {
		err = svc.grpcServer.Serve(listener)
		if err != nil {
			grpcServerErr := errors.Errorf("failed to serve: %w", err)
			svc.logger.Errorf("%+v", grpcServerErr)
		}
	}()

	return nil
}

// Stop stops the service
func (svc *PoolService) Stop() {
	svc.logger.Info("Stopping the iRODS FUSE Lite Pool service")

	defer irodsfs_common_util.StackTraceFromPanic(svc.logger)

	svc.terminateChan <- true

	if svc.grpcServer != nil {
		svc.grpcServer.Stop()
	}
}
