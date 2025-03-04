package service

import (
	"net"
	"os"
	"time"

	irodsfs_common_utils "github.com/cyverse/irodsfs-common/utils"
	"github.com/cyverse/irodsfs-pool/commons"
	"github.com/cyverse/irodsfs-pool/service/api"
	log "github.com/sirupsen/logrus"
	"golang.org/x/xerrors"
	"google.golang.org/grpc"
)

// PoolService is a service object
type PoolService struct {
	config *commons.Config

	poolServer  *PoolServer
	grpcServer  *grpc.Server
	statHandler *PoolServiceStatHandler

	terminateChan chan bool
}

// NewPoolService creates a new pool service
func NewPoolService(config *commons.Config) (*PoolService, error) {
	logger := log.WithFields(log.Fields{
		"package":  "service",
		"function": "NewPoolService",
	})

	defer irodsfs_common_utils.StackTraceFromPanic(logger)

	poolServerConfig := &PoolServerConfig{
		CacheSizeMax:         config.DataCacheSizeMax,
		CacheRootPath:        config.GetDataCacheRootDirPath(),
		CacheTimeoutSettings: config.CacheTimeoutSettings,
		OperationTimeout:     config.OperationTimeout,
		SessionTimeout:       config.SessionTimeout,
	}

	poolServer, err := NewPoolServer(poolServerConfig)
	if err != nil {
		poolErr := xerrors.Errorf("failed to create a new pool server: %w", err)
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

		terminateChan: make(chan bool),
	}

	return service, nil
}

// Release releases the service
func (svc *PoolService) Release() {
	logger := log.WithFields(log.Fields{
		"package":  "service",
		"struct":   "PoolService",
		"function": "Release",
	})

	defer irodsfs_common_utils.StackTraceFromPanic(logger)

	logger.Info("Releasing the iRODS FUSE Lite Pool service")
	defer logger.Info("Released the iRODS FUSE Lite Pool service")

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
	logger := log.WithFields(log.Fields{
		"package":  "service",
		"struct":   "PoolService",
		"function": "Start",
	})

	defer irodsfs_common_utils.StackTraceFromPanic(logger)

	logger.Info("Starting the iRODS FUSE Lite Pool service")

	var listener net.Listener
	scheme, endpoint, err := commons.ParsePoolServiceEndpoint(svc.config.GetServiceEndpoint())
	if err != nil {
		logger.Errorf("%+v", err)
		return err
	}

	logger.Infof("scheme: %s, endpoint: %s", scheme, endpoint)

	switch scheme {
	case "unix":
		unixListener, err := net.Listen("unix", endpoint)
		if err != nil {
			listenErr := xerrors.Errorf("failed to listen to unix socket %q: %w", endpoint, err)
			logger.Errorf("%+v", listenErr)
			return listenErr
		}

		logger.Infof("Listening unix socket: %q", endpoint)
		listener = unixListener
	case "tcp":
		tcpListener, err := net.Listen("tcp", endpoint)
		if err != nil {
			listenErr := xerrors.Errorf("failed to listen to tcp socket %q: %w", endpoint, err)
			logger.Errorf("%+v", listenErr)
			return listenErr
		}

		logger.Infof("Listening tcp socket: %q", endpoint)
		listener = tcpListener
	default:
		logger.Errorf("unknown protocol %q", scheme)
		return xerrors.Errorf("unknown protocol %q", scheme)
	}

	go func() {
		tickerConnDisplay := time.NewTicker(1 * time.Minute)
		tickerMetricsCollection := time.NewTicker(5 * time.Second)
		defer tickerConnDisplay.Stop()
		defer tickerMetricsCollection.Stop()

		for {
			select {
			case <-svc.terminateChan:
				// terminate
				return
			case <-tickerConnDisplay.C:
				svc.poolServer.PrintConnectionStat()
			case <-tickerMetricsCollection.C:
				svc.poolServer.CollectPrometheusMetrics()
			}
		}
	}()

	go func() {
		err = svc.grpcServer.Serve(listener)
		if err != nil {
			grpcServerErr := xerrors.Errorf("failed to serve: %w", err)
			logger.Errorf("%+v", grpcServerErr)
		}
	}()

	return nil
}

// Stop stops the service
func (svc *PoolService) Stop() {
	logger := log.WithFields(log.Fields{
		"package":  "service",
		"struct":   "PoolService",
		"function": "Stop",
	})

	logger.Info("Stopping the iRODS FUSE Lite Pool service")

	defer irodsfs_common_utils.StackTraceFromPanic(logger)

	svc.terminateChan <- true

	if svc.grpcServer != nil {
		svc.grpcServer.Stop()
	}
}
