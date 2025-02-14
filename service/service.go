package service

import (
	"context"
	"net"
	"os"
	"sync"
	"time"

	irodsfs_common_utils "github.com/cyverse/irodsfs-common/utils"
	"github.com/cyverse/irodsfs-pool/commons"
	"github.com/cyverse/irodsfs-pool/service/api"
	log "github.com/sirupsen/logrus"
	"golang.org/x/xerrors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/stats"
)

// PoolService is a service object
type PoolService struct {
	config *commons.Config

	poolServer  *PoolServer
	grpcServer  *grpc.Server
	statHandler *PoolServiceStatHandler

	terminateChan chan bool
}

type PoolServiceStatHandler struct {
	poolServer      *PoolServer
	liveConnections int
	mutex           sync.Mutex
}

func (handler *PoolServiceStatHandler) TagRPC(context.Context, *stats.RPCTagInfo) context.Context {
	return context.Background()
}

// HandleRPC processes the RPC stats.
func (handler *PoolServiceStatHandler) HandleRPC(context.Context, stats.RPCStats) {
}

func (handler *PoolServiceStatHandler) TagConn(context.Context, *stats.ConnTagInfo) context.Context {
	return context.Background()
}

// HandleConn processes the Conn stats.
func (handler *PoolServiceStatHandler) HandleConn(c context.Context, s stats.ConnStats) {
	logger := log.WithFields(log.Fields{
		"package":  "service",
		"struct":   "PoolServiceStatHandler",
		"function": "HandleConn",
	})

	defer irodsfs_common_utils.StackTraceFromPanic(logger)

	switch s.(type) {
	case *stats.ConnEnd:
		handler.mutex.Lock()
		defer handler.mutex.Unlock()

		handler.liveConnections--

		promCounterForGRPCClients.Dec()

		logger.Infof("Client is disconnected - total %d live connections", handler.liveConnections)

		if handler.liveConnections <= 0 {
			handler.liveConnections = 0
			handler.poolServer.LogoutAll()
		}

	case *stats.ConnBegin:
		handler.mutex.Lock()
		defer handler.mutex.Unlock()

		handler.liveConnections++

		promCounterForGRPCClients.Inc()

		logger.Infof("Client is connected - total %d connections", handler.liveConnections)
	}
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
		poolServer:      poolServer,
		liveConnections: 0,
	}
	grpcServer := grpc.NewServer(grpc.StatsHandler(statHandler), grpc.MaxConcurrentStreams(0))
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

	logger.Info("Releasing the iRODS FUSE Lite Pool service")

	defer irodsfs_common_utils.StackTraceFromPanic(logger)

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
		logger := log.WithFields(log.Fields{
			"package": "service",
			"struct":  "PoolService",
		})

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
				logger.Infof("Total %d pool sessions, %d FS client instances, %d iRODS connections", svc.poolServer.GetPoolSessions(), svc.poolServer.GetIRODSFSClientInstances(), svc.poolServer.GetIRODSConnections())
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
		"function": "Destroy",
	})

	logger.Info("Stopping the iRODS FUSE Lite Pool service")

	defer irodsfs_common_utils.StackTraceFromPanic(logger)

	svc.terminateChan <- true

	if svc.grpcServer != nil {
		svc.grpcServer.Stop()
	}
}
