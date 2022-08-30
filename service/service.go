package service

import (
	"context"
	"fmt"
	"net"
	"os"
	"sync"
	"time"

	irodsfs_common_utils "github.com/cyverse/irodsfs-common/utils"
	"github.com/cyverse/irodsfs-pool/commons"
	"github.com/cyverse/irodsfs-pool/service/api"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/stats"
)

// PoolService is a service object
type PoolService struct {
	config        *commons.Config
	poolServer    *PoolServer
	grpcServer    *grpc.Server
	statHandler   *PoolServiceStatHandler
	terminateChan chan bool
	terminated    bool
	mutex         sync.Mutex // for termination
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

		logger.Infof("Client is disconnected - total %d live connections", handler.liveConnections)

		if handler.liveConnections <= 0 {
			handler.liveConnections = 0
			handler.poolServer.LogoutAll()
		}

	case *stats.ConnBegin:
		handler.mutex.Lock()
		defer handler.mutex.Unlock()

		handler.liveConnections++

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
		CacheRootPath:        config.DataCacheRootPath,
		TempRootPath:         config.TempRootPath,
		CacheTimeoutSettings: config.CacheTimeoutSettings,
	}

	poolServer, err := NewPoolServer(poolServerConfig)
	if err != nil {
		logger.WithError(err).Error("failed to create a new pool server")
		return nil, err
	}

	statHandler := &PoolServiceStatHandler{
		poolServer:      poolServer,
		liveConnections: 0,
	}
	grpcServer := grpc.NewServer(grpc.StatsHandler(statHandler))
	api.RegisterPoolAPIServer(grpcServer, poolServer)

	service := &PoolService{
		config:        config,
		poolServer:    poolServer,
		grpcServer:    grpcServer,
		statHandler:   statHandler,
		terminateChan: make(chan bool),
	}

	return service, nil
}

// Init initializes the service
func (svc *PoolService) Init() error {
	return nil
}

// Start starts the service
func (svc *PoolService) Start() error {
	logger := log.WithFields(log.Fields{
		"package":  "service",
		"struct":   "PoolService",
		"function": "Start",
	})

	svc.terminated = true

	defer irodsfs_common_utils.StackTraceFromPanic(logger)

	logger.Info("Starting the iRODS FUSE Lite Pool service")

	var listener net.Listener
	scheme, endpoint, err := commons.ParsePoolServiceEndpoint(svc.config.ServiceEndpoint)
	if err != nil {
		logger.Error(err)
		return err
	}

	switch scheme {
	case "unix":
		unixListener, err := net.Listen("unix", endpoint)
		if err != nil {
			logger.Error(err)
			return err
		}

		logger.Infof("Listening unix socket: %s", endpoint)
		listener = unixListener
	case "tcp":
		tcpListener, err := net.Listen("tcp", endpoint)
		if err != nil {
			logger.Error(err)
			return err
		}

		logger.Infof("Listening tcp socket: %s", endpoint)
		listener = tcpListener
	default:
		logger.Error("unknown protocol")
		return fmt.Errorf("unknown protocol")
	}

	go func() {
		logger := log.WithFields(log.Fields{
			"package": "service",
			"struct":  "PoolService",
		})

		tickerConnDisplay := time.NewTicker(1 * time.Minute)
		tickerMetricsCollection := time.NewTicker(5 * time.Second)
		defer tickerConnDisplay.Stop()

		for {
			select {
			case <-svc.terminateChan:
				// terminate
				return
			case <-tickerConnDisplay.C:
				logger.Infof("Total %d pool sessions, %d FS client instances, %d iRODS connections", svc.poolServer.GetPoolSessions(), svc.poolServer.GetIRODSFSClientInstanceCount(), svc.poolServer.GetIRODSConnections())
			case <-tickerMetricsCollection.C:
				svc.poolServer.CollectPrometheusMetrics()
			}
		}
	}()

	svc.terminated = false
	err = svc.grpcServer.Serve(listener)
	if err != nil {
		logger.Error(err)
		return err
	}

	// should not return
	return nil
}

// Destroy destroys the service
func (svc *PoolService) Destroy() {
	logger := log.WithFields(log.Fields{
		"package":  "service",
		"struct":   "PoolService",
		"function": "Destroy",
	})

	svc.mutex.Lock()
	defer svc.mutex.Unlock()

	if svc.terminated {
		// already terminated
		return
	}

	logger.Info("Destroying the iRODS FUSE Lite Pool service")

	svc.terminated = true

	defer irodsfs_common_utils.StackTraceFromPanic(logger)

	svc.terminateChan <- true

	if svc.grpcServer != nil {
		svc.grpcServer.Stop()
	}

	if svc.poolServer != nil {
		svc.poolServer.Release()
	}

	scheme, endpoint, err := commons.ParsePoolServiceEndpoint(svc.config.ServiceEndpoint)
	if err == nil {
		if scheme == "unix" {
			os.Remove(endpoint)
		}
	}
}
