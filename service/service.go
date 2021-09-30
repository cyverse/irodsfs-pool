package service

import (
	"context"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/cyverse/irodsfs-pool/commons"
	"github.com/cyverse/irodsfs-pool/service/api"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/stats"
)

// PoolService is a service object
type PoolService struct {
	Config        *commons.Config
	APIServer     *Server
	GrpcServer    *grpc.Server
	StatHandler   *PoolServiceStatHandler
	TerminateChan chan bool
	Terminated    bool
	Mutex         sync.Mutex // for termination
}

type PoolServiceStatHandler struct {
	APIServer       *Server
	LiveConnections int
	Mutex           sync.Mutex
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

	switch s.(type) {
	case *stats.ConnEnd:
		handler.Mutex.Lock()
		defer handler.Mutex.Unlock()

		handler.LiveConnections--

		logger.Infof("Client is disconnected - total %d live connections", handler.LiveConnections)

		if handler.LiveConnections <= 0 {
			handler.LiveConnections = 0
			handler.APIServer.LogoutAll()
		}

	case *stats.ConnBegin:
		handler.Mutex.Lock()
		defer handler.Mutex.Unlock()

		handler.LiveConnections++

		logger.Infof("Client is connected - total %d connections", handler.LiveConnections)
	}
}

// NewPoolService creates a new pool service
func NewPoolService(config *commons.Config) (*PoolService, error) {
	logger := log.WithFields(log.Fields{
		"package":  "service",
		"function": "NewPoolService",
	})

	serverConfig := &ServerConfig{
		BufferSizeMax: config.BufferSizeMax,
		CacheSizeMax:  config.DataCacheSizeMax,
		CacheRootPath: config.DataCacheRootPath,
		CacheTimeout:  config.DataCacheTimeout,
		CacheCleanup:  config.DataCacheCleanupTime,
	}

	apiServer, err := NewServer(serverConfig)
	if err != nil {
		logger.WithError(err).Error("failed to create a new server")
		return nil, err
	}

	statHandler := &PoolServiceStatHandler{
		APIServer:       apiServer,
		LiveConnections: 0,
	}
	grpcServer := grpc.NewServer(grpc.StatsHandler(statHandler))
	api.RegisterPoolAPIServer(grpcServer, apiServer)

	service := &PoolService{
		Config:        config,
		APIServer:     apiServer,
		GrpcServer:    grpcServer,
		StatHandler:   statHandler,
		TerminateChan: make(chan bool),
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

	logger.Info("Starting the iRODS FUSE Lite Pool service")

	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", svc.Config.ServicePort))
	if err != nil {
		logger.Error(err)
		return err
	}

	go func() {
		logger := log.WithFields(log.Fields{
			"package": "service",
			"struct":  "PoolService",
		})

		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-svc.TerminateChan:
				// terminate
				return
			case <-ticker.C:
				logger.Infof("Total %d live connections, %d live iRODS connections", svc.StatHandler.LiveConnections, svc.APIServer.Connections())
			}
		}
	}()

	err = svc.GrpcServer.Serve(listener)
	if err != nil {
		logger.Error(err)
		return err
	}

	// should not return
	return nil
}

// Destroy destroys the service
func (svc *PoolService) Destroy() {
	svc.Mutex.Lock()
	defer svc.Mutex.Unlock()

	if svc.Terminated {
		// already terminated
		return
	}

	svc.Terminated = true

	logger := log.WithFields(log.Fields{
		"package":  "service",
		"struct":   "PoolService",
		"function": "Destroy",
	})

	logger.Info("Destroying the iRODS FUSE Lite Pool service")
	svc.TerminateChan <- true

	if svc.GrpcServer != nil {
		svc.GrpcServer.Stop()
	}

	if svc.APIServer != nil {
		svc.APIServer.Release()
	}
}
