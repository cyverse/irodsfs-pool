package service

import (
	"context"
	"fmt"
	"net"
	"runtime/debug"
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
	config        *commons.Config
	apiServer     *Server
	grpcServer    *grpc.Server
	statHandler   *PoolServiceStatHandler
	terminateChan chan bool
	terminated    bool
	mutex         sync.Mutex // for termination
}

type PoolServiceStatHandler struct {
	apiServer       *Server
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

	defer func() {
		if r := recover(); r != nil {
			logger.Errorf("stacktrace from panic: %s", string(debug.Stack()))
			logger.Panic(r)
		}
	}()

	switch s.(type) {
	case *stats.ConnEnd:
		handler.mutex.Lock()
		defer handler.mutex.Unlock()

		handler.liveConnections--

		logger.Infof("Client is disconnected - total %d live connections", handler.liveConnections)

		if handler.liveConnections <= 0 {
			handler.liveConnections = 0
			handler.apiServer.LogoutAll()
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

	defer func() {
		if r := recover(); r != nil {
			logger.Errorf("stacktrace from panic: %s", string(debug.Stack()))
			logger.Panic(r)
		}
	}()

	serverConfig := &ServerConfig{
		BufferSizeMax: config.BufferSizeMax,
		CacheSizeMax:  config.DataCacheSizeMax,
		CacheRootPath: config.DataCacheRootPath,
	}

	apiServer, err := NewServer(serverConfig)
	if err != nil {
		logger.WithError(err).Error("failed to create a new server")
		return nil, err
	}

	statHandler := &PoolServiceStatHandler{
		apiServer:       apiServer,
		liveConnections: 0,
	}
	grpcServer := grpc.NewServer(grpc.StatsHandler(statHandler))
	api.RegisterPoolAPIServer(grpcServer, apiServer)

	service := &PoolService{
		config:        config,
		apiServer:     apiServer,
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

	defer func() {
		if r := recover(); r != nil {
			logger.Errorf("stacktrace from panic: %s", string(debug.Stack()))
			logger.Panic(r)
		}
	}()

	logger.Info("Starting the iRODS FUSE Lite Pool service")

	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", svc.config.ServicePort))
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
			case <-svc.terminateChan:
				// terminate
				return
			case <-ticker.C:
				logger.Infof("Total %d clients, %d FS, %d iRODS connections", svc.apiServer.GetSessions(), svc.apiServer.GetIRODSFSCount(), svc.apiServer.GetIRODSConnections())
			}
		}
	}()

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
	svc.mutex.Lock()
	defer svc.mutex.Unlock()

	if svc.terminated {
		// already terminated
		return
	}

	svc.terminated = true

	logger := log.WithFields(log.Fields{
		"package":  "service",
		"struct":   "PoolService",
		"function": "Destroy",
	})

	defer func() {
		if r := recover(); r != nil {
			logger.Errorf("stacktrace from panic: %s", string(debug.Stack()))
			logger.Panic(r)
		}
	}()

	logger.Info("Destroying the iRODS FUSE Lite Pool service")
	svc.terminateChan <- true

	if svc.grpcServer != nil {
		svc.grpcServer.Stop()
	}

	if svc.apiServer != nil {
		svc.apiServer.Release()
	}
}
