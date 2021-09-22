package service

import (
	"context"
	"fmt"
	"net"
	"sync"

	"github.com/cyverse/irodsfs-pool/service/api"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/stats"
)

// PoolService is a service object
type PoolService struct {
	Config     *Config
	APIServer  *Server
	GrpcServer *grpc.Server
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
	handler.Mutex.Lock()
	defer handler.Mutex.Unlock()

	handler.LiveConnections++
	return context.Background()
}

// HandleConn processes the Conn stats.
func (handler *PoolServiceStatHandler) HandleConn(c context.Context, s stats.ConnStats) {
	switch s.(type) {
	case *stats.ConnEnd:
		handler.Mutex.Lock()
		defer handler.Mutex.Unlock()

		handler.LiveConnections--
		if handler.LiveConnections <= 0 {
			handler.LiveConnections = 0
			handler.APIServer.LogoutAll()
		}
	}
}

// NewPoolService creates a new pool service
func NewPoolService(config *Config) *PoolService {
	apiServer := NewServer(config.BufferSizeMax)

	statHandler := &PoolServiceStatHandler{
		APIServer: apiServer,
	}
	grpcServer := grpc.NewServer(grpc.StatsHandler(statHandler))
	api.RegisterPoolAPIServer(grpcServer, apiServer)

	service := &PoolService{
		Config:     config,
		APIServer:  apiServer,
		GrpcServer: grpcServer,
	}

	return service
}

// Init initializes the service
func (svc *PoolService) Init() error {
	return nil
}

// Start starts the service
func (svc *PoolService) Start() error {
	logger := log.WithFields(log.Fields{
		"package":  "service",
		"function": "PoolService.Start",
	})

	logger.Info("Starting the iRODS FUSE Lite Pool service")

	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", svc.Config.ServicePort))
	if err != nil {
		logger.Error(err)
		return err
	}

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
	logger := log.WithFields(log.Fields{
		"package":  "service",
		"function": "PoolService.Destroy",
	})

	logger.Info("Destroying the iRODS FUSE Lite Pool service")

	svc.GrpcServer.Stop()
}
