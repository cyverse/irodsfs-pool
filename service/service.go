package service

import (
	"context"
	"fmt"
	"net"
	"sync"

	"github.com/cyverse/irodsfs-proxy/service/api"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/stats"
)

// ProxyService is a service object
type ProxyService struct {
	Config     *Config
	APIServer  *api.Server
	GrpcServer *grpc.Server
}

type ProxyServiceStatHandler struct {
	APIServer       *api.Server
	LiveConnections int
	Mutex           sync.Mutex
}

func (handler *ProxyServiceStatHandler) TagRPC(context.Context, *stats.RPCTagInfo) context.Context {
	return context.Background()
}

// HandleRPC processes the RPC stats.
func (handler *ProxyServiceStatHandler) HandleRPC(context.Context, stats.RPCStats) {
}

func (handler *ProxyServiceStatHandler) TagConn(context.Context, *stats.ConnTagInfo) context.Context {
	handler.Mutex.Lock()
	defer handler.Mutex.Unlock()

	handler.LiveConnections++
	return context.Background()
}

// HandleConn processes the Conn stats.
func (handler *ProxyServiceStatHandler) HandleConn(c context.Context, s stats.ConnStats) {
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

// NewProxyService creates a new proxy service
func NewProxyService(config *Config) *ProxyService {
	apiServer := api.NewServer()

	statHandler := &ProxyServiceStatHandler{
		APIServer: apiServer,
	}
	grpcServer := grpc.NewServer(grpc.StatsHandler(statHandler))
	api.RegisterProxyAPIServer(grpcServer, apiServer)

	service := &ProxyService{
		Config:     config,
		APIServer:  apiServer,
		GrpcServer: grpcServer,
	}

	return service
}

// Init initializes the service
func (svc *ProxyService) Init() error {
	return nil
}

// Start starts the service
func (svc *ProxyService) Start() error {
	logger := log.WithFields(log.Fields{
		"package":  "service",
		"function": "ProxyService.Start",
	})

	logger.Info("Starting the iRODS FUSE Lite Proxy service")

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
func (svc *ProxyService) Destroy() {
	logger := log.WithFields(log.Fields{
		"package":  "service",
		"function": "ProxyService.Destroy",
	})

	logger.Info("Destroying the iRODS FUSE Lite Proxy service")

	svc.GrpcServer.Stop()
}
