package service

import (
	"fmt"
	"net"

	"github.com/cyverse/irodsfs-proxy/service/api"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
)

// ProxyService is a service object
type ProxyService struct {
	Config     *Config
	APIServer  *api.Server
	GrpcServer *grpc.Server
}

// NewProxyService creates a new proxy service
func NewProxyService(config *Config) *ProxyService {
	apiServer := &api.Server{
		Sessions:    map[string]*api.Session{},
		FileHandles: map[string]map[string]*api.FileHandle{},
	}

	grpcServer := grpc.NewServer()
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
