package service

import (
	"context"
	"fmt"
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
)

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
		maxStagingDataSize:                    config.MaxStagingDataSize,
		maxCacheFileSize:                      config.MaxCacheFileSize,
		stagingDataGracePeriod:                time.Duration(config.StagingDataGracePeriod),
		sessionCloseGracePeriod:               time.Duration(config.SessionCloseGracePeriod),
	}

	poolServer, err := NewPoolServer(poolServerConfig)
	if err != nil {
		poolErr := errors.Wrapf(err, "failed to create a new pool server")
		logger.Error(poolErr)
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
			grpcServerErr := errors.Wrapf(err, "failed to serve")
			svc.logger.Error(grpcServerErr)
		}
	}()

	if svc.config.MonitoringServicePort > 0 {
		monitoringHandler := NewMonitoringHandler(svc.poolServer, svc.config)
		mux := http.NewServeMux()
		mux.Handle("/monitor", monitoringHandler)
		mux.Handle("/metrics", promhttp.Handler())

		addr := fmt.Sprintf(":%d", svc.config.MonitoringServicePort)
		svc.monitoringServer = &http.Server{Addr: addr, Handler: mux}

		go func() {
			svc.logger.Infof("Starting monitoring service at %s (endpoints: /monitor, /metrics)", addr)
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
