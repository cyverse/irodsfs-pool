package service

import (
	"context"
	"strings"
	"sync/atomic"

	irodsfs_common_utils "github.com/cyverse/irodsfs-common/utils"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/stats"
	"google.golang.org/grpc/status"
)

type PoolServiceStatHandler struct {
	liveConnections int64

	poolServer *PoolServer
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
		atomic.AddInt64(&handler.liveConnections, -1)

		promCounterForGRPCClients.Dec()

		logger.Infof("Client is disconnected - total %d live connections", handler.liveConnections)

		if handler.liveConnections <= 0 {
			handler.poolServer.LogoutAll()
		}

		handler.poolServer.PrintConnectionStat()

	case *stats.ConnBegin:
		atomic.AddInt64(&handler.liveConnections, 1)

		promCounterForGRPCClients.Inc()

		logger.Infof("Client is connected - total %d connections", handler.liveConnections)

		handler.poolServer.PrintConnectionStat()
	}
}

func (handler *PoolServiceStatHandler) UnaryInterceptor(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, uhandler grpc.UnaryHandler) (interface{}, error) {
	logger := log.WithFields(log.Fields{
		"package":  "service",
		"function": "unaryInterceptor",
	})

	// request
	promCounterForGRPCRequests.Inc()

	// Create channels for the response and error
	respChan := make(chan interface{}, 1)
	errChan := make(chan error, 1)

	// Run the handler in a goroutine
	go func() {
		resp, err := uhandler(ctx, req)
		if err != nil {
			errChan <- err
		} else {
			respChan <- resp
		}
	}()

	// Wait for either the handler to complete or the context to be done
	select {
	case <-ctx.Done():
		// Timeout or cancellation occurred
		err := ctx.Err()
		if err == context.DeadlineExceeded {
			logger.Errorf("Handler %q did not return within timeout", info.FullMethod)
			promCounterForGRPCRequestsTimedout.Inc()
			return nil, status.Error(codes.DeadlineExceeded, "RPC timed out")
		}

		logger.Errorf("Handler %q canceled", info.FullMethod)
		promCounterForGRPCRequestsCanceled.Inc()

		if strings.HasSuffix(info.FullMethod, "/Login") {
			promCounterForLoginFailures.Inc()
		}

		return nil, status.Error(codes.Canceled, "RPC canceled")
	case err := <-errChan:
		// response
		promCounterForGRPCResponses.Inc()
		if strings.HasSuffix(info.FullMethod, "/Login") {
			promCounterForLoginFailures.Inc()
		}
		return nil, err
	case resp := <-respChan:
		// response
		promCounterForGRPCResponses.Inc()
		if strings.HasSuffix(info.FullMethod, "/Login") {
			promCounterForLogins.Inc()
		}
		return resp, nil
	}
}
