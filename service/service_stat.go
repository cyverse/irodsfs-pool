package service

import (
	"context"
	"strings"
	"sync/atomic"

	irodsfs_common_util "github.com/cyverse/irodsfs-common/util"
	"github.com/rs/xid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/stats"
	"google.golang.org/grpc/status"
)

type connIDKeyType struct{}

var connIDKey = connIDKeyType{}

func ConnIDFromContext(ctx context.Context) string {
	if id, ok := ctx.Value(connIDKey).(string); ok {
		return id
	}
	return ""
}

type PoolServiceStatHandler struct {
	liveConnections int64

	poolServer *PoolServer
}

func (handler *PoolServiceStatHandler) TagRPC(ctx context.Context, info *stats.RPCTagInfo) context.Context {
	return ctx
}

// HandleRPC processes the RPC stats.
func (handler *PoolServiceStatHandler) HandleRPC(context.Context, stats.RPCStats) {
}

func (handler *PoolServiceStatHandler) TagConn(ctx context.Context, info *stats.ConnTagInfo) context.Context {
	connID := xid.New().String()
	return context.WithValue(ctx, connIDKey, connID)
}

// HandleConn processes the Conn stats.
func (handler *PoolServiceStatHandler) HandleConn(ctx context.Context, s stats.ConnStats) {
	defer irodsfs_common_util.StackTraceFromPanic(handler.poolServer.logger)

	switch s.(type) {
	case *stats.ConnEnd:
		atomic.AddInt64(&handler.liveConnections, -1)

		promCounterForGRPCClients.Dec()

		connID := ConnIDFromContext(ctx)
		handler.poolServer.logger.Infof("Client disconnected (connID=%q) - total %d client connections", connID, handler.liveConnections)

		if connID != "" {
			handler.poolServer.sessionManager.RemoveConnection(connID)
		}

		handler.poolServer.PrintConnectionStat()

	case *stats.ConnBegin:
		atomic.AddInt64(&handler.liveConnections, 1)

		promCounterForGRPCClients.Inc()

		connID := ConnIDFromContext(ctx)
		handler.poolServer.logger.Infof("Client connected (connID=%q) - total %d client connections", connID, handler.liveConnections)

		handler.poolServer.PrintConnectionStat()
	}
}

func (handler *PoolServiceStatHandler) UnaryInterceptor(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, uhandler grpc.UnaryHandler) (interface{}, error) {
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
			handler.poolServer.logger.Errorf("Handler %q did not return within timeout", info.FullMethod)
			promCounterForGRPCRequestsTimedout.Inc()
			return nil, status.Error(codes.DeadlineExceeded, "RPC timed out")
		}

		handler.poolServer.logger.Errorf("Handler %q canceled", info.FullMethod)
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
