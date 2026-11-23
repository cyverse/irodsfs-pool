package service

import (
	"context"
	"testing"

	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// A handler that panics fails its own request and leaves the process alive.
func TestUnaryInterceptorRecoversHandlerPanic(t *testing.T) {
	handler := &PoolServiceStatHandler{
		poolServer: &PoolServer{logger: log.NewEntry(log.New())},
	}

	resp, err := handler.UnaryInterceptor(context.Background(), nil, &grpc.UnaryServerInfo{FullMethod: "/api.PoolAPI/Stat"},
		func(ctx context.Context, req interface{}) (interface{}, error) {
			var p *PoolSession
			_ = p.id // nil pointer dereference
			return nil, nil
		})

	if resp != nil || status.Code(err) != codes.Internal {
		t.Fatalf("resp = %v, err = %v, want Internal error", resp, err)
	}
}
