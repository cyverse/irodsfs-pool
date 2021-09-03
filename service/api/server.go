package api

import (
	context "context"

	log "github.com/sirupsen/logrus"
)

type Server struct {
	UnimplementedProxyAPIServer
}

func (server *Server) Connect(context context.Context, request *ConnectRequest) (*ConnectResponse, error) {
	logger := log.WithFields(log.Fields{
		"package":  "api",
		"function": "Server.Connect",
	})

	logger.Infof("Receive Connect request from client: %s - %s", request.Account.Host, request.Account.ClientUser)
	return nil, nil
}

func (server *Server) Disconnect(context context.Context, request *DisconnectRequest) (*Empty, error) {
	logger := log.WithFields(log.Fields{
		"package":  "api",
		"function": "Server.Disconnect",
	})

	logger.Infof("Receive Disconnect request from client: %s", request.ConnectionId)
	return nil, nil
}

func (server *Server) List(context context.Context, request *ListRequest) (*ListResponse, error) {
	logger := log.WithFields(log.Fields{
		"package":  "api",
		"function": "Server.List",
	})

	logger.Infof("Receive List request from client: %s", request.Path)
	return nil, nil
}
