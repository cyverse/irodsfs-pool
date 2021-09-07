package api

import (
	context "context"
	"fmt"

	irodsfs "github.com/cyverse/go-irodsclient/fs"
	irodsfs_clienttype "github.com/cyverse/go-irodsclient/irods/types"
	"github.com/cyverse/irodsfs-proxy/utils"
	"github.com/rs/xid"
	log "github.com/sirupsen/logrus"
)

type Server struct {
	UnimplementedProxyAPIServer
	Sessions map[string]*Session
}

type Session struct {
	ID      string
	Account *irodsfs_clienttype.IRODSAccount
	FS      *irodsfs.FileSystem
}

func (server *Server) Login(context context.Context, request *LoginRequest) (*LoginResponse, error) {
	logger := log.WithFields(log.Fields{
		"package":  "api",
		"function": "Server.Login",
	})

	logger.Infof("Receive Login request from client: %s - %s", request.Account.Host, request.Account.ClientUser)

	account := &irodsfs_clienttype.IRODSAccount{
		AuthenticationScheme:    irodsfs_clienttype.AuthScheme(request.Account.AuthenticationScheme),
		ClientServerNegotiation: request.Account.ClientServerNegotiation,
		CSNegotiationPolicy:     irodsfs_clienttype.CSNegotiationRequire(request.Account.CsNegotiationPolicy),
		Host:                    request.Account.Host,
		Port:                    int(request.Account.Port),
		ClientUser:              request.Account.ClientUser,
		ClientZone:              request.Account.ClientZone,
		ProxyUser:               request.Account.ProxyUser,
		ProxyZone:               request.Account.ProxyZone,
		ServerDN:                request.Account.ServerDn,
		Password:                request.Account.Password,
		Ticket:                  request.Account.Ticket,
		PamTTL:                  int(request.Account.PamTtl),
	}

	fs, err := irodsfs.NewFileSystemWithDefault(account, request.ApplicationName)
	if err != nil {
		logger.Error(err)
		return nil, err
	}

	sessionID := xid.New().String()
	session := &Session{
		ID:      sessionID,
		Account: account,
		FS:      fs,
	}

	server.Sessions[sessionID] = session
	response := &LoginResponse{
		SessionId: sessionID,
	}

	return response, nil
}

func (server *Server) Logout(context context.Context, request *LogoutRequest) (*Empty, error) {
	logger := log.WithFields(log.Fields{
		"package":  "api",
		"function": "Server.Logout",
	})

	logger.Infof("Receive Logout request from client: %s", request.SessionId)

	session, ok := server.Sessions[request.SessionId]
	if !ok {
		err := fmt.Errorf("cannot find session %s", request.SessionId)
		logger.Error(err)
		return nil, err
	}

	delete(server.Sessions, request.SessionId)

	session.FS.Release()
	return &Empty{}, nil
}

func (server *Server) List(context context.Context, request *ListRequest) (*ListResponse, error) {
	logger := log.WithFields(log.Fields{
		"package":  "api",
		"function": "Server.List",
	})

	logger.Infof("Receive List request from client: %s", request.Path)

	session, ok := server.Sessions[request.SessionId]
	if !ok {
		err := fmt.Errorf("cannot find session %s", request.SessionId)
		logger.Error(err)
		return nil, err
	}

	entries, err := session.FS.List(request.Path)
	if err != nil {
		logger.Error(err)
		return nil, err
	}

	responseEntries := []*Entry{}
	for _, entry := range entries {
		responseEntry := &Entry{
			Id:         entry.ID,
			Type:       string(entry.Type),
			Name:       entry.Name,
			Path:       entry.Path,
			Owner:      entry.Owner,
			Size:       entry.Size,
			CreateTime: utils.MakeTimeToString(entry.CreateTime),
			ModifyTime: utils.MakeTimeToString(entry.ModifyTime),
			Checksum:   entry.CheckSum,
		}
		responseEntries = append(responseEntries, responseEntry)
	}

	response := &ListResponse{
		Entries: responseEntries,
	}

	return response, nil
}
