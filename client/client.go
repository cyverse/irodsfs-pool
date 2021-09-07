package client

import (
	"context"

	irodsfs "github.com/cyverse/go-irodsclient/fs"
	irodsfs_clienttype "github.com/cyverse/go-irodsclient/irods/types"
	"github.com/cyverse/irodsfs-proxy/service/api"
	"github.com/cyverse/irodsfs-proxy/utils"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
)

// ProxyServiceClient is a struct that holds connection information
type ProxyServiceClient struct {
	Host       string // host:port
	Connection *grpc.ClientConn
	APIClient  api.ProxyAPIClient
}

type ProxyServiceSession struct {
	ID              string
	Account         *irodsfs_clienttype.IRODSAccount
	ApplicationName string
}

// NewProxyServiceClient creates a new proxy service client
func NewProxyServiceClient(proxyHost string) *ProxyServiceClient {
	return &ProxyServiceClient{
		Host:       proxyHost,
		Connection: nil,
	}
}

// Disconnect connects to proxy service
func (client *ProxyServiceClient) Connect() error {
	logger := log.WithFields(log.Fields{
		"package":  "client",
		"function": "ProxyServiceClient.Connect",
	})

	conn, err := grpc.Dial(client.Host, grpc.WithInsecure())
	if err != nil {
		logger.Error(err)
		return err
	}

	client.Connection = conn
	client.APIClient = api.NewProxyAPIClient(conn)
	return nil
}

// Disconnect disconnects connection from proxy service
func (client *ProxyServiceClient) Disconnect() {
	if client.APIClient != nil {
		client.APIClient = nil
	}

	if client.Connection != nil {
		client.Connection.Close()
		client.Connection = nil
	}
}

// Login logins to iRODS service using account info
func (client *ProxyServiceClient) Login(account *irodsfs_clienttype.IRODSAccount, applicationName string) (*ProxyServiceSession, error) {
	logger := log.WithFields(log.Fields{
		"package":  "client",
		"function": "ProxyServiceClient.Login",
	})

	request := &api.LoginRequest{
		Account: &api.Account{
			AuthenticationScheme:    string(account.AuthenticationScheme),
			ClientServerNegotiation: account.ClientServerNegotiation,
			CsNegotiationPolicy:     string(account.CSNegotiationPolicy),
			Host:                    account.Host,
			Port:                    int32(account.Port),
			ClientUser:              account.ClientUser,
			ClientZone:              account.ClientZone,
			ProxyUser:               account.ProxyUser,
			ProxyZone:               account.ProxyZone,
			ServerDn:                account.ServerDN,
			Password:                account.Password,
			Ticket:                  account.Ticket,
			PamTtl:                  int32(account.PamTTL),
		},
		ApplicationName: applicationName,
	}

	response, err := client.APIClient.Login(context.Background(), request)
	if err != nil {
		logger.Error(err)
		return nil, err
	}

	return &ProxyServiceSession{
		ID:              response.SessionId,
		Account:         account,
		ApplicationName: applicationName,
	}, nil
}

// Logout logouts from iRODS service
func (client *ProxyServiceClient) Logout(session *ProxyServiceSession) error {
	logger := log.WithFields(log.Fields{
		"package":  "client",
		"function": "ProxyServiceClient.Logout",
	})

	request := &api.LogoutRequest{
		SessionId: session.ID,
	}

	_, err := client.APIClient.Logout(context.Background(), request)
	if err != nil {
		logger.Error(err)
		return err
	}

	return nil
}

// List lists iRODS collection entries
func (client *ProxyServiceClient) List(session *ProxyServiceSession, path string) ([]*irodsfs.Entry, error) {
	logger := log.WithFields(log.Fields{
		"package":  "client",
		"function": "ProxyServiceClient.List",
	})

	request := &api.ListRequest{
		SessionId: session.ID,
		Path:      path,
	}

	response, err := client.APIClient.List(context.Background(), request)
	if err != nil {
		logger.Error(err)
		return nil, err
	}

	irodsEntries := []*irodsfs.Entry{}

	for _, entry := range response.Entries {
		createTime, err := utils.PrseTime(entry.CreateTime)
		if err != nil {
			logger.Error(err)
			return nil, err
		}

		modifyTime, err := utils.PrseTime(entry.ModifyTime)
		if err != nil {
			logger.Error(err)
			return nil, err
		}

		irodsEntry := &irodsfs.Entry{
			ID:         entry.Id,
			Type:       irodsfs.EntryType(entry.Type),
			Name:       entry.Name,
			Path:       entry.Path,
			Owner:      entry.Owner,
			Size:       entry.Size,
			CreateTime: createTime,
			ModifyTime: modifyTime,
			CheckSum:   entry.Checksum,
		}

		irodsEntries = append(irodsEntries, irodsEntry)
	}

	return irodsEntries, nil
}
