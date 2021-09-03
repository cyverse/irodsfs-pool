package client

import (
	"context"
	"fmt"
	"time"

	irodsfs "github.com/cyverse/go-irodsclient/fs"
	irodsfs_clienttype "github.com/cyverse/go-irodsclient/irods/types"
	"github.com/cyverse/irodsfs-proxy/service/api"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
)

// ProxyServiceClient is a struct that holds connection information
type ProxyServiceClient struct {
	Host       string // host:port
	Connection *grpc.ClientConn
	APIClient  api.ProxyAPIClient
}

type ProxyServiceConnection struct {
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
func (client *ProxyServiceClient) Login(account *irodsfs_clienttype.IRODSAccount, applicationName string) (*ProxyServiceConnection, error) {
	logger := log.WithFields(log.Fields{
		"package":  "client",
		"function": "ProxyServiceClient.Login",
	})

	request := &api.ConnectRequest{
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

	response, err := client.APIClient.Connect(context.Background(), request)
	if err != nil {
		logger.Error(err)
		return nil, err
	}

	if len(response.Error) > 0 {
		logger.Errorf(response.Error)
		return nil, fmt.Errorf(response.Error)
	}

	return &ProxyServiceConnection{
		ID:              response.ConnectionId,
		Account:         account,
		ApplicationName: applicationName,
	}, nil
}

// Logout logouts from iRODS service
func (client *ProxyServiceClient) Logout(connection *ProxyServiceConnection) error {
	logger := log.WithFields(log.Fields{
		"package":  "client",
		"function": "ProxyServiceClient.Logout",
	})

	request := &api.DisconnectRequest{
		ConnectionId: connection.ID,
	}

	_, err := client.APIClient.Disconnect(context.Background(), request)
	if err != nil {
		logger.Error(err)
		return err
	}

	return nil
}

// List lists iRODS collection entries
func (client *ProxyServiceClient) List(connection *ProxyServiceConnection, path string) ([]*irodsfs.Entry, error) {
	logger := log.WithFields(log.Fields{
		"package":  "client",
		"function": "ProxyServiceClient.List",
	})

	request := &api.ListRequest{
		ConnectionId: connection.ID,
		Path:         path,
	}

	response, err := client.APIClient.List(context.Background(), request)
	if err != nil {
		logger.Error(err)
		return nil, err
	}

	if len(response.Error) > 0 {
		logger.Errorf(response.Error)
		return nil, fmt.Errorf(response.Error)
	}

	irodsEntries := []*irodsfs.Entry{}

	for _, entry := range response.Entries {
		createTime, err := time.Parse(time.RFC3339, entry.CreateTime)
		if err != nil {
			logger.Error(err)
			return nil, err
		}

		modifyTime, err := time.Parse(time.RFC3339, entry.ModifyTime)
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
