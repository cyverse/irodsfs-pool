package service

import (
	irodsclient_types "github.com/cyverse/go-irodsclient/irods/types"
	"github.com/cyverse/irodsfs-pool/service/api"
)

func convertAccountFromAPIToIRODS(account *api.Account) *irodsclient_types.IRODSAccount {
	var sslConf *irodsclient_types.IRODSSSLConfig
	if account.SslConfiguration != nil {
		sslConf = &irodsclient_types.IRODSSSLConfig{
			CACertificateFile:       account.SslConfiguration.CaCertificateFile,
			CACertificatePath:       account.SslConfiguration.CaCertificatePath,
			EncryptionKeySize:       int(account.SslConfiguration.EncryptionKeySize),
			EncryptionAlgorithm:     account.SslConfiguration.EncryptionAlgorithm,
			EncryptionSaltSize:      int(account.SslConfiguration.EncryptionSaltSize),
			EncryptionNumHashRounds: int(account.SslConfiguration.EncryptionNumHashRounds),
			VerifyServer:            irodsclient_types.SSLVerifyServer(account.SslConfiguration.VerifyServer),
			DHParamsFile:            account.SslConfiguration.DhParamsFile,
			ServerName:              account.SslConfiguration.ServerName,
		}
	}

	return &irodsclient_types.IRODSAccount{
		AuthenticationScheme:    irodsclient_types.AuthScheme(account.AuthenticationScheme),
		ClientServerNegotiation: account.ClientServerNegotiation,
		CSNegotiationPolicy:     irodsclient_types.CSNegotiationPolicyRequest(account.CsNegotiationPolicy),
		Host:                    account.Host,
		Port:                    int(account.Port),
		ClientUser:              account.ClientUser,
		ClientZone:              account.ClientZone,
		ProxyUser:               account.ProxyUser,
		ProxyZone:               account.ProxyZone,
		Password:                account.Password,
		Ticket:                  account.Ticket,
		DefaultResource:         account.DefaultResource,
		DefaultHashScheme:       account.DefaultHashScheme,
		PamTTL:                  int(account.PamTtl),
		PAMToken:                account.PamToken,
		SSLConfiguration:        sslConf,
	}
}
